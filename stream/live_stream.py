"""
bet365 Live Stream Server
=========================
Single camoufox browser tab that captures all bet365 live InPlay data
via WebSocket hooks and exposes it through a local HTTP/WebSocket API.

Architecture:
  1. pproxy bridge  (socks5://127.0.0.1:1089 -> authenticated remote SOCKS5)
  2. camoufox       (navigates to bet365 InPlay with proxy)
  3. WS hooks       (page.on("websocket") -> framereceived/framesent)
  4. ZapParser       (protocol.zap_parser — full ZAP protocol parser)
  5. aiohttp server (HTTP + WS API on port 8365)

Usage:
    python -m stream.live_stream
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import warnings
from typing import Any

import aiohttp
from aiohttp import web
from camoufox.async_api import AsyncCamoufox

from protocol.zap_parser import ZapParser, ChangeEvent, ChangeType

# pproxy runs as a subprocess for stability — no import needed here.

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("live_stream")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROXY_LOCAL = "socks5://127.0.0.1:1089"
PROXY_REMOTE = "socks5://188.126.20.21:8081#px95358f:4hjzQNheHNjtz7vTXcPwEyxE"
PROXY_GEO_IP = "188.126.20.21"
TARGET_URL = "https://www.bet365.es/#/IP/"
API_HOST = "0.0.0.0"
API_PORT = 8365

# Set USE_PROXY = True to route through the SOCKS5 proxy
USE_PROXY = False
BROWSER_PROXY = {"server": PROXY_LOCAL} if USE_PROXY else None
RECONNECT_WAIT_WS = 5
RECONNECT_WAIT_RELOAD = 5
RECONNECT_WAIT_CRASH = 10

# English locale: topic suffix _1_3 (English). Spanish: _3_0.
# The data server serves based on topic name, not IP.
# We subscribe to English overview AFTER the page loads (the page uses whatever
# locale it wants; we override with our own subscriptions).
LOCALE = "_1_3"       # English
LOCALE_NATIVE = None  # Auto-detected from first OVInPlay subscription

# Detail subscription: how many topics per subscribe message.
# Larger = faster but risk rejection. 50 topics per message works.
TOPICS_PER_MSG = 50
# How many subscribe messages to send per drain cycle (every 0.5s)
MSGS_PER_CYCLE = 3
DRAIN_INTERVAL = 0.5  # seconds between drain cycles

# JS: Patch WebSocket.send to capture WS refs + latest auth token.
WS_HOOK_SCRIPT = """mw:() => {
    if (window.__wsHooked) return 'already_hooked';
    const origSend = WebSocket.prototype.send;
    window.__wsRefs = [];
    window.__lastToken = '';
    window.__tokenTs = 0;
    WebSocket.prototype.send = function(data) {
        if (!window.__wsRefs.includes(this)) window.__wsRefs.push(this);
        if (typeof data === 'string') {
            let m = data.match(/[,]A_([A-Za-z0-9+\\/=]{20,})/);
            if (m) { window.__lastToken = m[1]; window.__tokenTs = Date.now(); }
        }
        return origSend.call(this, data);
    };
    window.__wsHooked = true;
    return 'hooked';
}"""


# ---------------------------------------------------------------------------
# Proxy bridge (runs as a SEPARATE PROCESS to avoid event loop interference)
# ---------------------------------------------------------------------------
async def start_proxy_bridge() -> asyncio.subprocess.Process:
    """
    Start the local pproxy bridge as a separate subprocess.
    Running pproxy in the same event loop as camoufox + aiohttp causes
    connection drops due to asyncio task contention.
    """
    import sys
    from pathlib import Path
    bridge_script = Path(__file__).resolve().parent.parent / "capture" / "proxy_bridge.py"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(bridge_script),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    # Wait for the proxy to signal readiness.
    if proc.stdout:
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=15)
            if b"PROXY_READY" in line:
                log.info("Proxy bridge running as subprocess (PID %d)", proc.pid)
            else:
                log.warning("Proxy bridge started but unexpected output: %s", line)
        except asyncio.TimeoutError:
            log.warning("Proxy bridge did not signal readiness within 15s")
    return proc


# ---------------------------------------------------------------------------
# ChangeEvent serialisation helper
# ---------------------------------------------------------------------------
def _change_event_to_dict(ce: ChangeEvent) -> dict[str, Any]:
    """Convert a ChangeEvent to a JSON-serialisable dict."""
    d: dict[str, Any] = {
        "change_type": ce.change_type.value,
        "entity_type": ce.entity_type,
        "entity_id": ce.entity_id,
        "topic": ce.topic,
    }
    # Serialize old/new values — only include primitives and dicts, skip
    # dataclass instances to avoid serialisation issues.
    for key in ("old_value", "new_value"):
        val = getattr(ce, key)
        if val is None:
            d[key] = None
        elif isinstance(val, dict):
            d[key] = val
        elif isinstance(val, (str, int, float, bool)):
            d[key] = val
        else:
            # Dataclass — use its id/name if available
            d[key] = {
                "id": getattr(val, "id", ""),
                "name": getattr(val, "name", ""),
            }
    return d


# ---------------------------------------------------------------------------
# HTTP / WebSocket API  (aiohttp)
# ---------------------------------------------------------------------------
class LiveAPI:
    """aiohttp-based HTTP + WebSocket API server."""

    def __init__(self, parser: ZapParser, stats: dict[str, Any]) -> None:
        self.parser = parser
        self.stats = stats
        self._ws_subscribers: set[web.WebSocketResponse] = set()
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/events", self.handle_events)
        app.router.add_get("/events/{sport_id}", self.handle_events_by_sport)
        app.router.add_get("/event/{event_id}", self.handle_event_detail)
        app.router.add_get("/sports", self.handle_sports)
        app.router.add_get("/ws", self.handle_ws)
        app.router.add_get("/stats", self.handle_stats)
        app.router.add_get("/", self.handle_index)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, API_HOST, API_PORT)
        await site.start()
        log.info("HTTP API listening on http://%s:%d", API_HOST, API_PORT)

    async def stop(self) -> None:
        for ws in list(self._ws_subscribers):
            await ws.close()
        if self._runner:
            await self._runner.cleanup()

    # -- broadcast to WS subscribers ----------------------------------------

    async def broadcast(self, change: ChangeEvent) -> None:
        """Push a single ChangeEvent to all connected WebSocket subscribers."""
        if not self._ws_subscribers:
            return
        payload = json.dumps(_change_event_to_dict(change))
        closed: list[web.WebSocketResponse] = []
        for ws in self._ws_subscribers:
            try:
                await ws.send_str(payload)
            except (ConnectionResetError, ConnectionError):
                closed.append(ws)
        for ws in closed:
            self._ws_subscribers.discard(ws)

    # -- HTTP handlers ------------------------------------------------------

    async def handle_index(self, _request: web.Request) -> web.Response:
        return web.json_response({
            "service": "bet365 Live Stream API",
            "endpoints": [
                "GET /events",
                "GET /events/{sport_id}",
                "GET /event/{event_id}",
                "GET /sports",
                "GET /stats",
                "GET /ws  (WebSocket)",
            ],
        })

    async def handle_events(self, _request: web.Request) -> web.Response:
        return web.json_response(self.parser.get_all_live_events())

    async def handle_events_by_sport(self, request: web.Request) -> web.Response:
        sport_id = request.match_info["sport_id"]
        return web.json_response(self.parser.get_sport_events(sport_id))

    async def handle_event_detail(self, request: web.Request) -> web.Response:
        event_id = request.match_info["event_id"]
        detail = self.parser.get_event(event_id)
        if detail is None:
            return web.json_response({"error": "Event not found"}, status=404)
        return web.json_response(detail)

    async def handle_sports(self, _request: web.Request) -> web.Response:
        sports_list: list[dict[str, Any]] = []
        for sid, sport in self.parser.state.sports.items():
            comp_ids = self.parser.state.sport_competitions.get(sid, set())
            event_count = sum(
                len(self.parser.state.comp_events.get(cid, set()))
                for cid in comp_ids
            )
            sports_list.append({
                "id": sid,
                "name": sport.name,
                "competition_count": len(comp_ids),
                "event_count": event_count,
            })
        return web.json_response(sports_list)

    async def handle_stats(self, _request: web.Request) -> web.Response:
        uptime = time.time() - self.stats.get("start_time", time.time())
        parser_summary = self.parser.summary()
        data = {
            "uptime_seconds": round(uptime, 1),
            "ws_connections_seen": self.stats.get("ws_connections_seen", 0),
            "ws_active": self.stats.get("ws_active", 0),
            "frames_received": self.stats.get("frames_received", 0),
            "frames_sent": self.stats.get("frames_sent", 0),
            "reconnection_count": self.stats.get("reconnection_count", 0),
            "parser": parser_summary,
            "api_ws_subscribers": len(self._ws_subscribers),
        }
        return web.json_response(data)

    async def handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_subscribers.add(ws)
        log.info("API WebSocket subscriber connected (%d total)", len(self._ws_subscribers))
        try:
            snapshot = {
                "type": "snapshot",
                "sports": list({
                    "id": sid,
                    "name": s.name,
                } for sid, s in self.parser.state.sports.items()),
                "event_count": len(self.parser.state.events),
            }
            await ws.send_json(snapshot)
            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                    break
        finally:
            self._ws_subscribers.discard(ws)
            log.info("API WebSocket subscriber disconnected (%d remaining)", len(self._ws_subscribers))
        return ws


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
class LiveStream:
    """
    Orchestrates the proxy bridge, camoufox browser, WebSocket capture,
    data parsing, and HTTP API.
    """

    def __init__(self) -> None:
        self.parser = ZapParser()
        self.stats: dict[str, Any] = {
            "start_time": time.time(),
            "ws_connections_seen": 0,
            "ws_active": 0,
            "frames_received": 0,
            "frames_sent": 0,
            "reconnection_count": 0,
        }
        self.api = LiveAPI(self.parser, self.stats)
        self._active_ws_urls: set[str] = set()
        self._should_run = True

        # Register change callback: broadcast ChangeEvents to API WS subscribers
        self.parser.on_change(self._on_parser_change)

    def _on_parser_change(self, change: ChangeEvent) -> None:
        """Callback from ZapParser — schedule async broadcast."""
        asyncio.ensure_future(self.api.broadcast(change))

    async def run(self) -> None:
        """Main entry point — runs until interrupted."""
        log.info("=== bet365 Live Stream starting ===")

        # 1. Start proxy bridge (only if proxy is enabled)
        if USE_PROXY:
            self._proxy_proc = await start_proxy_bridge()
            await asyncio.sleep(1)
        else:
            log.info("Proxy disabled — connecting directly")

        # 2. Start HTTP API
        await self.api.start()

        # 3. Launch browser + capture loop (with reconnection)
        try:
            while self._should_run:
                try:
                    await self._browser_session()
                except Exception:
                    self.stats["reconnection_count"] += 1
                    log.exception(
                        "Browser session crashed (reconnect #%d) — restarting in %ds",
                        self.stats["reconnection_count"],
                        RECONNECT_WAIT_CRASH,
                    )
                    await asyncio.sleep(RECONNECT_WAIT_CRASH)
        except asyncio.CancelledError:
            log.info("Live stream cancelled")
        except KeyboardInterrupt:
            log.info("Live stream interrupted by user")
        finally:
            log.info("Shutting down...")
            await self.api.stop()
            if hasattr(self, "_proxy_proc") and self._proxy_proc.returncode is None:
                self._proxy_proc.terminate()
                log.info("Proxy bridge terminated")
            log.info("Shutdown complete")

    async def _browser_session(self) -> None:
        """Single browser session. Returns when reconnection is needed."""
        log.info("Launching camoufox browser...")
        self._subscribed_events: set[str] = set()
        self._pending_detail_topics: list[str] = []
        self._latest_auth_token: str = ""

        camoufox_kwargs: dict[str, Any] = {
            "headless": False,
            "humanize": True,
            "os": "windows",
            "main_world_eval": True,
        }
        if BROWSER_PROXY:
            camoufox_kwargs["proxy"] = BROWSER_PROXY
            camoufox_kwargs["geoip"] = PROXY_GEO_IP

        async with AsyncCamoufox(**camoufox_kwargs) as browser:
            page = await browser.new_page()
            self._page = page

            # Register WebSocket handler BEFORE navigation
            page.on("websocket", lambda ws: asyncio.ensure_future(self._on_websocket(ws)))

            log.info("Navigating to %s", TARGET_URL)
            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
                log.info("Page loaded")
            except Exception as exc:
                log.warning("Navigation: %s", exc)

            # Wait for initial WS data (reduced from 18s to 5s)
            await asyncio.sleep(5)

            # Hook WebSocket.send in main world — captures WS refs + auth tokens
            try:
                r = await page.evaluate(WS_HOOK_SCRIPT)
                log.info("WS hook: %s", r)
            except Exception as exc:
                log.warning("WS hook failed: %s", exc)

            # Wait briefly for browser's subscribe/keepalive to trigger the hook
            await asyncio.sleep(3)

            log.info(
                "Active WS: %d | Events: %d | Token: %s",
                self.stats["ws_active"],
                len(self.parser.state.events),
                "captured" if self._latest_auth_token else "pending (JS hook)",
            )

            # Queue ALL event detail subscriptions immediately
            if self.parser.state.events:
                self._queue_event_details()

            log.info("Startup: %d events, %d pending detail subs", len(self.parser.state.events), len(self._pending_detail_topics))

            # Main loop: fast drain + monitor + English override retry
            last_new_event_check = time.time()
            english_subscribed = False
            while self._should_run:
                # Drain subscription queue (fast: MSGS_PER_CYCLE messages every DRAIN_INTERVAL)
                if self._pending_detail_topics:
                    await self._drain_subscription_queue(page)
                    await asyncio.sleep(DRAIN_INTERVAL)
                else:
                    await asyncio.sleep(2)

                # Try English overview subscription until it works
                if not english_subscribed:
                    r = await self._subscribe_english_overview(page)
                    if r:
                        english_subscribed = True

                # Check for new events every 15s
                now = time.time()
                if now - last_new_event_check > 15:
                    new_events = set(self.parser.state.events.keys()) - self._subscribed_events
                    if new_events:
                        log.info("New events: %d", len(new_events))
                        self._queue_event_details(event_ids=new_events)
                    elif not self._pending_detail_topics:
                        log.info(
                            "Subs: %d sent | %s",
                            self.stats.get("detail_subs_sent", 0),
                            self.parser.summary(),
                        )
                    last_new_event_check = now

                # Reconnect if all WS closed
                if self.stats["ws_connections_seen"] > 0 and self.stats["ws_active"] == 0:
                    self.stats["reconnection_count"] += 1
                    log.warning("All WS closed (reconnect #%d)", self.stats["reconnection_count"])
                    await asyncio.sleep(RECONNECT_WAIT_WS)
                    self._active_ws_urls.clear()
                    self._subscribed_events.clear()
                    self._pending_detail_topics.clear()
                    english_subscribed = False

                    try:
                        await page.reload(wait_until="domcontentloaded", timeout=60000)
                        log.info("Page reloaded")
                        await asyncio.sleep(5)
                        try:
                            await page.evaluate(WS_HOOK_SCRIPT)
                        except Exception:
                            pass
                        await asyncio.sleep(3)
                        await self._subscribe_english_overview(page)
                        await asyncio.sleep(3)
                        self._queue_event_details()
                    except Exception as exc:
                        log.warning("Reload failed: %s — restarting browser", exc)
                        await asyncio.sleep(RECONNECT_WAIT_RELOAD)
                        return

    async def _subscribe_english_overview(self, page: Any) -> bool:
        """
        Subscribe to English locale overview topics, overriding whatever
        locale the page natively uses. The data server serves based on
        topic name, not IP/domain.
        """
        # Detect native locale from existing event topics
        native_locale = None
        for ev in list(self.parser.state.events.values())[:5]:
            if ev.topic:
                # Topic like OV190632012C1A_3_0 -> locale is _3_0
                parts = ev.topic.rsplit("_", 2)
                if len(parts) >= 3:
                    native_locale = f"_{parts[-2]}_{parts[-1]}"
                    break

        if native_locale and native_locale == LOCALE:
            log.info("Already in target locale %s", LOCALE)
            return True

        if native_locale:
            log.info("Native locale: %s -> switching to %s (English)", native_locale, LOCALE)

        # Subscribe to English overview topics via JS
        token = self._latest_auth_token
        if not token:
            # Try to get token from JS hook
            try:
                token = await page.evaluate("mw:() => window.__lastToken || ''")
            except Exception:
                pass

        if not token:
            return False

        eng_topics = f"OVInPlay{LOCALE},Media_L1_Z3,CONFIG{LOCALE}"
        try:
            result = await page.evaluate(f"""mw:() => {{
                let refs = window.__wsRefs || [];
                let ws = refs.find(w => w.url && w.url.includes('premws') && w.readyState === 1);
                if (!ws) return 'no_ws';
                let msg = String.fromCharCode(0x16) + String.fromCharCode(0x00) +
                          '{eng_topics},A_{token}' + String.fromCharCode(0x01);
                ws.send(msg);
                return 'ok';
            }}""")
            if result == "ok":
                log.info("English overview subscribed")
                return True
            else:
                return False
        except Exception as exc:
            log.debug("English overview error: %s", exc)
            return False

    def _queue_event_details(self, event_ids: set[str] | None = None) -> None:
        """Queue event detail topics for subscription (non-async, just queues).

        Detail topics use the NATIVE locale (same as the event was loaded with).
        English names come from the overview subscription, not per-event.
        """
        if event_ids is None:
            event_ids = set(self.parser.state.events.keys())

        to_subscribe = event_ids - self._subscribed_events
        if not to_subscribe:
            return

        topics: list[str] = []
        eids: list[str] = []
        for eid in to_subscribe:
            ev = self.parser.state.events.get(eid)
            if ev and ev.topic:
                detail_topic = ev.topic
                if detail_topic.startswith("OV"):
                    detail_topic = detail_topic[2:]
                # Strip trailing zone suffix (e.g. _3_0 -> _3)
                if detail_topic.endswith("_0"):
                    detail_topic = detail_topic[:-2]
                topics.append(detail_topic)
                eids.append(eid)

        if not topics:
            return

        self._pending_detail_topics.extend(topics)
        self._subscribed_events.update(eids)
        log.info("Queued %d detail topics (pending: %d)", len(topics), len(self._pending_detail_topics))

    async def _drain_subscription_queue(self, page: Any) -> None:
        """
        Send multiple batches of pending detail topics per cycle.
        Uses token from JS hook (preferred) or Python framesent capture.
        """
        if not self._pending_detail_topics:
            return

        # Get freshest token: try JS hook first, fall back to Python capture
        token = ""
        try:
            token = await page.evaluate("mw:() => window.__lastToken || ''")
        except Exception:
            pass
        if not token:
            token = self._latest_auth_token
        if not token:
            return

        sent_count = 0
        for _ in range(MSGS_PER_CYCLE):
            if not self._pending_detail_topics:
                break

            batch = self._pending_detail_topics[:TOPICS_PER_MSG]
            topic_list = ",".join(batch)

            try:
                result = await page.evaluate(f"""mw:() => {{
                    let refs = window.__wsRefs || [];
                    let ws = refs.find(w => w.url && w.url.includes('premws') && w.readyState === 1);
                    if (!ws) return 'no_ws';
                    let msg = String.fromCharCode(0x16) + String.fromCharCode(0x00) +
                              '{topic_list},A_{token}' + String.fromCharCode(0x01);
                    ws.send(msg);
                    return 'ok';
                }}""")
                if result == "ok":
                    self._pending_detail_topics = self._pending_detail_topics[TOPICS_PER_MSG:]
                    sent_count += len(batch)
                    self.stats["detail_subs_sent"] = self.stats.get("detail_subs_sent", 0) + len(batch)
                else:
                    break  # WS not available
            except Exception:
                break

        if sent_count:
            log.info("Sent %d detail subs (%d pending)", sent_count, len(self._pending_detail_topics))

    # -- WebSocket hooks ----------------------------------------------------

    async def _on_websocket(self, ws: Any) -> None:
        """Called when the page opens a new WebSocket."""
        url = ws.url
        self.stats["ws_connections_seen"] += 1
        self.stats["ws_active"] += 1
        self._active_ws_urls.add(url)
        log.info("WebSocket OPENED: %s (active: %d)", url, self.stats["ws_active"])

        def on_frame_received(payload: str | bytes) -> None:
            self.stats["frames_received"] += 1
            try:
                self.parser.feed(payload)
            except Exception:
                log.debug("Frame parse error", exc_info=True)

        def on_frame_sent(payload: str | bytes) -> None:
            self.stats["frames_sent"] += 1
            # Capture auth token from outgoing subscribe/keepalive messages
            try:
                text = payload if isinstance(payload, str) else payload.decode("utf-8", errors="replace")
                idx = text.find(",A_")
                if idx >= 0:
                    token_start = idx + 3  # skip ",A_"
                    # Token is base64: alphanumeric + /+=
                    token_end = token_start
                    while token_end < len(text) and text[token_end] in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=":
                        token_end += 1
                    if token_end - token_start > 20:
                        self._latest_auth_token = text[token_start:token_end]
            except Exception:
                pass

        def on_close(_: Any) -> None:
            self.stats["ws_active"] = max(0, self.stats["ws_active"] - 1)
            self._active_ws_urls.discard(url)
            log.warning("WebSocket CLOSED: %s (active: %d)", url, self.stats["ws_active"])

        def on_error(error: Any) -> None:
            log.error("WebSocket ERROR on %s: %s", url, error)

        ws.on("framereceived", on_frame_received)
        ws.on("framesent", on_frame_sent)
        ws.on("close", on_close)
        ws.on("socketerror", on_error)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    stream = LiveStream()
    await stream.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Exited.")
