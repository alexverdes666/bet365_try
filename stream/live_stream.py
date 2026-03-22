"""
bet365 Live Stream Server — Single Session with Subscribe/Unsubscribe Rotation
===============================================================================
One camoufox browser session with subscribe-unsubscribe rotation for 100%
market coverage.

Strategy:
  bet365 limits ~200 concurrent detail subscriptions per IP. But if we
  UNSUBSCRIBE after getting the data snapshot, we free the slots. The parser
  RETAINS market data even after unsubscribing. So we cycle:
    subscribe batch -> get data -> unsubscribe -> next batch.

Architecture:
  1. pproxy bridge  (socks5://127.0.0.1:1089 -> authenticated remote SOCKS5)
  2. 1 camoufox     (overview + rotation in one browser)
  3. WS hooks       (page.on("websocket") -> framereceived/framesent)
  4. ZapParser       (protocol.zap_parser — single instance)
  5. aiohttp server (HTTP + WS API on port 8365)

Usage:
    python -m stream.live_stream
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp
from aiohttp import web
from camoufox.async_api import AsyncCamoufox

from protocol.zap_parser import ZapParser, ChangeEvent, ChangeType

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

USE_PROXY = False
BROWSER_PROXY = {"server": PROXY_LOCAL} if USE_PROXY else None

LOCALE = "_1_3"  # English

ROTATION_BATCH = 200   # events per rotation batch
ROTATION_DWELL = 6     # seconds to wait for data before unsubscribing
TOPICS_PER_MSG = 50    # topics per subscribe/unsubscribe message

RECONNECT_WAIT = 5
RECONNECT_WAIT_CRASH = 10

# JS: Patch WebSocket.send to capture WS refs + latest auth token.
WS_HOOK_SCRIPT = """mw:() => {
    if (window.__wsHooked) return 'already_hooked';
    const origSend = WebSocket.prototype.send;
    window.__wsRefs = [];
    window.__lastToken = '';
    WebSocket.prototype.send = function(data) {
        if (!window.__wsRefs.includes(this)) window.__wsRefs.push(this);
        if (typeof data === 'string') {
            let m = data.match(/[,]A_([A-Za-z0-9+\\/=]{20,})/);
            if (m) { window.__lastToken = m[1]; }
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
    """Start the local pproxy bridge as a separate subprocess."""
    import sys
    from pathlib import Path

    bridge_script = Path(__file__).resolve().parent.parent / "capture" / "proxy_bridge.py"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(bridge_script),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
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
    for key in ("old_value", "new_value"):
        val = getattr(ce, key)
        if val is None:
            d[key] = None
        elif isinstance(val, dict):
            d[key] = val
        elif isinstance(val, (str, int, float, bool)):
            d[key] = val
        else:
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

    # -- broadcast ----------------------------------------------------------

    async def broadcast(self, change: ChangeEvent) -> None:
        """Push a ChangeEvent to all connected WebSocket subscribers."""
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
            "service": "bet365 Live Stream API (single-session rotation)",
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
            "rotation_cycles": self.stats.get("rotation_cycles", 0),
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
# Main orchestrator — single session with rotation
# ---------------------------------------------------------------------------
class LiveStream:
    """
    Single-session orchestrator with subscribe/unsubscribe rotation.

    One camoufox browser captures all events via OVInPlay and rotates through
    detail subscriptions in batches of ROTATION_BATCH, unsubscribing after each
    batch to free server slots for the next.
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
            "rotation_cycles": 0,
        }
        self.api = LiveAPI(self.parser, self.stats)
        self._should_run = True
        self._latest_auth_token: str = ""
        self._ws_connections_seen = 0
        self._active_ws_count = 0

        # Register change callback
        self.parser.on_change(self._on_parser_change)

    def _on_parser_change(self, change: ChangeEvent) -> None:
        """Callback from ZapParser — schedule async broadcast."""
        asyncio.ensure_future(self.api.broadcast(change))

    async def run(self) -> None:
        """Main entry point — runs until interrupted."""
        log.info("=== bet365 Live Stream starting (single-session rotation) ===")

        # 1. Start proxy bridge if enabled
        proxy_proc = None
        if USE_PROXY:
            proxy_proc = await start_proxy_bridge()
            await asyncio.sleep(1)
        else:
            log.info("Proxy disabled -- connecting directly")

        # 2. Start HTTP API
        await self.api.start()

        # 3. Run browser session loop (reconnects on failure)
        try:
            while self._should_run:
                try:
                    await self._browser_session()
                except asyncio.CancelledError:
                    log.info("Session cancelled")
                    break
                except Exception:
                    log.exception("Session crashed — restarting in %ds", RECONNECT_WAIT_CRASH)
                    await asyncio.sleep(RECONNECT_WAIT_CRASH)
        except KeyboardInterrupt:
            log.info("Interrupted by user")
        finally:
            log.info("Shutting down...")
            self._should_run = False
            await self.api.stop()
            if proxy_proc and proxy_proc.returncode is None:
                proxy_proc.terminate()
                log.info("Proxy bridge terminated")
            log.info("Shutdown complete")

    # -- Browser session ----------------------------------------------------

    async def _browser_session(self) -> None:
        """Single browser lifecycle. Returns on fatal WS loss for reconnect."""
        log.info("Launching camoufox browser...")
        self._active_ws_count = 0
        self._ws_connections_seen = 0

        camoufox_kwargs: dict[str, Any] = {
            "headless": False,
            "humanize": True,
            "os": "windows",
            "main_world_eval": True,
        }
        if USE_PROXY:
            camoufox_kwargs["proxy"] = BROWSER_PROXY
            camoufox_kwargs["geoip"] = PROXY_GEO_IP

        async with AsyncCamoufox(**camoufox_kwargs) as browser:
            page = await browser.new_page()

            # Hook WS BEFORE navigation
            page.on("websocket", lambda ws: asyncio.ensure_future(self._on_websocket(ws)))

            log.info("Navigating to %s", TARGET_URL)
            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
                log.info("Page loaded")
            except Exception as exc:
                log.warning("Navigation: %s", exc)

            # Wait for initial WS connections
            await asyncio.sleep(5)

            # Install WS hook
            try:
                r = await page.evaluate(WS_HOOK_SCRIPT)
                log.info("WS hook: %s", r)
            except Exception as exc:
                log.warning("WS hook failed: %s", exc)

            # Wait for hook to capture refs
            await asyncio.sleep(3)

            log.info(
                "Active WS: %d | Token: %s",
                self._active_ws_count,
                "captured" if self._latest_auth_token else "pending",
            )

            # Subscribe to English overview
            eng_topics = [f"OVInPlay{LOCALE}", f"CONFIG{LOCALE}", "Media_L1_Z3"]
            sent = await self._send_ws_messages(page, 0x16, eng_topics)
            if sent > 0:
                log.info("English overview subscribed (%d topics)", sent)
            else:
                log.warning("Failed to subscribe English overview")

            # Let overview data populate
            await asyncio.sleep(5)

            # Run rotation loop with interleaved WS health checks
            await self._rotation_loop(page)

    # -- Rotation loop ------------------------------------------------------

    async def _rotation_loop(self, page: Any) -> None:
        """Continuously rotate through all active events, subscribing and unsubscribing."""
        while self._should_run:
            # WS health check
            if self._ws_connections_seen > 0 and self._active_ws_count == 0:
                log.warning("All WS closed — triggering reconnect")
                self.stats["reconnection_count"] = self.stats.get("reconnection_count", 0) + 1
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=60000)
                    log.info("Page reloaded")
                    await asyncio.sleep(5)
                    try:
                        await page.evaluate(WS_HOOK_SCRIPT)
                    except Exception:
                        pass
                    await asyncio.sleep(3)
                    # Re-subscribe English overview
                    eng_topics = [f"OVInPlay{LOCALE}", f"CONFIG{LOCALE}", "Media_L1_Z3"]
                    await self._send_ws_messages(page, 0x16, eng_topics)
                    await asyncio.sleep(5)
                except Exception as exc:
                    log.warning("Reload failed: %s — restarting browser", exc)
                    return  # exit to reconnect in _browser_session caller

            # Gather active events
            active_eids = [
                eid for eid, ev in self.parser.state.events.items()
                if ev.score or ev.minute
            ]
            if not active_eids:
                log.info("No active events yet, waiting...")
                await asyncio.sleep(3)
                continue

            # Build detail topics for all active events
            all_topics: list[str] = []
            for eid in active_eids:
                t = self._get_detail_topic(eid)
                if t:
                    all_topics.append(t)

            total = len(all_topics)
            log.info("Rotation: %d active events to cover", total)

            # Process in batches
            for i in range(0, total, ROTATION_BATCH):
                if not self._should_run or self._active_ws_count == 0:
                    break

                batch = all_topics[i:i + ROTATION_BATCH]

                # SUBSCRIBE to batch
                sent = await self._send_ws_messages(page, 0x16, batch)
                if sent == 0:
                    await asyncio.sleep(2)
                    continue

                batch_num = i // ROTATION_BATCH + 1
                total_batches = (total + ROTATION_BATCH - 1) // ROTATION_BATCH
                log.info("Rotation %d/%d: subscribed %d topics", batch_num, total_batches, sent)

                # Wait for data to arrive
                await asyncio.sleep(ROTATION_DWELL)

                # UNSUBSCRIBE from batch to free server slots
                await self._send_ws_messages(page, 0x17, batch)

            # Log coverage after full cycle
            detail_count = sum(
                1 for eid in active_eids
                if len(self.parser.state.event_markets.get(eid, set())) > 1
            )
            self.stats["rotation_cycles"] = self.stats.get("rotation_cycles", 0) + 1
            log.info(
                "Rotation complete: %d/%d with detail (%d%%) | %s",
                detail_count,
                len(active_eids),
                100 * detail_count // max(len(active_eids), 1),
                self.parser.summary(),
            )

            # Brief pause before next cycle
            await asyncio.sleep(3)

    # -- WS message sending -------------------------------------------------

    async def _send_ws_messages(self, page: Any, msg_type: int, topics: list[str]) -> int:
        """Send subscribe (0x16) or unsubscribe (0x17) messages for topics.

        For subscribe: includes auth token. For unsubscribe: no token needed.
        Sends in batches of TOPICS_PER_MSG.
        """
        sent = 0
        for i in range(0, len(topics), TOPICS_PER_MSG):
            batch = topics[i:i + TOPICS_PER_MSG]
            topic_list = ",".join(batch)

            if msg_type == 0x16:  # subscribe needs token
                token = ""
                try:
                    token = await page.evaluate("mw:() => window.__lastToken || ''")
                except Exception:
                    pass
                if not token:
                    token = self._latest_auth_token
                if not token:
                    continue
                js = f"""mw:() => {{
                    let ws = (window.__wsRefs || []).find(w => w.url && w.url.includes('premws') && w.readyState === 1);
                    if (!ws) return 'no_ws';
                    ws.send(String.fromCharCode({msg_type}) + String.fromCharCode(0) + '{topic_list},A_{token}' + String.fromCharCode(1));
                    return 'ok';
                }}"""
            else:  # unsubscribe - no token
                js = f"""mw:() => {{
                    let ws = (window.__wsRefs || []).find(w => w.url && w.url.includes('premws') && w.readyState === 1);
                    if (!ws) return 'no_ws';
                    ws.send(String.fromCharCode({msg_type}) + String.fromCharCode(0) + '{topic_list}' + String.fromCharCode(1));
                    return 'ok';
                }}"""

            try:
                result = await page.evaluate(js)
                if result == "ok":
                    sent += len(batch)
            except Exception:
                pass
        return sent

    # -- Detail topic helper ------------------------------------------------

    def _get_detail_topic(self, ev_id: str) -> str | None:
        """Convert an event's overview topic to a detail subscription topic.

        Strip OV prefix and trailing _0 zone suffix.
        OV190302994C1A_3_0 -> 190302994C1A_3
        191029650C1A_3 -> 191029650C1A_3 (already correct)
        """
        ev = self.parser.state.events.get(ev_id)
        if not ev or not ev.topic:
            return None
        t = ev.topic
        if t.startswith("OV"):
            t = t[2:]
        if t.endswith("_0"):
            t = t[:-2]
        return t

    # -- WS hooks -----------------------------------------------------------

    async def _on_websocket(self, ws: Any) -> None:
        """Called when the page opens a new WebSocket."""
        url = ws.url
        self._ws_connections_seen += 1
        self._active_ws_count += 1
        self.stats["ws_connections_seen"] = self.stats.get("ws_connections_seen", 0) + 1
        self.stats["ws_active"] = self.stats.get("ws_active", 0) + 1
        log.info("WebSocket OPENED: %s (active: %d)", url, self._active_ws_count)

        def on_frame_received(payload: str | bytes) -> None:
            self.stats["frames_received"] = self.stats.get("frames_received", 0) + 1
            try:
                self.parser.feed(payload)
            except Exception:
                log.debug("Frame parse error", exc_info=True)

        def on_frame_sent(payload: str | bytes) -> None:
            self.stats["frames_sent"] = self.stats.get("frames_sent", 0) + 1
            try:
                text = payload if isinstance(payload, str) else payload.decode("utf-8", errors="replace")
                idx = text.find(",A_")
                if idx >= 0:
                    token_start = idx + 3
                    token_end = token_start
                    while token_end < len(text) and text[token_end] in \
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=":
                        token_end += 1
                    if token_end - token_start > 20:
                        self._latest_auth_token = text[token_start:token_end]
            except Exception:
                pass

        def on_close(_: Any) -> None:
            self._active_ws_count = max(0, self._active_ws_count - 1)
            self.stats["ws_active"] = max(0, self.stats.get("ws_active", 0) - 1)
            log.warning("WebSocket CLOSED: %s (active: %d)", url, self._active_ws_count)

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
