"""
bet365 Live Stream Server — Multi-Session Per Proxy
====================================================
Launches MULTIPLE independent browser sessions per proxy IP to test whether
separate sessions bypass the ~30% detail subscription limit.

Architecture:
  - 3 proxies × 3 sessions each = 9 total BrowserWorker instances
  - Each worker is a completely fresh AsyncCamoufox context (different
    fingerprint, cookies, session)
  - Events are partitioned across ALL 9 workers: hash(event_id) % 9
  - All workers feed into a SINGLE shared ZapParser
  - One aiohttp HTTP/WS API on port 8365

Usage:
    python -m stream.live_stream
"""

from __future__ import annotations

import asyncio
import hashlib
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

# Proxy list: (host:port, username, password)
PROXIES = [
    ("res.proxy-seller.com:10000", "4f6d12485ca053d6", "OzkAVbyM"),
    ("res.proxy-seller.com:10001", "4f6d12485ca053d6", "OzkAVbyM"),
    ("res.proxy-seller.com:10002", "4f6d12485ca053d6", "OzkAVbyM"),
]

NUM_SESSIONS_PER_PROXY = 3          # 3 proxies × 3 = 9 total workers
TOTAL_WORKERS = len(PROXIES) * NUM_SESSIONS_PER_PROXY

TARGET_URL = "https://www.bet365.es/#/IP/"
API_HOST = "0.0.0.0"
API_PORT = 8365

BROWSER_HEADLESS = True             # True since 9 windows is too many
STAGGER_DELAY = 10                  # seconds between launching sessions
TOPICS_PER_BATCH = 5                # drip-feed: 5 topics per batch
BATCH_INTERVAL = 3                  # seconds between batches
RECONNECT_WAIT_WS = 5
RECONNECT_WAIT_RELOAD = 5
RECONNECT_WAIT_CRASH = 15
NEW_EVENT_CHECK_INTERVAL = 30       # seconds between new-event scans

# JS: Patch WebSocket.prototype.send to capture WS refs for subscription injection
WS_REF_CAPTURE = """mw:() => {
    if (window.__wsRefPatched) return 'already_patched';
    const origSend = WebSocket.prototype.send;
    window.__wsRefs = [];
    WebSocket.prototype.send = function(data) {
        if (!window.__wsRefs.includes(this)) window.__wsRefs.push(this);
        return origSend.call(this, data);
    };
    window.__wsRefPatched = true;
    return 'patched';
}"""


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
# Event partitioning
# ---------------------------------------------------------------------------
def event_owner(event_id: str, total: int) -> int:
    """Deterministic partition: hash(event_id) mod total_workers."""
    h = int(hashlib.md5(event_id.encode()).hexdigest(), 16)
    return h % total


# ---------------------------------------------------------------------------
# HTTP / WebSocket API
# ---------------------------------------------------------------------------
class LiveAPI:
    """aiohttp-based HTTP + WebSocket API server."""

    def __init__(self, parser: ZapParser, stats: dict[str, Any],
                 worker_stats: list[dict[str, Any]]) -> None:
        self.parser = parser
        self.stats = stats
        self.worker_stats = worker_stats
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
        app.router.add_get("/workers", self.handle_workers)
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

    async def broadcast(self, change: ChangeEvent) -> None:
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

    # -- HTTP handlers -------------------------------------------------------

    async def handle_index(self, _req: web.Request) -> web.Response:
        return web.json_response({
            "service": "bet365 Live Stream API (multi-session)",
            "workers": TOTAL_WORKERS,
            "endpoints": [
                "GET /events", "GET /events/{sport_id}", "GET /event/{event_id}",
                "GET /sports", "GET /stats", "GET /workers", "GET /ws",
            ],
        })

    async def handle_events(self, _req: web.Request) -> web.Response:
        return web.json_response(self.parser.get_all_live_events())

    async def handle_events_by_sport(self, req: web.Request) -> web.Response:
        return web.json_response(self.parser.get_sport_events(req.match_info["sport_id"]))

    async def handle_event_detail(self, req: web.Request) -> web.Response:
        detail = self.parser.get_event(req.match_info["event_id"])
        if detail is None:
            return web.json_response({"error": "Event not found"}, status=404)
        return web.json_response(detail)

    async def handle_sports(self, _req: web.Request) -> web.Response:
        sports_list: list[dict[str, Any]] = []
        for sid, sport in self.parser.state.sports.items():
            comp_ids = self.parser.state.sport_competitions.get(sid, set())
            event_count = sum(
                len(self.parser.state.comp_events.get(cid, set()))
                for cid in comp_ids
            )
            sports_list.append({
                "id": sid, "name": sport.name,
                "competition_count": len(comp_ids), "event_count": event_count,
            })
        return web.json_response(sports_list)

    async def handle_stats(self, _req: web.Request) -> web.Response:
        uptime = time.time() - self.stats.get("start_time", time.time())
        parser_summary = self.parser.summary()
        total_frames = sum(w.get("frames_received", 0) for w in self.worker_stats)
        total_detail_subs = sum(w.get("detail_subs_sent", 0) for w in self.worker_stats)
        active_workers = sum(1 for w in self.worker_stats if w.get("alive", False))
        return web.json_response({
            "uptime_seconds": round(uptime, 1),
            "total_workers": TOTAL_WORKERS,
            "active_workers": active_workers,
            "total_frames_received": total_frames,
            "total_detail_subs_sent": total_detail_subs,
            "parser": parser_summary,
            "api_ws_subscribers": len(self._ws_subscribers),
        })

    async def handle_workers(self, _req: web.Request) -> web.Response:
        """Per-worker status endpoint."""
        return web.json_response(self.worker_stats)

    async def handle_ws(self, req: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(req)
        self._ws_subscribers.add(ws)
        log.info("API WS subscriber connected (%d total)", len(self._ws_subscribers))
        try:
            snapshot = {
                "type": "snapshot",
                "sports": list({"id": sid, "name": s.name}
                               for sid, s in self.parser.state.sports.items()),
                "event_count": len(self.parser.state.events),
            }
            await ws.send_json(snapshot)
            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                    break
        finally:
            self._ws_subscribers.discard(ws)
            log.info("API WS subscriber disconnected (%d remaining)",
                     len(self._ws_subscribers))
        return ws


# ---------------------------------------------------------------------------
# BrowserWorker — one independent browser session
# ---------------------------------------------------------------------------
class BrowserWorker:
    """
    A single browser session that:
      1. Opens a fresh camoufox context with a specific proxy
      2. Navigates to bet365.es InPlay
      3. Captures all overview data (every worker gets the full overview)
      4. Subscribes to detail topics ONLY for events assigned to this worker
    """

    def __init__(
        self,
        worker_id: int,
        proxy_spec: tuple[str, str, str],
        parser: ZapParser,
        stats: dict[str, Any],
    ) -> None:
        self.worker_id = worker_id
        self.proxy_host, self.proxy_user, self.proxy_pass = proxy_spec
        self.parser = parser
        self.stats = stats  # per-worker stats dict
        self.name = f"W{worker_id}"
        self.log = logging.getLogger(f"worker.{self.name}")

        self._subscribed_events: set[str] = set()
        self._pending_detail_topics: list[str] = []
        self._latest_auth_token: str = ""
        self._should_run = True
        self._page: Any = None

    async def run(self) -> None:
        """Outer loop with crash recovery."""
        while self._should_run:
            try:
                self.stats["alive"] = True
                await self._browser_session()
            except asyncio.CancelledError:
                self.log.info("Cancelled")
                break
            except Exception:
                self.stats["crashes"] = self.stats.get("crashes", 0) + 1
                self.stats["alive"] = False
                self.log.exception(
                    "Browser session crashed (crash #%d) — restarting in %ds",
                    self.stats["crashes"], RECONNECT_WAIT_CRASH,
                )
                await asyncio.sleep(RECONNECT_WAIT_CRASH)
        self.stats["alive"] = False

    async def _browser_session(self) -> None:
        """One full browser lifecycle."""
        self.log.info("Launching camoufox browser (proxy: %s)...", self.proxy_host)
        self._subscribed_events.clear()
        self._pending_detail_topics.clear()
        self._latest_auth_token = ""

        proxy_url = f"http://{self.proxy_host}"

        camoufox_kwargs: dict[str, Any] = {
            "headless": BROWSER_HEADLESS,
            "humanize": True,
            "os": "windows",
            "main_world_eval": True,
            "proxy": {
                "server": proxy_url,
                "username": self.proxy_user,
                "password": self.proxy_pass,
            },
        }

        async with AsyncCamoufox(**camoufox_kwargs) as browser:
            page = await browser.new_page()
            self._page = page

            # Register WS handler BEFORE navigation
            page.on("websocket", lambda ws: asyncio.ensure_future(self._on_websocket(ws)))

            self.log.info("Navigating to %s", TARGET_URL)
            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
                self.log.info("Page loaded")
            except Exception as exc:
                self.log.warning("Navigation partial/timeout: %s", exc)

            # Wait for initial data
            await asyncio.sleep(8)

            # Patch WS.send to capture refs
            try:
                r = await page.evaluate(WS_REF_CAPTURE)
                self.log.info("WS ref capture: %s", r)
            except Exception as exc:
                self.log.warning("WS ref capture failed: %s", exc)

            # Wait for keepalive to trigger patch
            await asyncio.sleep(10)

            self.log.info(
                "WS active: %d | Total events in parser: %d",
                self.stats.get("ws_active", 0),
                len(self.parser.state.events),
            )

            # Queue initial detail subscriptions for events assigned to this worker
            await self._queue_my_events()

            # Monitor loop
            last_event_scan = time.time()
            while self._should_run:
                await asyncio.sleep(BATCH_INTERVAL)

                # Drain subscription queue (drip-feed)
                if self._pending_detail_topics:
                    await self._drain_subscription_queue(page)

                # Periodically check for new events
                now = time.time()
                if now - last_event_scan > NEW_EVENT_CHECK_INTERVAL:
                    new_count = await self._queue_my_events()
                    if new_count == 0:
                        self.log.info(
                            "Subs sent: %d | Pending: %d | Parser: %s",
                            self.stats.get("detail_subs_sent", 0),
                            len(self._pending_detail_topics),
                            self.parser.summary(),
                        )
                    last_event_scan = now

                # Health check: all WS closed -> reload
                if (
                    self.stats.get("ws_connections_seen", 0) > 0
                    and self.stats.get("ws_active", 0) == 0
                ):
                    self.stats["reconnections"] = self.stats.get("reconnections", 0) + 1
                    self.log.warning(
                        "All WS closed (reconnect #%d) — reloading in %ds...",
                        self.stats["reconnections"], RECONNECT_WAIT_WS,
                    )
                    await asyncio.sleep(RECONNECT_WAIT_WS)
                    self._subscribed_events.clear()
                    self._pending_detail_topics.clear()
                    try:
                        await page.reload(wait_until="domcontentloaded", timeout=60000)
                        self.log.info("Page reloaded")
                        await asyncio.sleep(8)
                        try:
                            await page.evaluate(WS_REF_CAPTURE)
                        except Exception:
                            pass
                        await asyncio.sleep(10)
                        await self._queue_my_events()
                    except Exception as exc:
                        self.log.warning(
                            "Reload failed: %s — restarting browser in %ds",
                            exc, RECONNECT_WAIT_RELOAD,
                        )
                        await asyncio.sleep(RECONNECT_WAIT_RELOAD)
                        return  # exit session -> crash recovery restarts

    async def _queue_my_events(self) -> int:
        """
        Queue detail subscriptions for events assigned to this worker
        that haven't been subscribed yet.
        Returns count of newly queued events.
        """
        all_event_ids = set(self.parser.state.events.keys())
        my_event_ids = {
            eid for eid in all_event_ids
            if event_owner(eid, TOTAL_WORKERS) == self.worker_id
        }
        to_subscribe = my_event_ids - self._subscribed_events
        if not to_subscribe:
            return 0

        topics: list[str] = []
        eids: list[str] = []
        for eid in to_subscribe:
            ev = self.parser.state.events.get(eid)
            if ev and ev.topic:
                detail_topic = ev.topic
                if detail_topic.startswith("OV"):
                    detail_topic = detail_topic[2:]
                if detail_topic.endswith("_0"):
                    detail_topic = detail_topic[:-2]
                topics.append(detail_topic)
                eids.append(eid)

        if not topics:
            return 0

        self._pending_detail_topics.extend(topics)
        self._subscribed_events.update(eids)
        self.log.info(
            "Queued %d detail topics (total pending: %d, my events: %d/%d)",
            len(topics), len(self._pending_detail_topics),
            len(self._subscribed_events), len(all_event_ids),
        )
        return len(topics)

    async def _drain_subscription_queue(self, page: Any) -> None:
        """Send one batch of pending detail topics via the captured WS."""
        if not self._pending_detail_topics:
            return

        token = self._latest_auth_token
        if not token:
            return  # waiting for token capture from framesent

        batch = self._pending_detail_topics[:TOPICS_PER_BATCH]
        topic_list = ",".join(batch)

        try:
            result = await page.evaluate(f"""mw:() => {{
                let refs = window.__wsRefs || [];
                let ws = refs.find(w => w.url && w.url.includes('premws') && w.readyState === 1);
                if (!ws) return 'no_ws:' + refs.length;
                let msg = String.fromCharCode(0x16) + String.fromCharCode(0x00) +
                          '{topic_list},A_{token}' + String.fromCharCode(0x01);
                ws.send(msg);
                return 'sent:' + {len(batch)};
            }}""")
            if result and str(result).startswith("sent:"):
                self._pending_detail_topics = self._pending_detail_topics[TOPICS_PER_BATCH:]
                self.stats["detail_subs_sent"] = self.stats.get("detail_subs_sent", 0) + len(batch)
            elif "no_ws" not in str(result):
                self.log.warning("Subscribe drain: %s", result)
        except Exception as exc:
            self.log.debug("Subscribe drain error: %s", exc)

    # -- WebSocket hooks -----------------------------------------------------

    async def _on_websocket(self, ws: Any) -> None:
        """Called when the page opens a new WebSocket."""
        url = ws.url
        self.stats["ws_connections_seen"] = self.stats.get("ws_connections_seen", 0) + 1
        self.stats["ws_active"] = self.stats.get("ws_active", 0) + 1
        self.log.info("WS OPENED: %s (active: %d)", url, self.stats["ws_active"])

        def on_frame_received(payload: str | bytes) -> None:
            self.stats["frames_received"] = self.stats.get("frames_received", 0) + 1
            try:
                self.parser.feed(payload)
            except Exception:
                self.log.debug("Frame parse error", exc_info=True)

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
            self.stats["ws_active"] = max(0, self.stats.get("ws_active", 0) - 1)
            self.log.warning("WS CLOSED: %s (active: %d)", url, self.stats["ws_active"])

        def on_error(error: Any) -> None:
            self.log.error("WS ERROR on %s: %s", url, error)

        ws.on("framereceived", on_frame_received)
        ws.on("framesent", on_frame_sent)
        ws.on("close", on_close)
        ws.on("socketerror", on_error)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
class LiveStream:
    """
    Orchestrates 9 BrowserWorker instances (3 proxies × 3 sessions each),
    a shared ZapParser, and the HTTP API.
    """

    def __init__(self) -> None:
        self.parser = ZapParser()
        self.global_stats: dict[str, Any] = {"start_time": time.time()}

        # Per-worker stats
        self.worker_stats: list[dict[str, Any]] = []
        self.workers: list[BrowserWorker] = []

        for proxy_idx, proxy_spec in enumerate(PROXIES):
            for session_idx in range(NUM_SESSIONS_PER_PROXY):
                worker_id = proxy_idx * NUM_SESSIONS_PER_PROXY + session_idx
                wstats: dict[str, Any] = {
                    "worker_id": worker_id,
                    "proxy_idx": proxy_idx,
                    "session_idx": session_idx,
                    "proxy_host": proxy_spec[0],
                    "alive": False,
                    "ws_connections_seen": 0,
                    "ws_active": 0,
                    "frames_received": 0,
                    "frames_sent": 0,
                    "detail_subs_sent": 0,
                    "crashes": 0,
                    "reconnections": 0,
                }
                self.worker_stats.append(wstats)
                self.workers.append(BrowserWorker(worker_id, proxy_spec, self.parser, wstats))

        self.api = LiveAPI(self.parser, self.global_stats, self.worker_stats)

        # Broadcast parser changes to API WS subscribers
        self.parser.on_change(self._on_parser_change)

    def _on_parser_change(self, change: ChangeEvent) -> None:
        asyncio.ensure_future(self.api.broadcast(change))

    async def run(self) -> None:
        log.info("=== bet365 Live Stream starting ===")
        log.info(
            "Configuration: %d proxies × %d sessions = %d total workers",
            len(PROXIES), NUM_SESSIONS_PER_PROXY, TOTAL_WORKERS,
        )

        # Start HTTP API first
        await self.api.start()

        # Launch workers with staggered starts
        tasks: list[asyncio.Task] = []
        try:
            for i, worker in enumerate(self.workers):
                if i > 0:
                    log.info(
                        "Staggering: waiting %ds before launching %s...",
                        STAGGER_DELAY, worker.name,
                    )
                    await asyncio.sleep(STAGGER_DELAY)
                log.info(
                    "Launching %s (proxy %d, session %d)...",
                    worker.name, i // NUM_SESSIONS_PER_PROXY, i % NUM_SESSIONS_PER_PROXY,
                )
                task = asyncio.create_task(worker.run(), name=worker.name)
                tasks.append(task)

            # Wait for all (they run forever until cancelled)
            await asyncio.gather(*tasks, return_exceptions=True)

        except asyncio.CancelledError:
            log.info("Live stream cancelled")
        except KeyboardInterrupt:
            log.info("Live stream interrupted")
        finally:
            log.info("Shutting down all workers...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.api.stop()
            log.info("Shutdown complete")


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
