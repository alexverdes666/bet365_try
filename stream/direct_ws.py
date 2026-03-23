"""
bet365 Direct WebSocket Client — Full Coverage Mode
=====================================================
Achieves 100% real-time coverage of bet365 live events by opening MULTIPLE
direct WebSocket connections to the premws server.

Architecture:
  1. ONE camoufox browser navigates to bet365.es InPlay via HTTP proxy
  2. Browser hooks capture:  auth token, cookies, premws URL, User-Agent
  3. Browser is kept alive for session renewal (token refresh)
  4. N direct websockets connections to premws (via proxy), each handling
     a batch of ~20 event detail subscriptions
  5. All received frames are fed to a shared ZapParser
  6. aiohttp HTTP/WS API on port 8365 exposes the unified data

The ZAP protocol handshake:
  - Connect to wss://premws-pt3.bet365.es/zap/ with subprotocol 'zap-protocol-v1'
  - Server sends: 0x31 (ACK) + protocol_version + 0x02 + connection_id
  - Subscribe:    0x16 + 0x00 + "topic1,topic2,...,A_TOKEN" + 0x01

Usage:
    python -m stream.direct_ws
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp
from aiohttp import web
import websockets

from protocol.zap_parser import ZapParser, ChangeEvent, ChangeType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("direct_ws")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Proxy for both browser and direct WS connections
PROXY_HTTP = "http://res.proxy-seller.com:10000"
PROXY_USER = "4f6d12485ca053d6"
PROXY_PASS = "OzkAVbyM"

TARGET_URL = "https://www.bet365.es/#/IP/"
PREMWS_URL = "wss://premws-pt3.bet365.es/zap/"

API_HOST = "0.0.0.0"
API_PORT = 8365

# Direct WS connection settings
TOPICS_PER_CONNECTION = 20    # max detail topics per WS connection
MAX_DIRECT_CONNECTIONS = 50   # upper limit on direct WS connections
TOKEN_REFRESH_INTERVAL = 120  # seconds between token refreshes from browser
NEW_EVENT_SCAN_INTERVAL = 15  # seconds between scans for new events
CONNECTION_BATCH_DELAY = 2.0  # seconds between opening new WS connections

# ZAP protocol bytes
BYTE_ACK = 0x31       # ASCII '1' — server acknowledgment
BYTE_SUBSCRIBE = 0x16
BYTE_SOH = 0x01
BYTE_NUL = 0x00
BYTE_CLIENT_PING = 0x19
ZAP_KEEPALIVE_INTERVAL = 25  # seconds between ZAP client pings

# Use proxy for browser and WS connections
USE_PROXY = True


# ---------------------------------------------------------------------------
# ChangeEvent serialization helper (same as live_stream.py)
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
# HTTP / WebSocket API  (aiohttp) — identical to LiveAPI in live_stream.py
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

    async def handle_index(self, _request: web.Request) -> web.Response:
        return web.json_response({
            "service": "bet365 Direct WS — Full Coverage API",
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
            "browser_ws_active": self.stats.get("browser_ws_active", 0),
            "browser_frames_received": self.stats.get("browser_frames_received", 0),
            "direct_ws_connections": self.stats.get("direct_ws_connections", 0),
            "direct_ws_active": self.stats.get("direct_ws_active", 0),
            "direct_frames_received": self.stats.get("direct_frames_received", 0),
            "detail_topics_subscribed": self.stats.get("detail_topics_subscribed", 0),
            "events_with_details": self.stats.get("events_with_details", 0),
            "token_refreshes": self.stats.get("token_refreshes", 0),
            "parser": parser_summary,
            "api_ws_subscribers": len(self._ws_subscribers),
            "coverage_pct": self._calc_coverage(parser_summary),
        }
        return web.json_response(data)

    def _calc_coverage(self, summary: dict[str, int]) -> float:
        """Calculate detail coverage percentage."""
        total_events = summary.get("events", 0)
        if total_events == 0:
            return 0.0
        events_with_markets = sum(
            1 for eid in self.parser.state.events
            if self.parser.state.event_markets.get(eid)
        )
        return round(100.0 * events_with_markets / total_events, 1)

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
# DirectWSConnection — a single direct WebSocket connection to premws
# ---------------------------------------------------------------------------
class DirectWSConnection:
    """
    Manages a single direct WebSocket connection to the premws server.
    Handles the ZAP handshake, subscribes to a batch of topics, and feeds
    all received frames to the shared ZapParser.
    """

    def __init__(
        self,
        conn_id: int,
        premws_url: str,
        topics: list[str],
        auth_token: str,
        cookies: str,
        user_agent: str,
        parser: ZapParser,
        stats: dict[str, Any],
        proxy_url: str | None = None,
    ) -> None:
        self.conn_id = conn_id
        self.premws_url = premws_url
        self.topics = topics
        self.auth_token = auth_token
        self.cookies = cookies
        self.user_agent = user_agent
        self.parser = parser
        self.stats = stats
        self.proxy_url = proxy_url
        self._ws: Any = None
        self._running = False
        self._task: asyncio.Task | None = None
        self.connected = False
        self.zap_connection_id: str = ""
        self.frames_received = 0

    async def start(self) -> None:
        """Start the connection in a background task."""
        self._running = True
        self._task = asyncio.create_task(self._run(), name=f"direct_ws_{self.conn_id}")

    async def stop(self) -> None:
        """Stop the connection gracefully."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        """Main connection loop with reconnection."""
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning(
                    "DirectWS #%d error: %s — reconnecting in 5s",
                    self.conn_id, exc,
                )
                self.connected = False
                await asyncio.sleep(5)

    async def _connect_and_listen(self) -> None:
        """Connect, handshake, subscribe, and listen."""
        headers = {
            "Cookie": self.cookies,
            "Origin": "https://www.bet365.es",
        }

        connect_kwargs: dict[str, Any] = {
            "uri": self.premws_url,
            "subprotocols": ["zap-protocol-v1"],
            "additional_headers": headers,
            "user_agent_header": self.user_agent,
            "open_timeout": 30,
            "ping_interval": None,   # ZAP has its own keepalive
            "ping_timeout": None,
            "close_timeout": 5,
            "max_size": 2**22,       # 4 MB max message size
            "compression": None,     # ZAP doesn't use permessage-deflate
        }

        if self.proxy_url:
            connect_kwargs["proxy"] = self.proxy_url

        log.info(
            "DirectWS #%d: connecting to %s (%d topics)...",
            self.conn_id, self.premws_url, len(self.topics),
        )

        async with websockets.connect(**connect_kwargs) as ws:
            self._ws = ws
            self.stats["direct_ws_active"] = self.stats.get("direct_ws_active", 0) + 1
            log.info("DirectWS #%d: connected, waiting for server ACK...", self.conn_id)

            try:
                # Wait for server ACK (0x31 + version + 0x02 + connection_id)
                ack_msg = await asyncio.wait_for(ws.recv(), timeout=15)
                ack_text = ack_msg if isinstance(ack_msg, str) else ack_msg.decode("utf-8", errors="replace")

                if ack_text and ord(ack_text[0]) == BYTE_ACK:
                    # Feed to parser to extract connection_id / protocol_version
                    self.parser.feed(ack_text)
                    self.zap_connection_id = self.parser.connection_id or ""
                    log.info(
                        "DirectWS #%d: server ACK received (conn_id=%s)",
                        self.conn_id, self.zap_connection_id,
                    )
                else:
                    log.warning(
                        "DirectWS #%d: unexpected first message (byte=0x%02x, len=%d)",
                        self.conn_id, ord(ack_text[0]) if ack_text else 0, len(ack_text),
                    )

                self.connected = True

                # Send subscribe message
                await self._subscribe(ws)

                # Run frame listener and keepalive concurrently
                listener_task = asyncio.create_task(self._listen_frames(ws))
                keepalive_task = asyncio.create_task(self._keepalive_loop(ws))

                try:
                    # Wait for either task to finish (listener finishes when WS closes)
                    done, pending = await asyncio.wait(
                        [listener_task, keepalive_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in pending:
                        t.cancel()
                        try:
                            await t
                        except (asyncio.CancelledError, Exception):
                            pass
                    # Re-raise any exception from the listener
                    for t in done:
                        if t.exception():
                            raise t.exception()
                except asyncio.CancelledError:
                    listener_task.cancel()
                    keepalive_task.cancel()

            finally:
                self.connected = False
                self.stats["direct_ws_active"] = max(
                    0, self.stats.get("direct_ws_active", 0) - 1
                )
                log.warning("DirectWS #%d: disconnected", self.conn_id)

    async def _listen_frames(self, ws: Any) -> None:
        """Listen for incoming frames and feed them to the parser."""
        async for message in ws:
            if not self._running:
                break
            self.frames_received += 1
            self.stats["direct_frames_received"] = (
                self.stats.get("direct_frames_received", 0) + 1
            )
            try:
                self.parser.feed(message)
            except Exception:
                log.debug("DirectWS #%d: frame parse error", self.conn_id, exc_info=True)

    async def _keepalive_loop(self, ws: Any) -> None:
        """Send periodic ZAP client pings to keep the connection alive."""
        while self._running and self.connected:
            await asyncio.sleep(ZAP_KEEPALIVE_INTERVAL)
            if not self._running or not self.connected:
                break
            try:
                # ZAP client ping: 0x19
                await ws.send(chr(BYTE_CLIENT_PING))
                log.debug("DirectWS #%d: sent ZAP keepalive ping", self.conn_id)
            except Exception as exc:
                log.warning("DirectWS #%d: keepalive send failed: %s", self.conn_id, exc)
                break

    async def _subscribe(self, ws: Any) -> None:
        """Send a ZAP subscribe message for all topics in this connection's batch."""
        if not self.topics:
            return

        # Build subscribe payload:
        # 0x16 + 0x00 + "topic1,topic2,...,A_<token>" + 0x01
        topic_list = ",".join(self.topics)
        payload = (
            chr(BYTE_SUBSCRIBE)
            + chr(BYTE_NUL)
            + topic_list
            + ",A_" + self.auth_token
            + chr(BYTE_SOH)
        )

        await ws.send(payload)
        self.stats["detail_topics_subscribed"] = (
            self.stats.get("detail_topics_subscribed", 0) + len(self.topics)
        )
        log.info(
            "DirectWS #%d: subscribed to %d topics (total: %d)",
            self.conn_id, len(self.topics),
            self.stats.get("detail_topics_subscribed", 0),
        )

    async def resubscribe(self, new_topics: list[str], auth_token: str) -> None:
        """Add new topics to this connection's subscription."""
        if not new_topics or not self._ws or not self.connected:
            return

        self.topics.extend(new_topics)
        self.auth_token = auth_token

        topic_list = ",".join(new_topics)
        payload = (
            chr(BYTE_SUBSCRIBE)
            + chr(BYTE_NUL)
            + topic_list
            + ",A_" + auth_token
            + chr(BYTE_SOH)
        )

        try:
            await self._ws.send(payload)
            self.stats["detail_topics_subscribed"] = (
                self.stats.get("detail_topics_subscribed", 0) + len(new_topics)
            )
            log.info(
                "DirectWS #%d: added %d topics (conn total: %d)",
                self.conn_id, len(new_topics), len(self.topics),
            )
        except Exception as exc:
            log.warning("DirectWS #%d: resubscribe failed: %s", self.conn_id, exc)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
class DirectWSStream:
    """
    Orchestrates browser session extraction, direct WS connections,
    data parsing, and the HTTP API.
    """

    def __init__(self) -> None:
        self.parser = ZapParser()
        self.stats: dict[str, Any] = {
            "start_time": time.time(),
            "browser_ws_active": 0,
            "browser_frames_received": 0,
            "direct_ws_connections": 0,
            "direct_ws_active": 0,
            "direct_frames_received": 0,
            "detail_topics_subscribed": 0,
            "events_with_details": 0,
            "token_refreshes": 0,
        }
        self.api = LiveAPI(self.parser, self.stats)
        self._should_run = True
        self._direct_conns: list[DirectWSConnection] = []
        self._subscribed_event_ids: set[str] = set()
        self._next_conn_id = 0

        # Session data extracted from browser
        self._auth_token: str = ""
        self._cookies_str: str = ""
        self._user_agent: str = ""
        self._premws_url: str = PREMWS_URL

        # Register change callback
        self.parser.on_change(self._on_parser_change)

    def _on_parser_change(self, change: ChangeEvent) -> None:
        asyncio.ensure_future(self.api.broadcast(change))

    async def run(self) -> None:
        """Main entry point."""
        log.info("=" * 60)
        log.info("bet365 Direct WebSocket — Full Coverage Mode")
        log.info("=" * 60)

        # Start HTTP API
        await self.api.start()

        try:
            while self._should_run:
                try:
                    await self._session()
                except Exception:
                    log.exception("Session crashed — restarting in 10s")
                    await self._cleanup_direct_connections()
                    await asyncio.sleep(10)
        except (asyncio.CancelledError, KeyboardInterrupt):
            log.info("Shutting down...")
        finally:
            await self._cleanup_direct_connections()
            await self.api.stop()
            log.info("Shutdown complete")

    async def _session(self) -> None:
        """
        One full session:
        1. Launch browser, capture session data
        2. Open direct WS connections for event details
        3. Monitor loop: refresh tokens, scan for new events
        """
        from camoufox.async_api import AsyncCamoufox

        log.info("Launching camoufox browser for session extraction...")

        camoufox_kwargs: dict[str, Any] = {
            "headless": False,
            "humanize": True,
            "os": "windows",
            "main_world_eval": True,
        }

        if USE_PROXY:
            camoufox_kwargs["proxy"] = {
                "server": PROXY_HTTP,
                "username": PROXY_USER,
                "password": PROXY_PASS,
            }
            camoufox_kwargs["geoip"] = True

        async with AsyncCamoufox(**camoufox_kwargs) as browser:
            page = await browser.new_page()

            # Capture WS hooks from browser for overview data + token extraction
            page.on("websocket", lambda ws: asyncio.ensure_future(self._on_browser_ws(ws)))

            log.info("Navigating to %s", TARGET_URL)
            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
                log.info("Page loaded")
            except Exception as exc:
                log.warning("Navigation partial: %s", exc)

            # Wait for initial WS close/reconnect cycle + data load
            log.info("Waiting for WS reconnect + data load (25s)...")
            await asyncio.sleep(25)

            # Extract session data (cookies, UA)
            await self._extract_session_data(page, browser)

            # If token not yet captured, wait more for WS keepalive
            if not self._auth_token:
                log.info("Token not yet captured, waiting 15s more for WS keepalive...")
                await asyncio.sleep(15)

            if not self._auth_token:
                log.error("Failed to capture auth token — retrying session in 10s")
                await asyncio.sleep(10)
                return

            log.info(
                "Session extracted: token=%s...%s, cookies=%d chars, UA=%s",
                self._auth_token[:8],
                self._auth_token[-8:] if len(self._auth_token) > 16 else "",
                len(self._cookies_str),
                self._user_agent[:50],
            )
            log.info(
                "Initial state: %d events from browser overview",
                len(self.parser.state.events),
            )

            # Open direct WS connections for all known events
            await self._subscribe_all_events()

            # Monitor loop
            last_event_scan = time.time()
            last_token_refresh = time.time()

            while self._should_run:
                await asyncio.sleep(3)

                now = time.time()

                # Refresh token periodically from browser framesent
                if now - last_token_refresh > TOKEN_REFRESH_INTERVAL:
                    self.stats["token_refreshes"] = self.stats.get("token_refreshes", 0) + 1
                    last_token_refresh = now
                    log.info(
                        "Token still active: %s...%s",
                        self._auth_token[:8],
                        self._auth_token[-8:] if len(self._auth_token) > 16 else "",
                    )

                # Scan for new events
                if now - last_event_scan > NEW_EVENT_SCAN_INTERVAL:
                    new_events = set(self.parser.state.events.keys()) - self._subscribed_event_ids
                    if new_events:
                        log.info("Found %d new events — opening subscriptions", len(new_events))
                        await self._subscribe_new_events(new_events)
                    last_event_scan = now

                # Log coverage stats
                if int(now) % 30 < 3:  # every ~30s
                    total = len(self.parser.state.events)
                    with_markets = sum(
                        1 for eid in self.parser.state.events
                        if self.parser.state.event_markets.get(eid)
                    )
                    self.stats["events_with_details"] = with_markets
                    log.info(
                        "Coverage: %d/%d events with details (%.1f%%) | "
                        "Direct WS: %d active | Frames: browser=%d direct=%d | %s",
                        with_markets, total,
                        (100.0 * with_markets / total) if total else 0,
                        self.stats.get("direct_ws_active", 0),
                        self.stats.get("browser_frames_received", 0),
                        self.stats.get("direct_frames_received", 0),
                        self.parser.summary(),
                    )
                    await asyncio.sleep(3)  # avoid repeated log on same ~30s window

                # Check if browser WS died — need session restart
                if (
                    self.stats.get("browser_ws_active", 0) == 0
                    and self.stats.get("browser_frames_received", 0) > 0
                ):
                    log.warning("Browser WebSocket died — restarting session")
                    return

    async def _extract_session_data(self, page: Any, browser: Any) -> None:
        """Extract cookies, user-agent, and premws URL from the browser."""
        # Cookies
        try:
            cookies = await page.context.cookies()
            cookie_parts = []
            for c in cookies:
                cookie_parts.append(f"{c['name']}={c['value']}")
            self._cookies_str = "; ".join(cookie_parts)
            log.info("Captured %d cookies", len(cookies))
        except Exception as exc:
            log.warning("Cookie extraction failed: %s", exc)

        # User-Agent
        try:
            self._user_agent = await page.evaluate("navigator.userAgent")
            log.info("User-Agent: %s", self._user_agent[:80])
        except Exception as exc:
            log.warning("UA extraction failed: %s", exc)
            self._user_agent = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) "
                "Gecko/20100101 Firefox/134.0"
            )

    async def _on_browser_ws(self, ws: Any) -> None:
        """Handle WebSocket connections from the browser tab."""
        url = ws.url
        self.stats["browser_ws_active"] = self.stats.get("browser_ws_active", 0) + 1
        log.info("Browser WS opened: %s", url)

        # Detect premws URL
        if "premws" in url:
            # Extract the actual premws URL (might differ from our default)
            base = url.split("?")[0]
            if base != self._premws_url:
                log.info("Detected premws URL: %s (was: %s)", base, self._premws_url)
                self._premws_url = base

        def on_frame_received(payload: str | bytes) -> None:
            self.stats["browser_frames_received"] = (
                self.stats.get("browser_frames_received", 0) + 1
            )
            try:
                self.parser.feed(payload)
            except Exception:
                log.debug("Browser frame parse error", exc_info=True)

        def on_frame_sent(payload: str | bytes) -> None:
            # Capture auth token from outgoing messages
            try:
                text = payload if isinstance(payload, str) else payload.decode("utf-8", errors="replace")
                idx = text.find(",A_")
                if idx >= 0:
                    token_start = idx + 3
                    token_end = token_start
                    while token_end < len(text) and text[token_end] in (
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                        "0123456789+/="
                    ):
                        token_end += 1
                    if token_end - token_start > 20:
                        new_token = text[token_start:token_end]
                        if new_token != self._auth_token:
                            self._auth_token = new_token
                            log.info(
                                "Auth token captured: %s...%s (%d chars)",
                                new_token[:8],
                                new_token[-8:] if len(new_token) > 16 else "",
                                len(new_token),
                            )
            except Exception:
                pass

        def on_close(_: Any) -> None:
            self.stats["browser_ws_active"] = max(
                0, self.stats.get("browser_ws_active", 0) - 1
            )
            log.warning("Browser WS closed: %s (active: %d)", url, self.stats["browser_ws_active"])

        ws.on("framereceived", on_frame_received)
        ws.on("framesent", on_frame_sent)
        ws.on("close", on_close)

    # -- Direct WS connection management ------------------------------------

    def _event_to_detail_topic(self, event_id: str) -> str | None:
        """Convert an event ID to its detail subscription topic."""
        ev = self.parser.state.events.get(event_id)
        if not ev or not ev.topic:
            return None
        topic = ev.topic
        # Strip "OV" prefix and "_0" suffix
        if topic.startswith("OV"):
            topic = topic[2:]
        if topic.endswith("_0"):
            topic = topic[:-2]
        return topic

    async def _subscribe_all_events(self) -> None:
        """Open direct WS connections for all known events."""
        all_event_ids = set(self.parser.state.events.keys()) - self._subscribed_event_ids
        if not all_event_ids:
            log.info("No events to subscribe to")
            return
        await self._subscribe_new_events(all_event_ids)

    async def _subscribe_new_events(self, event_ids: set[str]) -> None:
        """Open new direct WS connections for the given event IDs."""
        # Build detail topics
        topics: list[str] = []
        eids: list[str] = []
        for eid in event_ids:
            topic = self._event_to_detail_topic(eid)
            if topic:
                topics.append(topic)
                eids.append(eid)

        if not topics:
            return

        self._subscribed_event_ids.update(eids)

        # Partition topics into batches of TOPICS_PER_CONNECTION
        batches: list[list[str]] = []
        for i in range(0, len(topics), TOPICS_PER_CONNECTION):
            batches.append(topics[i : i + TOPICS_PER_CONNECTION])

        # First, try to fill existing connections that have spare capacity
        remaining_batches: list[list[str]] = []
        for batch in batches:
            placed = False
            for conn in self._direct_conns:
                if conn.connected and len(conn.topics) < TOPICS_PER_CONNECTION:
                    space = TOPICS_PER_CONNECTION - len(conn.topics)
                    to_add = batch[:space]
                    leftover = batch[space:]
                    await conn.resubscribe(to_add, self._auth_token)
                    if leftover:
                        remaining_batches.append(leftover)
                    placed = True
                    break
            if not placed:
                remaining_batches.append(batch)

        # Open new connections for remaining batches
        for batch in remaining_batches:
            if len(self._direct_conns) >= MAX_DIRECT_CONNECTIONS:
                log.warning(
                    "Max direct connections (%d) reached — %d topics unsubscribed",
                    MAX_DIRECT_CONNECTIONS,
                    len(batch),
                )
                break

            proxy_url = None
            if USE_PROXY:
                proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@res.proxy-seller.com:10000"

            conn = DirectWSConnection(
                conn_id=self._next_conn_id,
                premws_url=self._premws_url,
                topics=batch,
                auth_token=self._auth_token,
                cookies=self._cookies_str,
                user_agent=self._user_agent,
                parser=self.parser,
                stats=self.stats,
                proxy_url=proxy_url,
            )
            self._next_conn_id += 1
            self._direct_conns.append(conn)
            self.stats["direct_ws_connections"] = len(self._direct_conns)
            await conn.start()

            # Small delay between connections to avoid rate limiting
            await asyncio.sleep(CONNECTION_BATCH_DELAY)

        log.info(
            "Subscription update: %d events, %d direct connections total",
            len(eids), len(self._direct_conns),
        )

    async def _cleanup_direct_connections(self) -> None:
        """Stop all direct WS connections."""
        log.info("Cleaning up %d direct connections...", len(self._direct_conns))
        tasks = [conn.stop() for conn in self._direct_conns]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._direct_conns.clear()
        self._subscribed_event_ids.clear()
        self.stats["direct_ws_connections"] = 0
        self.stats["direct_ws_active"] = 0
        self.stats["detail_topics_subscribed"] = 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    stream = DirectWSStream()
    await stream.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Exited.")
