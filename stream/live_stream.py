"""
bet365 Live Stream Server — Multi-Browser Real-Time Coverage
=============================================================
3 camoufox browsers with different proxies. Each browser subscribes to
its 1/3 of events via the main premws WS, drip-feeding subscriptions
slowly to maximize server acceptance. Persistent subs — no rotation.

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
TARGET_URL = "https://www.bet365.es/#/IP/"
API_HOST = "0.0.0.0"
API_PORT = 8365
LOCALE = "_1_3"  # English

PROXIES: list[dict[str, str]] = [
    {"server": "http://res.proxy-seller.com:10000", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
    {"server": "http://res.proxy-seller.com:10001", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
    {"server": "http://res.proxy-seller.com:10002", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
]

BROWSER_HEADLESS = False
BROWSER_LAUNCH_STAGGER = 15

# Drip subscription: subscribe N topics at a time, wait between batches
DRIP_BATCH = 5        # topics per drip batch
DRIP_INTERVAL = 3     # seconds between drip batches
TOPICS_PER_MSG = 50

RECONNECT_WAIT_CRASH = 10

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


def _event_owner(event_id: str, num_workers: int) -> int:
    return sum(ord(c) for c in event_id) % num_workers


def _change_event_to_dict(ce: ChangeEvent) -> dict[str, Any]:
    d: dict[str, Any] = {
        "change_type": ce.change_type.value, "entity_type": ce.entity_type,
        "entity_id": ce.entity_id, "topic": ce.topic,
    }
    for key in ("old_value", "new_value"):
        val = getattr(ce, key)
        if val is None: d[key] = None
        elif isinstance(val, dict): d[key] = val
        elif isinstance(val, (str, int, float, bool)): d[key] = val
        else: d[key] = {"id": getattr(val, "id", ""), "name": getattr(val, "name", "")}
    return d


# ---------------------------------------------------------------------------
# HTTP / WebSocket API
# ---------------------------------------------------------------------------
class LiveAPI:
    def __init__(self, parser: ZapParser, stats: dict[str, Any]) -> None:
        self.parser = parser
        self.stats = stats
        self._ws_subs: set[web.WebSocketResponse] = set()
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/events", self._events)
        app.router.add_get("/events/{sport_id}", self._events_sport)
        app.router.add_get("/event/{event_id}", self._event)
        app.router.add_get("/sports", self._sports)
        app.router.add_get("/ws", self._ws)
        app.router.add_get("/stats", self._stats)
        app.router.add_get("/", self._index)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        await web.TCPSite(self._runner, API_HOST, API_PORT).start()
        log.info("API on http://%s:%d", API_HOST, API_PORT)

    async def stop(self) -> None:
        for ws in list(self._ws_subs): await ws.close()
        if self._runner: await self._runner.cleanup()

    async def broadcast(self, ce: ChangeEvent) -> None:
        if not self._ws_subs: return
        payload = json.dumps(_change_event_to_dict(ce))
        dead = [ws for ws in self._ws_subs if not (await self._try_send(ws, payload))]
        for ws in dead: self._ws_subs.discard(ws)

    async def _try_send(self, ws, payload):
        try: await ws.send_str(payload); return True
        except: return False

    async def _index(self, _r): return web.json_response({"service": "bet365 Live Stream", "browsers": len(PROXIES)})
    async def _events(self, _r): return web.json_response(self.parser.get_all_live_events())
    async def _events_sport(self, r): return web.json_response(self.parser.get_sport_events(r.match_info["sport_id"]))
    async def _event(self, r):
        d = self.parser.get_event(r.match_info["event_id"])
        return web.json_response(d) if d else web.json_response({"error": "Not found"}, status=404)

    async def _sports(self, _r):
        out = []
        for sid, s in self.parser.state.sports.items():
            cids = self.parser.state.sport_competitions.get(sid, set())
            ec = sum(len(self.parser.state.comp_events.get(c, set())) for c in cids)
            out.append({"id": sid, "name": s.name, "competitions": len(cids), "events": ec})
        return web.json_response(out)

    async def _stats(self, _r):
        up = time.time() - self.stats.get("start_time", time.time())
        return web.json_response({
            "uptime": round(up, 1), "browsers": len(PROXIES),
            "ws_seen": self.stats.get("ws_seen", 0), "ws_active": self.stats.get("ws_active", 0),
            "frames": self.stats.get("frames", 0), "reconnects": self.stats.get("reconnects", 0),
            "parser": self.parser.summary(), "subscribers": len(self._ws_subs),
        })

    async def _ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_subs.add(ws)
        try:
            await ws.send_json({"type": "snapshot", "event_count": len(self.parser.state.events)})
            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE): break
        finally:
            self._ws_subs.discard(ws)
        return ws


# ---------------------------------------------------------------------------
# Browser worker
# ---------------------------------------------------------------------------
class BrowserWorker:
    def __init__(self, wid: int, n: int, proxy: dict, parser: ZapParser, stats: dict) -> None:
        self.wid = wid
        self.n = n
        self.proxy = proxy
        self.parser = parser
        self.stats = stats
        self._run = True
        self._ws_seen = 0
        self._ws_active = 0
        self._token = ""
        self._subs: set[str] = set()
        self._log = logging.getLogger(f"w{wid}")

    def mine(self, eid: str) -> bool:
        return _event_owner(eid, self.n) == self.wid

    async def run(self) -> None:
        while self._run:
            try:
                await self._session()
            except asyncio.CancelledError:
                break
            except Exception:
                self._log.exception("Crash — restart in %ds", RECONNECT_WAIT_CRASH)
                await asyncio.sleep(RECONNECT_WAIT_CRASH)

    async def _session(self) -> None:
        self._log.info("Launch (proxy %s)", self.proxy["server"].split(":")[-1])
        self._ws_seen = self._ws_active = 0
        self._subs.clear()

        async with AsyncCamoufox(
            headless=BROWSER_HEADLESS, humanize=True, os="windows",
            main_world_eval=True, proxy=self.proxy, geoip=True,
        ) as browser:
            page = await browser.new_page()
            page.on("websocket", lambda ws: asyncio.ensure_future(self._on_ws(ws)))

            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                self._log.warning("Nav: %s", e)

            await asyncio.sleep(5)
            try: await page.evaluate(WS_HOOK_SCRIPT)
            except: pass
            await asyncio.sleep(10)

            # Subscribe English overview
            token = await self._get_token(page)
            if token:
                await self._sub(page, [f"OVInPlay{LOCALE}", f"CONFIG{LOCALE}", "Media_L1_Z3"], token)
                self._log.info("Overview subscribed")

            await asyncio.sleep(5)
            await self._drip_loop(page)

    async def _drip_loop(self, page: Any) -> None:
        """Drip-feed subscriptions: subscribe DRIP_BATCH topics every DRIP_INTERVAL seconds."""
        while self._run:
            if self._ws_seen > 0 and self._ws_active == 0:
                self._log.warning("WS dead — restart")
                self.stats["reconnects"] = self.stats.get("reconnects", 0) + 1
                return

            my = [eid for eid, ev in self.parser.state.events.items()
                  if self.mine(eid) and (ev.score or ev.minute)]

            if not my:
                await asyncio.sleep(5)
                continue

            # Find new topics to subscribe
            new_topics = []
            for eid in my:
                t = self._detail_topic(eid)
                if t and t not in self._subs:
                    new_topics.append(t)

            # Drip-feed new subscriptions
            if new_topics:
                token = await self._get_token(page)
                if token:
                    for i in range(0, len(new_topics), DRIP_BATCH):
                        if not self._run or self._ws_active == 0:
                            break
                        batch = new_topics[i:i + DRIP_BATCH]
                        sent = await self._sub(page, batch, token)
                        if sent > 0:
                            self._subs.update(batch[:sent])
                        await asyncio.sleep(DRIP_INTERVAL)

            # Unsubscribe ended events
            current = set()
            for eid in my:
                t = self._detail_topic(eid)
                if t: current.add(t)
            ended = self._subs - current
            if ended:
                token = await self._get_token(page)
                if token:
                    await self._unsub(page, list(ended))
                    self._subs -= ended

            # Coverage
            det = sum(1 for eid in my if len(self.parser.state.event_markets.get(eid, set())) > 1)
            self._log.info("Detail: %d/%d (%d%%) | %d subs",
                           det, len(my), 100 * det // max(len(my), 1), len(self._subs))
            await asyncio.sleep(15)

    def _detail_topic(self, eid: str) -> str | None:
        ev = self.parser.state.events.get(eid)
        if not ev or not ev.topic: return None
        t = ev.topic
        if t.startswith("OV"): t = t[2:]
        if t.endswith("_0"): t = t[:-2]
        return t

    async def _get_token(self, page) -> str:
        try:
            t = await page.evaluate("mw:() => window.__lastToken || ''")
            if t: return t
        except: pass
        return self._token

    async def _sub(self, page, topics, token) -> int:
        tstr = ",".join(topics)
        js = f"""mw:() => {{
            let ws = (window.__wsRefs || []).find(w => w.url && w.url.includes('premws') && w.readyState === 1);
            if (!ws) return 'no_ws';
            ws.send(String.fromCharCode(0x16) + String.fromCharCode(0) + '{tstr},A_{token}' + String.fromCharCode(1));
            return 'ok';
        }}"""
        try:
            r = await page.evaluate(js)
            return len(topics) if r == "ok" else 0
        except: return 0

    async def _unsub(self, page, topics) -> int:
        tstr = ",".join(topics)
        js = f"""mw:() => {{
            let ws = (window.__wsRefs || []).find(w => w.url && w.url.includes('premws') && w.readyState === 1);
            if (!ws) return 'no_ws';
            ws.send(String.fromCharCode(0x17) + String.fromCharCode(0) + '{tstr}' + String.fromCharCode(1));
            return 'ok';
        }}"""
        try:
            r = await page.evaluate(js)
            return len(topics) if r == "ok" else 0
        except: return 0

    async def _on_ws(self, ws: Any) -> None:
        url = ws.url
        self._ws_seen += 1
        self._ws_active += 1
        self.stats["ws_seen"] = self.stats.get("ws_seen", 0) + 1
        self.stats["ws_active"] = self.stats.get("ws_active", 0) + 1
        self._log.info("WS+: %s (active %d)", url[:60], self._ws_active)

        def on_recv(p):
            self.stats["frames"] = self.stats.get("frames", 0) + 1
            try: self.parser.feed(p)
            except: pass

        def on_sent(p):
            try:
                text = p if isinstance(p, str) else p.decode("utf-8", "replace")
                i = text.find(",A_")
                if i >= 0:
                    s = i + 3; e = s
                    while e < len(text) and text[e] in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=":
                        e += 1
                    if e - s > 20: self._token = text[s:e]
            except: pass

        def on_close(_):
            self._ws_active = max(0, self._ws_active - 1)
            self.stats["ws_active"] = max(0, self.stats.get("ws_active", 0) - 1)
            self._log.warning("WS-: %s (active %d)", url[:60], self._ws_active)

        ws.on("framereceived", on_recv)
        ws.on("framesent", on_sent)
        ws.on("close", on_close)
        ws.on("socketerror", lambda e: self._log.error("WS err: %s", e))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
class LiveStream:
    def __init__(self) -> None:
        self.parser = ZapParser()
        self.stats: dict[str, Any] = {"start_time": time.time()}
        self.api = LiveAPI(self.parser, self.stats)
        self.workers: list[BrowserWorker] = []
        self.parser.on_change(lambda c: asyncio.ensure_future(self.api.broadcast(c)))

    async def run(self) -> None:
        log.info("=== Starting (%d browsers, drip %d topics every %ds) ===",
                 len(PROXIES), DRIP_BATCH, DRIP_INTERVAL)
        await self.api.start()
        self.workers = [BrowserWorker(i, len(PROXIES), p, self.parser, self.stats)
                        for i, p in enumerate(PROXIES)]
        try:
            tasks = []
            for i, w in enumerate(self.workers):
                if i > 0:
                    await asyncio.sleep(BROWSER_LAUNCH_STAGGER)
                tasks.append(asyncio.create_task(w.run(), name=f"w{i}"))
            tasks.append(asyncio.create_task(self._reporter()))
            await asyncio.gather(*tasks)
        except (asyncio.CancelledError, KeyboardInterrupt):
            for w in self.workers: w._run = False
        finally:
            await self.api.stop()

    async def _reporter(self) -> None:
        while True:
            await asyncio.sleep(30)
            total = len(self.parser.state.events)
            active = sum(1 for e in self.parser.state.events.values() if e.score or e.minute)
            detail = sum(1 for eid in self.parser.state.events
                         if len(self.parser.state.event_markets.get(eid, set())) > 1)
            pct = 100 * detail // max(active, 1)
            log.info("=== COVERAGE: %d/%d (%d%%) | %d total | %s ===",
                     detail, active, pct, total, self.parser.summary())


async def main() -> None:
    await LiveStream().run()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Exited.")
