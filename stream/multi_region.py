"""
bet365 Multi-Region Full Coverage — Real-Time Deduped
=====================================================
Connects to DIFFERENT bet365 regional domains that route to different
backend server clusters. Each cluster may serve different event detail
subsets. Events are deduplicated by base numeric ID so we count only
unique matches.

Known distinct clusters:
  - bet365.es       → premeslp-pt*.bet365.es (Spain)
  - bet365.com.au   → premws-pt*.bet365.com.au (Australia)
  - bet365.dk       → premdklp-pt*.bet365.dk (Denmark)
  - bet365.com      → premws-pt*.365lpodds.com (International)
  - bet365.it       → oddsfeed*.bet365.it (Italy)

We use 3 proxies × 1 browser each, choosing domains from different
clusters for maximum diversity.

Usage:
    python -m stream.multi_region
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any

import aiohttp
from aiohttp import web
from camoufox.async_api import AsyncCamoufox

from protocol.zap_parser import ZapParser, ChangeEvent, ChangeType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("multi_region")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROXIES = [
    {"server": "http://res.proxy-seller.com:10000", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
    {"server": "http://res.proxy-seller.com:10001", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
    {"server": "http://res.proxy-seller.com:10002", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
]

# Regions ordered by cluster diversity — each uses a DIFFERENT backend.
# Fallback URLs tried in order if primary fails.
REGIONS = [
    {
        "label": "ES",
        "urls": ["https://www.bet365.es/#/IP/"],
        "locale": "_1_3",
    },
    {
        "label": "AU",
        "urls": ["https://www.bet365.com.au/#/IP/"],
        "locale": "_1_3",
    },
    {
        "label": "INT",
        "urls": [
            "https://www.bet365.com/#/IP/",
            "https://www.bet365.dk/#/IP/",
            "https://www.bet365.it/#/IP/",
            "https://www.bet365.gr/#/IP/",
        ],
        "locale": "_1_3",
    },
]

API_HOST = "0.0.0.0"
API_PORT = 8365
DRIP_BATCH = 5
DRIP_INTERVAL = 2
TOPICS_PER_MSG = 50
RECONNECT_WAIT_CRASH = 10
LAUNCH_STAGGER = 15

WS_HOOK = """mw:() => {
    if (window.__wsHooked) return 'already';
    const origSend = WebSocket.prototype.send;
    window.__wsRefs = [];
    window.__lastToken = '';
    WebSocket.prototype.send = function(data) {
        if (!window.__wsRefs.includes(this)) window.__wsRefs.push(this);
        if (typeof data === 'string') {
            let m = data.match(/[,]A_([A-Za-z0-9+\\/=]{20,})/);
            if (m) window.__lastToken = m[1];
        }
        return origSend.call(this, data);
    };
    window.__wsHooked = true;
    return 'hooked';
}"""


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
_BASE_RE = re.compile(r"^(\d+)")

# Virtual sport IDs to exclude from coverage metrics
VIRTUAL_SPORT_IDS = {str(x) for x in list(range(2019, 2035)) + [145, 146, 998, 996, 999, 144]}


def base_id(full_id: str) -> str:
    """'191750846C1A_3_0' → '191750846' (universal across regions)."""
    m = _BASE_RE.match(full_id)
    return m.group(1) if m else full_id


def _is_virtual(parser: ZapParser, eids: list[str]) -> bool:
    """Check if event group belongs to a virtual sport."""
    for eid in eids:
        ev = parser.state.events.get(eid)
        if ev and ev.sport_id in VIRTUAL_SPORT_IDS:
            return True
    return False


def dedup_coverage(parser: ZapParser) -> tuple[int, int, int, int]:
    """(covered, full_detail, unique_active, unique_total) — deduped, REAL sports only.
    covered     = active events with ≥1 market
    full_detail = active events with >1 market (multi-market depth)
    unique_active = unique active real sport events
    unique_total = all unique real sport events
    """
    groups: dict[str, list[str]] = {}
    for eid in parser.state.events:
        groups.setdefault(base_id(eid), []).append(eid)

    total = active = covered = full_detail = 0
    for bid, eids in groups.items():
        if _is_virtual(parser, eids):
            continue
        total += 1
        is_active = any(
            (parser.state.events[e].score or parser.state.events[e].minute)
            for e in eids if e in parser.state.events
        )
        if not is_active:
            continue
        active += 1
        market_counts = [len(parser.state.event_markets.get(e, set())) for e in eids]
        best = max(market_counts) if market_counts else 0
        if best >= 1:
            covered += 1
        if best > 1:
            full_detail += 1
    return covered, full_detail, active, total


def dedup_event_list(parser: ZapParser) -> list[dict]:
    """Deduplicated event list — best variant (most markets) per base_id."""
    groups: dict[str, list[str]] = {}
    for eid in parser.state.events:
        groups.setdefault(base_id(eid), []).append(eid)

    out = []
    for bid, eids in groups.items():
        best = max(eids, key=lambda e: len(parser.state.event_markets.get(e, set())))
        ev_data = parser.get_event(best)
        if ev_data:
            ev_data["base_id"] = bid
            ev_data["region_variants"] = len(eids)
            out.append(ev_data)
    return out


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------
def _ce_dict(ce: ChangeEvent) -> dict:
    d = {"change_type": ce.change_type.value, "entity_type": ce.entity_type,
         "entity_id": ce.entity_id, "topic": ce.topic}
    for k in ("old_value", "new_value"):
        v = getattr(ce, k)
        if v is None: d[k] = None
        elif isinstance(v, (dict, str, int, float, bool)): d[k] = v
        else: d[k] = {"id": getattr(v, "id", ""), "name": getattr(v, "name", "")}
    return d


# ---------------------------------------------------------------------------
# HTTP API (deduped)
# ---------------------------------------------------------------------------
class LiveAPI:
    def __init__(self, parser: ZapParser, stats: dict, workers: list) -> None:
        self.parser = parser
        self.stats = stats
        self.workers = workers
        self._ws_subs: set[web.WebSocketResponse] = set()
        self._runner: web.AppRunner | None = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/", self._index)
        app.router.add_get("/events", self._events)
        app.router.add_get("/events/{sport_id}", self._events_sport)
        app.router.add_get("/event/{event_id}", self._event)
        app.router.add_get("/sports", self._sports)
        app.router.add_get("/stats", self._stats)
        app.router.add_get("/ws", self._ws)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        await web.TCPSite(self._runner, API_HOST, API_PORT).start()
        log.info("API on http://%s:%d", API_HOST, API_PORT)

    async def stop(self):
        for ws in list(self._ws_subs): await ws.close()
        if self._runner: await self._runner.cleanup()

    async def broadcast(self, ce):
        if not self._ws_subs: return
        p = json.dumps(_ce_dict(ce))
        dead = []
        for ws in self._ws_subs:
            try: await ws.send_str(p)
            except: dead.append(ws)
        for ws in dead: self._ws_subs.discard(ws)

    async def _index(self, _r):
        cov, det, tot, act = dedup_coverage(self.parser)
        return web.json_response({
            "service": "bet365 Multi-Region (deduped)",
            "regions": [w.label for w in self.workers],
            "unique_events": tot, "unique_active": act,
            "covered": cov, "full_detail": det,
            "coverage_pct": 100 * cov // max(tot, 1),
            "full_detail_pct": 100 * det // max(tot, 1),
        })

    async def _events(self, _r):
        return web.json_response(dedup_event_list(self.parser))

    async def _events_sport(self, r):
        sid = r.match_info["sport_id"]
        return web.json_response([
            e for e in dedup_event_list(self.parser)
            if e.get("sport_id") == sid
        ])

    async def _event(self, r):
        eid = r.match_info["event_id"]
        d = self.parser.get_event(eid)
        if not d:
            # Try as base_id
            groups: dict[str, list[str]] = {}
            for e in self.parser.state.events:
                groups.setdefault(base_id(e), []).append(e)
            eids = groups.get(eid, [])
            if eids:
                best = max(eids, key=lambda e: len(self.parser.state.event_markets.get(e, set())))
                d = self.parser.get_event(best)
        return web.json_response(d) if d else web.json_response({"error": "Not found"}, status=404)

    async def _sports(self, _r):
        out = []
        for sid, s in self.parser.state.sports.items():
            cids = self.parser.state.sport_competitions.get(sid, set())
            ec = sum(len(self.parser.state.comp_events.get(c, set())) for c in cids)
            out.append({"id": sid, "name": s.name, "competitions": len(cids), "events": ec})
        return web.json_response(out)

    async def _stats(self, _r):
        cov, det, tot, act = dedup_coverage(self.parser)
        return web.json_response({
            "uptime": round(time.time() - self.stats["t0"], 1),
            "unique_total": tot, "unique_active": act,
            "covered": cov, "full_detail": det,
            "coverage_pct": 100 * cov // max(tot, 1),
            "raw_parser": self.parser.summary(),
            "workers": [{
                "label": w.label, "premws": w.premws_server or "?",
                "ws_active": w._ws_active, "subs": len(w._subs),
                "frames": w._frames,
            } for w in self.workers],
        })

    async def _ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_subs.add(ws)
        try:
            cov, det, tot, act = dedup_coverage(self.parser)
            await ws.send_json({"type": "snapshot", "total": tot, "active": act, "covered": cov, "full_detail": det})
            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE): break
        finally:
            self._ws_subs.discard(ws)
        return ws


# ---------------------------------------------------------------------------
# Region worker
# ---------------------------------------------------------------------------
class RegionWorker:
    def __init__(self, wid: int, label: str, urls: list[str], locale: str,
                 proxy: dict, parser: ZapParser, stats: dict) -> None:
        self.wid = wid
        self.label = label
        self.urls = urls
        self.locale = locale
        self.proxy = proxy
        self.parser = parser
        self.stats = stats
        self._run = True
        self._ws_seen = 0
        self._ws_active = 0
        self._token = ""
        self._subs: set[str] = set()
        self._frames = 0
        self.premws_server: str | None = None
        self._log = logging.getLogger(f"[{label}]")

    async def run(self):
        while self._run:
            try:
                await self._session()
            except asyncio.CancelledError:
                break
            except Exception:
                self._log.exception("Crash — restart %ds", RECONNECT_WAIT_CRASH)
                await asyncio.sleep(RECONNECT_WAIT_CRASH)

    async def _session(self):
        self._ws_seen = self._ws_active = 0
        self._subs.clear()

        for url in self.urls:
            self._log.info("Trying %s", url)
            try:
                await self._run_browser(url)
                return
            except Exception as exc:
                self._log.warning("%s failed: %s — trying next", url, exc)
        self._log.error("All URLs failed for this region")
        await asyncio.sleep(RECONNECT_WAIT_CRASH)

    async def _run_browser(self, url: str):
        async with AsyncCamoufox(
            headless=True, humanize=True, os="windows",
            main_world_eval=True, proxy=self.proxy, geoip=True,
        ) as browser:
            page = await browser.new_page()
            page.on("websocket", lambda ws: asyncio.ensure_future(self._on_ws(ws)))

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            self._log.info("Page loaded")
            await asyncio.sleep(5)
            try: await page.evaluate(WS_HOOK)
            except: pass
            await asyncio.sleep(10)

            token = await self._get_token(page)
            if token:
                # Subscribe main overview + sport-specific OVM modules
                # OVM150=Table Tennis, OVM92=TT alt, OVM151=eSports
                overview_topics = [
                    f"OVInPlay{self.locale}", f"CONFIG{self.locale}",
                    "OVM150", "OVM92", "OVM151",
                ]
                await self._send(page, 0x16, overview_topics, token)
                self._log.info("Overview + sport modules subscribed")
            await asyncio.sleep(5)

            self._log.info("premws: %s | WS: %d", self.premws_server or "?", self._ws_active)
            await self._drip_loop(page)

    async def _drip_loop(self, page):
        while self._run:
            if self._ws_seen > 0 and self._ws_active == 0:
                self._log.warning("WS dead — reconnect")
                self.stats["reconnects"] = self.stats.get("reconnects", 0) + 1
                return

            all_events = list(self.parser.state.events.keys())
            if not all_events:
                await asyncio.sleep(5)
                continue

            new = []
            for eid in all_events:
                t = self._detail_topic(eid)
                if t and t not in self._subs:
                    new.append(t)

            if new:
                token = await self._get_token(page)
                if token:
                    for i in range(0, len(new), DRIP_BATCH):
                        if not self._run or self._ws_active == 0: break
                        batch = new[i:i + DRIP_BATCH]
                        sent = await self._send(page, 0x16, batch, token)
                        if sent > 0:
                            self._subs.update(batch[:sent])
                        await asyncio.sleep(DRIP_INTERVAL)

            self._log.info("Subs: %d | premws: %s", len(self._subs), self.premws_server or "?")
            await asyncio.sleep(15)

    def _detail_topic(self, eid: str) -> str | None:
        ev = self.parser.state.events.get(eid)
        if not ev or not ev.topic: return None
        t = ev.topic
        if t.startswith("OV"):
            t = "6V" + t[2:]  # Replace OV with 6V, keep _0 suffix
        return t

    async def _get_token(self, page) -> str:
        try:
            t = await page.evaluate("mw:() => window.__lastToken || ''")
            if t: return t
        except: pass
        return self._token

    async def _send(self, page, msg_type, topics, token) -> int:
        sent = 0
        for i in range(0, len(topics), TOPICS_PER_MSG):
            batch = topics[i:i + TOPICS_PER_MSG]
            tstr = ",".join(batch)
            if msg_type == 0x16:
                js = f"""mw:() => {{
                    let ws = (window.__wsRefs || []).find(w => w.url && w.url.includes('prem') && w.readyState === 1);
                    if (!ws) return 'no_ws';
                    ws.send(String.fromCharCode({msg_type}) + String.fromCharCode(0) + '{tstr},A_{token}' + String.fromCharCode(1));
                    return 'ok';
                }}"""
            else:
                js = f"""mw:() => {{
                    let ws = (window.__wsRefs || []).find(w => w.url && w.url.includes('prem') && w.readyState === 1);
                    if (!ws) return 'no_ws';
                    ws.send(String.fromCharCode({msg_type}) + String.fromCharCode(0) + '{tstr}' + String.fromCharCode(1));
                    return 'ok';
                }}"""
            try:
                r = await page.evaluate(js)
                if r == "ok": sent += len(batch)
            except: pass
        return sent

    async def _on_ws(self, ws):
        url = ws.url
        self._ws_seen += 1
        self._ws_active += 1
        self._log.info("WS+: %s", url[:70])

        if "prem" in url:
            server = url.split("/zap")[0].replace("wss://", "")
            if server != self.premws_server:
                self.premws_server = server
                self._log.info("*** premws server: %s ***", server)

        def on_recv(p):
            self._frames += 1
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
            self._log.warning("WS-: %s (active %d)", url[:60], self._ws_active)

        ws.on("framereceived", on_recv)
        ws.on("framesent", on_sent)
        ws.on("close", on_close)
        ws.on("socketerror", lambda e: self._log.error("WS err: %s", e))


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
class MultiRegionStream:
    def __init__(self):
        self.parser = ZapParser()
        self.stats: dict[str, Any] = {"t0": time.time(), "frames": 0, "reconnects": 0}
        self.workers: list[RegionWorker] = []
        self.parser.on_change(lambda c: asyncio.ensure_future(self._bc(c)))
        self.api: LiveAPI | None = None

    async def _bc(self, ce):
        if self.api: await self.api.broadcast(ce)

    async def run(self):
        n = min(len(PROXIES), len(REGIONS))
        log.info("=== Multi-Region starting (%d regions: %s) ===",
                 n, ", ".join(r["label"] for r in REGIONS[:n]))

        self.workers = [
            RegionWorker(i, REGIONS[i]["label"], REGIONS[i]["urls"],
                         REGIONS[i]["locale"], PROXIES[i], self.parser, self.stats)
            for i in range(n)
        ]
        self.api = LiveAPI(self.parser, self.stats, self.workers)
        await self.api.start()

        try:
            tasks = []
            for i, w in enumerate(self.workers):
                if i > 0: await asyncio.sleep(LAUNCH_STAGGER)
                tasks.append(asyncio.create_task(w.run(), name=w.label))
                log.info("[%s] launched", w.label)
            tasks.append(asyncio.create_task(self._reporter()))
            await asyncio.gather(*tasks)
        except (asyncio.CancelledError, KeyboardInterrupt):
            for w in self.workers: w._run = False
        finally:
            if self.api: await self.api.stop()

    async def _reporter(self):
        while True:
            await asyncio.sleep(30)
            cov, det, act, tot = dedup_coverage(self.parser)
            raw = self.parser.summary()
            servers = {w.premws_server for w in self.workers if w.premws_server}
            per_w = " | ".join(
                f"[{w.label}] {w.premws_server or '?'} subs={len(w._subs)}"
                for w in self.workers
            )
            log.info(
                "=== REAL SPORTS: %d/%d active covered (%d%%) | detail: %d | "
                "%d servers | raw %d | %s ===",
                cov, act, 100 * cov // max(act, 1),
                det, len(servers), raw["events"], per_w,
            )


async def main():
    await MultiRegionStream().run()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Exited.")
