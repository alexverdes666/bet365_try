"""
bet365 Full Coverage — Live (WebSocket) + Pre-Match (REST)
==========================================================
Unified system that captures ALL bet365 data by combining:

1. LIVE DATA (WebSocket): 3 browsers on different regions (ES, AU, DK)
   capturing real-time in-play data via WS, each using a different proxy
   and bet365 domain.

2. PRE-MATCH DATA (REST interception): 1 additional browser that navigates
   through all sport splash pages and intercepts REST API responses in
   ZAP format, feeding them directly to the shared ZapParser.

3. SINGLE SHARED ZapParser — all data from all browsers merges into one
   state tree.

4. SINGLE API on port 8365 with deduplication.

The key innovation is that bet365 REST responses use the SAME ZAP entity
format (F|CL;ID=1;NA=Football;EV;...) as WebSocket frames, so our
existing ZapParser.feed() method can parse them directly.

Usage:
    python -m stream.full_coverage
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
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
log = logging.getLogger("full_coverage")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROXIES = [
    {"server": "http://res.proxy-seller.com:10000", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
    {"server": "http://res.proxy-seller.com:10001", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
    {"server": "http://res.proxy-seller.com:10002", "username": "4f6d12485ca053d6", "password": "OzkAVbyM"},
]

# WS regions — each targets a different backend cluster for maximum diversity.
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
        "label": "DK",
        "urls": [
            "https://www.bet365.dk/#/IP/",
            "https://www.bet365.com/#/IP/",  # fallback if DK blocked
        ],
        "locale": "_1_3",
    },
]

# Sport pages for pre-match REST crawling
SPORTS = [
    ("1", "Football"), ("13", "Tennis"), ("18", "Basketball"),
    ("17", "Ice Hockey"), ("78", "Handball"), ("91", "Volleyball"),
    ("12", "American Football"), ("16", "Baseball"), ("3", "Cricket"),
    ("14", "Snooker"), ("92", "Table Tennis"), ("15", "Darts"),
    ("9", "Boxing/UFC"), ("8", "Rugby Union"), ("19", "Rugby League"),
    ("6", "Golf"), ("36", "Australian Rules"), ("83", "Futsal"),
    ("90", "Floorball"), ("151", "eSports"),
]

API_HOST = "0.0.0.0"
API_PORT = 8365
DRIP_BATCH = 5
DRIP_INTERVAL = 2
TOPICS_PER_MSG = 50
RECONNECT_WAIT_CRASH = 10
LAUNCH_STAGGER = 15

# REST crawler timing
REST_PAGE_WAIT = 10         # seconds to wait after navigation for REST responses
REST_BETWEEN_PAGES = 35     # seconds between page navigations (30-40 range)
REST_FULL_CYCLE_PAUSE = 60  # seconds to pause after a full crawl cycle

# REST API URL patterns to intercept
REST_INTERCEPT_PATTERNS = [
    "splashcontentapi",
    "couponapi",
    "matchbettingcontentapi",
    "defaultapi/sports-configuration",
]

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


def base_id(full_id: str) -> str:
    """'191750846C1A_3_0' -> '191750846' (universal across regions)."""
    m = _BASE_RE.match(full_id)
    return m.group(1) if m else full_id


def dedup_coverage(parser: ZapParser) -> dict[str, int]:
    """Compute deduplicated coverage statistics.

    Returns dict with keys:
        covered, full_detail, unique_total, unique_active,
        ws_events, rest_events
    """
    groups: dict[str, list[str]] = {}
    for eid in parser.state.events:
        groups.setdefault(base_id(eid), []).append(eid)

    total = len(groups)
    active = covered = full_detail = 0
    for bid, eids in groups.items():
        is_active = any(
            (parser.state.events[e].score or parser.state.events[e].minute)
            for e in eids if e in parser.state.events
        )
        if is_active:
            active += 1
        market_counts = [len(parser.state.event_markets.get(e, set())) for e in eids]
        best = max(market_counts) if market_counts else 0
        if best >= 1:
            covered += 1
        if best > 1:
            full_detail += 1

    return {
        "covered": covered,
        "full_detail": full_detail,
        "unique_total": total,
        "unique_active": active,
    }


def dedup_event_list(parser: ZapParser) -> list[dict]:
    """Deduplicated event list -- best variant (most markets) per base_id."""
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
        if v is None:
            d[k] = None
        elif isinstance(v, (dict, str, int, float, bool)):
            d[k] = v
        else:
            d[k] = {"id": getattr(v, "id", ""), "name": getattr(v, "name", "")}
    return d


# ---------------------------------------------------------------------------
# HTTP API (deduped, unified live + prematch)
# ---------------------------------------------------------------------------
class LiveAPI:
    def __init__(self, parser: ZapParser, stats: dict,
                 ws_workers: list, rest_worker) -> None:
        self.parser = parser
        self.stats = stats
        self.ws_workers = ws_workers
        self.rest_worker = rest_worker
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
        for ws in list(self._ws_subs):
            await ws.close()
        if self._runner:
            await self._runner.cleanup()

    async def broadcast(self, ce):
        if not self._ws_subs:
            return
        p = json.dumps(_ce_dict(ce))
        dead = []
        for ws in self._ws_subs:
            try:
                await ws.send_str(p)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._ws_subs.discard(ws)

    async def _index(self, _r):
        cov = dedup_coverage(self.parser)
        rest_stats = self.rest_worker.get_stats() if self.rest_worker else {}
        return web.json_response({
            "service": "bet365 Full Coverage (Live WS + Pre-Match REST)",
            "ws_regions": [w.label for w in self.ws_workers],
            "rest_crawler": "active" if self.rest_worker and self.rest_worker._run else "inactive",
            "unique_events": cov["unique_total"],
            "unique_active": cov["unique_active"],
            "covered": cov["covered"],
            "full_detail": cov["full_detail"],
            "coverage_pct": 100 * cov["covered"] // max(cov["unique_total"], 1),
            "full_detail_pct": 100 * cov["full_detail"] // max(cov["unique_total"], 1),
            "rest_responses_fed": rest_stats.get("responses_fed", 0),
            "rest_bytes_fed": rest_stats.get("bytes_fed", 0),
            "rest_sports_crawled": rest_stats.get("sports_crawled", 0),
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
        cov = dedup_coverage(self.parser)
        rest_stats = self.rest_worker.get_stats() if self.rest_worker else {}

        # Per-sport breakdown
        sport_breakdown = {}
        for ev in self.parser.state.events.values():
            sid = ev.sport_id
            if sid not in sport_breakdown:
                sport = self.parser.state.sports.get(sid)
                sport_breakdown[sid] = {
                    "name": sport.name if sport else sid,
                    "events": 0,
                    "with_markets": 0,
                }
            sport_breakdown[sid]["events"] += 1
            if self.parser.state.event_markets.get(ev.id):
                sport_breakdown[sid]["with_markets"] += 1

        return web.json_response({
            "uptime": round(time.time() - self.stats["t0"], 1),
            "unique_total": cov["unique_total"],
            "unique_active": cov["unique_active"],
            "covered": cov["covered"],
            "full_detail": cov["full_detail"],
            "coverage_pct": 100 * cov["covered"] // max(cov["unique_total"], 1),
            "raw_parser": self.parser.summary(),
            "ws_workers": [{
                "label": w.label, "premws": w.premws_server or "?",
                "ws_active": w._ws_active, "subs": len(w._subs),
                "frames": w._frames,
            } for w in self.ws_workers],
            "rest_crawler": rest_stats,
            "sport_breakdown": sport_breakdown,
        })

    async def _ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_subs.add(ws)
        try:
            cov = dedup_coverage(self.parser)
            await ws.send_json({
                "type": "snapshot",
                "total": cov["unique_total"],
                "active": cov["unique_active"],
                "covered": cov["covered"],
                "full_detail": cov["full_detail"],
            })
            async for msg in ws:
                if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                    break
        finally:
            self._ws_subs.discard(ws)
        return ws


# ---------------------------------------------------------------------------
# Region worker (Live WebSocket — from multi_region.py)
# ---------------------------------------------------------------------------
class RegionWorker:
    """Manages a single browser instance for live WS data from a specific region."""

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
        self._log = logging.getLogger(f"WS[{label}]")

    async def run(self):
        while self._run:
            try:
                await self._session()
            except asyncio.CancelledError:
                break
            except Exception:
                self._log.exception("Crash -- restart %ds", RECONNECT_WAIT_CRASH)
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
                self._log.warning("%s failed: %s -- trying next", url, exc)
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
            try:
                await page.evaluate(WS_HOOK)
            except Exception:
                pass
            await asyncio.sleep(10)

            token = await self._get_token(page)
            if token:
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
                self._log.warning("WS dead -- reconnect")
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
                        if not self._run or self._ws_active == 0:
                            break
                        batch = new[i:i + DRIP_BATCH]
                        sent = await self._send(page, 0x16, batch, token)
                        if sent > 0:
                            self._subs.update(batch[:sent])
                        await asyncio.sleep(DRIP_INTERVAL)

            self._log.info("Subs: %d | premws: %s", len(self._subs), self.premws_server or "?")
            await asyncio.sleep(15)

    def _detail_topic(self, eid: str) -> str | None:
        ev = self.parser.state.events.get(eid)
        if not ev or not ev.topic:
            return None
        t = ev.topic
        if t.startswith("OV"):
            t = t[2:]
        if t.endswith("_0"):
            t = t[:-2]
        return t

    async def _get_token(self, page) -> str:
        try:
            t = await page.evaluate("mw:() => window.__lastToken || ''")
            if t:
                return t
        except Exception:
            pass
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
                if r == "ok":
                    sent += len(batch)
            except Exception:
                pass
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
            self.stats["ws_frames"] = self.stats.get("ws_frames", 0) + 1
            try:
                self.parser.feed(p)
            except Exception:
                pass

        def on_sent(p):
            try:
                text = p if isinstance(p, str) else p.decode("utf-8", "replace")
                i = text.find(",A_")
                if i >= 0:
                    s = i + 3
                    e = s
                    while e < len(text) and text[e] in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=":
                        e += 1
                    if e - s > 20:
                        self._token = text[s:e]
            except Exception:
                pass

        def on_close(_):
            self._ws_active = max(0, self._ws_active - 1)
            self._log.warning("WS-: %s (active %d)", url[:60], self._ws_active)

        ws.on("framereceived", on_recv)
        ws.on("framesent", on_sent)
        ws.on("close", on_close)
        ws.on("socketerror", lambda e: self._log.error("WS err: %s", e))


# ---------------------------------------------------------------------------
# Pre-Match REST Crawler Worker
# ---------------------------------------------------------------------------
class RestCrawlerWorker:
    """
    Manages a single browser that navigates through bet365 sport splash pages
    and intercepts REST API responses containing ZAP-format data.

    The intercepted response bodies are fed directly to the shared ZapParser
    via parser.feed(), since they use the same F|CL;... entity format as
    WebSocket frames.
    """

    def __init__(self, proxy: dict, parser: ZapParser, stats: dict) -> None:
        self.proxy = proxy
        self.parser = parser
        self.stats = stats
        self._run = True
        self._responses_fed = 0
        self._bytes_fed = 0
        self._sports_crawled = 0
        self._cycles_completed = 0
        self._current_sport = ""
        self._errors = 0
        self._log = logging.getLogger("REST[crawler]")

    def get_stats(self) -> dict:
        return {
            "active": self._run,
            "responses_fed": self._responses_fed,
            "bytes_fed": self._bytes_fed,
            "sports_crawled": self._sports_crawled,
            "cycles_completed": self._cycles_completed,
            "current_sport": self._current_sport,
            "errors": self._errors,
        }

    async def run(self):
        while self._run:
            try:
                await self._session()
            except asyncio.CancelledError:
                break
            except Exception:
                self._log.exception("REST crawler crash -- restart %ds", RECONNECT_WAIT_CRASH)
                await asyncio.sleep(RECONNECT_WAIT_CRASH)

    async def _session(self):
        self._log.info("Starting REST crawler browser")
        async with AsyncCamoufox(
            headless=True, humanize=True, os="windows",
            main_world_eval=True, proxy=self.proxy, geoip=True,
        ) as browser:
            page = await browser.new_page()

            # Set up response interception for ZAP-format REST responses
            page.on("response", lambda r: asyncio.ensure_future(self._on_response(r)))

            # Initial navigation to bet365 home to establish session
            initial_url = "https://www.bet365.es/#/AS/B1/"
            self._log.info("Initial nav: %s", initial_url)
            try:
                await page.goto(initial_url, wait_until="domcontentloaded", timeout=60000)
            except Exception as exc:
                self._log.warning("Initial nav timeout (may still work): %s", exc)

            await asyncio.sleep(REST_PAGE_WAIT)
            self._log.info("REST crawler browser ready, starting sport crawl loop")

            # Continuous crawl loop through all sports
            await self._crawl_loop(page)

    async def _crawl_loop(self, page):
        """Continuously cycle through all sport splash pages."""
        while self._run:
            for sport_id, sport_name in SPORTS:
                if not self._run:
                    break

                self._current_sport = f"{sport_name} ({sport_id})"
                url = f"https://www.bet365.es/#/AS/B{sport_id}/"
                self._log.info("Navigating to %s: %s", sport_name, url)

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                except Exception as exc:
                    # Timeout is acceptable -- REST responses may still have been captured
                    self._log.debug("Nav timeout for %s (responses may still be captured): %s",
                                    sport_name, str(exc)[:80])

                self._sports_crawled += 1

                # Wait for REST responses to arrive and be intercepted
                await asyncio.sleep(REST_PAGE_WAIT)

                # Add jitter between pages (30-40 seconds total including page wait)
                wait = REST_BETWEEN_PAGES - REST_PAGE_WAIT + random.uniform(-2, 5)
                if wait > 0:
                    await asyncio.sleep(wait)

            # Completed a full cycle
            self._cycles_completed += 1
            self._log.info(
                "Completed crawl cycle #%d | responses fed: %d | bytes: %d",
                self._cycles_completed, self._responses_fed, self._bytes_fed,
            )

            # Pause before starting next cycle
            self._current_sport = "(paused between cycles)"
            await asyncio.sleep(REST_FULL_CYCLE_PAUSE)

    async def _on_response(self, response):
        """Intercept REST API responses and feed ZAP-format data to parser."""
        try:
            url = response.url

            # Check if this is a relevant API endpoint
            if not any(pattern in url for pattern in REST_INTERCEPT_PATTERNS):
                return

            # Only process successful responses
            if response.status != 200:
                return

            try:
                body = await response.text()
            except Exception:
                return

            if not body or len(body) < 50:
                return

            # Check if the response looks like ZAP format
            # ZAP responses typically contain pipe-separated entities starting
            # with F| or containing entity markers like CL;, CT;, EV;, MA;, PA;
            is_zap = False
            # Quick heuristic checks for ZAP format
            if "|" in body[:200]:
                # Check for entity type markers
                for marker in ("CL;", "CT;", "EV;", "MA;", "PA;"):
                    if marker in body[:2000]:
                        is_zap = True
                        break

            if not is_zap:
                # Some responses may be JSON wrapping ZAP data; skip those
                # that are clearly not ZAP
                if body.lstrip().startswith("{") or body.lstrip().startswith("["):
                    return
                # Could still be ZAP without standard markers in first 2000 chars
                # Try feeding if it has pipe separators (ZAP characteristic)
                if body.count("|") < 3:
                    return

            # Feed the ZAP-format response directly to the parser.
            # The parser's feed() method handles the format: it splits by
            # frame separator (0x08), identifies message types, and processes
            # entities. REST responses that start with F| will be parsed as
            # INITIAL_TOPIC_LOAD content via _parse_entities_from_payload.
            #
            # For REST responses, we need to wrap the raw content so the parser
            # recognizes it. The parser expects either:
            # - A frame starting with 0x14 (INITIAL_TOPIC_LOAD) byte
            # - Or raw F|... content which _parse_sub_frame handles
            #
            # The REST ZAP data typically starts with F| directly, which the
            # parser's _parse_sub_frame will treat as Unknown (since 'F' = 0x46
            # is not a recognized message type byte). We need to synthesize an
            # INITIAL_TOPIC_LOAD frame.
            try:
                # Wrap the body as an INITIAL_TOPIC_LOAD frame:
                # 0x14 + topic + SOH + payload
                # We use a synthetic topic for REST data
                endpoint = url.split("?")[0].split("/")[-1] if "/" in url else "rest"
                synthetic_frame = chr(0x14) + f"REST_{endpoint}" + chr(0x01) + body
                self.parser.feed(synthetic_frame)

                body_len = len(body)
                self._responses_fed += 1
                self._bytes_fed += body_len
                self.stats["rest_responses"] = self.stats.get("rest_responses", 0) + 1
                self.stats["rest_bytes"] = self.stats.get("rest_bytes", 0) + body_len

                self._log.info(
                    "Fed REST response to parser: %s (%d bytes) | total fed: %d",
                    url.split("?")[0].split("/")[-1], body_len, self._responses_fed,
                )
            except Exception as exc:
                self._errors += 1
                self._log.debug("Failed to feed REST response: %s", exc)

        except Exception as exc:
            self._errors += 1
            self._log.debug("Error in response handler: %s", exc)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
class FullCoverageStream:
    """
    Unified orchestrator that runs:
    - 3 RegionWorker instances for live WS data
    - 1 RestCrawlerWorker for pre-match REST data
    - 1 LiveAPI for serving the combined state
    - 1 reporter task for periodic logging
    """

    def __init__(self):
        self.parser = ZapParser()
        self.stats: dict[str, Any] = {
            "t0": time.time(),
            "ws_frames": 0,
            "rest_responses": 0,
            "rest_bytes": 0,
            "reconnects": 0,
        }
        self.ws_workers: list[RegionWorker] = []
        self.rest_worker: RestCrawlerWorker | None = None
        self.parser.on_change(lambda c: asyncio.ensure_future(self._bc(c)))
        self.api: LiveAPI | None = None

    async def _bc(self, ce):
        if self.api:
            await self.api.broadcast(ce)

    async def run(self):
        n_ws = min(len(PROXIES), len(REGIONS))
        log.info("=== Full Coverage starting ===")
        log.info("  WS regions: %d (%s)", n_ws, ", ".join(r["label"] for r in REGIONS[:n_ws]))
        log.info("  REST crawler: 1 browser, %d sports to crawl", len(SPORTS))
        log.info("  API port: %d", API_PORT)

        # Create WS workers (3 regions with proxies 0,1,2)
        self.ws_workers = [
            RegionWorker(i, REGIONS[i]["label"], REGIONS[i]["urls"],
                         REGIONS[i]["locale"], PROXIES[i], self.parser, self.stats)
            for i in range(n_ws)
        ]

        # Create REST crawler worker (reuses proxy 0)
        self.rest_worker = RestCrawlerWorker(PROXIES[0], self.parser, self.stats)

        # Start API
        self.api = LiveAPI(self.parser, self.stats, self.ws_workers, self.rest_worker)
        await self.api.start()

        try:
            tasks = []

            # Launch WS workers with stagger
            for i, w in enumerate(self.ws_workers):
                if i > 0:
                    await asyncio.sleep(LAUNCH_STAGGER)
                tasks.append(asyncio.create_task(w.run(), name=f"WS_{w.label}"))
                log.info("[WS/%s] launched", w.label)

            # Launch REST crawler after a delay to let WS establish first
            await asyncio.sleep(LAUNCH_STAGGER)
            tasks.append(asyncio.create_task(self.rest_worker.run(), name="REST_crawler"))
            log.info("[REST] crawler launched")

            # Launch reporter
            tasks.append(asyncio.create_task(self._reporter(), name="reporter"))

            await asyncio.gather(*tasks)
        except (asyncio.CancelledError, KeyboardInterrupt):
            for w in self.ws_workers:
                w._run = False
            if self.rest_worker:
                self.rest_worker._run = False
        finally:
            if self.api:
                await self.api.stop()

    async def _reporter(self):
        """Periodic coverage reporter."""
        while True:
            await asyncio.sleep(30)
            cov = dedup_coverage(self.parser)
            raw = self.parser.summary()
            servers = {w.premws_server for w in self.ws_workers if w.premws_server}
            rest_stats = self.rest_worker.get_stats() if self.rest_worker else {}

            # Per-WS-worker summary
            per_ws = " | ".join(
                f"[{w.label}] {w.premws_server or '?'} subs={len(w._subs)}"
                for w in self.ws_workers
            )

            # Per-sport breakdown
            sport_counts: dict[str, int] = {}
            for ev in self.parser.state.events.values():
                sid = ev.sport_id
                sport = self.parser.state.sports.get(sid)
                name = sport.name if sport else sid
                sport_counts[name] = sport_counts.get(name, 0) + 1

            # Sort by count descending, take top 8
            top_sports = sorted(sport_counts.items(), key=lambda x: -x[1])[:8]
            sport_str = ", ".join(f"{n}:{c}" for n, c in top_sports)

            log.info(
                "=== COVERAGE: %d/%d events (%d%%) | active: %d | full detail: %d | "
                "%d WS servers | raw: %d events, %d markets, %d sels | WS frames: %d | "
                "REST fed: %d (%dKB) | %s ===",
                cov["covered"], cov["unique_total"],
                100 * cov["covered"] // max(cov["unique_total"], 1),
                cov["unique_active"], cov["full_detail"],
                len(servers), raw["events"], raw["markets"], raw["selections"],
                self.stats.get("ws_frames", 0),
                rest_stats.get("responses_fed", 0),
                rest_stats.get("bytes_fed", 0) // 1024,
                per_ws,
            )
            if sport_str:
                log.info("  Sports: %s", sport_str)
            if rest_stats:
                log.info(
                    "  REST: cycle #%d | current: %s | errors: %d",
                    rest_stats.get("cycles_completed", 0),
                    rest_stats.get("current_sport", "?"),
                    rest_stats.get("errors", 0),
                )


async def main():
    await FullCoverageStream().run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Exited.")
