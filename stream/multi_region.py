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
import urllib.parse
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
# locale must match the regional domain (captured from native page traffic).
REGIONS = [
    {
        "label": "ES",
        "urls": ["https://www.bet365.es/#/IP/"],
        "locale": "_3_0",  # Spanish locale (native page uses _3_0)
    },
    {
        "label": "AU",
        "urls": ["https://www.bet365.com.au/#/IP/"],
        "locale": "_1_3",  # English locale
    },
    {
        "label": "INT",
        "urls": [
            "https://www.bet365.it/#/IP/",
            "https://www.bet365.gr/#/IP/",
            "https://www.bet365.dk/#/IP/",
            "https://www.bet365.com/#/IP/",
        ],
        "locale": "_1_3",  # English locale
    },
]

API_HOST = "0.0.0.0"
API_PORT = 8365
DRIP_BATCH = 50
DRIP_INTERVAL = 0.2
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


def _normalize_name(name: str) -> str:
    """Normalize event name for cross-region matching.
    Sorts participant names so 'A v B' and 'B v A' produce the same key.
    """
    if not name:
        return ""
    # Split by common separators: " v ", " vs ", " @ "
    for sep in (" v ", " vs ", " @ "):
        if sep in name:
            parts = sorted(p.strip().lower() for p in name.split(sep, 1))
            return "|".join(parts)
    return name.strip().lower()


def _group_events(parser: ZapParser) -> dict[str, list[str]]:
    """Group events by base_id, then merge cross-region duplicates by name.

    Different regions use different event ID numbering (e.g. 191xxxxx for ES,
    151316xxxxx for AU), so the same real-world match appears with different
    base_ids. We merge these by matching (sport_id, normalized_name).
    """
    # Phase 1: group by base_id
    groups: dict[str, list[str]] = {}
    for eid in parser.state.events:
        groups.setdefault(base_id(eid), []).append(eid)

    # Phase 2: merge groups that represent the same real match
    # Key: (sport_id, normalized_name) -> canonical group key
    name_to_group: dict[tuple[str, str], str] = {}
    merge_map: dict[str, str] = {}  # old_group_key -> canonical_group_key

    for bid, eids in groups.items():
        # Find best representative for name/sport
        for eid in eids:
            ev = parser.state.events.get(eid)
            if not ev or not ev.name:
                continue
            sid = ev.sport_id or ""
            nname = _normalize_name(ev.name)
            if not nname or not sid:
                continue
            key = (sid, nname)
            if key in name_to_group:
                existing_bid = name_to_group[key]
                if existing_bid != bid:
                    merge_map[bid] = existing_bid
            else:
                name_to_group[key] = bid
            break  # Only need one representative per group

    # Apply merges
    if merge_map:
        merged: dict[str, list[str]] = {}
        for bid, eids in groups.items():
            target = merge_map.get(bid, bid)
            # Follow chain
            while target in merge_map and merge_map[target] != target:
                target = merge_map[target]
            merged.setdefault(target, []).extend(eids)
        return merged

    return groups


def dedup_coverage(parser: ZapParser) -> tuple[int, int, int, int]:
    """(covered, full_detail, unique_active, unique_total) — deduped, REAL sports only."""
    groups = _group_events(parser)

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
    """Deduplicated event list — best variant (most markets) per group."""
    groups = _group_events(parser)

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
        app.router.add_get("/competitions", self._competitions)
        app.router.add_get("/stats", self._stats)
        app.router.add_get("/markets/{event_id}", self._markets)
        app.router.add_get("/selections/{market_id}", self._selections)
        app.router.add_get("/active", self._active_events)
        app.router.add_get("/prematch", self._prematch_events)
        app.router.add_get("/raw", self._raw_state)
        app.router.add_get("/debug/orphans", self._debug_orphans)
        app.router.add_get("/debug/coverage", self._debug_coverage)
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
            "endpoints": {
                "/": "This index",
                "/events": "All events (deduped)",
                "/events/{sport_id}": "Events for a sport",
                "/event/{event_id}": "Single event with all markets/selections",
                "/markets/{event_id}": "Markets for an event",
                "/selections/{market_id}": "Selections for a market",
                "/sports": "All sports with event counts",
                "/competitions": "All competitions",
                "/active": "Active/live events only",
                "/prematch": "Prematch events only",
                "/stats": "System stats and worker info",
                "/debug/coverage": "Coverage breakdown by sport",
                "/debug/orphans": "Orphaned markets debug",
                "/ws": "WebSocket stream for real-time changes",
            },
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

    async def _competitions(self, _r):
        out = []
        for cid, c in self.parser.state.competitions.items():
            eids = self.parser.state.comp_events.get(cid, set())
            out.append({
                "id": cid, "name": c.name, "short_name": c.short_name,
                "sport_id": c.sport_id, "events": len(eids),
                "raw": c.raw,
            })
        return web.json_response(out)

    async def _raw_state(self, _r):
        """Full raw state dump — for debugging."""
        return web.json_response({
            "sports": {k: {"id": v.id, "name": v.name, "raw": v.raw}
                       for k, v in self.parser.state.sports.items()},
            "competitions_count": len(self.parser.state.competitions),
            "events_count": len(self.parser.state.events),
            "markets_count": len(self.parser.state.markets),
            "selections_count": len(self.parser.state.selections),
            "topic_index_count": len(self.parser.state.topic_index),
        })

    async def _debug_orphans(self, _r):
        """Show markets not linked to any event."""
        # Find all markets linked to events
        linked = set()
        for eid, mids in self.parser.state.event_markets.items():
            linked.update(mids)
        # Find orphans
        orphans = {}
        for mid, mkt in self.parser.state.markets.items():
            if mid not in linked:
                topic = mkt.topic or mid
                # Extract prefix pattern
                prefix = topic[:20] if len(topic) > 20 else topic
                if prefix not in orphans:
                    orphans[prefix] = {"count": 0, "sample_id": mid,
                                        "sample_name": mkt.name, "event_id": mkt.event_id,
                                        "sample_topic": mkt.topic, "sample_raw": dict(list(mkt.raw.items())[:8])}
                orphans[prefix]["count"] += 1
        return web.json_response({
            "total_markets": len(self.parser.state.markets),
            "linked_markets": len(linked),
            "orphaned_markets": len(self.parser.state.markets) - len(linked),
            "orphan_groups": dict(sorted(orphans.items(), key=lambda x: -x[1]["count"])[:30]),
        })

    async def _markets(self, r):
        """Get all markets for a specific event."""
        eid = r.match_info["event_id"]
        # Try direct lookup first, then base_id
        ev = self.parser.state.events.get(eid)
        if not ev:
            groups: dict[str, list[str]] = {}
            for e in self.parser.state.events:
                groups.setdefault(base_id(e), []).append(e)
            eids = groups.get(eid, [])
            if eids:
                best = max(eids, key=lambda e: len(self.parser.state.event_markets.get(e, set())))
                eid = best
                ev = self.parser.state.events.get(eid)
        if not ev:
            return web.json_response({"error": "Event not found"}, status=404)

        markets = []
        for mid in sorted(self.parser.state.event_markets.get(eid, set())):
            mkt = self.parser.state.markets.get(mid)
            if not mkt:
                continue
            sels = []
            for sid in sorted(self.parser.state.market_selections.get(mid, set())):
                sel = self.parser.state.selections.get(sid)
                if sel:
                    sels.append({
                        "id": sel.id, "name": sel.name, "odds": sel.odds,
                        "order": sel.order, "handicap": sel.handicap,
                        "handicap_display": sel.handicap_display,
                        "suspended": sel.suspended, "raw": sel.raw,
                    })
            markets.append({
                "id": mkt.id, "name": mkt.name, "market_type": mkt.market_type,
                "columns": mkt.columns, "suspended": mkt.suspended,
                "selections": sels, "raw": mkt.raw,
            })
        return web.json_response({
            "event_id": eid, "event_name": ev.name,
            "sport_id": ev.sport_id, "score": ev.score,
            "market_count": len(markets), "markets": markets,
        })

    async def _selections(self, r):
        """Get all selections for a specific market."""
        mid = r.match_info["market_id"]
        mkt = self.parser.state.markets.get(mid)
        if not mkt:
            # Search by partial match
            for k, m in self.parser.state.markets.items():
                if mid in k:
                    mkt = m
                    mid = k
                    break
        if not mkt:
            return web.json_response({"error": "Market not found"}, status=404)
        sels = []
        for sid in sorted(self.parser.state.market_selections.get(mid, set())):
            sel = self.parser.state.selections.get(sid)
            if sel:
                sels.append({
                    "id": sel.id, "name": sel.name, "odds": sel.odds,
                    "order": sel.order, "handicap": sel.handicap,
                    "handicap_display": sel.handicap_display,
                    "suspended": sel.suspended, "raw": sel.raw,
                })
        return web.json_response({
            "market_id": mid, "market_name": mkt.name,
            "market_type": mkt.market_type, "suspended": mkt.suspended,
            "selection_count": len(sels), "selections": sels,
        })

    async def _active_events(self, _r):
        """Return only active/live events (deduped)."""
        events = dedup_event_list(self.parser)
        active = [e for e in events if e.get("score") or e.get("minute")]
        return web.json_response(active)

    async def _prematch_events(self, _r):
        """Return only prematch events (deduped)."""
        events = dedup_event_list(self.parser)
        prematch = [e for e in events if not e.get("score") and not e.get("minute")]
        return web.json_response(prematch)

    async def _debug_coverage(self, _r):
        """Detailed coverage breakdown by sport."""
        groups = _group_events(self.parser)

        by_sport: dict[str, dict] = {}
        for bid, eids in groups.items():
            ev = self.parser.state.events.get(eids[0])
            if not ev:
                continue
            sid = ev.sport_id or "unknown"
            sport = self.parser.state.sports.get(sid)
            sname = sport.name if sport else sid
            if sid not in by_sport:
                by_sport[sid] = {"name": sname, "total": 0, "active": 0,
                                 "with_markets": 0, "with_detail": 0,
                                 "total_markets": 0, "total_selections": 0,
                                 "odds_filled": 0, "odds_empty": 0}
            s = by_sport[sid]
            s["total"] += 1
            is_active = any(
                (self.parser.state.events[e].score or self.parser.state.events[e].minute)
                for e in eids if e in self.parser.state.events
            )
            if is_active:
                s["active"] += 1
            best_eid = max(eids, key=lambda e: len(self.parser.state.event_markets.get(e, set())))
            mids = self.parser.state.event_markets.get(best_eid, set())
            mc = len(mids)
            if mc >= 1:
                s["with_markets"] += 1
            if mc > 1:
                s["with_detail"] += 1
            s["total_markets"] += mc
            for mid in mids:
                sids = self.parser.state.market_selections.get(mid, set())
                s["total_selections"] += len(sids)
                for sid2 in sids:
                    sel = self.parser.state.selections.get(sid2)
                    if sel and sel.odds:
                        s["odds_filled"] += 1
                    else:
                        s["odds_empty"] += 1

        return web.json_response(dict(sorted(by_sport.items(), key=lambda x: -x[1]["active"])))

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
                "my_events": len(w._my_event_ids),
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
        # Track event IDs that arrived through THIS worker's WS
        self._my_event_ids: set[str] = set()

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
                # Subscribe all native topics (captured from real bet365 page traffic)
                # Phase 1: Core overview + config topics
                overview_topics = [
                    f"OVInPlay{self.locale}",
                    f"CONFIG{self.locale}",
                    f"OV_POPULAR{self.locale}",
                    "PVG_CONFIG_1",
                    "PVG_IPPG",
                    "PV_CHARTS",
                    "P-ENDP",
                    "P_CONFIG",
                ]
                await self._send(page, 0x16, overview_topics, token)
                self._log.info("Phase 1: core overview + config subscribed")
                await asyncio.sleep(3)

                # Phase 2: Sport OVM modules — IDs from MR (Market Renderer) field,
                # NOT sport IDs.  e.g. soccer = module 21, not sport 1.
                sport_modules = [
                    # Soccer (sport 1): modules 21, 163, 164, 166
                    "OVM21", "OVM163", "OVM164", "OVM166",
                    # Tennis (13): 156, 157
                    "OVM156", "OVM157",
                    # Basketball (18): 175, 176
                    "OVM175", "OVM176",
                    # Ice Hockey (17): 178, 179
                    "OVM178", "OVM179",
                    # Table Tennis (92): 150, 151, 152
                    "OVM150", "OVM151", "OVM152",
                    # Handball (78): 160, 161
                    "OVM160", "OVM161",
                    # Volleyball (91): 153, 154, 155
                    "OVM153", "OVM154", "OVM155",
                    # eSports (151): 125, 126, 127, 31
                    "OVM125", "OVM126", "OVM127", "OVM31",
                    # Baseball (16): 158, 159
                    "OVM158", "OVM159",
                    # American Football (12): 172, 173, 174
                    "OVM172", "OVM173", "OVM174",
                    # Floorball (90): 88, 89, 90
                    "OVM88", "OVM89", "OVM90",
                    # Badminton (94): 195, 196, 197
                    "OVM195", "OVM196", "OVM197",
                    # Cricket (3): 131, 132, 133
                    "OVM131", "OVM132", "OVM133",
                    # Squash (107): 198, 199
                    "OVM198", "OVM199",
                ]
                await self._send(page, 0x16, sport_modules, token)
                self._log.info("Phase 2: sport modules subscribed")
            await asyncio.sleep(5)

            self._log.info("premws: %s | WS: %d", self.premws_server or "?", self._ws_active)

            # If WS died during setup, raise to try next URL
            if self._ws_seen > 0 and self._ws_active == 0:
                raise RuntimeError("WS connections died during setup")

            await self._drip_loop(page)

    async def _drip_loop(self, page):
        resub_counter = 0
        while self._run:
            if self._ws_seen > 0 and self._ws_active == 0:
                self._log.warning("WS dead — reconnect")
                self.stats["reconnects"] = self.stats.get("reconnects", 0) + 1
                return

            # Only subscribe to detail topics for events owned by THIS worker
            my_events = list(self._my_event_ids)
            if not my_events:
                await asyncio.sleep(5)
                continue

            # Prioritize active/live events
            # An event is "active" if it has score/minute OR came from OVInPlay
            # (OVInPlay only contains in-play events, even if score isn't set yet)
            active_eids = []
            prematch_eids = []
            for eid in my_events:
                ev = self.parser.state.events.get(eid)
                if not ev:
                    continue
                is_active = (
                    ev.score or ev.minute or
                    (ev.topic and "OV" in ev.topic and "InPlay" not in ev.topic) or
                    ev.raw.get("ES", "") == "1"  # ES=1 means event started
                )
                if is_active:
                    active_eids.append(eid)
                else:
                    prematch_eids.append(eid)

            # Process active events first, then prematch
            # Subscribe to 3 topic types per event:
            #   1. OV/6V detail topic (full market list)
            #   2. M topic (deep market data with all selections)
            #   3. Event IT topic (real-time score/minute/field updates)
            new_active = []
            new_prematch = []
            for eid in active_eids:
                t = self._detail_topic(eid)
                if t and t not in self._subs:
                    new_active.append(t)
                mt = self._market_topic(eid)
                if mt and mt not in self._subs:
                    new_active.append(mt)
                # Subscribe to the event's own IT topic for real-time updates
                ev = self.parser.state.events.get(eid)
                if ev and ev.topic and ev.topic not in self._subs:
                    new_active.append(ev.topic)
            for eid in prematch_eids:
                t = self._detail_topic(eid)
                if t and t not in self._subs:
                    new_prematch.append(t)
                mt = self._market_topic(eid)
                if mt and mt not in self._subs:
                    new_prematch.append(mt)

            # Every 6th cycle (~30s), re-subscribe active events that still
            # only have OV data (6V response never arrived or was lost)
            # Also try alternative 6V topic format (OI-based) for stubborn events
            resub = []
            resub_counter += 1
            if resub_counter >= 6:
                resub_counter = 0
                low_count = 0
                for eid in active_eids:
                    ev = self.parser.state.events.get(eid)
                    if not ev:
                        continue
                    # Check if this event still only has OV-level data
                    markets = len(self.parser.state.event_markets.get(eid, set()))
                    if markets <= 3:
                        low_count += 1
                        # Try standard IT-based 6V
                        t = self._detail_topic(eid)
                        if t:
                            resub.append(t)
                        # Also try OI-based 6V (alt format for some sports)
                        alt = self._detail_topic_alt(eid)
                        if alt and alt not in self._subs:
                            resub.append(alt)
                        # Try M topic for deep market data
                        mt = self._market_topic(eid)
                        if mt:
                            resub.append(mt)
                if resub:
                    self._log.info("Re-subscribing %d topics for %d low-data active events",
                                   len(resub), low_count)
                    # Remove from _subs so they get re-sent
                    for t in resub:
                        self._subs.discard(t)

            # Subscribe active first, then prematch, then re-subs
            new = new_active + new_prematch + resub

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

            self._log.info("Subs: %d (events: %d, active: %d) | premws: %s",
                           len(self._subs), len(my_events), len(active_eids),
                           self.premws_server or "?")
            await asyncio.sleep(5)

    def _detail_topic(self, eid: str) -> str | None:
        """Convert OV event topic to 6V detail topic."""
        ev = self.parser.state.events.get(eid)
        if not ev or not ev.topic: return None
        t = ev.topic
        if t.startswith("OV"):
            t = "6V" + t[2:]  # Replace OV with 6V for detailed market data
        return t

    def _detail_topic_alt(self, eid: str) -> str | None:
        """Alternative 6V topic using OI (fixture ID) instead of IT number.
        Some sports (hockey, TT, handball) may use OI-based 6V topics.
        """
        ev = self.parser.state.events.get(eid)
        if not ev or not ev.topic:
            return None
        oi = ev.raw.get("OI", "")
        if not oi or oi == "0":
            return None
        # Extract sport code and locale from topic: OV191339287C17A_3_0 -> C17A_3_0
        t = ev.topic
        prefix = t.replace("OV", "").replace("6V", "")
        # Find where the sport code starts (after the event number)
        m = re.match(r"\d+(C\d+A.*)", prefix)
        if not m:
            return None
        suffix = m.group(1)  # e.g. C17A_3_0
        return f"6V{oi}{suffix}"

    def _market_topic(self, eid: str) -> str | None:
        """Generate market-level M topic for deep odds data.
        Captured from native bet365 page traffic, the format is:
            {raw_id_number}M{sport_id}_{locale_digit}
        Example: 151316214785M13_3  (tennis event, Spanish locale)
        """
        ev = self.parser.state.events.get(eid)
        if not ev:
            return None
        # Extract numeric prefix from raw ID
        raw_id = ev.raw.get("ID", "")
        id_match = re.match(r"(\d+)", raw_id)
        if not id_match:
            return None
        id_num = id_match.group(1)
        # Extract sport ID from event
        sport_id = ev.sport_id or ev.raw.get("CL", "")
        if not sport_id:
            # Try to extract from topic: OV191339287C17A_3_0 -> 17
            topic = ev.topic or ev.raw.get("IT", "")
            m = re.search(r"C(\d+)A", topic)
            if m:
                sport_id = m.group(1)
        if not sport_id:
            return None
        # Locale digit: _3_0 -> _3, _1_3 -> _3  (last digit before _0 or end)
        locale_digit = self.locale.split("_")[1] if "_" in self.locale else "3"
        return f"{id_num}M{sport_id}_{locale_digit}"

    # OVM market re-linking pattern: OVM{module}-{fixture_id}C{sport}-{market_type}
    _OVM_MKT_RE = re.compile(r"OVM\d+-(\d+)C(\d+)-")
    # Extract sport ID from event ID or topic: C{sport}A
    _SPORT_FROM_ID_RE = re.compile(r"C(\d+)A")

    def _relink_ovm_markets(self):
        """Re-link markets from OVM module container events to real events.

        OVM modules create phantom events with small numeric IDs (e.g. 160 for
        handball, 178 for hockey). Markets attached to these phantom events
        contain the real fixture ID in their IT field:
            OVM160-151316489105C78-780110
        The fixture ID (151316489105) matches the base_id of the corresponding
        real event from OVInPlay.
        """
        state = self.parser.state
        # Build index: base_id -> real event ID (skip module events)
        base_index: dict[str, str] = {}
        for eid in state.events:
            if eid.isdigit() and int(eid) < 10000:
                continue  # Skip OVM module container events
            m = _BASE_RE.match(eid)
            if m:
                base_index[m.group(1)] = eid

        # Find OVM module events and re-link their markets
        relinked = 0
        for eid in list(state.events.keys()):
            if not (eid.isdigit() and int(eid) < 10000):
                continue
            mkt_ids = list(state.event_markets.get(eid, set()))
            if not mkt_ids:
                continue
            # Group markets by fixture ID
            fixture_mkts: dict[str, list[str]] = {}
            for mid in mkt_ids:
                mkt = state.markets.get(mid)
                if not mkt:
                    continue
                it = mkt.raw.get("IT", mkt.topic or mid)
                m = self._OVM_MKT_RE.match(it)
                if m:
                    fixture_mkts.setdefault(m.group(1), []).append(mid)

            # Get sport_id from the module event
            module_ev = state.events.get(eid)
            module_sport = module_ev.sport_id if module_ev else ""

            for fixture_id, mkts in fixture_mkts.items():
                real_eid = base_index.get(fixture_id)
                if not real_eid:
                    continue
                # Set sport_id on target event if missing
                real_ev = state.events.get(real_eid)
                if real_ev and not real_ev.sport_id and module_sport:
                    real_ev.sport_id = module_sport
                # Move markets from module event to real event
                for mid in mkts:
                    mkt = state.markets.get(mid)
                    if mkt:
                        mkt.event_id = real_eid
                    # Update event_markets linkage
                    em = state.event_markets.get(eid)
                    if em and mid in em:
                        em.discard(mid)
                    state.event_markets.setdefault(real_eid, set()).add(mid)
                    relinked += 1

        if relinked > 0:
            self._log.info("OVM relink: %d markets moved to real events", relinked)

        # Fix events with missing sport_id: extract from ID pattern C{sport}A
        fixed_sport = 0
        for eid, ev in state.events.items():
            if ev.sport_id:
                continue
            m = self._SPORT_FROM_ID_RE.search(eid)
            if m:
                ev.sport_id = m.group(1)
                fixed_sport += 1
            elif ev.topic:
                m = self._SPORT_FROM_ID_RE.search(ev.topic)
                if m:
                    ev.sport_id = m.group(1)
                    fixed_sport += 1
        if fixed_sport > 0:
            self._log.info("Fixed sport_id on %d events from ID/topic pattern", fixed_sport)

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
                # Send through ALL active WS connections (premws + pshudws)
                js = f"""mw:() => {{
                    let wsList = (window.__wsRefs || []).filter(w => w.url && (w.url.includes('prem') || w.url.includes('pshud')) && w.readyState === 1);
                    if (!wsList.length) return 'no_ws';
                    for (let ws of wsList) {{
                        ws.send(String.fromCharCode({msg_type}) + String.fromCharCode(0) + '{tstr},A_{token}' + String.fromCharCode(1));
                    }}
                    return 'ok_' + wsList.length;
                }}"""
            else:
                js = f"""mw:() => {{
                    let wsList = (window.__wsRefs || []).filter(w => w.url && (w.url.includes('prem') || w.url.includes('pshud')) && w.readyState === 1);
                    if (!wsList.length) return 'no_ws';
                    for (let ws of wsList) {{
                        ws.send(String.fromCharCode({msg_type}) + String.fromCharCode(0) + '{tstr}' + String.fromCharCode(1));
                    }}
                    return 'ok_' + wsList.length;
                }}"""
            try:
                r = await page.evaluate(js)
                if r and r.startswith("ok"): sent += len(batch)
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

        # Snapshot event IDs before feeding to detect new events from this WS
        def on_recv(p):
            self._frames += 1
            self.stats["frames"] = self.stats.get("frames", 0) + 1
            # Log OVM initial loads to debug sport module responses
            text = p if isinstance(p, str) else p.decode("utf-8", "replace") if isinstance(p, (bytes, bytearray)) else str(p)
            if text and len(text) > 1 and ord(text[0]) == 0x14:
                body = text[1:]
                if '\x01' in body:
                    topic = body.split('\x01')[0]
                    if topic.startswith('OVM'):
                        pdata = body.split('\x01', 1)[1]
                        ma = pdata.count('MA;')
                        ev = pdata.count('EV;')
                        pa = pdata.count('PA;')
                        self._log.info("OVM RECV: %s — %dB %dEV %dMA %dPA",
                                       topic, len(pdata), ev, ma, pa)
            before = set(self.parser.state.events.keys())
            try:
                self.parser.feed(p)
            except: pass
            # Track new event IDs that appeared from THIS worker's frames
            after = self.parser.state.events.keys()
            new_eids = set(after) - before
            if new_eids:
                self._my_event_ids.update(new_eids)
            # Re-link OVM module markets to real events.
            # OVM modules create "container" events with small numeric IDs (e.g., 160, 178)
            # that accumulate markets. The market IT fields contain the real fixture ID:
            #   OVM160-{fixture_id}C{sport}-{market_type}
            # We extract the fixture_id and find the matching real event.
            if text and len(text) > 1 and ord(text[0]) == 0x14:
                body = text[1:]
                if '\x01' in body:
                    ftopic = body.split('\x01')[0]
                    if ftopic.startswith('OVM'):
                        self._relink_ovm_markets()

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
