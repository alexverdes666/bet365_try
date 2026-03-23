"""
bet365 Pre-Match Odds Scraper
==============================
Launches a camoufox browser to navigate bet365 pre-match pages, intercepts
REST API responses and scrapes DOM content to capture odds for upcoming events.
Exposes captured data through a local HTTP API on port 8366.

Architecture:
  1. camoufox browser (navigates bet365 pre-match pages with proxy)
  2. Response interceptor (page.on("response") captures REST JSON/text)
  3. DOM scraper (page.evaluate() extracts visible events and odds)
  4. aiohttp server (HTTP API on port 8366)
  5. Crawl loop (cycles through sports/competitions, refreshes data)

Usage:
    python -m stream.prematch
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from aiohttp import web
from camoufox.async_api import AsyncCamoufox

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prematch")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://www.bet365.es"
HOME_URL = f"{BASE_URL}/#/AS/B1/"  # All sports listing

PROXY = {
    "server": "http://res.proxy-seller.com:10001",
    "username": "4f6d12485ca053d6",
    "password": "OzkAVbyM",
}

API_HOST = "0.0.0.0"
API_PORT = 8366

# Use proxy by default; set to None to disable
USE_PROXY = True
BROWSER_PROXY = PROXY if USE_PROXY else None

# Crawl timing
PAGE_LOAD_WAIT = 8          # seconds to wait after navigation for page to load
BETWEEN_PAGES_WAIT = 35     # seconds between page navigations
FULL_CYCLE_PAUSE = 60       # seconds to pause after completing a full crawl cycle
MAX_RETRIES_PER_PAGE = 2    # retries if a page fails to load

# Known sport IDs and their hash routes on bet365
# These are discovered dynamically, but we seed with commonly known ones
# Use /#/AS/B{id}/ (splash format) which renders CouponPodModules with events+odds
SEED_SPORTS = {
    "1":  {"name": "Football",     "route": "/#/AS/B1/"},
    "13": {"name": "Tennis",       "route": "/#/AS/B13/"},
    "18": {"name": "Basketball",   "route": "/#/AS/B18/"},
    "17": {"name": "Ice Hockey",   "route": "/#/AS/B17/"},
    "78": {"name": "Handball",     "route": "/#/AS/B78/"},
    "91": {"name": "Volleyball",   "route": "/#/AS/B91/"},
    "12": {"name": "American Football", "route": "/#/AS/B12/"},
    "83": {"name": "Futsal",       "route": "/#/AS/B83/"},
    "3":  {"name": "Cricket",      "route": "/#/AS/B3/"},
    "14": {"name": "Snooker",      "route": "/#/AS/B14/"},
    "92": {"name": "Table Tennis",  "route": "/#/AS/B92/"},
    "36": {"name": "Australian Rules", "route": "/#/AS/B36/"},
    "66": {"name": "Bowls",        "route": "/#/AS/B66/"},
    "75": {"name": "Gaelic Sports", "route": "/#/AS/B75/"},
    "90": {"name": "Floorball",    "route": "/#/AS/B90/"},
    "15": {"name": "Darts",        "route": "/#/AS/B15/"},
    "9":  {"name": "Boxing/UFC",   "route": "/#/AS/B9/"},
    "8":  {"name": "Rugby Union",  "route": "/#/AS/B8/"},
    "19": {"name": "Rugby League", "route": "/#/AS/B19/"},
    "16": {"name": "Baseball",     "route": "/#/AS/B16/"},
    "6":  {"name": "Golf",         "route": "/#/AS/B6/"},
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class Selection:
    """A single betting selection/outcome."""
    name: str = ""
    odds: str = ""
    header: str = ""       # column header (e.g. "1", "X", "2")
    suspended: bool = False


@dataclass
class Market:
    """A betting market for an event."""
    name: str = ""
    selections: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PrematchEvent:
    """A pre-match event with its markets."""
    id: str = ""
    name: str = ""
    sport: str = ""
    sport_id: str = ""
    competition: str = ""
    competition_id: str = ""
    start_time: str = ""
    markets: List[Dict[str, Any]] = field(default_factory=list)
    url: str = ""
    last_updated: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "sport": self.sport,
            "sport_id": self.sport_id,
            "competition": self.competition,
            "competition_id": self.competition_id,
            "start_time": self.start_time,
            "markets": self.markets,
            "url": self.url,
            "last_updated": self.last_updated,
        }


# ---------------------------------------------------------------------------
# Pre-match data store
# ---------------------------------------------------------------------------
class PrematchStore:
    """Thread-safe store for pre-match event data."""

    def __init__(self) -> None:
        self.events: Dict[str, PrematchEvent] = {}
        self.sports: Dict[str, Dict[str, Any]] = {}
        self.competitions: Dict[str, Dict[str, Any]] = {}
        self.raw_responses: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def upsert_event(self, event: PrematchEvent) -> None:
        async with self._lock:
            event.last_updated = time.time()
            self.events[event.id] = event

    async def upsert_events_batch(self, events: List[PrematchEvent]) -> int:
        count = 0
        async with self._lock:
            now = time.time()
            for event in events:
                event.last_updated = now
                self.events[event.id] = event
                count += 1
        return count

    async def update_sport(self, sport_id: str, name: str, **extra: Any) -> None:
        async with self._lock:
            self.sports[sport_id] = {"id": sport_id, "name": name, **extra}

    async def update_competition(
        self, comp_id: str, name: str, sport_id: str, **extra: Any
    ) -> None:
        async with self._lock:
            self.competitions[comp_id] = {
                "id": comp_id,
                "name": name,
                "sport_id": sport_id,
                **extra,
            }

    async def store_raw_response(self, url: str, data: Any) -> None:
        async with self._lock:
            key = hashlib.md5(url.encode()).hexdigest()[:12]
            self.raw_responses[key] = {
                "url": url,
                "timestamp": time.time(),
                "size": len(str(data)),
            }

    def get_all_events(self) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self.events.values()]

    def get_events_by_sport(self, sport: str) -> List[Dict[str, Any]]:
        sport_lower = sport.lower()
        return [
            e.to_dict()
            for e in self.events.values()
            if e.sport.lower() == sport_lower or e.sport_id == sport
        ]

    def get_sports_summary(self) -> List[Dict[str, Any]]:
        sport_counts: Dict[str, Dict[str, Any]] = {}
        for event in self.events.values():
            key = event.sport_id or event.sport
            if key not in sport_counts:
                sport_counts[key] = {
                    "id": event.sport_id,
                    "name": event.sport,
                    "event_count": 0,
                    "competitions": set(),
                }
            sport_counts[key]["event_count"] += 1
            if event.competition:
                sport_counts[key]["competitions"].add(event.competition)
        result = []
        for info in sport_counts.values():
            result.append({
                "id": info["id"],
                "name": info["name"],
                "event_count": info["event_count"],
                "competition_count": len(info["competitions"]),
            })
        return sorted(result, key=lambda x: x["event_count"], reverse=True)


# ---------------------------------------------------------------------------
# REST response parser
# ---------------------------------------------------------------------------
class ResponseParser:
    """Parses bet365 REST API responses into structured data."""

    @staticmethod
    def parse_sports_config(data: Any) -> List[Dict[str, Any]]:
        """Parse /defaultapi/sports-configuration response."""
        sports = []
        if isinstance(data, dict):
            # Various possible structures
            for key in ("sports", "Sports", "data", "Data"):
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        sport = {
                            "id": str(item.get("id", item.get("ID", ""))),
                            "name": item.get("name", item.get("Name", item.get("DN", ""))),
                        }
                        if sport["id"]:
                            sports.append(sport)
                    break
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    sport = {
                        "id": str(item.get("id", item.get("ID", ""))),
                        "name": item.get("name", item.get("Name", item.get("DN", ""))),
                    }
                    if sport["id"]:
                        sports.append(sport)
        return sports

    @staticmethod
    def parse_zap_text(text: str, sport_name: str = "", sport_id: str = "") -> List[PrematchEvent]:
        """
        Parse ZAP-like text format that bet365 uses in some REST responses.
        The format uses | separated fields with ; separated entities.
        """
        events = []
        if not text or len(text) < 10:
            return events

        # Split into segments by record separator
        current_competition = ""
        current_comp_id = ""

        # ZAP format: entities separated by ; with fields separated by |
        # Entity types: CL (classification), CT (competition), EV (event),
        # MA (market), PA (participant/selection)
        segments = re.split(r'(?:;|\x01)', text)

        current_event: Optional[Dict[str, Any]] = None
        current_market: Optional[Dict[str, Any]] = None
        events_raw: List[Dict[str, Any]] = []

        for segment in segments:
            segment = segment.strip()
            if not segment:
                continue

            # Parse key=value pairs
            fields: Dict[str, str] = {}
            entity_type = ""

            # Check for entity type prefix (e.g., "EV;", "CT;", etc.)
            if len(segment) >= 2 and segment[:2] in ("CL", "CT", "EV", "MA", "PA", "MG", "SC"):
                entity_type = segment[:2]
                segment = segment[2:]

            parts = segment.split("|")
            for part in parts:
                if "=" in part:
                    k, _, v = part.partition("=")
                    fields[k.strip()] = v.strip()
                elif part.startswith(("CL", "CT", "EV", "MA", "PA", "MG")):
                    entity_type = part[:2]

            if not entity_type and not fields:
                continue

            if entity_type == "CT":
                current_competition = fields.get("NA", fields.get("DN", ""))
                current_comp_id = fields.get("ID", fields.get("FI", ""))

            elif entity_type == "EV":
                # Save previous event
                if current_event and current_event.get("name"):
                    events_raw.append(current_event)

                name = fields.get("NA", fields.get("DN", ""))
                event_id = fields.get("ID", fields.get("FI", ""))
                start_time = fields.get("DI", fields.get("BC", ""))

                current_event = {
                    "id": event_id or hashlib.md5(
                        f"{name}{current_competition}{start_time}".encode()
                    ).hexdigest()[:16],
                    "name": name,
                    "sport": sport_name,
                    "sport_id": sport_id,
                    "competition": current_competition,
                    "competition_id": current_comp_id,
                    "start_time": start_time,
                    "markets": [],
                    "current_market": None,
                }
                current_market = None

            elif entity_type == "MA":
                market_name = fields.get("NA", fields.get("DN", ""))
                current_market = {
                    "name": market_name,
                    "selections": [],
                }
                if current_event is not None:
                    current_event["markets"].append(current_market)

            elif entity_type == "PA":
                odds = fields.get("OD", fields.get("LP", ""))
                sel_name = fields.get("NA", fields.get("DN", ""))
                header = fields.get("HD", fields.get("HN", ""))
                selection = {
                    "name": sel_name,
                    "odds": odds,
                    "header": header,
                }
                if current_market is not None:
                    current_market["selections"].append(selection)
                elif current_event is not None and current_event["markets"]:
                    current_event["markets"][-1]["selections"].append(selection)

        # Don't forget the last event
        if current_event and current_event.get("name"):
            events_raw.append(current_event)

        # Convert to PrematchEvent objects
        for raw in events_raw:
            markets = []
            for m in raw.get("markets", []):
                if isinstance(m, dict) and m.get("selections"):
                    markets.append({
                        "name": m.get("name", ""),
                        "selections": m.get("selections", []),
                    })
            ev = PrematchEvent(
                id=raw.get("id", ""),
                name=raw.get("name", ""),
                sport=raw.get("sport", sport_name),
                sport_id=raw.get("sport_id", sport_id),
                competition=raw.get("competition", ""),
                competition_id=raw.get("competition_id", ""),
                start_time=raw.get("start_time", ""),
                markets=markets,
            )
            if ev.name:
                events.append(ev)

        return events


# ---------------------------------------------------------------------------
# DOM Scraper — extracts events/odds from the visible page
# ---------------------------------------------------------------------------
# JavaScript executed via page.evaluate() to extract events from the DOM.
# bet365 pre-match DOM: cpm-CouponPodModule containers with
# cpm-ParticipantFixtureDetails for teams and cpm-ParticipantOdds for odds.
# Odds are SIBLINGS of the fixture (inside MarketGroup_Wrapper), not children.
DOM_EXTRACT_JS = r"""() => {
    const results = {events: [], competitions: [], meta: {}};
    function txt(el) { return el ? el.textContent.trim() : ''; }
    const seen = new Set();

    // Strategy 1: CouponPodModule (All Sports / splash pages)
    const pods = document.querySelectorAll('[class*="CouponPodModule"]');
    results.meta.pods = pods.length;
    for (const pod of pods) {
        const hdr = pod.querySelector('[class*="Header"]');
        const comp = hdr ? txt(hdr) : '';
        if (comp) results.competitions.push(comp);
        // Use MarketGroup_Wrapper (innermost row containing fixture + odds)
        const rows = pod.querySelectorAll('[class*="MarketGroup_Wrapper"]');
        for (const row of rows) {
            const d = row.querySelector('[class*="ParticipantFixtureDetails"]');
            if (!d) continue;
            const lines = d.innerText.split(String.fromCharCode(10)).map(function(s){return s.trim()}).filter(function(s){return s.length>1});
            const teams = lines.filter(function(t){return !/^\d{1,2}:\d{2}$/.test(t) && !/^\d+$/.test(t) && !/^(Lun|Mar|Mi|Jue|Vie|S|Dom|Hoy)/i.test(t)});
            const tm = d.innerText.match(/(\d{1,2}:\d{2})/);
            const startTime = tm ? tm[1] : '';
            // Odds are siblings of the fixture inside this wrapper
            const oddsEls = row.querySelectorAll('[class*="ParticipantOdds"]');
            const odds = [];
            for (const o of oddsEls) { const v = txt(o); if (/^\d+\.\d+$/.test(v) && odds[odds.length-1] !== v) odds.push(v); }
            if (teams.length >= 2) {
                const name = teams[0] + ' v ' + teams[1];
                if (!seen.has(name + startTime)) {
                    seen.add(name + startTime);
                    results.events.push({name, teams: teams.slice(0,2), competition: comp, start_time: startTime, odds});
                }
            }
        }
    }

    // Strategy 2: gl-MarketGroup on sport-specific coupon pages
    if (results.events.length === 0) {
        const groups = document.querySelectorAll('[class*="gl-MarketGroup "]');
        results.meta.marketGroups = groups.length;
        for (const group of groups) {
            const hdr = group.querySelector('[class*="MarketGroupButton"]');
            const comp = hdr ? txt(hdr) : '';
            const rows = group.querySelectorAll('[class*="MarketGroup_Wrapper"]');
            for (const row of rows) {
                const teams = []; const odds = [];
                const nameEls = row.querySelectorAll('[class*="Team"], [class*="Name"]');
                for (const ne of nameEls) { const n = txt(ne); if (n && n.length > 1) teams.push(n); }
                const oddsEls = row.querySelectorAll('[class*="Odds"]');
                for (const oe of oddsEls) { const v = txt(oe); if (/^\d+\.\d+$/.test(v) && odds[odds.length-1] !== v) odds.push(v); }
                if (teams.length >= 2) {
                    const name = teams[0] + ' v ' + teams[1];
                    if (!seen.has(name)) { seen.add(name); results.events.push({name, teams: teams.slice(0,2), competition: comp, start_time: '', odds}); }
                }
            }
        }
    }

    // Strategy 3: Fallback text scan for " v " patterns
    if (results.events.length === 0) {
        const body = document.body.innerText || '';
        const lines = body.split('\n').map(function(l){return l.trim()}).filter(Boolean);
        for (let i = 0; i < lines.length; i++) {
            if (lines[i].includes(' v ') && lines[i].length < 100 && lines[i].length > 5) {
                const odds = [];
                for (let j = i+1; j < Math.min(i+5, lines.length); j++) {
                    if (/^\d+\.\d{2}$/.test(lines[j])) odds.push(lines[j]);
                }
                const name = lines[i];
                if (!seen.has(name)) {
                    seen.add(name);
                    results.events.push({name, teams: name.split(' v '), competition: '', start_time: '', odds});
                }
            }
        }
    }

    results.meta.url = window.location.href;
    results.meta.hash = window.location.hash;

    return results;
}"""

# JavaScript to extract navigation links (sports + competitions)
NAV_EXTRACT_JS = """() => {
    const navLinks = [];

    // Look for left navigation menu items
    const menuItems = document.querySelectorAll(
        '.wn-Classification, .lsn-ClassificationHeader, ' +
        '[class*="Classification"], [class*="SportHeader"], ' +
        '.wn-PreMatchItem, [class*="PreMatch"], ' +
        '.alm-SportHeader, .alm-CompetitionItem, ' +
        '[class*="LeftNav"] a, [class*="leftnav"] a, ' +
        '.sm-SplashMarket, .sm-Market'
    );

    for (const item of menuItems) {
        const name = item.textContent.trim();
        const link = item.querySelector('a');
        const href = link ? link.getAttribute('href') : (item.getAttribute('href') || '');
        const classList = Array.from(item.classList || []).join(' ');

        if (name && name.length < 80) {
            navLinks.push({
                name: name,
                href: href,
                classes: classList,
            });
        }
    }

    // Also try to find sport links in the page
    const sportLinks = document.querySelectorAll('a[href*="#/AC/B"], a[href*="#/AS/B"]');
    for (const a of sportLinks) {
        navLinks.push({
            name: a.textContent.trim(),
            href: a.getAttribute('href'),
            classes: 'sport-link',
        });
    }

    // Check for All Sports menu
    const allSportsItems = document.querySelectorAll(
        '.alm-ClassificationHeader, .ovm-ClassificationHeader, ' +
        '[class*="ClassificationHeader"]'
    );
    for (const item of allSportsItems) {
        navLinks.push({
            name: item.textContent.trim(),
            href: '',
            classes: 'classification-header ' + Array.from(item.classList || []).join(' '),
        });
    }

    return {
        links: navLinks,
        url: window.location.href,
        hash: window.location.hash,
    };
}"""


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------
class PrematchAPI:
    """aiohttp-based HTTP API serving pre-match data."""

    def __init__(self, store: PrematchStore, stats: Dict[str, Any]) -> None:
        self.store = store
        self.stats = stats
        self._runner: Optional[web.AppRunner] = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/sports", self.handle_sports)
        app.router.add_get("/events", self.handle_events)
        app.router.add_get("/events/{sport}", self.handle_events_by_sport)
        app.router.add_get("/stats", self.handle_stats)
        app.router.add_get("/raw", self.handle_raw)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, API_HOST, API_PORT)
        await site.start()
        log.info("Pre-match HTTP API listening on http://%s:%d", API_HOST, API_PORT)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()

    async def handle_index(self, _request: web.Request) -> web.Response:
        return web.json_response({
            "service": "bet365 Pre-Match Odds API",
            "port": API_PORT,
            "endpoints": [
                "GET /           — this info",
                "GET /sports     — all sports with event counts",
                "GET /events     — all pre-match events with odds",
                "GET /events/{sport} — events for a specific sport (by name or ID)",
                "GET /stats      — crawl statistics",
                "GET /raw        — raw intercepted response metadata",
            ],
        })

    async def handle_sports(self, _request: web.Request) -> web.Response:
        return web.json_response(self.store.get_sports_summary())

    async def handle_events(self, _request: web.Request) -> web.Response:
        events = self.store.get_all_events()
        return web.json_response({
            "count": len(events),
            "events": events,
        })

    async def handle_events_by_sport(self, request: web.Request) -> web.Response:
        sport = request.match_info["sport"]
        events = self.store.get_events_by_sport(sport)
        return web.json_response({
            "sport": sport,
            "count": len(events),
            "events": events,
        })

    async def handle_stats(self, _request: web.Request) -> web.Response:
        uptime = time.time() - self.stats.get("start_time", time.time())
        return web.json_response({
            "uptime_seconds": round(uptime, 1),
            "total_events": len(self.store.events),
            "total_sports": len(set(e.sport_id for e in self.store.events.values())),
            "total_competitions": len(
                set(e.competition for e in self.store.events.values() if e.competition)
            ),
            "pages_crawled": self.stats.get("pages_crawled", 0),
            "pages_failed": self.stats.get("pages_failed", 0),
            "responses_intercepted": self.stats.get("responses_intercepted", 0),
            "dom_extractions": self.stats.get("dom_extractions", 0),
            "events_from_responses": self.stats.get("events_from_responses", 0),
            "events_from_dom": self.stats.get("events_from_dom", 0),
            "crawl_cycles_completed": self.stats.get("crawl_cycles_completed", 0),
            "current_sport": self.stats.get("current_sport", ""),
            "last_crawl_time": self.stats.get("last_crawl_time", ""),
            "browser_restarts": self.stats.get("browser_restarts", 0),
            "raw_responses_stored": len(self.store.raw_responses),
        })

    async def handle_raw(self, _request: web.Request) -> web.Response:
        return web.json_response({
            "count": len(self.store.raw_responses),
            "responses": list(self.store.raw_responses.values()),
        })


# ---------------------------------------------------------------------------
# Main crawler
# ---------------------------------------------------------------------------
class PrematchCrawler:
    """
    Orchestrates browser navigation, response interception, DOM scraping,
    and the HTTP API for pre-match odds.
    """

    def __init__(self) -> None:
        self.store = PrematchStore()
        self.stats: Dict[str, Any] = {
            "start_time": time.time(),
            "pages_crawled": 0,
            "pages_failed": 0,
            "responses_intercepted": 0,
            "dom_extractions": 0,
            "events_from_responses": 0,
            "events_from_dom": 0,
            "crawl_cycles_completed": 0,
            "current_sport": "",
            "last_crawl_time": "",
            "browser_restarts": 0,
        }
        self.api = PrematchAPI(self.store, self.stats)
        self._should_run = True
        self._discovered_routes: Dict[str, Dict[str, Any]] = {}
        self._response_buffer: List[Dict[str, Any]] = []
        self._current_sport_name = ""
        self._current_sport_id = ""

    async def run(self) -> None:
        """Main entry point — start API and crawl loop."""
        log.info("=== bet365 Pre-Match Scraper starting ===")

        # Start HTTP API
        await self.api.start()

        # Initialize routes with seed sports
        for sid, info in SEED_SPORTS.items():
            self._discovered_routes[sid] = {
                "sport_id": sid,
                "name": info["name"],
                "route": info["route"],
            }

        # Main crawl loop with automatic browser restart
        try:
            while self._should_run:
                try:
                    await self._browser_session()
                except Exception:
                    self.stats["browser_restarts"] += 1
                    log.exception(
                        "Browser session crashed (restart #%d) — restarting in 15s",
                        self.stats["browser_restarts"],
                    )
                    await asyncio.sleep(15)
        except asyncio.CancelledError:
            log.info("Pre-match crawler cancelled")
        except KeyboardInterrupt:
            log.info("Pre-match crawler interrupted by user")
        finally:
            log.info("Shutting down...")
            await self.api.stop()
            log.info("Shutdown complete")

    async def _browser_session(self) -> None:
        """Single browser session with full crawl cycle."""
        log.info("Launching camoufox browser (headless)...")

        camoufox_kwargs: Dict[str, Any] = {
            "headless": True,
            "humanize": True,
            "os": "windows",
        }
        if BROWSER_PROXY:
            camoufox_kwargs["proxy"] = BROWSER_PROXY

        async with AsyncCamoufox(**camoufox_kwargs) as browser:
            page = await browser.new_page()

            # Register response interceptor
            page.on(
                "response",
                lambda resp: asyncio.ensure_future(self._on_response(resp)),
            )

            # Navigate to homepage first
            log.info("Navigating to %s", HOME_URL)
            try:
                await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60000)
                log.info("Homepage loaded")
            except Exception as exc:
                log.warning("Homepage navigation issue: %s", exc)

            await asyncio.sleep(PAGE_LOAD_WAIT)

            # Try to discover navigation from the homepage
            await self._discover_navigation(page)

            # Main crawl loop: cycle through all sports
            while self._should_run:
                routes = list(self._discovered_routes.values())
                if not routes:
                    log.warning("No routes discovered — using seed sports")
                    for sid, info in SEED_SPORTS.items():
                        self._discovered_routes[sid] = {
                            "sport_id": sid,
                            "name": info["name"],
                            "route": info["route"],
                        }
                    routes = list(self._discovered_routes.values())

                log.info(
                    "Starting crawl cycle: %d sports to crawl",
                    len(routes),
                )

                for route_info in routes:
                    if not self._should_run:
                        break

                    sport_id = route_info["sport_id"]
                    sport_name = route_info["name"]
                    route = route_info["route"]

                    self._current_sport_name = sport_name
                    self._current_sport_id = sport_id
                    self.stats["current_sport"] = sport_name

                    await self._crawl_sport_page(page, sport_id, sport_name, route)

                    # Wait between pages to avoid detection
                    if self._should_run:
                        log.info(
                            "Waiting %ds before next sport...",
                            BETWEEN_PAGES_WAIT,
                        )
                        await asyncio.sleep(BETWEEN_PAGES_WAIT)

                self.stats["crawl_cycles_completed"] += 1
                log.info(
                    "Crawl cycle #%d complete. Total events: %d. "
                    "Pausing %ds before next cycle...",
                    self.stats["crawl_cycles_completed"],
                    len(self.store.events),
                    FULL_CYCLE_PAUSE,
                )
                await asyncio.sleep(FULL_CYCLE_PAUSE)

    async def _on_response(self, response: Any) -> None:
        """Intercept HTTP responses from bet365 REST APIs."""
        url = response.url
        if "bet365" not in url:
            return

        try:
            status = response.status
            ct = response.headers.get("content-type", "")

            # Only process successful responses with relevant content types
            if status != 200:
                return
            if not ("json" in ct or "text" in ct or "javascript" in ct):
                return

            # Filter for API-like URLs
            interesting_patterns = [
                "defaultapi",
                "sportsconfiguration",
                "sports-configuration",
                "leftnavcontentapi",
                "allsportsmenu",
                "couponapi",
                "prematch",
                "pre-match",
                "eventapi",
                "fixturescheduling",
                "fixture",
                "contentapi",
                "/api/",
                "getevents",
                "sportmenu",
                "marketgroup",
                "competition",
            ]

            url_lower = url.lower()
            is_interesting = any(p in url_lower for p in interesting_patterns)

            # Also capture anything that looks like it contains event data
            if not is_interesting:
                # Check for generic bet365 API paths
                if "/v1/" in url_lower or "/v2/" in url_lower or "/api/" in url_lower:
                    is_interesting = True

            if not is_interesting:
                return

            body = await response.text()
            if not body or len(body) < 20:
                return

            self.stats["responses_intercepted"] += 1
            log.info(
                "Intercepted REST response: %s (%d bytes, type=%s)",
                url[:120],
                len(body),
                ct[:40],
            )

            # Store raw response metadata
            await self.store.store_raw_response(url, body)

            # Try to parse as JSON
            data = None
            try:
                data = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                pass

            # Parse sports configuration
            if "sports-configuration" in url_lower or "sportsconfiguration" in url_lower:
                sports = ResponseParser.parse_sports_config(data or body)
                for sport in sports:
                    await self.store.update_sport(sport["id"], sport["name"])
                    if sport["id"] not in self._discovered_routes:
                        self._discovered_routes[sport["id"]] = {
                            "sport_id": sport["id"],
                            "name": sport["name"],
                            "route": f"/#/AC/B{sport['id']}/",
                        }
                if sports:
                    log.info("Discovered %d sports from config API", len(sports))

            # Try to parse event data from ZAP-like text responses
            if data is None and body and ("|" in body or ";" in body):
                events = ResponseParser.parse_zap_text(
                    body,
                    sport_name=self._current_sport_name,
                    sport_id=self._current_sport_id,
                )
                if events:
                    count = await self.store.upsert_events_batch(events)
                    self.stats["events_from_responses"] += count
                    log.info(
                        "Parsed %d events from REST response (ZAP text)",
                        count,
                    )

            # Try to extract events from JSON responses
            if data and isinstance(data, (dict, list)):
                events = self._extract_events_from_json(data)
                if events:
                    count = await self.store.upsert_events_batch(events)
                    self.stats["events_from_responses"] += count
                    log.info("Parsed %d events from REST JSON response", count)

        except Exception:
            # Silently skip responses that fail to process — this is expected
            # for many non-data responses
            pass

    def _extract_events_from_json(self, data: Any) -> List[PrematchEvent]:
        """Try to extract events from various JSON response structures."""
        events = []

        def _walk(obj: Any, competition: str = "", depth: int = 0) -> None:
            if depth > 10:
                return
            if isinstance(obj, dict):
                # Check if this dict looks like an event
                has_name = any(
                    k in obj for k in ("NA", "name", "Name", "DN", "eventName")
                )
                has_odds = any(
                    k in obj for k in ("OD", "odds", "Odds", "LP", "price")
                )
                has_teams = any(
                    k in obj for k in ("T1", "T2", "home", "away", "teams")
                )

                # Extract competition name if present
                comp = obj.get("CN", obj.get("competitionName", obj.get("CT", competition)))

                if has_name and (has_odds or has_teams):
                    name = obj.get("NA", obj.get("name", obj.get("Name", obj.get("DN", obj.get("eventName", "")))))
                    eid = str(obj.get("ID", obj.get("id", obj.get("FI", ""))))
                    if not eid:
                        eid = hashlib.md5(f"{name}{comp}".encode()).hexdigest()[:16]

                    start_time = obj.get("DI", obj.get("startTime", obj.get("BC", "")))

                    # Try to extract markets/odds
                    markets = []
                    odds_val = obj.get("OD", obj.get("odds", obj.get("Odds", [])))
                    if isinstance(odds_val, list):
                        for m in odds_val:
                            if isinstance(m, dict):
                                market_name = m.get("NA", m.get("name", ""))
                                sels = m.get("PA", m.get("selections", []))
                                selections = []
                                if isinstance(sels, list):
                                    for s in sels:
                                        if isinstance(s, dict):
                                            selections.append({
                                                "name": s.get("NA", s.get("name", "")),
                                                "odds": str(s.get("OD", s.get("odds", s.get("LP", "")))),
                                            })
                                if selections:
                                    markets.append({"name": market_name, "selections": selections})

                    ev = PrematchEvent(
                        id=eid,
                        name=name,
                        sport=self._current_sport_name,
                        sport_id=self._current_sport_id,
                        competition=comp if isinstance(comp, str) else "",
                        start_time=str(start_time),
                        markets=markets,
                    )
                    if ev.name:
                        events.append(ev)

                # Recurse into dict values
                for v in obj.values():
                    _walk(v, comp if isinstance(comp, str) else competition, depth + 1)

            elif isinstance(obj, list):
                for item in obj:
                    _walk(item, competition, depth + 1)

        _walk(data)
        return events

    async def _discover_navigation(self, page: Any) -> None:
        """Try to discover sport navigation from the current page."""
        try:
            nav_data = await page.evaluate(NAV_EXTRACT_JS)
            links = nav_data.get("links", [])
            log.info("Navigation discovery: found %d nav items", len(links))

            for link in links:
                href = link.get("href", "")
                name = link.get("name", "")
                if not href or not name:
                    continue

                # Extract sport ID from href (e.g., /#/AC/B1/ -> sport_id=1)
                match = re.search(r"/B(\d+)/", href)
                if match:
                    sport_id = match.group(1)
                    if sport_id not in self._discovered_routes:
                        self._discovered_routes[sport_id] = {
                            "sport_id": sport_id,
                            "name": name,
                            "route": href,
                        }
                        log.info(
                            "Discovered sport: %s (ID=%s, route=%s)",
                            name,
                            sport_id,
                            href,
                        )
        except Exception as exc:
            log.warning("Navigation discovery failed: %s", exc)

    async def _crawl_sport_page(
        self, page: Any, sport_id: str, sport_name: str, route: str
    ) -> None:
        """Navigate to a sport page and extract events."""
        full_url = f"{BASE_URL}{route}" if route.startswith("/") else route
        log.info("Crawling %s (%s): %s", sport_name, sport_id, full_url)

        for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
            try:
                # Navigate to the sport page
                await page.goto(full_url, wait_until="domcontentloaded", timeout=45000)
                log.info("Page loaded for %s (attempt %d)", sport_name, attempt)

                # Wait for content to render (SPA needs time after hash navigation)
                await asyncio.sleep(PAGE_LOAD_WAIT)

                # Extract events from DOM
                await self._extract_from_dom(page, sport_id, sport_name)

                # Also try to discover competition sub-pages
                await self._discover_competition_links(page, sport_id, sport_name)

                self.stats["pages_crawled"] += 1
                self.stats["last_crawl_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
                return  # success

            except Exception as exc:
                log.warning(
                    "Failed to crawl %s (attempt %d/%d): %s",
                    sport_name,
                    attempt,
                    MAX_RETRIES_PER_PAGE,
                    exc,
                )
                if attempt < MAX_RETRIES_PER_PAGE:
                    await asyncio.sleep(5)

        self.stats["pages_failed"] += 1
        log.error("Failed to crawl %s after %d attempts", sport_name, MAX_RETRIES_PER_PAGE)

    async def _extract_from_dom(
        self, page: Any, sport_id: str, sport_name: str
    ) -> None:
        """Extract event data from the visible page DOM."""
        try:
            result = await page.evaluate(DOM_EXTRACT_JS)
            dom_events = result.get("events", [])
            meta = result.get("meta", {})

            log.info(
                "DOM extraction for %s: %d events found (pods=%s, groups=%s)",
                sport_name,
                len(dom_events),
                meta.get("pods", 0),
                meta.get("marketGroups", 0),
            )

            self.stats["dom_extractions"] += 1

            if not dom_events:
                return

            events_to_store: List[PrematchEvent] = []
            for raw in dom_events:
                name = raw.get("name", "")
                if not name or len(name) < 3:
                    continue

                # Generate stable ID from event content
                event_id = hashlib.md5(
                    f"{sport_id}:{raw.get('competition', '')}:{name}".encode()
                ).hexdigest()[:16]

                # Build market from extracted odds
                markets = []
                odds_list = raw.get("odds", [])
                if odds_list:
                    teams = raw.get("teams", [])
                    selections = []

                    # Common patterns: 2-way (ML), 3-way (1X2)
                    if len(odds_list) == 3:
                        headers = ["1", "X", "2"]
                        labels = [
                            teams[0] if len(teams) > 0 else "Home",
                            "Draw",
                            teams[1] if len(teams) > 1 else "Away",
                        ]
                        for i, odd in enumerate(odds_list):
                            selections.append({
                                "name": labels[i],
                                "odds": odd,
                                "header": headers[i],
                            })
                        markets.append({
                            "name": "Full Time Result",
                            "selections": selections,
                        })
                    elif len(odds_list) == 2:
                        labels = [
                            teams[0] if len(teams) > 0 else "Home",
                            teams[1] if len(teams) > 1 else "Away",
                        ]
                        for i, odd in enumerate(odds_list):
                            selections.append({
                                "name": labels[i],
                                "odds": odd,
                                "header": str(i + 1),
                            })
                        markets.append({
                            "name": "Match Winner",
                            "selections": selections,
                        })
                    else:
                        for i, odd in enumerate(odds_list):
                            selections.append({
                                "name": f"Selection {i + 1}",
                                "odds": odd,
                            })
                        if selections:
                            markets.append({
                                "name": "Main Market",
                                "selections": selections,
                            })

                ev = PrematchEvent(
                    id=event_id,
                    name=name,
                    sport=sport_name,
                    sport_id=sport_id,
                    competition=raw.get("competition", ""),
                    start_time=raw.get("start_time", ""),
                    markets=markets,
                    url=f"{BASE_URL}/#/AC/B{sport_id}/",
                )
                events_to_store.append(ev)

            if events_to_store:
                count = await self.store.upsert_events_batch(events_to_store)
                self.stats["events_from_dom"] += count
                log.info("Stored %d events from DOM for %s", count, sport_name)

        except Exception as exc:
            log.warning("DOM extraction failed for %s: %s", sport_name, exc)

    async def _discover_competition_links(
        self, page: Any, sport_id: str, sport_name: str
    ) -> None:
        """Find competition/league links on the current sport page."""
        try:
            links = await page.evaluate("""() => {
                const results = [];
                // Look for competition links in the page
                const compLinks = document.querySelectorAll(
                    'a[href*="#/AC/B"], a[href*="#/AS/B"], ' +
                    '[class*="Competition"] a, [class*="League"] a'
                );
                for (const a of compLinks) {
                    const href = a.getAttribute('href') || '';
                    const name = a.textContent.trim();
                    if (href && name && name.length < 80) {
                        results.push({name, href});
                    }
                }
                return results;
            }""")

            for link in links:
                href = link.get("href", "")
                name = link.get("name", "")
                match = re.search(r"/B(\d+)/", href)
                if match:
                    found_id = match.group(1)
                    # Only add if it's a new route for the same sport
                    route_key = f"{found_id}_{name}"
                    if route_key not in self._discovered_routes:
                        self._discovered_routes[route_key] = {
                            "sport_id": found_id,
                            "name": f"{sport_name} - {name}",
                            "route": href,
                        }
        except Exception:
            pass  # Non-critical — competition discovery is best-effort


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    crawler = PrematchCrawler()
    await crawler.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Exited.")
