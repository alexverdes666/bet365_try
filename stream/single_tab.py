"""
bet365 Single-Tab All-Markets Streamer
=======================================
1 headless browser tab. Uses Camoufox's mw: prefix to evaluate JavaScript
in the page's actual main world (not Playwright's utility world), patching
WebSocket.prototype.send to capture the premws WS reference and token.
Then sends 6V detail subscribe messages through the captured WS.

Usage:  python -m stream.single_tab
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp
from aiohttp import web

from protocol.zap_parser import ZapParser, ChangeEvent, ChangeType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("single_tab")

# Configuration
PROXY_HTTP = "http://res.proxy-seller.com:10000"
PROXY_USER = "4f6d12485ca053d6"
PROXY_PASS = "OzkAVbyM"
TARGET_URL = "https://www.bet365.es/#/IP/"
API_HOST, API_PORT = "0.0.0.0", 8365
DRIP_BATCH = 5
DRIP_INTERVAL = 0.5
NEW_EVENT_SCAN = 10
USE_PROXY = True

# Camoufox mw: prefix evaluations — run in the PAGE's actual main world
JS_HOOK = r"""mw:() => {
    if (window.__wsHooked) return 'already';
    const origSend = WebSocket.prototype.send;
    window.__wsRefs = [];
    window.__lastToken = '';
    WebSocket.prototype.send = function(data) {
        if (!window.__wsRefs.includes(this)) window.__wsRefs.push(this);
        if (typeof data === 'string') {
            let m = data.match(/[,]A_([A-Za-z0-9+\/=]{20,})/);
            if (m) window.__lastToken = m[1];
        }
        return origSend.call(this, data);
    };
    window.__wsHooked = true;
    return 'hooked';
}"""

JS_STATUS = r"mw:() => ({refs: (window.__wsRefs||[]).length, token: (window.__lastToken||'').length})"
JS_TOKEN = r"mw:() => window.__lastToken || ''"

def js_subscribe(csv: str, token: str) -> str:
    # Escape any problematic chars for JS string literal
    csv_safe = csv.replace("'", "\\'")
    token_safe = token.replace("'", "\\'")
    return f"""mw:() => {{
        let ws = (window.__wsRefs || []).find(w => w.url && w.url.includes('premws') && w.readyState === 1);
        if (!ws) return 'no_ws:' + (window.__wsRefs||[]).length;
        ws.send(String.fromCharCode(0x16) + String.fromCharCode(0) + '{csv_safe},A_{token_safe}' + String.fromCharCode(1));
        return 'ok';
    }}"""


def _detail_topic(ev):
    if not ev or not ev.topic: return None
    return "6V" + ev.topic[2:] if ev.topic.startswith("OV") else None

def _ce_dict(ce):
    d = {"change_type": ce.change_type.value, "entity_type": ce.entity_type,
         "entity_id": ce.entity_id, "topic": ce.topic}
    for k in ("old_value", "new_value"):
        v = getattr(ce, k)
        if v is None: d[k] = None
        elif isinstance(v, (dict, str, int, float, bool)): d[k] = v
        else: d[k] = {"id": getattr(v, "id", ""), "name": getattr(v, "name", "")}
    return d


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------
class LiveAPI:
    def __init__(self, parser, stats):
        self.p, self.s = parser, stats
        self._ws: set = set()
        self._r = None

    async def start(self):
        app = web.Application()
        for path, h in [("/events", self.h_ev), ("/events/{sid}", self.h_sp),
                         ("/event/{eid}", self.h_det), ("/sports", self.h_sports),
                         ("/stats", self.h_stats), ("/ws", self.h_ws), ("/", self.h_idx)]:
            app.router.add_get(path, h)
        self._r = web.AppRunner(app)
        await self._r.setup()
        await web.TCPSite(self._r, API_HOST, API_PORT).start()
        log.info("API http://%s:%d", API_HOST, API_PORT)

    async def stop(self):
        for w in list(self._ws): await w.close()
        if self._r: await self._r.cleanup()

    async def cast(self, c):
        if not self._ws: return
        j = json.dumps(_ce_dict(c))
        for w in list(self._ws):
            try: await w.send_str(j)
            except: self._ws.discard(w)

    async def h_idx(self, _): return web.json_response({"service": "bet365 single-tab all-markets"})
    async def h_ev(self, _): return web.json_response(self.p.get_all_live_events())
    async def h_sp(self, r): return web.json_response(self.p.get_sport_events(r.match_info["sid"]))
    async def h_det(self, r):
        d = self.p.get_event(r.match_info["eid"])
        return web.json_response(d) if d else web.json_response({"error": "not found"}, status=404)
    async def h_sports(self, _):
        return web.json_response([{"id": sid, "name": s.name,
            "events": sum(len(self.p.state.comp_events.get(c, set())) for c in self.p.state.sport_competitions.get(sid, set()))}
            for sid, s in self.p.state.sports.items()])
    async def h_stats(self, _):
        ps = self.p.summary(); t = ps.get("events", 0)
        wm = sum(1 for e in self.p.state.events if self.p.state.event_markets.get(e))
        return web.json_response({
            "uptime": round(time.time() - self.s["t0"], 1),
            "frames": self.s.get("f", 0), "topics": self.s.get("topics", 0),
            "events": t, "with_markets": wm,
            "coverage": round(100 * wm / t, 1) if t else 0,
            "parser": ps, "ws_subs": len(self._ws),
        })
    async def h_ws(self, r):
        w = web.WebSocketResponse(); await w.prepare(r); self._ws.add(w)
        try:
            await w.send_json({"type": "snapshot", "events": len(self.p.state.events)})
            async for m in w:
                if m.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE): break
        finally: self._ws.discard(w)
        return w


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
class Stream:
    def __init__(self):
        self.parser = ZapParser()
        self.stats = {"t0": time.time(), "f": 0, "topics": 0}
        self.api = LiveAPI(self.parser, self.stats)
        self._go = True
        self._subbed: set[str] = set()
        self.parser.on_change(lambda c: asyncio.ensure_future(self.api.cast(c)))

    async def run(self):
        log.info("=" * 60)
        log.info("bet365 Single-Tab All-Markets Streamer")
        log.info("1 tab + mw: JS eval + 6V detail topics")
        log.info("=" * 60)
        await self.api.start()
        try:
            while self._go:
                try:
                    await self._session()
                except Exception:
                    log.exception("crash — retry 10s")
                    await asyncio.sleep(10)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            await self.api.stop()

    async def _session(self):
        from camoufox.async_api import AsyncCamoufox

        log.info("Launching browser...")
        kw: dict = {
            "headless": True, "humanize": True, "os": "windows",
            "main_world_eval": True,
        }
        if USE_PROXY:
            kw["proxy"] = {"server": PROXY_HTTP, "username": PROXY_USER, "password": PROXY_PASS}
            kw["geoip"] = True

        async with AsyncCamoufox(**kw) as browser:
            page = await browser.new_page()

            # Capture all WS frames for the parser
            page.on("websocket", lambda ws: asyncio.ensure_future(self._on_ws(ws)))

            log.info("Navigating to %s", TARGET_URL)
            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                log.warning("Nav: %s", e)

            # Apply the WS hook using mw: prefix (Camoufox main world)
            r = await page.evaluate(JS_HOOK)
            log.info("WS hook: %s", r)

            # Wait for WS capture + token + data
            log.info("Waiting for WS capture + data...")
            for i in range(30):
                await asyncio.sleep(2)
                st = await page.evaluate(JS_STATUS)
                if st["refs"] > 0 and st["token"] > 0:
                    break
            else:
                log.error("WS not captured after 60s — retry")
                return

            token = await page.evaluate(JS_TOKEN)
            log.info("Ready: refs=%d token=%d events=%d", st["refs"], len(token), len(self.parser.state.events))

            # Subscribe all events
            await self._subscribe_all(page, token)

            # Monitor loop
            last_scan = last_log = time.time()
            while self._go:
                await asyncio.sleep(3)
                now = time.time()

                if now - last_scan > NEW_EVENT_SCAN:
                    new = set(self.parser.state.events.keys()) - self._subbed
                    if new:
                        log.info("%d new events", len(new))
                        token = await page.evaluate(JS_TOKEN) or token
                        await self._subscribe_events(page, new, token)
                    last_scan = now

                if now - last_log > 30:
                    t = len(self.parser.state.events)
                    wm = sum(1 for e in self.parser.state.events if self.parser.state.event_markets.get(e))
                    log.info(
                        "COVERAGE: %d/%d (%.1f%%) | %d mkts | %d sels | frames=%d | 6V topics=%d",
                        wm, t, 100 * wm / t if t else 0, len(self.parser.state.markets),
                        len(self.parser.state.selections), self.stats["f"], self.stats["topics"],
                    )
                    last_log = now
                    # Check WS health
                    st = await page.evaluate(JS_STATUS)
                    if st["refs"] == 0:
                        log.warning("WS died — restarting")
                        self._subbed.clear()
                        self.stats["topics"] = 0
                        return

    async def _on_ws(self, ws):
        url = ws.url
        log.info("WS: %s", url[:80])

        def on_recv(p):
            self.stats["f"] = self.stats.get("f", 0) + 1
            try:
                self.parser.feed(p)
            except Exception:
                pass

        ws.on("framereceived", on_recv)
        ws.on("close", lambda _: log.warning("WS closed: %s", url[:60]))

    async def _subscribe_all(self, page, token):
        eids = set(self.parser.state.events.keys()) - self._subbed
        if not eids:
            return
        log.info("Subscribing %d events via 6V...", len(eids))
        await self._subscribe_events(page, eids, token)

    async def _subscribe_events(self, page, event_ids: set[str], token: str):
        topics = []
        for eid in event_ids:
            ev = self.parser.state.events.get(eid)
            t = _detail_topic(ev)
            if t:
                topics.append(t)
                self._subbed.add(eid)
        if not topics:
            return

        ok = 0
        for i in range(0, len(topics), DRIP_BATCH):
            batch = topics[i : i + DRIP_BATCH]
            csv = ",".join(batch)
            try:
                # Refresh token periodically
                if i > 0 and i % 50 == 0:
                    fresh = await page.evaluate(JS_TOKEN)
                    if fresh:
                        token = fresh
                r = await page.evaluate(js_subscribe(csv, token))
                if r == "ok":
                    ok += len(batch)
                    self.stats["topics"] = self.stats.get("topics", 0) + len(batch)
                else:
                    log.warning("sub: %s", r)
            except Exception as e:
                log.warning("sub err: %s", e)
            if i + DRIP_BATCH < len(topics):
                await asyncio.sleep(DRIP_INTERVAL)

        log.info("Subscribed %d/%d 6V topics", ok, len(topics))


# ---------------------------------------------------------------------------
async def main():
    await Stream().run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
