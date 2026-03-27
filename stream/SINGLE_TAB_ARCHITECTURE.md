# bet365 Single-Tab All-Markets Streamer — Architecture & Lessons

## The Goal
Get ALL markets, odds, and selections for ALL live events in real-time from bet365, using only **1 browser tab** (no spawning 30+ browsers/tabs).

## The Key Breakthrough: `mw:` Prefix

Camoufox (Firefox-based anti-detect browser) has a critical distinction between:
- **Playwright's utility world** — where `page.evaluate()` and `addInitScript()` run by default
- **The page's actual main world** — where bet365's JavaScript runs

Even with `main_world_eval=True`, Playwright's evaluations run in an isolated context. The `mw:` prefix is Camoufox's directive to force evaluation in the **page's real JavaScript main world**.

```python
# This runs in Playwright's utility world (FAILS — bet365 can't see our patches):
await page.evaluate('() => { WebSocket.prototype.send = ... }')

# This runs in the PAGE's actual main world (WORKS):
await page.evaluate('mw:() => { WebSocket.prototype.send = ... }')
```

## Problems Encountered & Why They Failed

### 1. Direct Python WebSocket connections (websockets library)
- **Result**: HTTP 403 Forbidden
- **Reason**: bet365 uses TLS fingerprinting (JA3). Python's `ssl` module has a completely different TLS fingerprint than Firefox. The server rejects non-browser connections.

### 2. curl_cffi with browser TLS impersonation
- **Result**: HTTP 400 Bad Request
- **Reason**: curl_cffi's WebSocket implementation doesn't properly support the `zap-protocol-v1` subprotocol required by bet365's premws server.

### 3. mitmproxy WebSocket frame injection
- **Result**: Cloudflare blocks the page
- **Reason**: mitmproxy acts as a MITM — the upstream TLS connection is made by mitmproxy (Python), not the browser. Cloudflare's bot detection sees the Python TLS fingerprint and blocks.

### 4. JavaScript patching via `page.evaluate()` (without `mw:`)
- **Result**: Patch applied but never triggered (sendCount=0)
- **Reason**: The patch runs in Playwright's utility world. Even though `window.__wsPatched = true` is visible, the actual `WebSocket.prototype.send` in the utility world is a *different object* than the one in the page's main world (Firefox Xray wrappers).

### 5. `addInitScript()` with WebSocket constructor override
- **Result**: Constructor replaced but never called (instances=0)
- **Reason**: Same world isolation issue. `addInitScript` runs in Playwright's context. bet365's JS uses the main world's native `WebSocket` constructor, which is untouched.

### 6. `page.route()` for HTML/JS injection
- **Result**: Route handler never called (intercepted 0 files)
- **Reason**: Camoufox doesn't support `page.route()` for request interception. Also, bet365 loads ALL JavaScript via `fetch`/`xhr` (not `<script>` tags), then `eval()`s it — an anti-tampering technique.

### 7. Web Worker theory
- **Result**: Disproven — `page.workers` returns 0 workers
- **Reason**: bet365 does NOT use Web Workers for WebSocket. The WS is in the main thread.

### 8. `Object.defineProperty` to make WebSocket non-writable
- **Result**: Property set, but bet365's code still uses the native constructor
- **Reason**: Same utility world vs main world isolation.

## The Working Solution

### Architecture
```
┌─────────────────────────────────────────────────────┐
│  1 Camoufox Browser (headless)                      │
│  ┌───────────────────────────────────────────┐      │
│  │  bet365.es InPlay page                    │      │
│  │                                           │      │
│  │  WebSocket.prototype.send PATCHED (mw:)   │      │
│  │  ├── Captures premws WS reference         │      │
│  │  ├── Extracts auth token from sends       │      │
│  │  └── window.__wsRefs = [premws_ws]        │      │
│  │                                           │      │
│  │  Overview WS ──────► ZapParser            │      │
│  │    (all events, scores, basic data)       │      │
│  │                                           │      │
│  │  6V Subscribe (injected via mw: eval)     │      │
│  │    ──────► Same premws WS                 │      │
│  │    ──────► Server returns ALL markets     │      │
│  │    ──────► ZapParser gets full detail     │      │
│  └───────────────────────────────────────────┘      │
│                                                     │
│  Playwright captures ALL WS frames                  │
│  via page.on("websocket") → framereceived           │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │    ZapParser    │
              │  (state tree)   │
              │                 │
              │  sports         │
              │  competitions   │
              │  events         │
              │  markets        │
              │  selections     │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │  aiohttp API    │
              │  port 8365      │
              │                 │
              │  /events        │
              │  /event/{id}    │
              │  /sports        │
              │  /stats         │
              │  /ws (realtime) │
              └─────────────────┘
```

### Topic Format
- **Overview topic**: `OV{eventID}C{sportCode}A_{locale}_{zone}` — returns basic event data
- **Detail topic (6V)**: `6V{eventID}C{sportCode}A_{locale}_{zone}` — returns ALL markets, selections, odds

The `6V` prefix tells the server to send the full event detail payload. Without it, you only get 1-2 top markets per event. With it, you get 200-300+ markets per event.

### Subscribe Message Format (ZAP Protocol)
```
0x16 + 0x00 + "topic1,topic2,...,A_<auth_token>" + 0x01
```

### Drip-Feed Strategy
- Subscribe in batches of 5 topics per message
- 0.5s delay between batches
- Refresh token every 50 topics
- Scan for new events every 10 seconds

## Results (Single Tab)
- **580+ events** across 18 sports
- **3,900+ markets** with full odds
- **16,700+ selections** with decimal odds
- **337 markets** on a single baseball game
- **2.4 MB** events API response
- Real-time updates via WebSocket feed on `/ws`

## Key Files
- `stream/single_tab.py` — Main streamer (1 tab, mw: eval, 6V subscribe)
- `protocol/zap_parser.py` — ZAP protocol parser + state tree
- `stream/ws_injector.py` — mitmproxy addon (attempted, not needed)
