"""
bet365 WebSocket Capture Script
================================
Uses camoufox to open bet365 InPlay page and capture all WebSocket traffic.
Logs connection details, handshake, subscriptions, and data messages.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from camoufox.async_api import AsyncCamoufox

# Configuration
# Uses local proxy bridge (run proxy_bridge.py first) since Playwright
# doesn't support SOCKS5 auth directly
PROXY = {
    "server": "socks5://127.0.0.1:1089",
}
TARGET_URL = "https://www.bet365.es/#/IP/"
CAPTURE_DIR = Path(__file__).parent / "captures"
CAPTURE_DURATION_SECONDS = 120  # How long to capture data


class WebSocketCapture:
    def __init__(self, capture_dir: Path):
        self.capture_dir = capture_dir
        self.capture_dir.mkdir(parents=True, exist_ok=True)
        self.session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.session_dir = self.capture_dir / self.session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.ws_connections = {}
        self.message_count = 0
        self.start_time = time.time()

    def _sanitize_url(self, url: str) -> str:
        """Create a safe filename from URL."""
        # Strip query params and sanitize for Windows filenames
        url = url.split("?")[0]
        for ch in ["wss://", "ws://", "https://", "http://"]:
            url = url.replace(ch, "")
        for ch in ["/", ":", "?", "&", "=", "*", "<", ">", "|", '"']:
            url = url.replace(ch, "_")
        return url[:80]

    def _format_binary(self, data) -> str:
        """Format binary data for readable logging."""
        if isinstance(data, bytes):
            # Show hex + printable chars
            hex_str = data.hex()
            printable = "".join(chr(b) if 32 <= b < 127 else f"\\x{b:02x}" for b in data)
            return f"[BINARY len={len(data)}] hex={hex_str[:200]}... printable={printable[:200]}"
        return str(data)

    def _parse_zap_message(self, data) -> dict:
        """Attempt to parse ZAP protocol message."""
        parsed = {
            "raw_length": len(data) if data else 0,
            "type": "unknown",
        }

        if not data:
            return parsed

        raw = data if isinstance(data, bytes) else data.encode("utf-8", errors="replace")

        # Check first byte for message type
        if len(raw) > 0:
            first_byte = raw[0] if isinstance(raw[0], int) else ord(raw[0])
            type_map = {
                0x00: "CLIENT_CONNECT",
                0x01: "CLIENT_POLL",
                0x02: "CLIENT_SEND",
                0x14: "INITIAL_TOPIC_LOAD",
                0x15: "DELTA",
                0x16: "CLIENT_SUBSCRIBE",
                0x17: "CLIENT_UNSUBSCRIBE",
                0x18: "SERVER_PING",
                0x19: "CLIENT_PING",
                0x1C: "CLIENT_ABORT",
                0x1D: "CLIENT_CLOSE",
                0x1E: "ACK_ITL",
                0x1F: "ACK_DELTA",
                0x20: "ACK_RESPONSE",
                0x23: "TOPIC_STATUS_NOTIFICATION",
            }
            parsed["first_byte"] = f"0x{first_byte:02x}"
            parsed["type"] = type_map.get(first_byte, f"UNKNOWN(0x{first_byte:02x})")

        # Try to extract readable content
        if isinstance(data, str):
            parsed["text_preview"] = data[:500]
            # Look for topic patterns
            for pattern in ["OVInPlay", "InPlay", "CONFIG", "Media", "XL_", "__time", "__host", "SPTBK"]:
                if pattern in data:
                    parsed.setdefault("topics_found", []).append(pattern)
            # Look for entity types
            for entity in ["CL;", "CT;", "EV;", "MA;", "PA;", "SC;", "MG;"]:
                if entity in data:
                    parsed.setdefault("entities_found", []).append(entity.rstrip(";"))
        else:
            try:
                text = data.decode("utf-8", errors="replace")
                parsed["text_preview"] = text[:500]
            except Exception:
                parsed["text_preview"] = self._format_binary(data)[:500]

        return parsed

    async def on_websocket(self, ws):
        """Handle new WebSocket connection."""
        url = ws.url
        ws_id = f"ws_{len(self.ws_connections)}"
        timestamp = datetime.now(timezone.utc).isoformat()
        elapsed = time.time() - self.start_time

        print(f"\n{'='*80}")
        print(f"[{elapsed:.1f}s] NEW WebSocket connection: {url}")
        print(f"{'='*80}")

        # Create log file for this connection
        ws_file = self.session_dir / f"{ws_id}_{self._sanitize_url(url)}.jsonl"
        raw_file = self.session_dir / f"{ws_id}_raw.log"

        conn_info = {
            "ws_id": ws_id,
            "url": url,
            "opened_at": timestamp,
            "file": str(ws_file),
        }
        self.ws_connections[ws_id] = conn_info

        # Log connection info
        with open(self.session_dir / "connections.json", "w") as f:
            json.dump(self.ws_connections, f, indent=2)

        def on_frame_sent(payload):
            self.message_count += 1
            elapsed = time.time() - self.start_time
            parsed = self._parse_zap_message(payload)
            entry = {
                "direction": "SENT",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "elapsed_s": round(elapsed, 3),
                "msg_num": self.message_count,
                "parsed": parsed,
            }

            # Console output
            type_str = parsed.get("type", "?")
            topics = parsed.get("topics_found", [])
            preview = parsed.get("text_preview", "")[:100]
            print(f"[{elapsed:.1f}s] >>> SENT [{type_str}] topics={topics} | {preview}")

            # File output
            with open(ws_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            with open(raw_file, "a", encoding="utf-8") as f:
                f.write(f"\n--- SENT {entry['timestamp']} ---\n")
                if isinstance(payload, str):
                    f.write(payload + "\n")
                else:
                    f.write(repr(payload) + "\n")

        def on_frame_received(payload):
            self.message_count += 1
            elapsed = time.time() - self.start_time
            parsed = self._parse_zap_message(payload)
            entry = {
                "direction": "RECV",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "elapsed_s": round(elapsed, 3),
                "msg_num": self.message_count,
                "parsed": parsed,
            }

            # Console output (abbreviated for high-frequency messages)
            type_str = parsed.get("type", "?")
            topics = parsed.get("topics_found", [])
            entities = parsed.get("entities_found", [])
            data_len = parsed.get("raw_length", 0)
            print(f"[{elapsed:.1f}s] <<< RECV [{type_str}] len={data_len} topics={topics} entities={entities}")

            # File output
            with open(ws_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            with open(raw_file, "a", encoding="utf-8") as f:
                f.write(f"\n--- RECV {entry['timestamp']} ---\n")
                if isinstance(payload, str):
                    f.write(payload + "\n")
                else:
                    f.write(repr(payload) + "\n")

        def on_close(_):
            elapsed = time.time() - self.start_time
            print(f"[{elapsed:.1f}s] WebSocket CLOSED: {url}")
            conn_info["closed_at"] = datetime.now(timezone.utc).isoformat()
            with open(self.session_dir / "connections.json", "w") as f:
                json.dump(self.ws_connections, f, indent=2)

        def on_error(error):
            elapsed = time.time() - self.start_time
            print(f"[{elapsed:.1f}s] WebSocket ERROR: {url} - {error}")

        ws.on("framesent", on_frame_sent)
        ws.on("framereceived", on_frame_received)
        ws.on("close", on_close)
        ws.on("socketerror", on_error)


async def capture_session():
    """Main capture session."""
    capture = WebSocketCapture(CAPTURE_DIR)

    print(f"Starting capture session: {capture.session_id}")
    print(f"Output directory: {capture.session_dir}")
    print(f"Proxy: {PROXY['server']}")
    print(f"Target: {TARGET_URL}")
    print(f"Duration: {CAPTURE_DURATION_SECONDS}s")
    print()

    async with AsyncCamoufox(
        headless=False,
        proxy=PROXY,
        geoip="188.126.20.21",
        humanize=True,
        os="windows",
    ) as browser:
        page = await browser.new_page()

        # Capture all HTTP requests/responses for auth analysis
        request_log = capture.session_dir / "http_requests.jsonl"

        async def on_request(request):
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "method": request.method,
                "url": request.url,
                "headers": dict(request.headers),
            }
            # Look for important auth-related requests
            url = request.url.lower()
            if any(k in url for k in ["sports-configuration", "defaultapi", "inplaydiaryapi", "nst", "sync-term"]):
                print(f"[AUTH] {request.method} {request.url}")
                entry["important"] = True
            with open(request_log, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        async def on_response(response):
            url = response.url.lower()
            if any(k in url for k in ["sports-configuration", "defaultapi", "inplaydiaryapi"]):
                try:
                    body = await response.text()
                    resp_file = capture.session_dir / f"response_{url.split('/')[-1][:50]}.json"
                    with open(resp_file, "w", encoding="utf-8") as f:
                        f.write(body)
                    print(f"[AUTH RESPONSE] {response.url} -> saved to {resp_file.name}")
                except Exception as e:
                    print(f"[AUTH RESPONSE] Failed to capture body: {e}")

        page.on("request", on_request)
        page.on("response", on_response)

        # Register WebSocket handler BEFORE navigation
        page.on("websocket", lambda ws: asyncio.ensure_future(capture.on_websocket(ws)))

        # Also capture cookies after page load
        print("Navigating to bet365 InPlay...")
        try:
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
            print("Page loaded (domcontentloaded)")
        except Exception as e:
            print(f"Navigation note: {e}")

        # Wait a bit for page to fully initialize
        await asyncio.sleep(5)

        # Capture cookies
        cookies = await page.context.cookies()
        with open(capture.session_dir / "cookies.json", "w") as f:
            json.dump(cookies, f, indent=2)
        print(f"\nCaptured {len(cookies)} cookies")

        # Look for important cookies
        for c in cookies:
            if c["name"] in ("pstk", "aps03", "rmbs", "session"):
                print(f"  [AUTH COOKIE] {c['name']}={c['value'][:50]}...")

        # Capture page source for NST token analysis
        try:
            page_source = await page.content()
            with open(capture.session_dir / "page_source.html", "w", encoding="utf-8") as f:
                f.write(page_source)
            print("Saved page source")

            # Try to extract NST-related JavaScript
            nst_scripts = await page.evaluate("""() => {
                const scripts = document.querySelectorAll('script');
                const relevant = [];
                for (const s of scripts) {
                    const text = s.textContent || '';
                    if (text.includes('var a=') || text.includes('nst') || text.includes('NST') ||
                        text.includes('session') || text.includes('SESSION')) {
                        relevant.push(text.substring(0, 2000));
                    }
                }
                return relevant;
            }""")
            if nst_scripts:
                with open(capture.session_dir / "nst_scripts.json", "w", encoding="utf-8") as f:
                    json.dump(nst_scripts, f, indent=2)
                print(f"Found {len(nst_scripts)} potentially relevant scripts")
        except Exception as e:
            print(f"Source capture error: {e}")

        # Now wait and capture WebSocket traffic
        print(f"\n{'='*80}")
        print(f"Capturing WebSocket traffic for {CAPTURE_DURATION_SECONDS}s...")
        print(f"{'='*80}\n")

        await asyncio.sleep(CAPTURE_DURATION_SECONDS)

        # Final summary
        print(f"\n{'='*80}")
        print(f"CAPTURE COMPLETE")
        print(f"{'='*80}")
        print(f"Session: {capture.session_id}")
        print(f"Directory: {capture.session_dir}")
        print(f"WebSocket connections: {len(capture.ws_connections)}")
        print(f"Total messages captured: {capture.message_count}")
        for ws_id, info in capture.ws_connections.items():
            print(f"  {ws_id}: {info['url']}")
        print()


if __name__ == "__main__":
    asyncio.run(capture_session())
