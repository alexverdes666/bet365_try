"""
Coverage Validation & Monitoring Tool
======================================
Connects to the bet365 live stream API (port 8365) and validates/reports
complete data coverage across all sports and events.

Usage:
    python -m stream.validate
    python -m stream.validate --once          # single check, no loop
    python -m stream.validate --interval 30   # custom refresh interval
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = "http://localhost:8365"
DEFAULT_INTERVAL = 60  # seconds between checks


# ---------------------------------------------------------------------------
# API helpers (stdlib only -- no dependencies beyond Python 3.9+)
# ---------------------------------------------------------------------------

def api_get(endpoint: str, timeout: int = 10) -> Any:
    """Fetch JSON from the live stream API. Returns parsed data or None on error."""
    url = f"{API_BASE}{endpoint}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        return None
    except Exception as exc:
        return None


def fetch_all() -> Tuple[Optional[list], Optional[list], Optional[dict]]:
    """Fetch events, sports, and stats from the API. Returns (events, sports, stats)."""
    events = api_get("/events")
    sports = api_get("/sports")
    stats = api_get("/stats")
    return events, sports, stats


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

class CoverageSnapshot:
    """A single point-in-time coverage analysis."""

    def __init__(
        self,
        events: List[Dict[str, Any]],
        sports: List[Dict[str, Any]],
        stats: Dict[str, Any],
    ) -> None:
        self.timestamp = datetime.now(timezone.utc)
        self.events = events
        self.sports_list = sports
        self.stats = stats

        # Build sport name lookup from /sports endpoint
        self.sport_names: Dict[str, str] = {}
        for s in self.sports_list:
            self.sport_names[str(s.get("id", ""))] = s.get("name", f"Sport {s.get('id', '?')}")

        # Group events by sport_id
        self.by_sport: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for ev in self.events:
            sid = str(ev.get("sport_id", "unknown"))
            self.by_sport[sid].append(ev)
            # Enrich sport_names from event data if missing
            if sid not in self.sport_names:
                self.sport_names[sid] = ev.get("sport_name", f"Sport {sid}")

        self.sport_reports = self._analyse_sports()
        self.overall = self._compute_overall()
        self.gaps = self._find_gaps()

    def _analyse_sports(self) -> List[Dict[str, Any]]:
        """Build per-sport coverage report."""
        reports = []
        for sid in sorted(self.by_sport.keys()):
            evts = self.by_sport[sid]
            total = len(evts)
            with_markets = sum(1 for e in evts if e.get("markets"))
            with_full = sum(1 for e in evts if len(e.get("markets", [])) > 1)
            active = sum(
                1 for e in evts
                if e.get("score") or e.get("minute") or e.get("time_type") == "1"
            )
            pre_match = total - active

            coverage_pct = (with_markets / total * 100) if total else 0.0

            reports.append({
                "sport_id": sid,
                "sport_name": self.sport_names.get(sid, f"Sport {sid}"),
                "total": total,
                "with_markets": with_markets,
                "with_full": with_full,
                "active": active,
                "pre_match": pre_match,
                "coverage_pct": coverage_pct,
            })

        # Sort by total events descending for readability
        reports.sort(key=lambda r: r["total"], reverse=True)
        return reports

    def _compute_overall(self) -> Dict[str, Any]:
        total = len(self.events)
        with_markets = sum(r["with_markets"] for r in self.sport_reports)
        with_full = sum(r["with_full"] for r in self.sport_reports)
        active = sum(r["active"] for r in self.sport_reports)
        coverage_pct = (with_markets / total * 100) if total else 0.0
        full_pct = (with_full / total * 100) if total else 0.0

        # Parser stats from /stats
        parser = self.stats.get("parser", {}) if self.stats else {}

        return {
            "total_events": total,
            "with_markets": with_markets,
            "with_full_detail": with_full,
            "active_in_play": active,
            "pre_match": total - active,
            "coverage_pct": coverage_pct,
            "full_detail_pct": full_pct,
            "sports_count": len(self.by_sport),
            "total_markets": parser.get("markets", 0),
            "total_selections": parser.get("selections", 0),
            "uptime": self.stats.get("uptime_seconds", 0) if self.stats else 0,
        }

    def _find_gaps(self) -> List[Dict[str, Any]]:
        """Identify sports or events with zero coverage."""
        gaps = []

        # Sports with no market data at all
        for r in self.sport_reports:
            if r["with_markets"] == 0 and r["total"] > 0:
                gaps.append({
                    "type": "sport",
                    "sport_id": r["sport_id"],
                    "sport_name": r["sport_name"],
                    "events": r["total"],
                    "reason": "0 events have any market data",
                })

        # Sports listed in /sports but with no events at all
        for s in self.sports_list:
            sid = str(s.get("id", ""))
            if sid and sid not in self.by_sport:
                gaps.append({
                    "type": "sport_empty",
                    "sport_id": sid,
                    "sport_name": s.get("name", "?"),
                    "events": 0,
                    "reason": "sport listed but has 0 events",
                })

        # Individual events with no markets (only report if most others do)
        for r in self.sport_reports:
            if r["coverage_pct"] > 50 and r["with_markets"] < r["total"]:
                missing = r["total"] - r["with_markets"]
                gaps.append({
                    "type": "partial_sport",
                    "sport_id": r["sport_id"],
                    "sport_name": r["sport_name"],
                    "events": missing,
                    "reason": f"{missing}/{r['total']} events missing market data",
                })

        return gaps


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"

# Disable colours when not a terminal or on Windows without VT support
_NO_COLOR = os.environ.get("NO_COLOR") or not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty()
if _NO_COLOR:
    RESET = BOLD = DIM = GREEN = YELLOW = RED = CYAN = WHITE = ""


def _pct_color(pct: float) -> str:
    if pct >= 95:
        return GREEN
    elif pct >= 50:
        return YELLOW
    return RED


def _bar(pct: float, width: int = 20) -> str:
    filled = int(pct / 100 * width)
    empty = width - filled
    color = _pct_color(pct)
    return f"{color}{'█' * filled}{'░' * empty}{RESET}"


def _fmt_uptime(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


def clear_screen() -> None:
    """Clear terminal screen cross-platform."""
    if os.name == "nt":
        os.system("cls")
    else:
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def render(snap: CoverageSnapshot, run_number: int, history: List[Dict[str, Any]]) -> str:
    """Build the complete formatted output."""
    lines: List[str] = []
    ov = snap.overall
    ts = snap.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── Header ─────────────────────────────────────────────────
    lines.append("")
    lines.append(f"  {BOLD}{CYAN}bet365 Coverage Validator{RESET}  {DIM}run #{run_number}  {ts}{RESET}")
    lines.append(f"  {DIM}API: {API_BASE}  |  Uptime: {_fmt_uptime(ov['uptime'])}{RESET}")
    lines.append("")

    # ── Overall Summary ────────────────────────────────────────
    cpct = ov["coverage_pct"]
    fpct = ov["full_detail_pct"]
    lines.append(f"  {BOLD}Overall Coverage{RESET}")
    lines.append(
        f"    Events: {ov['total_events']:>5}   "
        f"Markets: {ov['total_markets']:>6}   "
        f"Selections: {ov['total_selections']:>7}"
    )
    lines.append(
        f"    With any data:   {ov['with_markets']:>5}/{ov['total_events']}"
        f"  {_bar(cpct)} {_pct_color(cpct)}{cpct:5.1f}%{RESET}"
    )
    lines.append(
        f"    With full detail:{ov['with_full_detail']:>5}/{ov['total_events']}"
        f"  {_bar(fpct)} {_pct_color(fpct)}{fpct:5.1f}%{RESET}"
    )
    lines.append(
        f"    In-play: {ov['active_in_play']:>5}   Pre-match: {ov['pre_match']:>5}   "
        f"Sports: {ov['sports_count']}"
    )
    lines.append("")

    # ── Per-sport Table ────────────────────────────────────────
    header = (
        f"  {'Sport':<25} {'Total':>6} {'w/Data':>6} {'Full':>6} "
        f"{'Live':>5} {'Pre':>5}  {'Coverage':>20}"
    )
    lines.append(f"  {BOLD}Per-Sport Breakdown{RESET}")
    lines.append(f"  {'-' * 88}")
    lines.append(f"{BOLD}{header}{RESET}")
    lines.append(f"  {'-' * 88}")

    for r in snap.sport_reports:
        pct = r["coverage_pct"]
        name = r["sport_name"][:24]
        color = _pct_color(pct)
        lines.append(
            f"  {name:<25} {r['total']:>6} {r['with_markets']:>6} "
            f"{r['with_full']:>6} {r['active']:>5} {r['pre_match']:>5}  "
            f"{_bar(pct, 14)} {color}{pct:5.1f}%{RESET}"
        )

    lines.append(f"  {'-' * 88}")
    lines.append("")

    # ── Gaps ───────────────────────────────────────────────────
    if snap.gaps:
        lines.append(f"  {BOLD}{RED}Coverage Gaps ({len(snap.gaps)}){RESET}")
        for g in snap.gaps:
            icon = "!!" if g["type"] == "sport" else "--"
            lines.append(
                f"    {RED}{icon}{RESET} {g['sport_name']:<25} "
                f"{DIM}[{g['sport_id']}]{RESET}  {g['reason']}"
            )
        lines.append("")
    else:
        lines.append(f"  {GREEN}No coverage gaps detected -- all sports have data.{RESET}")
        lines.append("")

    # ── Progress over time ─────────────────────────────────────
    if len(history) > 1:
        lines.append(f"  {BOLD}Progress (last {min(len(history), 10)} checks){RESET}")
        lines.append(
            f"  {'Time':>10}  {'Events':>7}  {'Markets':>8}  "
            f"{'Coverage':>9}  {'Full':>9}  {'Delta':>7}"
        )
        display = history[-10:]
        for i, h in enumerate(display):
            delta = ""
            if i > 0:
                diff = h["coverage_pct"] - display[i - 1]["coverage_pct"]
                if diff > 0:
                    delta = f"{GREEN}+{diff:.1f}%{RESET}"
                elif diff < 0:
                    delta = f"{RED}{diff:.1f}%{RESET}"
                else:
                    delta = f"{DIM}  0.0%{RESET}"
            lines.append(
                f"  {h['time']:>10}  {h['events']:>7}  {h['markets']:>8}  "
                f"{h['coverage_pct']:>8.1f}%  {h['full_pct']:>8.1f}%  {delta:>7}"
            )
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_once(run_number: int, history: List[Dict[str, Any]]) -> Optional[CoverageSnapshot]:
    """Execute one validation cycle. Returns the snapshot or None if API unreachable."""
    events, sports, stats = fetch_all()

    if events is None:
        return None

    # Fallback for empty responses
    if sports is None:
        sports = []
    if stats is None:
        stats = {}

    snap = CoverageSnapshot(events, sports, stats)

    # Record history entry
    ov = snap.overall
    history.append({
        "time": snap.timestamp.strftime("%H:%M:%S"),
        "events": ov["total_events"],
        "markets": ov["total_markets"],
        "coverage_pct": ov["coverage_pct"],
        "full_pct": ov["full_detail_pct"],
    })

    output = render(snap, run_number, history)

    clear_screen()
    print(output)

    return snap


def main() -> None:
    parser = argparse.ArgumentParser(
        description="bet365 coverage validation & monitoring tool",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single check and exit (no periodic refresh)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Seconds between checks (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear screen between refreshes",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output raw JSON instead of formatted table",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8365,
        help="API port (default: 8365)",
    )
    args = parser.parse_args()

    global API_BASE
    API_BASE = f"http://localhost:{args.port}"

    if args.no_clear:
        global clear_screen
        clear_screen = lambda: None  # noqa: E731

    history: List[Dict[str, Any]] = []
    run_number = 0

    print(f"\n  Connecting to {API_BASE} ...")

    if args.once:
        run_number += 1
        snap = run_once(run_number, history)
        if snap is None:
            print(f"\n  ERROR: Cannot reach API at {API_BASE}")
            print("  Is the live stream server running?  (python -m stream.live_stream)\n")
            sys.exit(1)
        if args.json_output:
            print(json.dumps({
                "overall": snap.overall,
                "sports": snap.sport_reports,
                "gaps": snap.gaps,
            }, indent=2))
        sys.exit(0)

    # Periodic loop
    try:
        while True:
            run_number += 1
            snap = run_once(run_number, history)
            if snap is None:
                print(f"\n  WARNING: API unreachable at {API_BASE} -- retrying in {args.interval}s ...")
                if run_number == 1:
                    print("  Is the live stream server running?  (python -m stream.live_stream)\n")

            if args.json_output and snap:
                print(json.dumps({
                    "overall": snap.overall,
                    "sports": snap.sport_reports,
                    "gaps": snap.gaps,
                }, indent=2))

            # Sleep in small increments to allow quick Ctrl+C response
            for _ in range(args.interval * 2):
                time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"\n  Stopped after {run_number} checks.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
