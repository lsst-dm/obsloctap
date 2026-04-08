"""Render the obsloctap skymap to a local HTML file and open it in a browser.

Loads a local fixture by default; use --live or --refresh-live to hit the
deployed service.

Usage (from the repo root):
    python3.13 scripts/render_skymap.py
    python3.13 scripts/render_skymap.py --live [--time 48] [--start now]
    python3.13 scripts/render_skymap.py --refresh-live
    [--time 48] [--start now]
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import tempfile
import webbrowser
from typing import Any

from obsloctap.models import Obsplan
from obsloctap.skymap import make_sky_html

_REPO_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

LIVE_URL = "https://usdf-rsp.slac.stanford.edu/obsloctap/schedule"
LIVE_PREFIX = "https://usdf-rsp.slac.stanford.edu/obsloctap"
_FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "schedule_48h.json"


def _should_open_browser(no_open: bool) -> bool:
    if no_open:
        return False
    return os.environ.get("CI", "").lower() not in {"1", "true", "yes"}


def _fetch_live_data(start: str, time: int) -> list[dict[str, Any]] | None:
    import requests

    params: dict[str, Any] = {"time": time}
    if start.lower() != "now":
        params["start"] = start

    print(
        f"Checking live service at {LIVE_URL} with params {params} …",
        flush=True,
    )
    try:
        resp = requests.get(LIVE_URL, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as exc:
        print(f"Live service is down or not returning data: {exc}")
        return None
    except ValueError as exc:
        print(f"Live service returned invalid JSON: {exc}")
        return None

    if not raw:
        print("Live service is up but returned no schedule data.")
        return None

    print(f"  → {len(raw)} observations received")
    return raw


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--time",
        type=int,
        default=48,
        help="Lookahead window in hours (default: 48)",
    )
    parser.add_argument(
        "--start",
        default="now",
        help="Start time: 'now', ISO, or MJD (default: now)",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Write the HTML file but do not open a browser",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Render using the live service, but do not refresh the fixture",
    )
    parser.add_argument(
        "--refresh-live",
        action="store_true",
        help=(
            f"Fetch the live service and overwrite "
            f"{_FIXTURE.relative_to(_REPO_ROOT)}."
        ),
    )
    args = parser.parse_args()

    if args.live and args.refresh_live:
        print("Choose only one of --live or --refresh-live.")
        sys.exit(2)

    if args.live or args.refresh_live:
        raw = _fetch_live_data(args.start, args.time)
        if raw is None:
            sys.exit(0)
        if args.refresh_live:
            _FIXTURE.parent.mkdir(parents=True, exist_ok=True)
            _FIXTURE.write_text(json.dumps(raw))
            print(f"  → Fixture updated: {_FIXTURE.relative_to(_REPO_ROOT)}")
    else:
        if not _FIXTURE.exists():
            print(f"Fixture not found: {_FIXTURE}")
            print(
                "Refresh it explicitly with "
                "'python scripts/render_skymap.py --refresh-live'."
            )
            sys.exit(1)
        print(f"Loading fixture {_FIXTURE} …", flush=True)
        raw = json.loads(_FIXTURE.read_text())
        print(f"  → {len(raw)} observations loaded from fixture")

    schedule = [Obsplan(**obs) for obs in raw]

    html = make_sky_html(
        schedule,
        start_val=args.start,
        time_val=args.time,
        path_prefix=LIVE_PREFIX,
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", prefix="skymap_", delete=False
    ) as fh:
        fh.write(html)
        path = fh.name

    print(f" -- Wrote {path}")
    if _should_open_browser(args.no_open):
        webbrowser.open(f"file://{path}")  # noqa: E231
        print(" -- Opened in browser")
    else:
        print(f"  → Open manually: file://{path}")  # noqa: E231


if __name__ == "__main__":
    main()
