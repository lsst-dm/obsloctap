#!/usr/bin/env python3
"""
Monitor the obsloctap schedule endpoint periodically
and report timeouts or issues.

Usage:
    python3 scripts/monitor_cons_schedule.py [--interval SECONDS]
    [--timeout SECONDS] [--url URL] [--once]

Default URL:
    https://usdf-rsp.slac.stanford.edu/obsloctap/schedule?time=10
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

DEFAULT_URL = "https://usdf-rsp.slac.stanford.edu/obsloctap/schedule?time=10"

stop_flag = False


def _signal_handler(sig, frame):
    global stop_flag
    logging.debug(f"Received signal {sig}, stopping...")
    stop_flag = True


_OBSPLAN_KEYS = {
    "obs_id",
    "obsid",
    "obsId",
    "observationid",
    "observation_id",
    "obsplan",
    "obs_plan",
    "ObsPlan",
    "ObsPlanID",
}


def _has_obsplan_element(obj: Any) -> bool:
    """Recursively check if a JSON object contains keys that
    indicate an ObsPlan element."""
    if isinstance(obj, dict):
        for k in obj.keys():
            if k in _OBSPLAN_KEYS:
                return True
        # recurse
        for v in obj.values():
            if _has_obsplan_element(v):
                return True
        return False
    if isinstance(obj, list):
        for item in obj:
            if _has_obsplan_element(item):
                return True
        return False
    return False


def _extract_entries(j: Any) -> list | None:
    """Try to extract a list of entries from parsed JSON.

    Returns a list if found, otherwise None.
    """
    if j is None:
        return None
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        # common container keys that may hold entries
        for key in (
            "results",
            "items",
            "observations",
            "schedule",
            "data",
            "entries",
        ):
            if key in j and isinstance(j[key], list):
                return j[key]
        # try to find any list value in the dict
        for v in j.values():
            if isinstance(v, list):
                return v
        # last resort: treat the dict itself as a single entry list
        return [j]
    return None


def check_schedule(
    url: str, timeout: float = 10.0
) -> tuple[bool, str, int | None, str | None, bool, int, int]:
    """Call URL and return (ok, info, status_code, body,
    cert_issue, entries_count, no_obsplan_count).

    entries_count and no_obsplan_count are integers
    (0 if unavailable or on error).
    """
    cert_issue = False

    def _do_request(context):
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "obsloctap-monitor/1.0",
            },
        )
        with urllib.request.urlopen(
            req, timeout=timeout, context=context
        ) as resp:
            status = resp.getcode()
            raw = resp.read()
            try:
                body = raw.decode("utf-8")
            except Exception:
                body = raw.decode("latin-1", errors="ignore")
            return status, body

    try:
        try:
            default_ctx = ssl.create_default_context()
            status, body = _do_request(default_ctx)
        except ssl.SSLCertVerificationError:
            cert_issue = True
            try:
                unverified = ssl._create_unverified_context()
            except Exception:
                unverified = ssl.SSLContext()
                unverified.check_hostname = False
                unverified.verify_mode = ssl.CERT_NONE
            status, body = _do_request(unverified)

        # Default counts
        entries_count = 0
        no_obsplan_count = 0

        # quick checks
        lower = (body or "").lower()
        if status is not None and status >= 500:
            return (
                False,
                f"server error {status}",
                status,
                body,
                cert_issue,
                entries_count,
                no_obsplan_count,
            )
        if status is not None and status >= 400:
            return (
                False,
                f"client error {status}",
                status,
                body,
                cert_issue,
                entries_count,
                no_obsplan_count,
            )

        # If response appears to be JSON, parse and look for entries
        try:
            j = json.loads(body) if body else None
        except Exception:
            j = None

        entries = _extract_entries(j)
        if entries is not None:
            entries_count = len(entries)
            # count how many entries do NOT have an ObsPlan-like element
            no_obsplan_count = 0
            for entry in entries:
                if not _has_obsplan_element(entry):
                    no_obsplan_count += 1

        # additional JSON heuristic checks for errors
        if j is not None and isinstance(j, dict):
            if any(k in j for k in ("error", "timeout")):
                # treat as problem
                return (
                    False,
                    "json indicates error",
                    status,
                    body,
                    cert_issue,
                    entries_count,
                    no_obsplan_count,
                )

        if "timeout" in lower or "error" in lower or "exception" in lower:
            return (
                False,
                "body contains error/timeout keywords",
                status,
                body,
                cert_issue,
                entries_count,
                no_obsplan_count,
            )

        return (
            True,
            "ok",
            status,
            body,
            cert_issue,
            entries_count,
            no_obsplan_count,
        )

    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = None
        return False, f"http error {e.code}", e.code, body, cert_issue, 0, 0
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", None)
        if isinstance(reason, ssl.SSLCertVerificationError):
            cert_issue = True
            try:
                unverified = ssl._create_unverified_context()
                status, body = _do_request(unverified)
                # try to parse entries if possible
                try:
                    j = json.loads(body) if body else None
                except Exception:
                    j = None
                entries = _extract_entries(j)
                entries_count = len(entries) if entries is not None else 0
                no_obsplan_count = 0
                if entries is not None:
                    for entry in entries:
                        if not _has_obsplan_element(entry):
                            no_obsplan_count += 1
                return (
                    True,
                    "ok (cert unverified)",
                    status,
                    body,
                    cert_issue,
                    entries_count,
                    no_obsplan_count,
                )
            except Exception:
                return (
                    False,
                    f"url error: {e.reason}",
                    None,
                    None,
                    cert_issue,
                    0,
                    0,
                )
        return False, f"url error: {e.reason}", None, None, cert_issue, 0, 0
    except Exception as e:
        return False, f"exception: {e}", None, None, cert_issue, 0, 0


def run_loop(
    url: str,
    interval: float = 60.0,
    timeout: float = 10.0,
    once: bool = False,
    report_interval: float = 300.0,
) -> int:
    """Run the periodic check loop. Returns exit code.

    Press Ctrl+C to stop.
    """
    global stop_flag
    stop_flag = False

    # register signals
    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        # not all platforms support all signals
        pass

    msg = (
        f"Starting monitor: url={url} interval={interval} "
        f"timeout={timeout} once={once} report_interval={report_interval}"
    )
    logging.info(msg)

    success_count = 0
    fail_count = 0
    cert_issue_count = 0
    entries_total = 0
    no_obsplan_total = 0
    last_report = time.time()

    while not stop_flag:
        start = time.time()
        ts = datetime.now(timezone.utc).isoformat()
        ok, info, status, body, cert_issue, entries_count, no_obsplan_count = (
            check_schedule(url, timeout=timeout)
        )

        # Update counters
        if ok:
            # Treat zero entries as an error condition
            if entries_count == 0:
                fail_count += 1
                logging.error(f"{ts} ERROR no entries returned from {url}")
            else:
                success_count += 1
                entries_total += entries_count
                no_obsplan_total += no_obsplan_count
            # per-user request: do not log per-success messages
        else:
            fail_count += 1
            logging.warning(f"{ts} ISSUE {info} status={status}")
            if body:
                excerpt = body[:400].replace("\n", " ")
                logging.warning(f"Response excerpt: {excerpt}")

        if cert_issue:
            cert_issue_count += 1
            # do not log cert issues unless they manifest as failures
            # (counted)

        # periodic report (every report_interval seconds)
        now = time.time()
        if now - last_report >= report_interval:
            summary = (
                f"Periodic summary (last {int(now - last_report)} seconds): "
                f"successes={success_count} failures={fail_count} "
                f"cert_issues={cert_issue_count} "
                f"entries_total={entries_total} "
                f"no_obsplan_total={no_obsplan_total}"
            )
            logging.info(summary)
            # reset counters for next period
            success_count = 0
            fail_count = 0
            cert_issue_count = 0
            entries_total = 0
            no_obsplan_total = 0
            last_report = now

        if once:
            # treat zero-entry responses as failures for exit code
            final_ok = ok and (entries_count > 0)
            return 0 if final_ok else 2

        # sleep remainder of interval
        elapsed = time.time() - start
        to_sleep = max(0.0, interval - elapsed)
        # break early if stop requested
        for _ in range(int(to_sleep)):
            if stop_flag:
                break
            time.sleep(1)
        # sleep fractional remainder
        frac = to_sleep - int(to_sleep)
        if frac > 0 and not stop_flag:
            time.sleep(frac)

    return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Monitor obsloctap schedule endpoint"
    )
    p.add_argument(
        "--interval",
        "-i",
        type=float,
        default=60.0,
        help="seconds between checks (default 60)",
    )
    p.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=10.0,
        help="request timeout seconds (default 10)",
    )
    p.add_argument("--url", type=str, default=DEFAULT_URL, help="URL to poll")
    p.add_argument(
        "--once",
        action="store_true",
        help="perform a single check and exit with nonzero on failure",
    )
    p.add_argument(
        "--report-interval",
        type=float,
        default=300.0,
        help="seconds between summary reports (default 300)",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )
    args = parse_args(argv)
    try:
        return run_loop(
            args.url,
            interval=args.interval,
            timeout=args.timeout,
            once=args.once,
            report_interval=args.report_interval,
        )
    except KeyboardInterrupt:
        logging.debug("Interrupted by user")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
