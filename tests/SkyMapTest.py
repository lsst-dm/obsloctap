"""Test the obsloctap skymap visualisation using saved schedule data.

Loads the fixture once and exercises ``make_sky_html`` across the parameter
permutations: lookahead window (12 h / 24 h / 48 h), a non-"now" start
value, an empty schedule, and a non-empty path prefix.  No browser is opened
and no network calls are made.
"""

from __future__ import annotations

import csv
import io
import json
import pathlib
import sys
import unittest
from typing import Any

import pandas as pd

from obsloctap.models import Obsplan
from obsloctap.skymap import (
    make_schedule_csv,
    make_schedule_json,
    make_schedule_parquet,
    make_sky_html,
    make_sky_pdf,
)

_REPO_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

LIVE_PREFIX = "https://usdf-rsp.slac.stanford.edu/obsloctap"
_FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "schedule_48h.json"


class TestSkyMap(unittest.TestCase):
    """Parametric tests for ``make_sky_html`` using the local fixture."""

    schedule: list[Obsplan] = []

    @classmethod
    def setUpClass(cls) -> None:
        raw: list[dict[str, Any]] = json.loads(_FIXTURE.read_text())
        cls.schedule = [Obsplan(**obs) for obs in raw]

    # -- lookahead window permutations

    def test_skymap_12h(self) -> None:
        html = make_sky_html(
            self.schedule, start_val="now", time_val=12, path_prefix=""
        )
        self.assertIsInstance(html, str)
        self.assertGreater(len(html), 0)

    def test_skymap_24h(self) -> None:
        html = make_sky_html(
            self.schedule, start_val="now", time_val=24, path_prefix=""
        )
        self.assertIsInstance(html, str)
        self.assertGreater(len(html), 0)

    def test_skymap_48h(self) -> None:
        html = make_sky_html(
            self.schedule, start_val="now", time_val=48, path_prefix=""
        )
        self.assertIsInstance(html, str)
        self.assertGreater(len(html), 0)

    # -- start-time permutations

    def test_skymap_start_iso(self) -> None:
        html = make_sky_html(
            self.schedule,
            start_val="2025-01-01 00:00:00",
            time_val=48,
            path_prefix="",
        )
        self.assertIsInstance(html, str)
        self.assertGreater(len(html), 0)

    # -- edge cases

    def test_skymap_empty_schedule(self) -> None:
        html = make_sky_html([], start_val="now", time_val=48, path_prefix="")
        self.assertIsInstance(html, str)

    def test_skymap_path_prefix(self) -> None:
        prefix = "/obsloctap"
        html = make_sky_html(
            self.schedule, start_val="now", time_val=48, path_prefix=prefix
        )
        self.assertIsInstance(html, str)
        self.assertIn(prefix, html)

    # Data export tests

    def test_skymap_pdf(self) -> None:
        pdf = make_sky_pdf(self.schedule, start_val="now", time_val=48)
        self.assertIsInstance(pdf, bytes)
        self.assertTrue(pdf.startswith(b"%PDF"), "Output is not a valid PDF")
        out_dir = pathlib.Path(__file__).parent / "output"
        out_dir.mkdir(exist_ok=True)
        out = out_dir / "skymap_test.pdf"
        out.write_bytes(pdf)
        self.assertTrue(out.exists())

    def test_schedule_json(self) -> None:
        data = make_schedule_json(self.schedule)
        self.assertIsInstance(data, str)
        parsed = json.loads(data)
        self.assertEqual(len(parsed), len(self.schedule))
        out_dir = pathlib.Path(__file__).parent / "output"
        out_dir.mkdir(exist_ok=True)
        (out_dir / "schedule.json").write_text(data)

    def test_schedule_csv(self) -> None:
        data = make_schedule_csv(self.schedule)
        self.assertIsInstance(data, str)
        rows = list(csv.DictReader(io.StringIO(data)))
        self.assertEqual(len(rows), len(self.schedule))
        out_dir = pathlib.Path(__file__).parent / "output"
        out_dir.mkdir(exist_ok=True)
        (out_dir / "schedule.csv").write_text(data)

    def test_schedule_parquet(self) -> None:
        data = make_schedule_parquet(self.schedule)
        self.assertIsInstance(data, bytes)
        df = pd.read_parquet(io.BytesIO(data))
        self.assertEqual(len(df), len(self.schedule))
        out_dir = pathlib.Path(__file__).parent / "output"
        out_dir.mkdir(exist_ok=True)
        (out_dir / "schedule.parquet").write_bytes(data)
