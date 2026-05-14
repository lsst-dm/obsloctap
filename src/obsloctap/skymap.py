# This file is part of obsloctap.
#
# Developed for the Rubin Data Management System.
# This product includes software developed by the Rubin Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# Use of this source code is governed by a 3-clause BSD-style
# license that can be found in the LICENSE file.

"""Interactive all-sky schedule map using Bokeh (Hammer-Aitoff projection)."""

from __future__ import annotations

import logging
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from urllib.parse import quote

import astropy.units as au
import jinja2
import matplotlib as mpl
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.time import Time
from bokeh.embed import components
from bokeh.layouts import column
from bokeh.layouts import row as bk_row
from bokeh.models import (
    BooleanFilter,
    CDSView,
    CheckboxGroup,
    ColumnDataSource,
    CustomJS,
    Div,
    HoverTool,
    RadioButtonGroup,
    Select,
    Spinner,
    Toggle,
)
from bokeh.plotting import figure
from bokeh.resources import CDN
from matplotlib.axes import Axes
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.lines import Line2D

from .config import config
from .models import Obsplan, spectral_ranges

__all__ = [
    "make_sky_html",
    "make_sky_pdf",
    "make_schedule_json",
    "make_schedule_csv",
    "make_schedule_parquet",
    "ScheduleSkyMap",
]

log = logging.getLogger(__name__)

# Jinja2 template
# Loaded once at module import; reused for every request.
_SKYMAP_TEMPLATE = jinja2.Environment(
    loader=jinja2.FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=False,
    undefined=jinja2.StrictUndefined,
).get_template("skymap.html.jinja")

# Rubin/LSST official filter palettes — sourced from lsst.utils.plotting
# Dark-background palette used by the Bokeh interactive map.
FILTER_COLORS: dict[str, str] = {
    "u": "#3eb7ff",
    "g": "#30c39f",
    "r": "#ff7e00",
    "i": "#2af5ff",
    "z": "#a7f9c1",
    "y": "#fdc900",
    "other": "#888888",  # grey
}
# Light-background palette used by the PDF export.
_FILTER_COLORS_PRINT: dict[str, str] = {
    "u": "#1600EA",
    "g": "#31DE1F",
    "r": "#B52626",
    "i": "#370201",
    "z": "#BA52FF",
    "y": "#61A2B3",
    "other": "#FF8C00",  # dark orange - distinct from existing colors
}

_BAND_NAMES: set[str] = set(FILTER_COLORS.keys())

# Marker shape per execution status
STATUS_MARKER: dict[str, str] = {
    "Scheduled": "circle",
    "Performed": "square",
    "Aborted": "triangle",
}

STATUS_ALPHA: dict[str, float] = {
    "Scheduled": 1.0,
    "Performed": 0.85,
    "Aborted": 0.30,
}

_GAL_PLANE_COLOR = "#ccaa00"  # gold
_ECL_PLANE_COLOR = "#9966cc"  # muted purple

# Status priority for deduplication
# Lower number = kept over higher number.
# Both "Performed" and "Aborted" are resolved outcomes that supersede
# "Scheduled" — a scheduled entry is redundant once the fate of the pointing
# is known, regardless of whether it was actually executed.
# Duplicates will be addressed upstream and eventually this will not be needed
_STATUS_PRIORITY: dict[str, int] = {
    "Performed": 0,
    "Aborted": 1,
    "Scheduled": 2,
}

# rubin.mplstyle settings applied during PDF generation.
_RUBIN_STYLE: dict = {
    "figure.dpi": 300,
    "figure.facecolor": "white",
    "font.size": 14,
    "axes.labelsize": "large",
    "axes.labelweight": "normal",
    "axes.titlesize": "small",
    "axes.titleweight": "normal",
    "figure.titlesize": "small",
    "figure.titleweight": "normal",
    "lines.linewidth": 2,
    "lines.markersize": 10,
    "xtick.direction": "in",
    "ytick.direction": "in",
    "xtick.minor.visible": True,
    "ytick.minor.visible": True,
    "xtick.top": True,
    "ytick.right": True,
    "legend.fontsize": "x-small",
}

_FIGURE_WIDTH = 1080
_FIGURE_HEIGHT = 620
_COORD_SELECT_WIDTH = 300
_TOGGLE_WIDTH = 155

_JS_LIB = (Path(__file__).parent / "js" / "skymap_callbacks.js").read_text()


def _cb(call: str) -> str:
    """Return the JS library plus a single callback entry-point call."""
    return _JS_LIB + "\n" + call


_GAL_TOGGLE_JS = _cb("galToggle(cb_obj, coord_sel, r_gal_eq);")
_ECL_TOGGLE_JS = _cb("eclToggle(cb_obj, coord_sel, r_ecl_eq, r_ecl_gal);")
_COORD_SELECT_JS = _cb(
    "coordSelect(cb_obj, src, t_gal, t_ecl,"
    " r_gal_eq, r_ecl_eq, r_ecl_gal,"
    " r_ax_bot_eq, r_ax_side_eq, r_ax_bot_gal, r_ax_side_gal);"
)
_PLOT_FILTER_JS = _cb(
    "plotFilter(src, band_cb, band_names, nexp_spinner,"
    " status_select, target_select, filters, renderers);"
)
_PDF_LINK_SYNC_JS = _cb("pdfLinkSync(coord_sel, t_gal, t_ecl);")


# Utility functions
def _get_band(em_min: float, em_max: float) -> str:
    """Return filter band name computed from the spectral range values.

    Raises
    ------
    ValueError
        If the spectral range is unset (both zero) or does not match any
        known band.
    """
    undetermined = "other"
    if em_min == 0.0 and em_max == 0.0:
        return undetermined
    # Exact match against catalogue (stored values match catalogue exactly)
    for band, (lo, hi) in spectral_ranges.items():
        if band not in _BAND_NAMES:
            continue
        if abs(em_min - lo) < 1e-11 and abs(em_max - hi) < 1e-11:
            return band
    # Nearest midpoint fallback
    mid = (em_min + em_max) / 2.0
    best_band: str | None = None
    best_dist = float("inf")
    for band, (lo, hi) in spectral_ranges.items():
        if band not in _BAND_NAMES:
            continue
        dist = abs(mid - (lo + hi) / 2.0)
        if dist < best_dist:
            best_dist, best_band = dist, band
    if best_band is None:
        return undetermined

    return best_band


def _hammer_aitoff(
    ra_deg: np.ndarray | float,
    dec_deg: np.ndarray | float,
) -> tuple[np.ndarray, np.ndarray]:
    """Hammer-Aitoff projection: RA/Dec (degrees) → (x, y).

    Origin is at RA = 0°.  RA increases to the *left* (standard astronomical
    convention: East is left when looking at the sky).

    Output coordinate ranges:
        x ∈ [−2√2, +2√2]   (≈ ±2.828)
        y ∈ [−√2,  +√2]    (≈ ±1.414)
    """
    ra = np.asarray(ra_deg, dtype=float)
    dec = np.asarray(dec_deg, dtype=float)
    # Wrap RA to [−180, +180]
    l_deg = np.where(ra > 180.0, ra - 360.0, ra)
    l_radians = np.radians(l_deg)
    b_radians = np.radians(dec)
    denom = np.sqrt(1.0 + np.cos(b_radians) * np.cos(l_radians / 2.0))
    # Negate x so that RA increases to the left
    x = (
        -2.0
        * np.sqrt(2.0)
        * np.cos(b_radians)
        * np.sin(l_radians / 2.0)
        / denom
    )
    y = np.sqrt(2.0) * np.sin(b_radians) / denom
    return x, y


def _coord_convert_galactic(
    ra_deg: np.ndarray, dec_deg: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """RA/Dec → Galactic l/b then Hammer-Aitoff (x, y)."""

    sc = SkyCoord(ra=ra_deg * au.deg, dec=dec_deg * au.deg, frame="icrs")
    return _hammer_aitoff(sc.galactic.l.deg, sc.galactic.b.deg)


def _mjd_to_utc(mjd: float) -> str:
    """Convert MJD float to a UTC ISO-8601 string."""
    try:
        return str(Time(mjd, format="mjd").iso)
    except Exception:
        return f"MJD {mjd: .5f}"


def _fmt_exptime(seconds: float) -> str:
    """Format an exposure-time total as hours (≥ 2 h) or minutes."""
    if seconds >= 7200:
        return f"{seconds / 3600:.1f} h"
    return f"{seconds / 60:.1f} min"


def _sky_line_segments(
    ra_deg: np.ndarray,
    dec_deg: np.ndarray,
    threshold: float = 0.8,
) -> tuple[list[list[float]], list[list[float]]]:
    """Project RA/Dec to Hammer-Aitoff and split at wrap discontinuities.

    Returns (xs, ys) suitable for Bokeh ``multi_line``.  Consecutive points
    whose projected x or y values jump by more than ``threshold`` indicate a
    RA=0/360 wrap and are treated as a segment boundary.
    """
    xs, ys = _hammer_aitoff(ra_deg, dec_deg)
    xs, ys = np.asarray(xs, float), np.asarray(ys, float)
    seg_xs: list[list[float]] = []
    seg_ys: list[list[float]] = []
    cur_x: list[float] = [float(xs[0])]
    cur_y: list[float] = [float(ys[0])]
    for x, y in zip(xs[1:], ys[1:]):
        if abs(x - cur_x[-1]) > threshold or abs(y - cur_y[-1]) > threshold:
            if len(cur_x) > 1:
                seg_xs.append(cur_x)
                seg_ys.append(cur_y)
            cur_x, cur_y = [float(x)], [float(y)]
        else:
            cur_x.append(float(x))
            cur_y.append(float(y))
    if len(cur_x) > 1:
        seg_xs.append(cur_x)
        seg_ys.append(cur_y)
    return seg_xs, seg_ys


class ScheduleSkyMap:
    """Renders a Rubin LSST observation schedule as an interactive all-sky map.

    Parameters
    ----------
    schedule
        List of `Obsplan` objects to render.
    start_val
        Start time string (``"now"``, ISO, or MJD) — pre-filled in the
        control form and used in the PDF title.
    time_val
        Lookahead window in hours — pre-filled in the control form and used
        in the PDF title.

    Examples
    --------
    .. code-block:: python

        skymap = ScheduleSkyMap(schedule, start_val="now", time_val=24)
        sky_html = skymap.to_html()
        sky_pdf  = skymap.to_pdf()
    """

    # Class-level geometry caches — computed once on first use, shared across
    # all instances.  These replace the former module-level globals.
    _gal_plane_segs: tuple[list, list] | None = None
    _ecl_plane_segs: tuple[list, list] | None = None
    _ecl_in_gal_segs: tuple[list, list] | None = None

    def __init__(
        self,
        schedule: list[Obsplan],
        *,
        start_val: str = "now",
        time_val: int = 48,
        path_prefix: str | None = None,
        coordinates: str = "equatorial",
        galactic_plane: bool = True,
        ecliptic_plane: bool = True,
    ) -> None:
        self.schedule = schedule
        self.start_val = start_val
        self.time_val = time_val
        self.path_prefix = (
            config.path_prefix if path_prefix is None else path_prefix
        )
        self.coordinates = coordinates
        self.galactic_plane = galactic_plane
        self.ecliptic_plane = ecliptic_plane

    def to_html(self) -> str:
        """
        Return a self-contained HTML page with an
        interactive all-sky Bokeh map.
        """
        p, extra_renderers, coord_data = self._build_figure()
        bottom_controls = self._build_control_layout(
            extra_renderers, coord_data
        )
        plot_layout = column(children=[p, bottom_controls])
        band_panel = self._build_band_panel(coord_data)
        if band_panel is not None:
            script, divs = components(
                {"plot": plot_layout, "band_panel": band_panel}
            )
            plot_div = divs["plot"]
            band_panel_div = divs["band_panel"]
        else:
            script, plot_div = components(plot_layout)
            band_panel_div = ""
        cdn_tags = CDN.render()

        return _SKYMAP_TEMPLATE.render(
            cdn_tags=cdn_tags,
            start_val=self.start_val,
            time_val=self.time_val,
            path_prefix=self.path_prefix,
            swatch_html=self._build_swatch_html(),
            start_encoded=quote(self.start_val, safe=""),
            time_encoded=quote(str(self.time_val), safe=""),
            stats_html=self._build_stats_html(),
            band_panel_div=band_panel_div,
            div=plot_div,
            script=script,
        )

    def to_pdf(self) -> bytes:
        """Return a publication-quality Hammer-Aitoff all-sky map as PDF bytes.

        Follows Rubin/LSST plotting conventions (RTN-045):

        * Uses ``FigureCanvasAgg`` directly.
        * Applies ``rubin.mplstyle`` settings via ``mpl.rc_context``.
        * Uses the print colour palette for colour.
        * All points use circle markers; status shown in legend by label only.

        Returns
        -------
        bytes
            Raw PDF file content (vector, 300 dpi metadata).
        """
        schedule = self._deduplicate()
        use_galactic = self.coordinates == "galactic"

        with mpl.rc_context(_RUBIN_STYLE):
            # Figure: use make_figure() pattern, no pyplot, no caching
            fig = Figure(figsize=(14, 7), facecolor="white", dpi=300)
            FigureCanvasAgg(fig)
            ax = fig.add_subplot(111)
            ax.set_facecolor("white")
            ax.set_aspect("equal")
            ax.axis("off")
            ax.set_xlim(-3.05, 3.05)
            ax.set_ylim(-1.65, 1.65)

            # Outer ellipse boundary
            _theta = np.linspace(0.0, 2.0 * np.pi, 721)
            ax.plot(
                2.0 * np.sqrt(2.0) * np.cos(_theta),
                np.sqrt(2.0) * np.sin(_theta),
                color="#333333",
                linewidth=0.8,
                zorder=1,
            )

            if use_galactic:
                self._add_pdf_galactic_grid(ax)
                self._add_pdf_plane_lines(ax, coordinates="galactic")
            else:
                self._add_pdf_equatorial_grid(ax)
                self._add_pdf_plane_lines(ax, coordinates="equatorial")

            #  Deduplicate then scatter: bucket by (band, status)
            plotted_bands: set[str] = set()
            plotted_statuses: set[str] = set()
            groups: dict[tuple[str, str], dict[str, list]] = defaultdict(
                lambda: {"x": [], "y": []}
            )
            for obs in schedule:
                band = _get_band(obs.em_min, obs.em_max)
                status = obs.execution_status
                if use_galactic:
                    x_arr, y_arr = _coord_convert_galactic(
                        np.array([obs.s_ra], dtype=float),
                        np.array([obs.s_dec], dtype=float),
                    )
                    x, y = float(x_arr[0]), float(y_arr[0])
                else:
                    x, y = _hammer_aitoff(obs.s_ra, obs.s_dec)
                groups[(band, status)]["x"].append(float(x))
                groups[(band, status)]["y"].append(float(y))
                plotted_bands.add(band)
                plotted_statuses.add(status)

            for (band, status), coords in groups.items():
                ax.scatter(
                    coords["x"],
                    coords["y"],
                    # colour = filter band (RTN-045 light-background palette)
                    c=_FILTER_COLORS_PRINT.get(band, "#888888"),
                    marker="o",
                    alpha=0.85,
                    s=18,
                    edgecolors="none",
                    linewidths=0,
                    zorder=3,
                )

            # Filter-band legend (colour only; shape encodes status)
            band_order = [
                b
                for b in ("u", "g", "r", "i", "z", "y", "other")
                if b in plotted_bands
            ]
            band_handles = [
                Line2D(
                    [0],
                    [0],
                    marker="o",
                    color="none",
                    markerfacecolor=_FILTER_COLORS_PRINT.get(b, "#888888"),
                    markeredgecolor="none",
                    markersize=7,
                    label=b,
                )
                for b in band_order
            ]
            leg1 = ax.legend(
                handles=band_handles,
                loc="lower left",
                title="Band",
                framealpha=0.92,
                edgecolor="#cccccc",
            )
            ax.add_artist(leg1)

            # Status legend
            status_handles = [
                Line2D(
                    [0],
                    [0],
                    marker="o",
                    color="none",
                    markerfacecolor="#555555",
                    markeredgecolor="none",
                    markersize=7,
                    label=s,
                )
                for s in sorted(plotted_statuses)
            ]
            ax.legend(
                handles=status_handles,
                loc="lower right",
                title="Status",
                framealpha=0.92,
                edgecolor="#cccccc",
            )

            # Title
            n = len(schedule)
            # Derive the date string from the earliest planned observation;
            # fall back to start_val (or "now") when the schedule is empty.
            if schedule:
                date_str = (
                    _mjd_to_utc(min(obs.t_planning for obs in schedule))[:19]
                    + " UTC"
                )
            elif self.start_val and self.start_val.lower() != "now":
                date_str = self.start_val[:19] + " UTC"
            else:
                date_str = Time.now().iso[:19] + " UTC"
            ax.set_title(
                f"Rubin LSST Observing Schedule"
                f"  ({date_str} + {self.time_val} h,"
                f" N observations = {n})",
                pad=10,
            )

            buf = BytesIO()
            fig.savefig(buf, format="pdf", bbox_inches="tight")
            # No plt.close() needed — Figure is not registered with pyplot
        return buf.getvalue()

    # Private instance methods

    def _build_figure(self) -> tuple[figure, dict[str, object], dict]:
        """Return ``(figure, extra_renderers, coord_data)`` for the map.

        ``extra_renderers`` maps layer names to Bokeh renderers for toggles:

        * ``"galactic"``  — Galactic plane line in Equatorial coords
        * ``"ecliptic"``  — Ecliptic plane line in Equatorial coords

        ``coord_data`` is a dict of Bokeh renderers and the scatter
        ``ColumnDataSource`` needed by ``to_html`` to wire up the coordinate
        system selector and toggle buttons.  ``coord_data["scatter_src"]`` is
        ``None`` when the schedule is empty.
        """
        schedule = self._deduplicate()
        p = self._create_bokeh_figure()
        self._add_projection_grid(p)
        axis_renderers = self._add_axis_labels(p)
        extra, r_ecl_in_gal = self._add_plane_renderers(p)

        if not schedule:
            p.text(
                x=[0.0],
                y=[0.0],
                text=["No observations in this time window"],
                text_color="#cccccc",
                text_font_size="13pt",
                text_align="center",
                text_baseline="middle",
            )
            coord_data: dict = {
                "scatter_src": None,
                "ecl_in_gal": r_ecl_in_gal,
                **axis_renderers,
            }
            return p, extra, coord_data

        # Build column data source and merge repeated visits to same pointing
        # _merge_repeat_visits groups by (ra, dec, band, exptime, target,
        # rot_sky, priority).  Where > 1 time exists, time_utc is a <br>-
        # separated list and nexp is the total.
        rows = self._merge_repeat_visits(schedule)

        # Pre-compute observation positions in both coordinate systems so the
        # coordinate system selector can swap them client-side via CustomJS.
        ra_arr = np.array([float(r["ra"]) for r in rows])
        dec_arr = np.array([float(r["dec"]) for r in rows])
        try:
            x_gal_arr, y_gal_arr = _coord_convert_galactic(ra_arr, dec_arr)
        except Exception as exc:
            log.warning(
                "Coordinate conversion failed: %s — coord select disabled", exc
            )
            x_gal_arr = y_gal_arr = np.zeros_like(ra_arr)

        cols = self._build_plot_columns(rows, x_gal_arr, y_gal_arr)
        source = ColumnDataSource(cols)
        bands_present = self._bands_present(cols["band"])
        band_renderers, band_filters = self._add_band_renderers(
            p, source, cols, bands_present
        )

        # Hover tooltip — active on all band renderers
        hover = HoverTool(
            renderers=list(band_renderers.values()),
            tooltips=[
                ("Target", "@target"),
                ("RA,Dec", "@ra_dec"),
                ("Band", "@band"),
                ("Status", "@status"),
                ("Time (UTC)", "@time_utc{safe}"),
                ("Exp Time", "@exptime"),
                ("N Exp", "@nexp"),
            ],
            point_policy="follow_mouse",
        )
        p.add_tools(hover)

        coord_data = {
            "scatter_src": source,
            "band_renderers": band_renderers,
            "band_filters": band_filters,
            "bands_present": bands_present,
            "ecl_in_gal": r_ecl_in_gal,
            **axis_renderers,
        }
        return p, extra, coord_data

    def _build_control_layout(
        self, extra_renderers: dict[str, object], coord_data: dict
    ) -> object:
        """Return the centered bottom controls row."""
        fp_toggle = Toggle(
            label="LSST Footprint",
            active=True,
            width=_TOGGLE_WIDTH,
            button_type="default",
            disabled=True,
        )
        coord_select = self._build_coord_select(extra_renderers, coord_data)
        t_gal, t_ecl = self._build_plane_toggles(
            extra_renderers, coord_data, coord_select
        )
        top_items = ([coord_select] if coord_select is not None else []) + [
            fp_toggle,
            t_gal,
            t_ecl,
        ]
        return bk_row(children=top_items, align="center")

    def _build_band_panel(self, coord_data: dict) -> object | None:
        """Return the Filters row: band, exposure, status, target."""
        band_renderers = coord_data.get("band_renderers", {})
        band_filters = coord_data.get("band_filters", {})
        bands_present = coord_data.get("bands_present", [])
        source = coord_data.get("scatter_src")
        if (
            not band_renderers
            or not band_filters
            or not bands_present
            or source is None
        ):
            return None
        _s = "color:#8888bb;font-size:12.8px;font-weight:600;"
        band_label = Div(
            text=f'<span style="{_s}">Band:</span>',
        )
        band_cb = CheckboxGroup(
            labels=list(bands_present),
            active=list(range(len(bands_present))),
            inline=True,
            css_classes=["band-select-panel"],
        )
        max_nexp = max(
            (int(v) for v in source.data.get("nexp_num", [1])), default=1
        )
        spacer = Div(text="", width=38)
        nexp_label = Div(
            text=f'<span style="{_s}">N exp &gt;=</span>',
        )
        nexp_spinner = Spinner(
            low=1,
            high=max(1, max_nexp),
            step=1,
            value=1,
            width=84,
        )
        status_label = Div(
            text=f'<span style="{_s}">Execution Status:</span>',
        )
        known_statuses = ["Scheduled", "Performed", "Aborted"]
        status_values = [str(v) for v in source.data.get("status", [])]
        status_options = ["All"] + [
            s for s in known_statuses if s in status_values
        ]
        status_options.extend(
            sorted({s for s in status_values if s not in set(known_statuses)})
        )
        status_select = Select(value="All", options=status_options, width=125)
        target_label = Div(
            text=f'<span style="{_s}">Target Name:</span>',
        )
        target_options = ["All"] + sorted(
            {str(v) for v in source.data.get("target", []) if str(v)}
        )
        target_select = Select(value="All", options=target_options, width=200)
        filter_callback = CustomJS(
            args={
                "src": source,
                "renderers": [band_renderers[b] for b in bands_present],
                "filters": [band_filters[b] for b in bands_present],
                "band_names": list(bands_present),
                "band_cb": band_cb,
                "nexp_spinner": nexp_spinner,
                "status_select": status_select,
                "target_select": target_select,
            },
            code=_PLOT_FILTER_JS,
        )
        band_cb.js_on_change("active", filter_callback)
        nexp_spinner.js_on_change("value", filter_callback)
        status_select.js_on_change("value", filter_callback)
        target_select.js_on_change("value", filter_callback)
        top_row = bk_row(
            children=[band_label, band_cb, spacer, nexp_label, nexp_spinner],
            align="start",
            css_classes=["band-panel-inline", "band-panel-top-row"],
        )
        bottom_row = bk_row(
            children=[
                status_label,
                status_select,
                target_label,
                target_select,
            ],
            align="start",
            css_classes=["band-panel-inline", "band-panel-bottom-row"],
        )
        return column(
            children=[top_row, bottom_row],
            css_classes=["band-panel-stack"],
        )

    def _build_coord_select(
        self, extra_renderers: dict[str, object], coord_data: dict
    ) -> RadioButtonGroup | None:
        """Return the coordinate-system selector, or ``None`` if no data."""
        if coord_data["scatter_src"] is None:
            return None
        coord_select = RadioButtonGroup(
            labels=["Equatorial (RA/Dec)", "Galactic (l/b)"],
            active=0,
            width=_COORD_SELECT_WIDTH,
        )
        coord_select.js_on_change(
            "active",
            CustomJS(
                args={
                    "src": coord_data["scatter_src"],
                    "r_gal_eq": extra_renderers["galactic"],
                    "r_ecl_eq": extra_renderers["ecliptic"],
                    "r_ecl_gal": coord_data["ecl_in_gal"],
                    "t_gal": None,
                    "t_ecl": None,
                    "r_ax_bot_eq": coord_data["ax_bot_eq"],
                    "r_ax_side_eq": coord_data["ax_side_eq"],
                    "r_ax_bot_gal": coord_data["ax_bot_gal"],
                    "r_ax_side_gal": coord_data["ax_side_gal"],
                },
                code=_COORD_SELECT_JS,
            ),
        )
        return coord_select

    def _build_plane_toggles(
        self,
        extra_renderers: dict[str, object],
        coord_data: dict,
        coord_select: RadioButtonGroup | None,
    ) -> tuple[Toggle, Toggle]:
        """Return Galactic-plane and ecliptic-plane toggle widgets."""
        t_gal = Toggle(
            label="Galactic Plane",
            active=True,
            width=_TOGGLE_WIDTH,
            button_type="primary",
        )
        t_ecl = Toggle(
            label="Ecliptic Plane",
            active=True,
            width=_TOGGLE_WIDTH,
            button_type="primary",
        )
        t_gal.js_on_click(
            CustomJS(
                args={
                    "r_gal_eq": extra_renderers["galactic"],
                    "coord_sel": coord_select,
                    "t_gal": t_gal,
                    "t_ecl": t_ecl,
                },
                code=_GAL_TOGGLE_JS + _PDF_LINK_SYNC_JS,
            )
        )
        t_ecl.js_on_click(
            CustomJS(
                args={
                    "r_ecl_eq": extra_renderers["ecliptic"],
                    "r_ecl_gal": coord_data["ecl_in_gal"],
                    "coord_sel": coord_select,
                    "t_gal": t_gal,
                    "t_ecl": t_ecl,
                },
                code=_ECL_TOGGLE_JS + _PDF_LINK_SYNC_JS,
            )
        )
        if coord_select is not None and coord_select.js_property_callbacks:
            callback = coord_select.js_property_callbacks["change:active"][0]
            callback.args["t_gal"] = t_gal
            callback.args["t_ecl"] = t_ecl
            callback.code = callback.code + _PDF_LINK_SYNC_JS
        return t_gal, t_ecl

    def _build_swatch_html(self) -> str:
        """Return the compact HTML band legend shown under the controls."""
        return "".join(
            f'<span class="legend-item">'
            f'<span class="swatch" style="background:{color}"></span>{band}'
            f"</span>"
            for band, color in FILTER_COLORS.items()
        )

    def _build_stats_html(self) -> str:
        """Return the summary-statistics HTML block for the schedule."""
        obs_info = self._observation_info()
        if not self.schedule:
            return f'<div class="info">{obs_info}</div>'
        band_rows, field_rows = self._compute_stats()
        return (
            f'<div class="stat-obs-info">{obs_info}</div>'
            '<div class="stat-grid">'
            f'<div class="stat-wrap">{self._band_table_html(band_rows)}</div>'
            '<div class="stat-wrap stat-wrap--field">'
            f"{self._field_table_html(field_rows)}</div>"
            "</div>"
        )

    def _observation_info(self) -> str:
        """Return the short observation-count/time-range summary string."""
        obs_count = len(self.schedule)
        if self.schedule:
            t_lo = min(obs.t_planning for obs in self.schedule)
            t_hi = max(obs.t_planning for obs in self.schedule)
            time_range = (
                f"{Time(t_lo, format='mjd').iso[:16]}"
                f" \u2192 {Time(t_hi, format='mjd').iso[:16]} UTC"
            )
        else:
            time_range = "No observations in range"
        return (
            f"{obs_count} observation{'s' if obs_count != 1 else ''}"
            f" &middot; {time_range}"
        )

    def _band_table_html(self, band_rows: list[tuple]) -> str:
        """Return the per-band summary table as HTML."""
        rows_html = "".join(
            f"<tr>"
            f'<td><span class="stat-swatch"'
            f' style="background:{FILTER_COLORS.get(b, "#aaaaaa")}">'
            f"</span>{b}</td>"
            f"<td>{n_vis:,}</td><td>{_fmt_exptime(exp)}</td>"
            f"</tr>"
            for b, n_vis, _n_img, exp in band_rows
        )
        tot_vis = sum(r[1] for r in band_rows)
        tot_exp = sum(r[3] for r in band_rows)
        _thead = (
            "<thead>"
            "<tr><th>Band</th><th>Visits</th><th>Total Exp. Time</th></tr>"
            "</thead>"
        )
        _tfoot = (
            f"<tfoot><tr><td>Total</td>"
            f"<td>{tot_vis:,}</td>"
            f"<td>{_fmt_exptime(tot_exp)}</td></tr></tfoot>"
        )
        return (
            '<table class="stat-table">'
            "<caption>Per band</caption>"
            f"{_thead}"
            f"<tbody>{rows_html}</tbody>"
            f"{_tfoot}"
            "</table>"
        )

    def _field_table_html(self, field_rows: list[tuple]) -> str:
        """Return the per-field summary table as HTML.

        The caption and header row are rendered outside the scrolling div so
        they remain visible while the body rows scroll.
        """
        rows_html = "".join(
            f"<tr>"
            f"<td>{field}</td>"
            f"<td>{n_vis:,}</td><td>{_fmt_exptime(exp)}</td>"
            f"</tr>"
            for field, n_vis, _n_img, exp in field_rows
        )
        tot_vis = sum(r[1] for r in field_rows)
        tot_exp = sum(r[3] for r in field_rows)
        _fth = (
            "<thead>"
            "<tr><th>Field</th><th>Visits</th><th>Total Exp. Time</th></tr>"
            "</thead>"
        )
        _ftf = (
            f"<tfoot><tr><td>Total</td>"
            f"<td>{tot_vis:,}</td>"
            f"<td>{_fmt_exptime(tot_exp)}</td></tr></tfoot>"
        )
        header_table = (
            '<table class="stat-table stat-table--field-header">'
            f"{_fth}</table>"
        )
        body_table = (
            '<table class="stat-table stat-table--field-body">'
            f"<tbody>{rows_html}</tbody>{_ftf}</table>"
        )
        return (
            '<div class="stat-field-title">Per field</div>'
            f'<div class="stat-field-header">{header_table}</div>'
            f'<div class="stat-field-scroll">{body_table}</div>'
        )

    def _create_bokeh_figure(self) -> figure:
        """Create the base Bokeh figure with shared map styling."""
        p = figure(
            width=_FIGURE_WIDTH,
            height=_FIGURE_HEIGHT,
            x_range=(-3.05, 3.05),
            y_range=(-1.60, 1.60),
            toolbar_location="above",
            tools="pan,wheel_zoom,box_zoom,reset",
            background_fill_color="#0d0d1f",
            border_fill_color="#0d0d1f",
            title=None,
        )
        p.xgrid.visible = False
        p.ygrid.visible = False
        p.xaxis.visible = False
        p.yaxis.visible = False
        p.outline_line_color = None
        return p

    def _add_projection_grid(self, p: figure) -> None:
        """Add the Hammer-Aitoff boundary and coordinate grid to ``p``."""
        theta = np.linspace(0.0, 2.0 * np.pi, 721)
        p.line(
            (2.0 * np.sqrt(2.0) * np.cos(theta)).tolist(),
            (np.sqrt(2.0) * np.sin(theta)).tolist(),
            line_color="#4444aa",
            line_width=1.5,
        )

        decs = np.linspace(-89.9, 89.9, 300)
        ra_xs, ra_ys, ra_lbl_x, ra_lbl_y, ra_lbl_t = [], [], [], [], []
        for ra_line in range(0, 360, 30):
            xs, ys = _hammer_aitoff(np.full_like(decs, float(ra_line)), decs)
            ra_xs.append(xs.tolist())
            ra_ys.append(ys.tolist())
            lx, ly = _hammer_aitoff(float(ra_line), 8.0)
            ra_lbl_x.append(float(lx))
            ra_lbl_y.append(float(ly))
            ra_lbl_t.append(f"{ra_line}°")
        p.multi_line(ra_xs, ra_ys, line_color="#252545", line_width=0.7)
        p.text(
            x=ra_lbl_x,
            y=ra_lbl_y,
            text=ra_lbl_t,
            text_color="#5555aa",
            text_font_size="8pt",
            text_align="center",
            text_baseline="middle",
        )

        ras = np.linspace(0.5, 359.5, 720)
        dec_xs, dec_ys, dec_lbl_x, dec_lbl_y, dec_lbl_t = [], [], [], [], []
        for dec_line in (-60, -30, 0, 30, 60):
            xs, ys = _hammer_aitoff(ras, np.full_like(ras, float(dec_line)))
            dec_xs.append(xs.tolist())
            dec_ys.append(ys.tolist())
            lx, ly = _hammer_aitoff(1.0, float(dec_line))
            dec_lbl_x.append(float(lx))
            dec_lbl_y.append(float(ly))
            dec_lbl_t.append(f"{float(dec_line):+.0f}°")
        p.multi_line(dec_xs, dec_ys, line_color="#252545", line_width=0.7)
        p.text(
            x=dec_lbl_x,
            y=dec_lbl_y,
            text=dec_lbl_t,
            text_color="#5555aa",
            text_font_size="8pt",
            text_align="left",
            text_baseline="middle",
        )

    def _add_axis_labels(self, p: figure) -> dict[str, object]:
        """Add equatorial and galactic axis labels and return renderers."""
        ax_kw = dict(
            text_color="#7777bb",
            text_font_size="9pt",
            text_align="center",
            text_baseline="middle",
        )
        ax_side_kw = dict(
            text_color="#7777bb",
            text_font_size="9pt",
            text_align="left",
            text_baseline="middle",
            angle=1.5708,
        )
        return {
            "ax_bot_eq": p.text(
                x=[0.0], y=[-1.52], text=["Right Ascension"], **ax_kw
            ),
            "ax_bot_gal": p.text(
                x=[0.0],
                y=[-1.52],
                text=["Galactic Longitude (l)"],
                visible=False,
                **ax_kw,
            ),
            "ax_side_eq": p.text(
                x=[-3.0], y=[0.0], text=["Dec"], **ax_side_kw
            ),
            "ax_side_gal": p.text(
                x=[-3.0],
                y=[0.0],
                text=["Galactic b"],
                visible=False,
                **ax_side_kw,
            ),
        }

    def _add_plane_renderers(
        self, p: figure
    ) -> tuple[dict[str, object], object]:
        """Add galactic/ecliptic plane renderers and return them."""
        extra: dict[str, object] = {}
        gal_xs, gal_ys = self._galactic_plane_segments()
        extra["galactic"] = p.multi_line(
            gal_xs,
            gal_ys,
            line_color=_GAL_PLANE_COLOR,
            line_width=1.5,
            line_alpha=0.85,
        )
        ecl_xs, ecl_ys = self._ecliptic_plane_segments()
        extra["ecliptic"] = p.multi_line(
            ecl_xs,
            ecl_ys,
            line_color=_ECL_PLANE_COLOR,
            line_width=1.5,
            line_alpha=0.85,
        )
        ecl_gal_xs, ecl_gal_ys = self._ecliptic_in_galactic_segments()
        r_ecl_in_gal = p.multi_line(
            ecl_gal_xs,
            ecl_gal_ys,
            line_color=_ECL_PLANE_COLOR,
            line_width=1.5,
            line_alpha=0.85,
            visible=False,
        )
        return extra, r_ecl_in_gal

    def _add_pdf_equatorial_grid(self, ax: Axes) -> None:
        """Add RA/Dec grid lines and labels to the PDF plot."""
        decs = np.linspace(-89.9, 89.9, 300)
        for ra_line in range(0, 360, 30):
            xs, ys = _hammer_aitoff(np.full_like(decs, float(ra_line)), decs)
            ax.plot(xs, ys, color="#cccccc", linewidth=0.4, zorder=1)
            lx, ly = _hammer_aitoff(float(ra_line), 11.0)
            ax.text(
                float(lx),
                float(ly),
                f"{ra_line}°",
                fontsize=7,
                ha="center",
                va="bottom",
                color="#555555",
            )

        ras = np.linspace(0.5, 359.5, 720)
        for dec_line in (-60, -30, 0, 30, 60):
            xs, ys = _hammer_aitoff(ras, np.full_like(ras, float(dec_line)))
            ax.plot(xs, ys, color="#cccccc", linewidth=0.4, zorder=1)
            lx, ly = _hammer_aitoff(1.5, float(dec_line))
            ax.text(
                float(lx),
                float(ly),
                f"{dec_line:+.0f}°",
                fontsize=7,
                ha="left",
                va="center",
                color="#555555",
            )

        ax.text(
            0.0,
            -1.58,
            "Right Ascension (°)",
            ha="center",
            va="top",
            fontsize=9,
            color="#333333",
        )
        ax.text(
            -3.03,
            0.0,
            "Declination (°)",
            ha="center",
            va="center",
            fontsize=9,
            color="#333333",
            rotation=90,
        )

    def _add_pdf_galactic_grid(self, ax: Axes) -> None:
        """Add Galactic longitude/latitude grid lines and labels to PDF."""
        lat_vals = np.linspace(-89.9, 89.9, 300)
        for lon_line in range(0, 360, 30):
            xs, ys = _hammer_aitoff(
                np.full_like(lat_vals, float(lon_line)), lat_vals
            )
            ax.plot(xs, ys, color="#cccccc", linewidth=0.4, zorder=1)
            lx, ly = _hammer_aitoff(float(lon_line), 8.0)
            ax.text(
                float(lx),
                float(ly),
                f"{lon_line}°",
                fontsize=7,
                ha="center",
                va="bottom",
                color="#555555",
            )

        lon_vals = np.linspace(0.5, 359.5, 720)
        for lat_line in (-60, -30, 0, 30, 60):
            xs, ys = _hammer_aitoff(
                lon_vals, np.full_like(lon_vals, float(lat_line))
            )
            ax.plot(xs, ys, color="#cccccc", linewidth=0.4, zorder=1)
            lx, ly = _hammer_aitoff(1.0, float(lat_line))
            ax.text(
                float(lx),
                float(ly),
                f"{lat_line: +.0f}°",
                fontsize=7,
                ha="left",
                va="center",
                color="#555555",
            )

        ax.text(
            0.0,
            -1.58,
            "Galactic Longitude (°)",
            ha="center",
            va="top",
            fontsize=9,
            color="#333333",
        )
        ax.text(
            -3.03,
            0.0,
            "Galactic Latitude (°)",
            ha="center",
            va="center",
            fontsize=9,
            color="#333333",
            rotation=90,
        )

    def _add_pdf_plane_lines(self, ax: Axes, *, coordinates: str) -> None:
        """Add optional plane overlays to the PDF plot
        for the chosen coordinates."""
        if coordinates == "equatorial":
            if self.galactic_plane:
                xs, ys = self._galactic_plane_segments()
                for xseg, yseg in zip(xs, ys, strict=False):
                    ax.plot(
                        xseg,
                        yseg,
                        color=_GAL_PLANE_COLOR,
                        linewidth=1.1,
                        zorder=2,
                    )
            if self.ecliptic_plane:
                xs, ys = self._ecliptic_plane_segments()
                for xseg, yseg in zip(xs, ys, strict=False):
                    ax.plot(
                        xseg,
                        yseg,
                        color=_ECL_PLANE_COLOR,
                        linewidth=1.1,
                        zorder=2,
                    )
            return

        if self.ecliptic_plane:
            xs, ys = self._ecliptic_in_galactic_segments()
            for xseg, yseg in zip(xs, ys, strict=False):
                ax.plot(
                    xseg, yseg, color=_ECL_PLANE_COLOR, linewidth=1.1, zorder=2
                )

    def _build_plot_columns(
        self, rows: list[dict], x_gal_arr: np.ndarray, y_gal_arr: np.ndarray
    ) -> dict[str, list]:
        """Return the ColumnDataSource column mapping for merged rows."""
        cols: dict[str, list] = {
            "x": [],
            "y": [],
            "x_eq": [],
            "y_eq": [],
            "x_gal": x_gal_arr.tolist(),
            "y_gal": y_gal_arr.tolist(),
            "ra": [],
            "dec": [],
            "ra_dec": [],
            "band": [],
            "color": [],
            "status": [],
            "alpha": [],
            "marker": [],
            "target": [],
            "time_utc": [],
            "exptime": [],
            "rot_sky": [],
            "nexp": [],
            "nexp_num": [],
            "priority": [],
        }
        plot_columns = (
            "x",
            "y",
            "ra",
            "dec",
            "ra_dec",
            "band",
            "color",
            "status",
            "alpha",
            "marker",
            "target",
            "time_utc",
            "exptime",
            "rot_sky",
            "nexp",
            "nexp_num",
            "priority",
        )
        for row in rows:
            for col_name in plot_columns:
                cols[col_name].append(row[col_name])
            cols["x_eq"].append(row["x"])
            cols["y_eq"].append(row["y"])
        return cols

    def _bands_present(self, bands_in_columns: list[str]) -> list[str]:
        """Return the ordered set of filter bands to expose in the UI."""
        band_order = ["u", "g", "r", "i", "z", "y", "other"]
        bands_in_data = set(bands_in_columns)
        bands_present: list[str] = ["u", "g", "r", "i", "z", "y", "other"]
        for band in sorted(bands_in_data - set(band_order)):
            bands_present.append(band)
        return bands_present

    def _add_band_renderers(
        self,
        p: figure,
        source: ColumnDataSource,
        cols: dict[str, list],
        bands_present: list[str],
    ) -> tuple[dict[str, object], dict[str, BooleanFilter]]:
        """Add one scatter renderer per band; return renderers and filters."""
        band_renderers: dict[str, object] = {}
        band_filters: dict[str, BooleanFilter] = {}
        for band in bands_present:
            mask = [b == band for b in cols["band"]]
            band_filter = BooleanFilter(booleans=mask)
            view = CDSView(filter=band_filter)
            renderer = p.scatter(
                x="x",
                y="y",
                source=source,
                view=view,
                size=10,
                color="color",
                alpha="alpha",
                marker="marker",
                line_color="white",
                line_alpha=0.35,
                line_width=0.6,
            )
            band_renderers[band] = renderer
            band_filters[band] = band_filter
        return band_renderers, band_filters

    def _deduplicate(self) -> list[Obsplan]:
        """Return a *new* list with one entry per unique pointing.

        The input ``self.schedule`` is **never modified** — this method only
        builds a collapsed list suitable for rendering the skymap or PDF.

        The service returns two records for the same physical observation when
        a resolved outcome is known: one 'Scheduled' entry (from the plan) and
        one 'Aborted' or 'Performed' entry (from the execution record).
        The 'Scheduled' entry is redundant once the outcome is known, so we
        keep only the highest-priority status per unique pointing:

            Performed > Aborted > Scheduled

        Any duplicate found is written to ``obsloctap_duplicates.txt``
        (appended) so data-quality issues can be investigated.

        Raises
        ------
        ValueError
            If the same pointing carries both 'Performed' and 'Aborted'
            statuses — a data inconsistency in the upstream service.
        """
        seen: dict[tuple, Obsplan] = {}
        duplicates: list[dict] = []
        for obs in self.schedule:
            key = (obs.s_ra, obs.s_dec, obs.t_planning)
            existing = seen.get(key)
            if existing is None:
                seen[key] = obs
            else:
                pair = {obs.execution_status, existing.execution_status}
                if pair == {"Performed", "Aborted"}:
                    raise ValueError(
                        f"Data inconsistency: pointing {key} is recorded as"
                        " both 'Performed' and 'Aborted'. An observation"
                        " cannot be both executed and not executed — this"
                        " indicates a bug in the upstream scheduling data."
                    )
                # Lower priority number = higher priority; keep that one.
                if _STATUS_PRIORITY.get(
                    obs.execution_status, 99
                ) < _STATUS_PRIORITY.get(existing.execution_status, 99):
                    kept, dropped = obs, existing
                    seen[key] = obs
                else:
                    kept, dropped = existing, obs
                duplicates.append(
                    {
                        "reported_at": Time.now().iso,
                        "s_ra": key[0],
                        "s_dec": key[1],
                        "t_planning": key[2],
                        "kept_status": kept.execution_status,
                        "kept_obs_id": kept.obs_id,
                        "dropped_status": dropped.execution_status,
                        "dropped_obs_id": dropped.obs_id,
                    }
                )

        if duplicates:
            report_path = Path("obsloctap_duplicates.txt")
            with report_path.open("a") as fh:
                n_dup = len(duplicates)
                fh.write(f"=== {Time.now().iso} — {n_dup} duplicate(s) ===\n")
                for i, rec in enumerate(duplicates, 1):
                    fh.write(
                        f"\nDuplicate {i}\n"
                        f"  Pointing : RA={rec['s_ra']}  Dec={rec['s_dec']}"
                        f"  t_planning={rec['t_planning']}\n"
                        f"  RETAINED : status={rec['kept_status']}"
                        f"  obs_id={rec['kept_obs_id']}\n"
                        f"  DROPPED  : status={rec['dropped_status']}"
                        f"  obs_id={rec['dropped_obs_id']}\n"
                    )
                fh.write("\n")
            log.warning(
                f"Found {len(duplicates)} duplicate pointing(s) "
                f"written to {report_path.resolve()}"
            )

        return list(seen.values())

    def _merge_repeat_visits(self, schedule: list[Obsplan]) -> list[dict]:
        """Collapse repeated visits to the same pointing into one display row.

        Where the same RA, Dec, band, exposure time, target name, sky rotation,
        and priority appear more than once (i.e. the same pointing is scheduled
        at multiple times, or carries mixed statuses), the entries are merged
        into a single Bokeh data row:

        * ``time_utc`` becomes a ``<br>``-separated list of all times (sorted).
        * ``nexp``     becomes the **total** across all merged visits.
        * ``status``   is the highest-priority status in the group
                       (Performed > Aborted > Scheduled).

        Parameters
        ----------
        schedule
            The (typically deduplicated) list of observations to merge.
            Pass ``self._deduplicate()`` to avoid double-counting.
        """
        groups: dict[tuple, list[Obsplan]] = {}
        for obs in schedule:
            band = _get_band(obs.em_min, obs.em_max)
            raw_target = obs.target_name or ""
            norm_target = ", ".join(
                sorted(t.strip() for t in raw_target.split(","))
            )
            key = (
                obs.s_ra,
                obs.s_dec,
                band,
                round(obs.t_exptime, 2),
                norm_target,
                round(obs.rubin_rot_sky_pos, 2),
                obs.priority,
            )
            groups.setdefault(key, []).append(obs)

        rows: list[dict] = []
        for (ra, dec, band, *_rest), members in groups.items():
            members_sorted = sorted(members, key=lambda o: o.t_planning)
            rep = members_sorted[0]  # representative for scalar fields

            # Highest-priority status wins for the merged group
            best_status = min(
                (m.execution_status for m in members),
                key=lambda s: _STATUS_PRIORITY.get(s, 99),
            )

            times = [_mjd_to_utc(o.t_planning) for o in members_sorted]
            total_nexp = sum(o.rubin_nexp for o in members_sorted)

            x, y = _hammer_aitoff(ra, dec)
            rows.append(
                {
                    "x": float(x),
                    "y": float(y),
                    "ra": f"{ra:.4f}",
                    "dec": f"{dec:.4f}",
                    "ra_dec": f"{ra:.4f} °, {dec:.4f} °",
                    "band": band,
                    "color": FILTER_COLORS.get(band, "#aaaaaa"),
                    "status": best_status,
                    "alpha": STATUS_ALPHA.get(best_status, 0.7),
                    "marker": STATUS_MARKER.get(best_status, "circle"),
                    "target": ", ".join(
                        sorted(
                            t.strip()
                            for t in (rep.target_name or "(unknown)").split(
                                ","
                            )
                        )
                    ),
                    "time_utc": "<br>".join(times),
                    "exptime": f"{rep.t_exptime:.1f} s",
                    "rot_sky": f"{rep.rubin_rot_sky_pos:.1f}°",
                    "nexp": str(total_nexp),
                    "nexp_num": total_nexp,
                    "priority": str(rep.priority),
                }
            )

        return rows

    def _compute_stats(self) -> tuple[list[tuple], list[tuple]]:
        """Return per-band and per-field summary rows from ``self.schedule``.

        Uses the deduplicated view (same dataset shown on the map) so that a
        pointing recorded under both 'Scheduled' and a resolved status is
        counted only once.

        Returns
        -------
        band_rows
            List of ``(band, n_visits, n_images, exptime_s)`` tuples ordered
            u → g → r → i → z → y → other.  The six standard bands are always
            present even when their count is zero; "other" is omitted if empty.
        field_rows
            List of ``(field, n_visits, n_images, exptime_s)`` tuples sorted by
            ``n_images`` descending so the most-observed fields appear first.
        """
        deduped = self._deduplicate()

        # [n_visits, n_images, exptime_s]
        band_acc: dict[str, list] = defaultdict(lambda: [0, 0, 0.0])
        field_acc: dict[str, list] = defaultdict(lambda: [0, 0, 0.0])

        for obs in deduped:
            band = _get_band(obs.em_min, obs.em_max)
            raw_field = obs.target_name or "(unknown)"
            # Sort sub-fields so comma-separated order doesn't matter.
            field = ", ".join(sorted(t.strip() for t in raw_field.split(",")))

            band_acc[band][0] += 1
            band_acc[band][1] += obs.rubin_nexp
            band_acc[band][2] += obs.t_exptime

            field_acc[field][0] += 1
            field_acc[field][1] += obs.rubin_nexp
            field_acc[field][2] += obs.t_exptime

        _STANDARD_BANDS = ["u", "g", "r", "i", "z", "y"]
        # Always emit all six standard bands, even when count is zero.
        band_rows: list[tuple] = [
            (b, band_acc[b][0], band_acc[b][1], band_acc[b][2])
            for b in _STANDARD_BANDS
        ]
        # "other" only when present; non-canonical names appended after.
        known = set(_STANDARD_BANDS) | {"other"}
        if "other" in band_acc:
            band_rows.append(
                (
                    "other",
                    band_acc["other"][0],
                    band_acc["other"][1],
                    band_acc["other"][2],
                )
            )
        for b in sorted(band_acc):
            if b not in known:
                band_rows.append(
                    (b, band_acc[b][0], band_acc[b][1], band_acc[b][2])
                )

        field_rows: list[tuple] = sorted(
            [(f, v[0], v[1], v[2]) for f, v in field_acc.items()],
            key=lambda r: r[2],  # n_images descending
            reverse=True,
        )

        return band_rows, field_rows

    # Class methods: geometry caches

    @classmethod
    def _galactic_plane_segments(
        cls,
    ) -> tuple[list[list[float]], list[list[float]]]:
        """Hammer-Aitoff multi_line segments for the Galactic plane (b = 0°).

        Result is cached at class level after the first call.
        """
        if cls._gal_plane_segs is not None:
            return cls._gal_plane_segs

        l_vals = np.linspace(0.0, 360.0, 1800, endpoint=False)
        sc = SkyCoord(
            l=l_vals * au.deg,
            b=np.zeros_like(l_vals) * au.deg,
            frame="galactic",
        )
        cls._gal_plane_segs = _sky_line_segments(
            sc.icrs.ra.deg, sc.icrs.dec.deg
        )
        return cls._gal_plane_segs

    @classmethod
    def _ecliptic_plane_segments(
        cls,
    ) -> tuple[list[list[float]], list[list[float]]]:
        """Hammer-Aitoff multi_line segments for the Ecliptic plane (lat = 0°).

        Result is cached at class level after the first call.
        """
        if cls._ecl_plane_segs is not None:
            return cls._ecl_plane_segs

        lon_vals = np.linspace(0.0, 360.0, 1800, endpoint=False)
        sc = SkyCoord(
            lon=lon_vals * au.deg,
            lat=np.zeros_like(lon_vals) * au.deg,
            frame="barycentrictrueecliptic",
        )
        cls._ecl_plane_segs = _sky_line_segments(
            sc.icrs.ra.deg, sc.icrs.dec.deg
        )
        return cls._ecl_plane_segs

    @classmethod
    def _ecliptic_in_galactic_segments(
        cls,
    ) -> tuple[list[list[float]], list[list[float]]]:
        """Ecliptic plane (lat=0) in Galactic Hammer-Aitoff coordinates.

        Converts each point on the ecliptic plane to Galactic (l, b) with
        astropy, then applies the standard Hammer-Aitoff projection.
        Result is cached at class level after the first call.
        """
        if cls._ecl_in_gal_segs is not None:
            return cls._ecl_in_gal_segs

        lon_vals = np.linspace(0.0, 360.0, 1800, endpoint=False)
        sc = SkyCoord(
            lon=lon_vals * au.deg,
            lat=np.zeros_like(lon_vals) * au.deg,
            frame="barycentrictrueecliptic",
        )
        cls._ecl_in_gal_segs = _sky_line_segments(
            sc.galactic.l.deg, sc.galactic.b.deg
        )
        return cls._ecl_in_gal_segs


def make_sky_html(
    schedule: list[Obsplan],
    start_val: str = "now",
    time_val: int = 48,
    path_prefix: str | None = None,
) -> str:
    """Return a self-contained HTML page with an interactive all-sky Bokeh map.

    Parameters
    ----------
    schedule
        List of `Obsplan` objects to plot.
    start_val
        The start parameter value to pre-fill in the control form.
    time_val
        The lookahead hours to pre-fill in the control form.
    path_prefix
        URL prefix or full base URL for static assets and service links in
        the rendered page.
    """
    return ScheduleSkyMap(
        schedule,
        start_val=start_val,
        time_val=time_val,
        path_prefix=path_prefix,
    ).to_html()


def make_sky_pdf(
    schedule: list[Obsplan],
    start_val: str = "now",
    time_val: int = 48,
    coordinates: str = "equatorial",
    galactic_plane: bool = True,
    ecliptic_plane: bool = True,
) -> bytes:
    """Return a publication-quality Hammer-Aitoff all-sky map as PDF bytes.

    Parameters
    ----------
    schedule
        List of `Obsplan` objects to plot.
    start_val
        Start time string (``"now"``, ISO, or MJD) — used in the figure title.
    time_val
        Lookahead window in hours — used in the figure title.
    coordinates
        Coordinate system for the PDF map: ``"equatorial"`` or ``"galactic"``.
    galactic_plane
        Whether to draw the Galactic plane overlay.
    ecliptic_plane
        Whether to draw the Ecliptic plane overlay.

    Returns
    -------
    bytes
        Raw PDF file content (vector, 300 dpi metadata).
    """
    return ScheduleSkyMap(
        schedule,
        start_val=start_val,
        time_val=time_val,
        coordinates=coordinates,
        galactic_plane=galactic_plane,
        ecliptic_plane=ecliptic_plane,
    ).to_pdf()


def make_schedule_json(schedule: list[Obsplan]) -> str:
    """Return the schedule serialised as a JSON string."""
    import json as _json

    return _json.dumps([obs.model_dump() for obs in schedule])


def make_schedule_csv(schedule: list[Obsplan]) -> str:
    """Return the schedule serialised as a CSV string."""
    import csv
    import io

    if not schedule:
        return ""
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=list(schedule[0].model_dump().keys())
    )
    writer.writeheader()
    for obs in schedule:
        writer.writerow(obs.model_dump())
    return buf.getvalue()


def make_schedule_parquet(schedule: list[Obsplan]) -> bytes:
    """Return the schedule serialised as Parquet bytes."""
    import pandas as pd

    df = pd.DataFrame([obs.model_dump() for obs in schedule])
    import io

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()
