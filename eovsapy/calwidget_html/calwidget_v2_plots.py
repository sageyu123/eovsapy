"""Matplotlib rendering helpers for the browser widget and benchmark reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import matplotlib

matplotlib.use("Agg")

from matplotlib import dates as mdates
from matplotlib.colors import to_hex
from matplotlib import patches
from matplotlib import pyplot as plt
import numpy as np

from eovsapy.util import Time, lin_phase_fit, lobe

from .calwidget_v2_analysis import (
    ScanAnalysis,
    combined_channel_vis_with_time_flags,
    ensure_time_flag_groups,
    legacy_refcal_display_summary,
    phacal_comparison_metrics,
    refcal_comparison_metrics,
    scan_feed_kind,
    solve_residual_delay_phi0,
    sql_phacal_to_scan,
    sql_refcal_to_scan,
    yx_residual_threshold,
)
from .calwidget_v2_smooth_phase import weighted_polyfit, wrapped_line_segments


TAB_NAMES = (
    "time_history",
    "sum_amp",
    "sum_pha",
    "inband_fit",
    "inband_applied",
)

HEATMAP_AXES_RECT = (0.20, 0.10, 0.58, 0.82)
HEATMAP_COLORBAR_RECT = (0.83, 0.10, 0.06, 0.82)


def _figure_to_png_bytes(fig: Any, tight: bool = True, dpi: int = 120) -> bytes:
    """Serialize a matplotlib figure to PNG bytes."""

    import io

    buf = io.BytesIO()
    save_kwargs = {"format": "png", "dpi": dpi}
    if tight:
        save_kwargs["bbox_inches"] = "tight"
    fig.savefig(buf, **save_kwargs)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _blank_figure(message: str, title: str = "") -> bytes:
    """Render a text-only placeholder figure."""

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.axis("off")
    if title:
        fig.suptitle(title)
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=12)
    return _figure_to_png_bytes(fig)


def heatmap_plot_meta(scan: Optional[ScanAnalysis]) -> Optional[Dict[str, float]]:
    """Return normalized plot bounds for frontend click mapping."""

    if scan is None:
        return None
    left, bottom, width, height = HEATMAP_AXES_RECT
    return {
        "plot_left": left,
        "plot_top": 1.0 - bottom - height,
        "plot_width": width,
        "plot_height": height,
        "nsolant": scan.layout.nsolant,
        "maxnbd": scan.layout.maxnbd,
    }


def render_heatmap(
    scan: Optional[ScanAnalysis],
    ant: Optional[int],
    band: Optional[int],
    scan_label: Optional[str] = None,
) -> bytes:
    """Render the sigma/flag map."""

    if scan is None:
        return _blank_figure("No scan selected.", title="Sigma Map")
    flags = _heatmap_flag_map(scan)
    fig = plt.figure(figsize=(4.4, 6.8))
    ax = fig.add_axes(HEATMAP_AXES_RECT)
    mesh = ax.pcolormesh(
        np.arange(scan.layout.nsolant + 1),
        np.arange(scan.layout.maxnbd + 1),
        flags,
        shading="flat",
        cmap="viridis",
        vmin=0.0,
        vmax=2.0,
        linewidth=0.18,
        edgecolors=(1.0, 1.0, 1.0, 0.10),
    )
    cax = fig.add_axes(HEATMAP_COLORBAR_RECT)
    fig.colorbar(mesh, cax=cax, label="Flag Sum (XX+YY)")
    fig.suptitle("Sigma Map", y=0.985)
    ax.set_title(scan_label or scan.t_bg.iso[11:19], fontsize=12)
    ax.set_xlabel("Antenna Number")
    ax.set_ylabel("Band Number")
    ax.set_xlim(0, scan.layout.nsolant)
    ax.set_ylim(0, scan.layout.maxnbd)
    ax.set_xticks(np.arange(scan.layout.nsolant) + 0.5)
    ax.set_xticklabels(["{0:d}".format(i + 1) for i in range(scan.layout.nsolant)], rotation=45, ha="right")
    ax.set_yticks(np.arange(0, scan.layout.maxnbd + 1, 10))
    if ant is not None and band is not None and 0 <= ant < scan.layout.nsolant and 0 <= band < scan.layout.maxnbd:
        ax.add_patch(patches.Rectangle((ant, band), 1.0, 1.0, fill=False, ec="r", lw=2.0))
    return _figure_to_png_bytes(fig, tight=False)


def _heatmap_flag_map(scan: ScanAnalysis) -> np.ndarray:
    """Return the live display flag map used by the browser flag map.

    The browser flag map is a discrete tuned-state view. Use the current live
    flag cube so mask edits, manual antenna overrides, and Y-X quality logic
    are reflected immediately.
    """

    if scan.base_flags is not None:
        flag_cube = np.asarray(scan.base_flags[: scan.layout.nsolant], dtype=np.int32).copy()
    else:
        flag_cube = np.asarray(scan.flags[: scan.layout.nsolant], dtype=np.int32).copy()
    if scan.delay_solution is not None and scan.raw and scan.scan_kind == "refcal" and scan_feed_kind(scan) == "hi":
        yx_rms = np.asarray(scan.raw.get("yx_residual_rms", np.full(scan.layout.nsolant, np.nan)), dtype=float)
        if yx_rms.shape == (scan.layout.nsolant,):
            keep_mask = np.isfinite(yx_rms) & (yx_rms <= yx_residual_threshold(scan))
            flag_cube[keep_mask, :2, :] = 0
        manual = np.asarray(scan.delay_solution.manual_ant_flag_override, dtype=bool)
        if manual.shape == (scan.layout.nsolant,):
            flag_cube[manual, :2, :] = 1
    return np.sum(flag_cube[:, :2], axis=1).T


def _selected_band_time_series(scan: ScanAnalysis, ant: int, band: int) -> Dict[str, np.ndarray]:
    """Band-average one selected band on demand for time-history plotting."""

    if not scan.raw or "channel_band" not in scan.raw:
        raise ValueError("No raw time history is available.")
    band_value = int(band) + 1
    idx = np.where(scan.raw["channel_band"] == band_value)[0]
    if idx.size == 0:
        raise ValueError("Selected band has no channels in this scan.")
    vis = scan.corrected_channel_vis[ant, :2, idx]
    return {
        "xx": np.nanmean(vis[0], axis=0),
        "yy": np.nanmean(vis[1], axis=0),
        "times": np.asarray(scan.raw["raw"]["time"], dtype=float),
    }


def render_time_history(scan: Optional[ScanAnalysis], ant: Optional[int], band: Optional[int]) -> bytes:
    """Render the selected-band time history."""

    if scan is None or ant is None or band is None:
        return _blank_figure("Select a scan, antenna, and band.", title="Time History")
    if not scan.raw:
        return _blank_figure("No raw time history is available for SQL-only results.", title="Time History")
    try:
        data = _selected_band_time_series(scan, ant, band)
    except ValueError as exc:
        return _blank_figure(str(exc), title="Time History")
    plot_times = Time(data["times"], format="jd").plot_date
    fig, ax = plt.subplots(2, 2, figsize=(11, 6.4), sharex=True)
    labels = [("xx", "X"), ("yy", "Y")]
    for col, (key, label) in enumerate(labels):
        ax[0, col].plot_date(plot_times, np.abs(data[key]), "C0.", ms=3)
        ax[1, col].plot_date(plot_times, np.angle(data[key]), "C1.", ms=3)
        ax[0, col].set_title("Pol {0} Amplitude".format(label))
        ax[1, col].set_title("Pol {0} Phase".format(label))
        ax[0, col].set_ylabel("Amplitude [arb]")
        ax[1, col].set_ylabel("Phase [rad]")
        ax[1, col].set_xlabel("Time [UT]")
        for row in range(2):
            ax[row, col].grid(alpha=0.2)
            ax[row, col].xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=5))
            ax[row, col].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    if scan.tflags is not None and ant < scan.tflags.shape[0] and band < scan.tflags.shape[1]:
        for slot, style, color in ((0, "-", "g"), (1, "--", "r")):
            tflag_values = scan.tflags[ant, band, :, slot]
            if np.allclose(tflag_values, 0.0):
                continue
            for col in range(2):
                for row in range(2):
                    ax[row, col].axvline(tflag_values[0], color=color, linestyle=style, alpha=0.7)
                    ax[row, col].axvline(tflag_values[1], color=color, linestyle=style, alpha=0.7)
    fig.suptitle("Time History: Ant {0:d}, Band {1:d}".format(ant + 1, band + 1))
    return _figure_to_png_bytes(fig)


def _used_bands(scan: ScanAnalysis) -> np.ndarray:
    """Return used band numbers for one scan."""

    return np.asarray([int(v) for v in scan.bands_band if v > 0], dtype=int)


def _median_center_phase(values: np.ndarray) -> np.ndarray:
    """Median-center one phase series while preserving NaNs."""

    out = np.asarray(values, dtype=float).copy()
    finite = np.isfinite(out)
    if np.any(finite):
        out[finite] -= float(np.nanmedian(out[finite]))
    return out


def _browser_sum_phase_series(scan: ScanAnalysis, ant_idx: int, pol: int, good: np.ndarray) -> Any:
    """Return the browser-native phase series for one antenna/pol.

    The browser overview should use one explicit phase convention instead of the
    legacy Tk mix of absolute reference-antenna phase and wrapped relative phase.
    For refcals, display the phase relative to antenna 1 for every antenna and
    median-center each series to keep the small multiples visually comparable.
    For phacals, use the solved phase-difference product and likewise
    median-center after unwrapping.
    """

    if scan.scan_kind == "phacal" and scan.pdiff is not None:
        valid = np.where(np.logical_and(scan.bands_band > 0, scan.flags[ant_idx, pol] == 0))[0]
        xvals = scan.bands_band[valid].astype(float)
        if valid.size == 0:
            return xvals, np.array([], dtype=float)
        yvals = _median_center_phase(np.unwrap(np.asarray(scan.pdiff[ant_idx, pol, valid], dtype=float)))
        return xvals, yvals

    xvals = scan.bands_band[good].astype(float)
    if ant_idx == 0:
        return xvals, np.zeros_like(xvals, dtype=float)

    ref = np.asarray(scan.corrected_band_vis[0, pol, good], dtype=complex)
    cur = np.asarray(scan.corrected_band_vis[ant_idx, pol, good], dtype=complex)
    valid = np.isfinite(cur) & np.isfinite(ref) & (np.abs(cur) > 0.0) & (np.abs(ref) > 0.0)
    if not np.any(valid):
        return np.array([], dtype=float), np.array([], dtype=float)
    xsel = xvals[valid]
    ratio = cur[valid] / ref[valid]
    yvals = _median_center_phase(np.unwrap(np.angle(ratio)))
    return xsel, yvals


def _all_ant_axes(scan: ScanAnalysis, title: str, xlabel: str) -> Any:
    """Create a dense 2 x Nant small-multiples layout."""

    nsolant = scan.layout.nsolant
    fig, ax = plt.subplots(
        2,
        nsolant,
        figsize=(max(15.0, 1.15 * nsolant), 5.8),
        sharex=True,
        sharey="row",
        squeeze=False,
    )
    fig.suptitle(title)
    fig.text(0.5, 0.03, xlabel, ha="center")
    for ant in range(nsolant):
        ax[0, ant].set_title("Ant {0:d}".format(ant + 1), fontsize=9)
        for pol in range(2):
            ax[pol, ant].grid(alpha=0.15)
            ax[pol, ant].tick_params(labelsize=7)
            if ant != 0:
                ax[pol, ant].set_yticklabels([])
    return fig, ax


def render_sum_amp(scan: Optional[ScanAnalysis], ant: Optional[int]) -> bytes:
    """Render band-averaged amplitude for all antennas."""

    if scan is None:
        return _blank_figure("Select a scan.", title="Sum Amp")
    good = np.where(scan.bands_band > 0)[0]
    if good.size == 0:
        return _blank_figure("No band-averaged data are available.", title="Sum Amp")
    fig, ax = _all_ant_axes(scan, "Band-Averaged Amplitude", "Band")
    amp = np.abs(scan.corrected_band_vis[: scan.layout.nsolant, :2, good])
    finite = amp[np.isfinite(amp)]
    ymax = max(0.5, float(np.nanmax(finite)) * 1.05) if finite.size else 1.0
    for ant_idx in range(scan.layout.nsolant):
        for pol, pol_name in enumerate(("X", "Y")):
            ax[pol, ant_idx].plot(scan.bands_band[good], np.abs(scan.corrected_band_vis[ant_idx, pol, good]), ".", ms=3)
            ax[pol, ant_idx].set_ylim(0.0, ymax)
            if ant_idx == 0:
                ax[pol, ant_idx].set_ylabel("{0} Amp".format(pol_name))
    return _figure_to_png_bytes(fig)


def render_sum_phase(scan: Optional[ScanAnalysis], ant: Optional[int]) -> bytes:
    """Render browser-native phase summary for all antennas."""

    if scan is None:
        return _blank_figure("Select a scan.", title="Sum Pha")
    good = np.where(scan.bands_band > 0)[0]
    if good.size == 0:
        return _blank_figure("No band-averaged data are available.", title="Sum Pha")
    title = "Band-Averaged Phase Difference" if scan.scan_kind == "phacal" else "Reference-Relative Band Phase"
    fig, ax = _all_ant_axes(scan, title, "Band")
    phase_values = []
    for ant_idx in range(scan.layout.nsolant):
        for pol in range(2):
            bands, phase = _browser_sum_phase_series(scan, ant_idx, pol, good)
            if phase.size:
                phase_values.append(phase)
                ax[pol, ant_idx].plot(bands, phase, ".", ms=3)
            if ant_idx == 0:
                if scan.scan_kind == "phacal":
                    ylabel = "{0} Phase Diff [rad]".format("X" if pol == 0 else "Y")
                else:
                    ylabel = "{0} Phase Rel. Ant 1 [rad]".format("X" if pol == 0 else "Y")
                ax[pol, ant_idx].set_ylabel(ylabel)
    if phase_values:
        finite_sets = [values[np.isfinite(values)] for values in phase_values if np.any(np.isfinite(values))]
        if finite_sets:
            phase_all = np.concatenate(finite_sets)
            limit = max(np.pi, float(np.nanpercentile(np.abs(phase_all), 95)) * 1.15)
            for pol in range(2):
                for ant_idx in range(scan.layout.nsolant):
                    ax[pol, ant_idx].set_ylim(-limit, limit)
    return _figure_to_png_bytes(fig)


def render_inband_fit(scan: Optional[ScanAnalysis], ant: Optional[int]) -> bytes:
    """Render in-band fit diagnostics for all antennas."""

    if scan is None:
        return _blank_figure("Select a scan.", title="Inband Fit")
    if scan.delay_solution is None:
        return _blank_figure("No in-band fit metadata are available.", title="Inband Fit")
    if not scan.raw:
        return _blank_figure(
            "No raw channel data are available for this scan. Reanalyze it in v2 to inspect the in-band fit.",
            title="Inband Fit",
        )
    freq_ghz = np.asarray(scan.raw["channel_freq_ghz"], dtype=float)
    band_id = np.asarray(scan.raw["channel_band"], dtype=int)
    fig, ax = _all_ant_axes(scan, "In-Band Fit Diagnostics", "Frequency [GHz]")
    for ant_idx in range(scan.layout.nsolant):
        chan_avg = np.nanmean(scan.raw["channel_vis"][ant_idx, :2], axis=2)
        for pol, pol_name in enumerate(("X", "Y")):
            for band_idx, band_value in enumerate(scan.delay_solution.band_values):
                idx = np.where(band_id == band_value)[0]
                if idx.size < 2:
                    continue
                phase = np.angle(chan_avg[pol, idx])
                ax[pol, ant_idx].plot(freq_ghz[idx], phase, ".", ms=1.8)
                delay_ns = scan.delay_solution.per_band_delay_ns[ant_idx, pol, band_idx]
                phase0 = scan.delay_solution.per_band_phase0[ant_idx, pol, band_idx]
                if np.isfinite(delay_ns) and np.isfinite(phase0):
                    model = np.angle(np.exp(1j * (phase0 + 2.0 * np.pi * freq_ghz[idx] * delay_ns)))
                    ax[pol, ant_idx].plot(freq_ghz[idx], model, "-", lw=0.9)
            ax[pol, ant_idx].set_ylim(-3.4, 3.4)
            if ant_idx == 0:
                ax[pol, ant_idx].set_ylabel("{0} phase [rad]".format(pol_name))
            ax[pol, ant_idx].text(
                0.03,
                0.93,
                "tau={0:.3f} ns".format(scan.delay_solution.active_ns[ant_idx, pol]),
                transform=ax[pol, ant_idx].transAxes,
                va="top",
                fontsize=7,
            )
    return _figure_to_png_bytes(fig)


def render_inband_applied(scan: Optional[ScanAnalysis], ant: Optional[int], refcal: Optional[ScanAnalysis] = None) -> bytes:
    """Render phase after applying the active in-band delays for all antennas."""

    if scan is None:
        return _blank_figure("Select a scan.", title="Inband Applied")
    if not scan.raw:
        return _blank_figure(
            "No raw channel data are available for this scan. Reanalyze it in v2 to inspect the applied in-band delay.",
            title="Inband Applied",
        )
    freq_ghz = np.asarray(scan.raw["channel_freq_ghz"], dtype=float)
    band_id = np.asarray(scan.raw["channel_band"], dtype=int)
    use_refdiff = bool(
        scan.scan_kind == "phacal"
        and refcal is not None
        and refcal.raw
        and refcal.corrected_channel_vis.shape[2] == scan.corrected_channel_vis.shape[2]
    )
    title = "Phacal Minus Refcal After In-Band Delay" if use_refdiff else "Phase After Active In-Band Delay"
    fig, ax = _all_ant_axes(scan, title, "Frequency [GHz]")
    for ant_idx in range(scan.layout.nsolant):
        scan_avg = np.nanmean(scan.corrected_channel_vis[ant_idx, :2], axis=2)
        for pol, pol_name in enumerate(("X", "Y")):
            if use_refdiff:
                ref_avg = np.nanmean(refcal.corrected_channel_vis[ant_idx, :2], axis=2)
                value = scan_avg[pol] / ref_avg[pol]
                ylabel = "{0} phase diff [rad]".format(pol_name)
            else:
                value = scan_avg[pol]
                ylabel = "{0} phase [rad]".format(pol_name)
            for band_value in np.unique(band_id[band_id > 0]):
                idx = np.where(band_id == band_value)[0]
                if idx.size < 2:
                    continue
                ax[pol, ant_idx].plot(freq_ghz[idx], np.angle(value[idx]), ".", ms=1.8)
            ax[pol, ant_idx].set_ylim(-3.4, 3.4)
            if ant_idx == 0:
                ax[pol, ant_idx].set_ylabel(ylabel)
    return _figure_to_png_bytes(fig)


def render_tab(
    tab_name: str,
    scan: Optional[ScanAnalysis],
    ant: Optional[int],
    band: Optional[int],
    refcal: Optional[ScanAnalysis] = None,
) -> bytes:
    """Dispatch one tab render."""

    if tab_name == "time_history":
        return render_time_history(scan, ant, band)
    if tab_name == "sum_amp":
        return render_sum_amp(scan, ant)
    if tab_name == "sum_pha":
        return render_sum_phase(scan, ant)
    if tab_name == "inband_fit":
        return render_inband_fit(scan, ant)
    if tab_name == "inband_applied":
        return render_inband_applied(scan, ant, refcal=refcal)
    return _blank_figure("Unknown tab: {0}".format(tab_name), title="Plot")


def _blank_payload(message: str, title: str) -> Dict[str, Any]:
    """Return a JSON payload for a blank plot state."""

    return {"message": message, "title": title}


def refresh_model_flag_state(scan: ScanAnalysis, antenna_indices: Optional[Sequence[int]] = None) -> np.ndarray:
    """Apply the current tuned v2/model flag logic to one refcal.

    :param scan: Current analyzed refcal.
    :type scan: ScanAnalysis
    :param antenna_indices: Optional zero-based antenna subset to refresh.
    :type antenna_indices: Sequence[int] | None
    :returns: Per-antenna Y-X residual RMS in radians.
    :rtype: np.ndarray
    """

    if scan.base_flags is None:
        scan.base_flags = np.asarray(scan.flags, dtype=np.int32).copy()
    base_flags = np.asarray(scan.base_flags, dtype=np.int32)
    effective = np.asarray(scan.flags, dtype=np.int32).copy()
    if effective.shape != base_flags.shape:
        effective = base_flags.copy()
    if antenna_indices is None:
        ants = list(range(scan.layout.nsolant))
    else:
        ants = sorted({int(max(0, min(int(ant), scan.layout.nsolant - 1))) for ant in antenna_indices})
    yx_rms = np.asarray(scan.raw.get("yx_residual_rms", np.full(scan.layout.nsolant, np.nan)), dtype=float)
    if yx_rms.shape != (scan.layout.nsolant,):
        yx_rms = np.full(scan.layout.nsolant, np.nan, dtype=float)
    threshold = yx_residual_threshold(scan)

    for ant_i in ants:
        effective[ant_i, :, :] = base_flags[ant_i, :, :]

    if scan.scan_kind != "refcal" or scan.delay_solution is None or not scan.raw:
        scan.flags = effective
        if scan.raw is not None:
            scan.raw["yx_residual_rms"] = yx_rms
        return yx_rms

    if scan_feed_kind(scan) == "hi":
        context = _inband_diag_context(scan)
        for ant_i in ants:
            antenna_payload = _inband_diagnostic_for_antenna(scan, ant_i, context=context)
            rms = float(antenna_payload["xy_panel"].get("yx_residual_rms", np.nan))
            yx_rms[ant_i] = rms
            if np.isfinite(rms) and rms <= threshold:
                effective[ant_i, :2, :] = 0

    manual = np.asarray(scan.delay_solution.manual_ant_flag_override, dtype=bool)
    for ant_i in ants:
        if ant_i < manual.size and manual[ant_i]:
            effective[ant_i, :2, :] = 1

    scan.flags = effective
    scan.raw["yx_residual_rms"] = yx_rms
    return yx_rms


def heatmap_payload(
    scan: Optional[ScanAnalysis],
    ant: Optional[int],
    band: Optional[int],
    scan_label: Optional[str] = None,
) -> Dict[str, Any]:
    """Serialize the sigma map for browser-side rendering."""

    if scan is None:
        return _blank_payload("No scan selected.", "Flag Map")
    flags = _heatmap_flag_map(scan)
    ensure_time_flag_groups(scan)
    applied_cells = sorted(
        {
            (int(ant), int(band))
            for group in scan.time_flag_groups
            for ant, band in group.targets
            if ant < scan.layout.nsolant and band < scan.layout.maxnbd
        }
    )
    return {
        "message": None,
        "title": "Flag Map",
        "subtitle": scan_label or scan.t_bg.iso[11:19],
        "x_label": "Antenna Number",
        "y_label": "Band Number",
        "colorbar_label": "Flag Count",
        "cmap": "discrete-flag-sum",
        "vmin": -0.5,
        "vmax": 2.5,
        "nsolant": scan.layout.nsolant,
        "maxnbd": scan.layout.maxnbd,
        "values": flags.tolist(),
        "selected_ant": ant,
        "selected_band": band,
        "applied_cells": [{"antenna": item[0], "band": item[1]} for item in applied_cells],
        "color_levels": [
            {"value": 0.0, "color": "#440154"},
            {"value": 1.0, "color": "#21918c"},
            {"value": 2.0, "color": "#fde725"},
        ],
        "color_bins": [
            {"min": -0.5, "max": 0.5, "label": "0", "color": "#440154"},
            {"min": 0.5, "max": 1.5, "label": "1", "color": "#21918c"},
            {"min": 1.5, "max": 2.5, "label": "2", "color": "#fde725"},
        ],
    }


def _auto_quality_flagged_antennas(scan: ScanAnalysis) -> np.ndarray:
    """Return antennas whose Y-X residual RMS exceeds the auto-keep threshold."""

    if scan.delay_solution is None or not scan.raw or scan.scan_kind != "refcal" or scan_feed_kind(scan) != "hi":
        return np.zeros(scan.layout.nsolant, dtype=bool)
    yx_rms = np.asarray(scan.raw.get("yx_residual_rms", np.full(scan.layout.nsolant, np.nan)), dtype=float)
    if yx_rms.shape != (scan.layout.nsolant,):
        return np.zeros(scan.layout.nsolant, dtype=bool)
    flagged = np.isfinite(yx_rms) & (yx_rms > yx_residual_threshold(scan))
    keep_override = np.asarray(scan.delay_solution.manual_ant_keep_override, dtype=bool)
    if keep_override.shape == flagged.shape:
        flagged &= ~keep_override
    return flagged


def _tick_values(values: np.ndarray, max_ticks: int = 4) -> Dict[str, Any]:
    """Return compact shared tick positions and labels."""

    finite = np.asarray(values[np.isfinite(values)], dtype=float)
    if finite.size == 0:
        return {"values": [], "labels": []}
    unique = np.unique(finite)
    if unique.size <= max_ticks:
        ticks = unique
    else:
        idx = np.linspace(0, unique.size - 1, max_ticks, dtype=int)
        ticks = unique[idx]
    labels = []
    for value in ticks:
        if np.isclose(value, np.round(value)):
            labels.append(str(int(np.round(value))))
        else:
            labels.append("{0:.2f}".format(value))
    return {"values": ticks.tolist(), "labels": labels}


def _shared_column_controls(scan: ScanAnalysis) -> list[dict[str, Any]]:
    """Return per-antenna manual keep/flag controls for relative-phase panels.

    :param scan: Current analyzed refcal.
    :type scan: ScanAnalysis
    :returns: One checkbox control descriptor per antenna column.
    :rtype: list[dict[str, Any]]
    """

    manual = (
        np.asarray(scan.delay_solution.manual_ant_flag_override, dtype=bool)
        if scan.delay_solution is not None
        else np.zeros(scan.layout.nsolant, dtype=bool)
    )
    manual_keep = (
        np.asarray(scan.delay_solution.manual_ant_keep_override, dtype=bool)
        if scan.delay_solution is not None
        else np.zeros(scan.layout.nsolant, dtype=bool)
    )
    yx_rms = np.asarray(scan.raw.get("yx_residual_rms", np.full(scan.layout.nsolant, np.nan)), dtype=float)
    auto_quality_raw = (
        np.isfinite(yx_rms) & (yx_rms > yx_residual_threshold(scan))
        if yx_rms.shape == (scan.layout.nsolant,)
        else np.zeros(scan.layout.nsolant, dtype=bool)
    )
    auto_quality = auto_quality_raw & ~manual_keep
    return [
        {
            "antenna": int(ant_idx),
            "label": "Ant {0:d}".format(ant_idx + 1),
            "checked": not bool(manual[ant_idx] or auto_quality[ant_idx]),
            "flagged": bool(manual[ant_idx]),
            "auto_flagged": bool(auto_quality[ant_idx]),
        }
        for ant_idx in range(scan.layout.nsolant)
    ]


def _finite_series(x: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
    """Drop non-finite points from one x/y series."""

    valid = np.isfinite(x) & np.isfinite(y)
    return {"x": np.asarray(x[valid], dtype=float).tolist(), "y": np.asarray(y[valid], dtype=float).tolist()}


def _panel_grid_payload(
    title: str,
    x_label: str,
    row_labels: Any,
    x_limits: Any,
    x_ticks: Dict[str, Any],
    y_limits: Any,
    panels: Any,
    auto_scale_rows: bool = False,
) -> Dict[str, Any]:
    """Build one generic panel-grid payload."""

    return {
        "message": None,
        "type": "panel-grid",
        "title": title,
        "x_label": x_label,
        "row_labels": list(row_labels),
        "x_limits": [float(x_limits[0]), float(x_limits[1])],
        "x_ticks": x_ticks,
        "y_limits": [[float(bounds[0]), float(bounds[1])] for bounds in y_limits],
        "panels": panels,
        "legend": [],
        "auto_scale_rows": bool(auto_scale_rows),
    }


def _band_color_map(band_values: np.ndarray) -> Dict[int, str]:
    """Return a stable per-band color map for in-band diagnostics.

    :param band_values: Used band numbers.
    :type band_values: np.ndarray
    :returns: Mapping from band number to CSS hex color.
    :rtype: dict[int, str]
    """

    band_values = np.asarray(band_values, dtype=int)
    if band_values.size == 0:
        return {}
    cmap = matplotlib.colormaps.get_cmap("turbo")
    denom = max(band_values.size - 1, 1)
    return {int(band): to_hex(cmap(float(idx) / denom)) for idx, band in enumerate(band_values.tolist())}


def _band_edges(freq_ghz: np.ndarray, band_id: np.ndarray, band_values: np.ndarray, band_colors: Dict[int, str]) -> list[dict[str, Any]]:
    """Return per-band x extents used for snapping and dimming.

    :param freq_ghz: Channel frequencies in GHz.
    :type freq_ghz: np.ndarray
    :param band_id: Band identifier per channel.
    :type band_id: np.ndarray
    :param band_values: Used band numbers.
    :type band_values: np.ndarray
    :param band_colors: Shared band-color map.
    :type band_colors: dict[int, str]
    :returns: Ordered band-edge metadata.
    :rtype: list[dict[str, Any]]
    """

    out = []
    for band in np.asarray(band_values, dtype=int):
        idx = np.where(band_id == int(band))[0]
        if idx.size == 0:
            continue
        xvals = np.asarray(freq_ghz[idx], dtype=float)
        out.append(
            {
                "band": int(band),
                "x_min": float(np.nanmin(xvals)),
                "x_max": float(np.nanmax(xvals)),
                "x_center": float(np.nanmean(xvals)),
                "color": band_colors.get(int(band), "#4c6571"),
            }
        )
    return out


def _unwrap_valid_phase(values: np.ndarray) -> np.ndarray:
    """Unwrap one phase series while preserving NaN gaps."""

    arr = np.asarray(values, dtype=float)
    out = np.full(arr.shape, np.nan, dtype=float)
    valid = np.isfinite(arr)
    if np.any(valid):
        out[valid] = np.unwrap(arr[valid])
    return out


def _band_series_payload(
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    band_values: np.ndarray,
    y_values: np.ndarray,
) -> list[dict[str, Any]]:
    """Split one channel-domain y-series into per-band x/y payloads."""

    out = []
    for band_value in np.asarray(band_values, dtype=int):
        idx = np.where(band_id == int(band_value))[0]
        if idx.size == 0:
            continue
        out.append(
            {
                "band": int(band_value),
                "x": np.asarray(freq_ghz[idx], dtype=float),
                "y": np.asarray(y_values[idx], dtype=float),
            }
        )
    return out


def _per_band_phase_fit(xvals: np.ndarray, yvals: np.ndarray, band_value: int) -> dict[str, Any]:
    """Return one in-band-style linear fit for a band-domain phase series.

    :param xvals: Frequency values in GHz for one band.
    :type xvals: np.ndarray
    :param yvals: Phase-like values for one band.
    :type yvals: np.ndarray
    :param band_value: Band number.
    :type band_value: int
    :returns: Fit metadata including wrapped display segments.
    :rtype: dict[str, Any]
    """

    xarr = np.asarray(xvals, dtype=float)
    yarr = np.asarray(yvals, dtype=float)
    valid = np.isfinite(xarr) & np.isfinite(yarr)
    if np.count_nonzero(valid) < 2:
        return {}
    try:
        pfit = lin_phase_fit(xarr[valid], yarr[valid])
    except Exception:
        return {}
    xfit = np.linspace(float(np.nanmin(xarr[valid])), float(np.nanmax(xarr[valid])), 64)
    yfit = np.angle(np.exp(1j * (float(pfit[0]) + float(pfit[1]) * xfit)))
    return {
        "phase0": float(pfit[0]),
        "slope": float(pfit[1]),
        "std": float(pfit[2]),
        "delay_ns": float(pfit[1] / (2.0 * np.pi)),
        "segments": wrapped_line_segments(
            xfit,
            yfit,
            band_id=np.full(xfit.shape, int(band_value), dtype=int),
        ),
    }


def _band_mean_complex_series(
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    band_values: np.ndarray,
    vis_values: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return one complex mean per used band at the band-center frequency."""

    bands_out = []
    centers = []
    means = []
    vis_values = np.asarray(vis_values, dtype=np.complex128)
    for band_value in np.asarray(band_values, dtype=int):
        idx = np.where(band_id == int(band_value))[0]
        if idx.size == 0:
            continue
        band_vis = np.asarray(vis_values[idx], dtype=np.complex128)
        valid = np.isfinite(band_vis.real) & np.isfinite(band_vis.imag)
        if not np.any(valid):
            continue
        bands_out.append(int(band_value))
        centers.append(float(np.nanmean(np.asarray(freq_ghz[idx], dtype=float))))
        means.append(np.nanmean(band_vis[valid]))
    return (
        np.asarray(bands_out, dtype=int),
        np.asarray(centers, dtype=float),
        np.asarray(means, dtype=np.complex128),
    )


def _interp_complex_model(freq_src_hz: np.ndarray, vis_src: np.ndarray, freq_dest_hz: np.ndarray) -> np.ndarray:
    """Interpolate a complex model in real/imag parts onto destination frequencies."""

    freq_src_hz = np.asarray(freq_src_hz, dtype=float)
    vis_src = np.asarray(vis_src, dtype=np.complex128)
    freq_dest_hz = np.asarray(freq_dest_hz, dtype=float)
    valid = np.isfinite(freq_src_hz) & np.isfinite(vis_src.real) & np.isfinite(vis_src.imag)
    if np.count_nonzero(valid) < 2:
        return np.full(freq_dest_hz.shape, np.nan + 1j * np.nan, dtype=np.complex128)
    xsrc = freq_src_hz[valid]
    ysrc = vis_src[valid]
    order = np.argsort(xsrc)
    xsrc = xsrc[order]
    ysrc = ysrc[order]
    real_interp = np.interp(freq_dest_hz, xsrc, ysrc.real, left=np.nan, right=np.nan)
    imag_interp = np.interp(freq_dest_hz, xsrc, ysrc.imag, left=np.nan, right=np.nan)
    out = real_interp + 1j * imag_interp
    out[~np.isfinite(freq_dest_hz)] = np.nan + 1j * np.nan
    return out


def _lowfreq_weights(freq_ghz: np.ndarray) -> np.ndarray:
    """Return low-frequency-upweighted fitting weights."""

    freq_ghz = np.asarray(freq_ghz, dtype=float)
    return np.divide(
        1.0,
        np.sqrt(np.clip(freq_ghz, 1e-6, np.inf)),
        out=np.ones_like(freq_ghz, dtype=float),
        where=np.isfinite(freq_ghz),
    )


def _weighted_circular_mean_phase(phases: np.ndarray, weights: np.ndarray) -> float:
    """Return a weighted circular mean phase angle."""

    phases = np.asarray(phases, dtype=float)
    weights = np.asarray(weights, dtype=float)
    valid = np.isfinite(phases) & np.isfinite(weights) & (weights > 0.0)
    if not np.any(valid):
        return 0.0
    z = np.nansum(weights[valid] * np.exp(1j * phases[valid]))
    if not np.isfinite(z.real) or not np.isfinite(z.imag) or np.abs(z) == 0.0:
        return 0.0
    return float(np.angle(z))


def _weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    """Return a weighted mean with sane fallbacks."""

    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)
    valid = np.isfinite(values) & np.isfinite(weights) & (weights > 0.0)
    if not np.any(valid):
        return 0.0
    return float(np.nansum(values[valid] * weights[valid]) / np.nansum(weights[valid]))


def _refit_phase_intercept(
    freq_ghz: np.ndarray,
    phase_rad: np.ndarray,
    weights: np.ndarray,
    valid_mask: np.ndarray,
    slope_rad_per_ghz: float,
) -> float:
    """Return the best-fit phase intercept for a fixed slope.

    :param freq_ghz: Frequency samples in GHz.
    :type freq_ghz: np.ndarray
    :param phase_rad: Unwrapped phase samples aligned to one common branch.
    :type phase_rad: np.ndarray
    :param weights: Sample weights.
    :type weights: np.ndarray
    :param valid_mask: Boolean mask selecting samples used in the fit.
    :type valid_mask: np.ndarray
    :param slope_rad_per_ghz: Fixed slope in radians per GHz.
    :type slope_rad_per_ghz: float
    :returns: Weighted-mean intercept for the supplied slope.
    :rtype: float
    """

    freq = np.asarray(freq_ghz, dtype=float)
    phase = np.asarray(phase_rad, dtype=float)
    wts = np.asarray(weights, dtype=float)
    valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(freq) & np.isfinite(phase) & np.isfinite(wts) & (wts > 0.0)
    if not np.any(valid):
        return 0.0
    return _weighted_mean(phase[valid] - float(slope_rad_per_ghz) * freq[valid], wts[valid])


def _band_phase_samples(
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    band_values: np.ndarray,
    phase_wrapped: np.ndarray,
    weights: np.ndarray,
    valid: np.ndarray,
) -> dict[str, np.ndarray]:
    """Return weighted per-band phase samples suitable for one coarse fit."""

    centers: list[float] = []
    phases: list[float] = []
    totals: list[float] = []
    for band in np.asarray(band_values, dtype=int):
        idx = np.where(valid & (band_id == int(band)))[0]
        if idx.size == 0:
            continue
        centers.append(float(np.nanmean(freq_ghz[idx])))
        phases.append(_weighted_circular_mean_phase(phase_wrapped[idx], weights[idx]))
        totals.append(float(np.nansum(weights[idx])))
    if not centers:
        return {
            "freq": np.asarray([], dtype=float),
            "phase": np.asarray([], dtype=float),
            "weights": np.asarray([], dtype=float),
        }
    freq = np.asarray(centers, dtype=float)
    phase = np.asarray(phases, dtype=float)
    wsum = np.asarray(totals, dtype=float)
    order = np.argsort(freq)
    return {
        "freq": freq[order],
        "phase": np.unwrap(phase[order]),
        "weights": wsum[order],
    }


def _fit_weighted_chebyshev(freq_ghz: np.ndarray, phase_rad: np.ndarray, weights: np.ndarray, degree: int = 3) -> dict[str, Any]:
    """Fit a weighted Chebyshev model on normalized frequency."""

    freq = np.asarray(freq_ghz, dtype=float)
    phase = np.asarray(phase_rad, dtype=float)
    weights = np.asarray(weights, dtype=float)
    valid = np.isfinite(freq) & np.isfinite(phase) & np.isfinite(weights) & (weights > 0.0)
    if np.count_nonzero(valid) <= degree:
        return {"coeffs": np.zeros(degree + 1, dtype=float), "x_min": 0.0, "x_max": 1.0}
    xfit = freq[valid]
    yfit = phase[valid]
    wfit = weights[valid]
    x_min = float(np.nanmin(xfit))
    x_max = float(np.nanmax(xfit))
    span = x_max - x_min
    if not np.isfinite(span) or span <= 0.0:
        span = 1.0
    xnorm = 2.0 * (xfit - x_min) / span - 1.0
    matrix = np.polynomial.chebyshev.chebvander(xnorm, int(degree))
    sqrt_w = np.sqrt(wfit)
    coeffs, *_rest = np.linalg.lstsq(matrix * sqrt_w[:, None], yfit * sqrt_w, rcond=None)
    return {"coeffs": coeffs, "x_min": x_min, "x_max": x_max}


def _eval_weighted_chebyshev(freq_ghz: np.ndarray, fit: dict[str, Any]) -> np.ndarray:
    """Evaluate a weighted Chebyshev model on frequencies in GHz."""

    freq = np.asarray(freq_ghz, dtype=float)
    x_min = float(fit["x_min"])
    x_max = float(fit["x_max"])
    span = x_max - x_min
    if not np.isfinite(span) or span <= 0.0:
        span = 1.0
    xnorm = 2.0 * (freq - x_min) / span - 1.0
    return np.polynomial.chebyshev.chebval(xnorm, np.asarray(fit["coeffs"], dtype=float))


def _fit_shared_phase_model(
    ant: int,
    freq_ghz: np.ndarray,
    phase_rad: np.ndarray,
    weights: np.ndarray,
) -> dict[str, Any]:
    """Fit one shared analytic phase model."""

    freq_ghz = np.asarray(freq_ghz, dtype=float)
    phase_rad = np.asarray(phase_rad, dtype=float)
    weights = np.asarray(weights, dtype=float)
    if ant == 0:
        return {"kind": "chebyshev", "fit": _fit_weighted_chebyshev(freq_ghz, phase_rad, weights, degree=4)}
    valid = np.isfinite(freq_ghz) & np.isfinite(phase_rad) & np.isfinite(weights) & (weights > 0.0)
    if np.count_nonzero(valid) < 2:
        return {"kind": "linear", "coeffs": np.asarray([0.0, 0.0], dtype=float)}
    coeffs, _model = weighted_polyfit(freq_ghz, phase_rad, weights, deg=1, ridge=1e-6)
    return {"kind": "linear", "coeffs": coeffs}


def _eval_shared_phase_model(freq_ghz: np.ndarray, fit: dict[str, Any]) -> np.ndarray:
    """Evaluate one shared analytic phase model."""

    if fit.get("kind") == "chebyshev":
        return _eval_weighted_chebyshev(freq_ghz, fit["fit"])
    coeffs = np.asarray(fit["coeffs"], dtype=float)
    return coeffs[0] + coeffs[1] * np.asarray(freq_ghz, dtype=float)


def _align_phase_segments_to_model(
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    band_values: np.ndarray,
    phase_wrapped: np.ndarray,
    weights: np.ndarray,
    valid: np.ndarray,
    model_phase: np.ndarray,
) -> np.ndarray:
    """Align each band's wrapped phase branch to the supplied model."""

    aligned = np.full(np.asarray(phase_wrapped, dtype=float).shape, np.nan, dtype=float)
    for band in np.asarray(band_values, dtype=int):
        idx = np.where(valid & (band_id == int(band)))[0]
        if idx.size == 0:
            continue
        order = np.argsort(freq_ghz[idx])
        idx = idx[order]
        phase_seg = np.unwrap(np.asarray(phase_wrapped[idx], dtype=float))
        model_seg = np.asarray(model_phase[idx], dtype=float)
        delta = _weighted_mean(model_seg - phase_seg, weights[idx])
        phase_seg = phase_seg + np.round(delta / (2.0 * np.pi)) * 2.0 * np.pi
        aligned[idx] = phase_seg
    return aligned


def _kept_ranges_from_mask(band_values: np.ndarray, mask: np.ndarray) -> list[dict[str, int]]:
    """Convert one boolean kept-band mask into contiguous range metadata."""

    band_values = np.asarray(band_values, dtype=int)
    mask = np.asarray(mask, dtype=bool)
    if band_values.size == 0 or mask.size != band_values.size or not np.any(mask):
        return []
    kept = band_values[mask]
    out: list[dict[str, int]] = []
    start = int(kept[0])
    previous = int(kept[0])
    for value in kept[1:]:
        current = int(value)
        if current != previous + 1:
            out.append({"start_band": start, "end_band": previous})
            start = current
        previous = current
    out.append({"start_band": start, "end_band": previous})
    return out


def _fit_joint_xy_relative_model(
    ant: int,
    band_id: np.ndarray,
    band_values: np.ndarray,
    freq_ghz: np.ndarray,
    corrected_avg: np.ndarray,
    fit_mask_x: Optional[np.ndarray] = None,
    fit_mask_y: Optional[np.ndarray] = None,
    manual_delay_ns: Optional[np.ndarray] = None,
) -> dict[str, Any]:
    """Fit a joint X/Y relative-phase model with Y-X = const.

    Ant 1 uses a shared weighted Chebyshev baseline plus a constant
    polarization offset. Antennas 2+ use a shared weighted linear baseline
    plus a constant polarization offset.
    """

    if ant == 0:
        vis_x = np.asarray(corrected_avg[0, 0], dtype=np.complex128)
        vis_y = np.asarray(corrected_avg[0, 1], dtype=np.complex128)
        title = "Ant 1"
    else:
        vis_x = np.divide(
            np.asarray(corrected_avg[ant, 0], dtype=np.complex128),
            np.asarray(corrected_avg[0, 0], dtype=np.complex128),
            out=np.full(np.asarray(corrected_avg[ant, 0]).shape, np.nan + 0j, dtype=np.complex128),
            where=np.isfinite(corrected_avg[0, 0]) & (np.abs(corrected_avg[0, 0]) > 0.0),
        )
        vis_y = np.divide(
            np.asarray(corrected_avg[ant, 1], dtype=np.complex128),
            np.asarray(corrected_avg[0, 1], dtype=np.complex128),
            out=np.full(np.asarray(corrected_avg[ant, 1]).shape, np.nan + 0j, dtype=np.complex128),
            where=np.isfinite(corrected_avg[0, 1]) & (np.abs(corrected_avg[0, 1]) > 0.0),
        )
        title = "Ant {0:d}-Ant1".format(ant + 1)

    freq_ghz = np.asarray(freq_ghz, dtype=float)
    band_id = np.asarray(band_id, dtype=int)
    band_values = np.asarray(band_values, dtype=int)
    freq_hz = freq_ghz * 1e9
    weights = _lowfreq_weights(freq_ghz)
    valid_x = np.isfinite(freq_ghz) & np.isfinite(vis_x.real) & np.isfinite(vis_x.imag)
    valid_y = np.isfinite(freq_ghz) & np.isfinite(vis_y.real) & np.isfinite(vis_y.imag)
    fit_mask_x = np.ones(freq_ghz.shape, dtype=bool) if fit_mask_x is None else np.asarray(fit_mask_x, dtype=bool).reshape(-1)
    fit_mask_y = np.ones(freq_ghz.shape, dtype=bool) if fit_mask_y is None else np.asarray(fit_mask_y, dtype=bool).reshape(-1)
    if fit_mask_x.shape != freq_ghz.shape:
        fit_mask_x = np.ones(freq_ghz.shape, dtype=bool)
    if fit_mask_y.shape != freq_ghz.shape:
        fit_mask_y = np.ones(freq_ghz.shape, dtype=bool)
    fit_valid_x = valid_x & fit_mask_x
    fit_valid_y = valid_y & fit_mask_y
    common = fit_valid_x & fit_valid_y
    if not np.any(common):
        common = valid_x & valid_y

    if np.any(common):
        ratio = np.divide(
            vis_y[common],
            vis_x[common],
            out=np.full(np.count_nonzero(common), np.nan + 0j, dtype=np.complex128),
            where=np.abs(vis_x[common]) > 0.0,
        )
        dxy = _weighted_circular_mean_phase(np.angle(ratio), weights[common])
    else:
        dxy = 0.0

    display_phase_x = np.angle(vis_x)
    display_phase_y = np.angle(vis_y)
    display_phase_y_rot = np.angle(vis_y * np.exp(-1j * dxy))

    coarse_x = _band_phase_samples(freq_ghz, band_id, band_values, display_phase_x, weights, fit_valid_x)
    coarse_y = _band_phase_samples(freq_ghz, band_id, band_values, display_phase_y_rot, weights, fit_valid_y)
    coarse_freq = np.concatenate([coarse_x["freq"], coarse_y["freq"]]) if coarse_x["freq"].size or coarse_y["freq"].size else np.asarray([], dtype=float)
    coarse_phase = np.concatenate([coarse_x["phase"], coarse_y["phase"]]) if coarse_x["phase"].size or coarse_y["phase"].size else np.asarray([], dtype=float)
    coarse_weights = np.concatenate([coarse_x["weights"], coarse_y["weights"]]) if coarse_x["weights"].size or coarse_y["weights"].size else np.asarray([], dtype=float)
    coarse_fit = _fit_shared_phase_model(ant, coarse_freq, coarse_phase, coarse_weights)
    coarse_model_phase = _eval_shared_phase_model(freq_ghz, coarse_fit)

    aligned_phase_x = _align_phase_segments_to_model(
        freq_ghz,
        band_id,
        band_values,
        display_phase_x,
        weights,
        fit_valid_x,
        coarse_model_phase,
    )
    aligned_phase_y_rot = _align_phase_segments_to_model(
        freq_ghz,
        band_id,
        band_values,
        display_phase_y_rot,
        weights,
        fit_valid_y,
        coarse_model_phase,
    )
    common_aligned = fit_valid_x & fit_valid_y & np.isfinite(aligned_phase_x) & np.isfinite(aligned_phase_y_rot)
    if not np.any(common_aligned):
        common_aligned = common & np.isfinite(aligned_phase_x) & np.isfinite(aligned_phase_y_rot)
    if np.any(common_aligned):
        align_delta = _weighted_mean(aligned_phase_y_rot[common_aligned] - aligned_phase_x[common_aligned], weights[common_aligned])
    else:
        align_delta = 0.0
    aligned_phase_y = aligned_phase_y_rot - align_delta
    dxy_total = float(dxy + align_delta)

    fit_x = fit_valid_x & np.isfinite(aligned_phase_x)
    fit_y = fit_valid_y & np.isfinite(aligned_phase_y)
    shared_freq = np.concatenate([freq_ghz[fit_x], freq_ghz[fit_y]]) if np.any(fit_x) or np.any(fit_y) else np.asarray([], dtype=float)
    shared_phase = np.concatenate([aligned_phase_x[fit_x], aligned_phase_y[fit_y]]) if np.any(fit_x) or np.any(fit_y) else np.asarray([], dtype=float)
    shared_weights = np.concatenate([weights[fit_x], weights[fit_y]]) if np.any(fit_x) or np.any(fit_y) else np.asarray([], dtype=float)
    fit = _fit_shared_phase_model(ant, shared_freq, shared_phase, shared_weights)
    base_model_x_phase = _eval_shared_phase_model(freq_ghz, fit)
    base_model_y_phase = base_model_x_phase + dxy_total
    manual_delay_ns = np.zeros(2, dtype=float) if manual_delay_ns is None else np.asarray(manual_delay_ns, dtype=float).reshape(-1)
    if manual_delay_ns.size < 2:
        manual_delay_ns = np.pad(manual_delay_ns, (0, max(0, 2 - manual_delay_ns.size)), constant_values=0.0)
    if fit.get("kind") == "linear":
        coeffs = np.asarray(fit["coeffs"], dtype=float)
        slope_x = float(coeffs[1] + 2.0 * np.pi * float(manual_delay_ns[0]))
        slope_y = float(coeffs[1] + 2.0 * np.pi * float(manual_delay_ns[1]))
        intercept_x = _refit_phase_intercept(freq_ghz, aligned_phase_x, weights, fit_x, slope_x)
        intercept_y = _refit_phase_intercept(freq_ghz, aligned_phase_y, weights, fit_y, slope_y)
        model_x_phase = intercept_x + slope_x * freq_ghz
        model_y_phase = intercept_y + slope_y * freq_ghz + dxy_total
    else:
        valid_any = np.isfinite(freq_ghz)
        pivot_ghz = float(np.nanmean(freq_ghz[valid_any])) if np.any(valid_any) else 0.0
        model_x_phase = base_model_x_phase + 2.0 * np.pi * (freq_ghz - pivot_ghz) * float(manual_delay_ns[0])
        model_y_phase = base_model_y_phase + 2.0 * np.pi * (freq_ghz - pivot_ghz) * float(manual_delay_ns[1])
        intercept_x = _weighted_mean(aligned_phase_x[fit_x] - model_x_phase[fit_x], weights[fit_x]) if np.any(fit_x) else 0.0
        intercept_y = _weighted_mean(aligned_phase_y[fit_y] - model_y_phase[fit_y], weights[fit_y]) if np.any(fit_y) else 0.0
        model_x_phase = model_x_phase + intercept_x
        model_y_phase = model_y_phase + intercept_y

    residual_x = np.full(freq_ghz.shape, np.nan, dtype=float)
    residual_y = np.full(freq_ghz.shape, np.nan, dtype=float)
    residual_x[valid_x] = np.angle(np.exp(1j * (display_phase_x[valid_x] - model_x_phase[valid_x])))
    residual_y[valid_y] = np.angle(np.exp(1j * (display_phase_y[valid_y] - model_y_phase[valid_y])))
    xy_ratio = np.divide(
        vis_y,
        vis_x,
        out=np.full(vis_x.shape, np.nan + 0j, dtype=np.complex128),
        where=valid_x & valid_y & (np.abs(vis_x) > 0.0),
    )
    xy_phase = np.angle(xy_ratio)
    xy_model_phase = model_y_phase - model_x_phase
    xy_model_mask = valid_x & valid_y & np.isfinite(freq_ghz)
    if np.any(xy_model_mask):
        xy_mean = _weighted_circular_mean_phase(np.angle(np.exp(1j * xy_model_phase[xy_model_mask])), weights[xy_model_mask])
        xy_residual = np.angle(np.exp(1j * (xy_phase[xy_model_mask] - xy_mean)))
        yx_residual_rms = float(np.sqrt(np.average(np.square(xy_residual), weights=weights[xy_model_mask])))
    else:
        xy_mean = float(dxy_total)
        yx_residual_rms = np.nan
    base_delay_ns = np.nan
    auto_delay_ns_by_pol = [0.0, 0.0]
    suggested_delay_ns_by_pol = [0.0, 0.0]
    residual_fit_segments_by_pol: list[list[dict[str, np.ndarray]]] = [[], []]
    if ant != 0:
        coeffs = np.asarray(fit["coeffs"], dtype=float)
        base_delay_ns = float(coeffs[1] / (2.0 * np.pi))
        auto_delay_ns_by_pol = [base_delay_ns, base_delay_ns]
        residual_freq = np.concatenate([freq_hz[fit_x], freq_hz[fit_y]]) if np.any(fit_x) or np.any(fit_y) else np.asarray([], dtype=float)
        residual_vis = (
            np.concatenate(
                [
                    np.exp(1j * residual_x[fit_x]),
                    np.exp(1j * residual_y[fit_y]),
                ]
            )
            if np.any(fit_x) or np.any(fit_y)
            else np.asarray([], dtype=np.complex128)
        )
        residual_weights = np.concatenate([weights[fit_x], weights[fit_y]]) if np.any(fit_x) or np.any(fit_y) else np.asarray([], dtype=float)
        residual_delay = solve_residual_delay_phi0(residual_freq, residual_vis, weights=residual_weights)
        residual_delay_ns = float(residual_delay["dly_res_s"] * 1e9) if np.isfinite(residual_delay["dly_res_s"]) else 0.0
        suggested_delay_ns_by_pol = [residual_delay_ns, residual_delay_ns]
        if np.isfinite(residual_delay["dly_res_s"]) and np.isfinite(residual_delay["phi0_rad"]):
            for pol_idx, fit_mask_pol in enumerate((fit_x, fit_y)):
                valid = fit_mask_pol & np.isfinite(freq_hz)
                if np.count_nonzero(valid) < 2:
                    continue
                residual_fit_phase = np.full(freq_ghz.shape, np.nan, dtype=float)
                residual_fit_phase[valid] = np.angle(
                    np.exp(
                        1j
                        * (
                            2.0 * np.pi * freq_hz[valid] * float(residual_delay["dly_res_s"])
                            + float(residual_delay["phi0_rad"])
                        )
                    )
                )
                residual_fit_segments_by_pol[pol_idx] = wrapped_line_segments(
                    freq_ghz,
                    residual_fit_phase,
                    band_id=band_id,
                    mask=valid,
                )
    annotation_x = "fit Δdelay={0:.3f} ns | Δ(Y-X)={1:.2f} rad".format(
        float(manual_delay_ns[0]) if ant == 0 else base_delay_ns + float(manual_delay_ns[0]),
        xy_mean,
    )
    annotation_y = "fit Δdelay={0:.3f} ns | Δ(Y-X)={1:.2f} rad".format(
        float(manual_delay_ns[1]) if ant == 0 else base_delay_ns + float(manual_delay_ns[1]),
        xy_mean,
    )
    return {
        "title": title,
        "annotation_by_pol": [annotation_x, annotation_y],
        "auto_delay_ns_by_pol": auto_delay_ns_by_pol,
        "suggested_delay_ns_by_pol": suggested_delay_ns_by_pol,
        "delay_ns_by_pol": [
            float(manual_delay_ns[0]) if ant == 0 else base_delay_ns + float(manual_delay_ns[0]),
            float(manual_delay_ns[1]) if ant == 0 else base_delay_ns + float(manual_delay_ns[1]),
        ],
        "phase_by_pol": [display_phase_x, display_phase_y],
        "model_by_pol": [model_x_phase, model_y_phase],
        "model_mask_by_pol": [valid_x, valid_y],
        "residual_by_pol": [residual_x, residual_y],
        "residual_fit_segments_by_pol": residual_fit_segments_by_pol,
        "xy_phase": xy_phase,
        "xy_model_phase": xy_model_phase,
        "xy_model_mask": xy_model_mask,
        "xy_annotation": "Δ(Y-X)={0:.2f} rad | RMS={1:.2f} rad".format(xy_mean, yx_residual_rms),
        "yx_residual_rms": yx_residual_rms,
    }


def _line_series_payload(segments: list[dict[str, np.ndarray]]) -> list[dict[str, Any]]:
    """Convert wrapped line segments into frontend-friendly payload entries."""

    out = []
    for segment in segments:
        out.append(
            {
                "x": np.asarray(segment["x"], dtype=float),
                "y": np.asarray(segment["y"], dtype=float),
            }
        )
    return out


def _fit_curve(freq_ghz: np.ndarray, phase_rad: np.ndarray, degree: int) -> Optional[dict[str, Any]]:
    """Fit one polynomial trend in frequency and return a plotted curve."""

    xvals = np.asarray(freq_ghz, dtype=float)
    yvals = np.asarray(phase_rad, dtype=float)
    valid = np.isfinite(xvals) & np.isfinite(yvals)
    if np.count_nonzero(valid) <= degree:
        return None
    xfit = xvals[valid]
    yfit = yvals[valid]
    order = np.argsort(xfit)
    xfit = xfit[order]
    yfit = yfit[order]
    coeffs = np.polyfit(xfit, yfit, degree)
    return {
        "x": xfit,
        "y": np.polyval(coeffs, xfit),
        "coeffs": coeffs,
    }


def _wrapped_poly_fit(freq_ghz: np.ndarray, phase_rad_wrapped: np.ndarray, degree: int) -> Optional[dict[str, Any]]:
    """Fit one polynomial to an unwrapped copy, then wrap the model for display."""

    fit = _fit_curve(freq_ghz, _unwrap_valid_phase(phase_rad_wrapped), degree)
    if fit is None:
        return None
    return {
        "x": np.asarray(fit["x"], dtype=float),
        "y": np.angle(np.exp(1j * np.asarray(fit["y"], dtype=float))),
        "coeffs": fit["coeffs"],
    }


def _format_kept_ranges(ranges: list[dict[str, int]]) -> str:
    """Format one kept-band range list for compact panel annotations."""

    if not ranges:
        return "none"
    parts = []
    for item in ranges:
        start_band = int(item["start_band"])
        end_band = int(item["end_band"])
        parts.append("{0:d}".format(start_band) if start_band == end_band else "{0:d}-{1:d}".format(start_band, end_band))
    return ", ".join(parts)


def _inband_diag_cache_key(scan: ScanAnalysis) -> str:
    """Return the cache key for in-band diagnostic products.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :returns: Cache signature string for residual diagnostics.
    :rtype: str
    """

    cache_key = scan.delay_solution.window_signature()
    relative_signature = scan.delay_solution.relative_signature()
    return json.dumps({"window": cache_key, "relative": relative_signature}, separators=(",", ":"))


def _set_relative_diag_arrays(scan: ScanAnalysis, ant: int, payload: dict[str, Any]) -> None:
    """Copy one antenna's relative-delay metadata into the live solution.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param ant: Zero-based antenna index.
    :type ant: int
    :param payload: Cached or freshly-computed antenna diagnostics.
    :type payload: dict[str, Any]
    """

    if scan.delay_solution is None:
        return
    ant_i = int(ant)
    scan.delay_solution.relative_auto_ns[ant_i, :] = np.asarray(payload["relative_auto_ns"], dtype=float)
    scan.delay_solution.relative_suggested_ns[ant_i, :] = np.asarray(payload["relative_suggested_ns"], dtype=float)
    if scan.raw is not None:
        yx_rms = np.asarray(scan.raw.get("yx_residual_rms", np.full(scan.layout.nsolant, np.nan)), dtype=float)
        if yx_rms.shape != (scan.layout.nsolant,):
            yx_rms = np.full(scan.layout.nsolant, np.nan, dtype=float)
        yx_rms[ant_i] = float(payload["xy_panel"].get("yx_residual_rms", np.nan))
        scan.raw["yx_residual_rms"] = yx_rms


def _extract_inband_diagnostic_antenna(full_payload: dict[str, Any], ant: int) -> dict[str, Any]:
    """Extract one antenna's diagnostic subset from a full cached payload.

    :param full_payload: Full residual-diagnostics cache payload.
    :type full_payload: dict[str, Any]
    :param ant: Zero-based antenna index.
    :type ant: int
    :returns: One-antenna diagnostic payload.
    :rtype: dict[str, Any]
    """

    ant_i = int(ant)
    return {
        "panels": [
            full_payload["panels"][0][ant_i],
            full_payload["panels"][1][ant_i],
        ],
        "xy_panel": full_payload["xy_panels"][ant_i],
        "relative_auto_ns": np.asarray(full_payload["relative_auto_ns"][ant_i], dtype=float),
        "relative_suggested_ns": np.asarray(full_payload["relative_suggested_ns"][ant_i], dtype=float),
    }


def _inband_diag_context(scan: ScanAnalysis) -> dict[str, Any]:
    """Return shared per-scan arrays used by in-band diagnostics.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :returns: Shared context arrays for diagnostic builders.
    :rtype: dict[str, Any]
    """

    freq_ghz = np.asarray(scan.raw["channel_freq_ghz"], dtype=float)
    band_id = np.asarray(scan.raw["channel_band"], dtype=int)
    band_values = np.asarray(scan.delay_solution.band_values, dtype=int)
    band_colors = _band_color_map(band_values)
    band_edges = _band_edges(freq_ghz, band_id, band_values, band_colors)
    raw_avg, corrected_avg = combined_channel_vis_with_time_flags(scan)
    return {
        "freq_ghz": freq_ghz,
        "freq_hz": freq_ghz * 1e9,
        "band_id": band_id,
        "band_values": band_values,
        "band_colors": band_colors,
        "band_edges": band_edges,
        "raw_avg": raw_avg,
        "corrected_avg": corrected_avg,
    }


def _compute_inband_diagnostic_antenna(scan: ScanAnalysis, ant: int, context: dict[str, Any]) -> dict[str, Any]:
    """Compute residual diagnostics for one antenna only.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param ant: Zero-based antenna index.
    :type ant: int
    :param context: Shared per-scan diagnostic arrays.
    :type context: dict[str, Any]
    :returns: One-antenna diagnostic payload.
    :rtype: dict[str, Any]
    """

    ant_i = int(max(0, min(int(ant), scan.layout.nsolant - 1)))
    freq_ghz = np.asarray(context["freq_ghz"], dtype=float)
    freq_hz = np.asarray(context["freq_hz"], dtype=float)
    band_id = np.asarray(context["band_id"], dtype=int)
    band_values = np.asarray(context["band_values"], dtype=int)
    raw_avg = np.asarray(context["raw_avg"], dtype=np.complex128)
    corrected_avg = np.asarray(context["corrected_avg"], dtype=np.complex128)

    joint_fit = _fit_joint_xy_relative_model(
        ant_i,
        band_id,
        band_values,
        freq_ghz,
        corrected_avg,
        fit_mask_x=np.isin(band_id, band_values[scan.delay_solution.included_band_mask(ant_i, 0)]),
        fit_mask_y=np.isin(band_id, band_values[scan.delay_solution.included_band_mask(ant_i, 1)]),
        manual_delay_ns=scan.delay_solution.relative_ns[ant_i],
    )
    panels: list[dict[str, Any]] = []
    for pol in range(2):
        kept_mask = scan.delay_solution.included_band_mask(ant_i, pol)
        kept_ranges = [
            {"start_band": int(start_band), "end_band": int(end_band)}
            for start_band, end_band in scan.delay_solution.kept_band_ranges(ant_i, pol)
        ]
        included_bands = band_values[kept_mask]
        included_channels = np.isin(band_id, included_bands)
        residual_all = solve_residual_delay_phi0(
            freq_hz,
            corrected_avg[ant_i, pol],
            weights=np.where(included_channels, 1.0, 0.0),
        )
        residual_per_band_delay_ns = np.full(band_values.shape, np.nan, dtype=float)
        raw_phase_by_band: list[dict[str, Any]] = []
        fit_phase_by_band: list[dict[str, Any]] = []
        corrected_phase = np.angle(corrected_avg[ant_i, pol])
        applied_phase_by_band = _band_series_payload(freq_ghz, band_id, band_values, corrected_phase)
        relative_phase = np.asarray(joint_fit["phase_by_pol"][pol], dtype=float)
        relative_model_phase = np.asarray(joint_fit["model_by_pol"][pol], dtype=float)
        relative_model_mask = np.asarray(joint_fit["model_mask_by_pol"][pol], dtype=bool)
        relative_phase_by_band = _band_series_payload(freq_ghz, band_id, band_values, relative_phase)
        relative_fit_segments = _line_series_payload(
            wrapped_line_segments(
                freq_ghz,
                np.angle(np.exp(1j * relative_model_phase)),
                band_id=band_id,
                mask=relative_model_mask,
            )
        )
        smooth_residual = np.asarray(joint_fit["residual_by_pol"][pol], dtype=float)
        residual_phase_by_band = _band_series_payload(freq_ghz, band_id, band_values, smooth_residual)
        residual_fit_segments = _line_series_payload(joint_fit["residual_fit_segments_by_pol"][pol])
        residual_inband_delay_ns = np.nan
        residual_inband_weights = []
        residual_inband_delays = []
        for band_idx, band_value in enumerate(band_values):
            idx = np.where(band_id == int(band_value))[0]
            if idx.size == 0:
                continue
            raw_phase = np.angle(raw_avg[ant_i, pol, idx])
            raw_phase_by_band.append(
                {
                    "band": int(band_value),
                    "x": np.asarray(freq_ghz[idx], dtype=float),
                    "y": np.asarray(raw_phase, dtype=float),
                }
            )
            delay_ns = scan.delay_solution.per_band_delay_ns[ant_i, pol, band_idx]
            phase0 = scan.delay_solution.per_band_phase0[ant_i, pol, band_idx]
            if np.isfinite(delay_ns) and np.isfinite(phase0):
                model = np.angle(np.exp(1j * (phase0 + 2.0 * np.pi * np.asarray(freq_ghz[idx], dtype=float) * delay_ns)))
                fit_phase_by_band.append(
                    {
                        "band": int(band_value),
                        "x": np.asarray(freq_ghz[idx], dtype=float),
                        "y": np.asarray(model, dtype=float),
                    }
                )
            residual_band = solve_residual_delay_phi0(freq_hz[idx], corrected_avg[ant_i, pol, idx])
            if np.isfinite(residual_band["dly_res_s"]):
                residual_per_band_delay_ns[band_idx] = residual_band["dly_res_s"] * 1e9
            band_fit = _per_band_phase_fit(
                np.asarray(freq_ghz[idx], dtype=float),
                np.asarray(smooth_residual[idx], dtype=float),
                int(band_value),
            )
            if (
                int(band_value) in included_bands
                and band_fit
                and np.isfinite(band_fit["delay_ns"])
                and np.isfinite(band_fit["std"])
                and band_fit["std"] > 0.0
            ):
                residual_inband_delays.append(float(band_fit["delay_ns"]))
                residual_inband_weights.append(1.0 / float(band_fit["std"]) ** 2)
        if residual_inband_weights:
            residual_inband_delay_ns = float(
                np.nansum(np.asarray(residual_inband_delays, dtype=float) * np.asarray(residual_inband_weights, dtype=float))
                / np.nansum(np.asarray(residual_inband_weights, dtype=float))
            )
        panels.append(
            {
                "start_band": int(scan.delay_solution.band_window(ant_i, pol)[0]),
                "end_band": int(scan.delay_solution.band_window(ant_i, pol)[1]),
                "kept_ranges": kept_ranges,
                "included_bands": [int(value) for value in included_bands.tolist()],
                "all_residual_delay_ns": float(residual_all["dly_res_s"] * 1e9) if np.isfinite(residual_all["dly_res_s"]) else np.nan,
                "raw_phase_by_band": raw_phase_by_band,
                "fit_phase_by_band": fit_phase_by_band,
                "residual_phase_by_band": residual_phase_by_band,
                "residual_fit_segments": residual_fit_segments,
                "applied_phase_by_band": applied_phase_by_band,
                "relative_phase_by_band": relative_phase_by_band,
                "relative_fit_segments": relative_fit_segments,
                "relative_fit_annotation": joint_fit["annotation_by_pol"][pol],
                "relative_title": joint_fit["title"],
                "relative_model_phase": relative_model_phase,
                "relative_model_mask": relative_model_mask,
                "residual_delay_per_band_ns": residual_per_band_delay_ns,
                "residual_inband_delay_ns": residual_inband_delay_ns,
                "fit_method": "complex_poly_lowfreq",
            }
        )
    xy_kept_mask = scan.delay_solution.included_band_mask(ant_i, 0) & scan.delay_solution.included_band_mask(ant_i, 1)
    xy_included_bands = band_values[xy_kept_mask]
    xy_phase_by_band = _band_series_payload(freq_ghz, band_id, band_values, np.asarray(joint_fit["xy_phase"], dtype=float))
    xy_fit_segments = _line_series_payload(
        wrapped_line_segments(
            freq_ghz,
            np.asarray(joint_fit["xy_model_phase"], dtype=float),
            band_id=band_id,
            mask=np.asarray(joint_fit["xy_model_mask"], dtype=bool),
        )
    )
    return {
        "panels": panels,
        "xy_panel": {
            "title": joint_fit["title"],
            "annotation": joint_fit["xy_annotation"],
            "kept_ranges": _kept_ranges_from_mask(band_values, xy_kept_mask),
            "included_bands": [int(value) for value in xy_included_bands.tolist()],
            "phase_by_band": xy_phase_by_band,
            "fit_segments": xy_fit_segments,
            "model_phase": np.asarray(joint_fit["xy_model_phase"], dtype=float),
            "model_mask": np.asarray(joint_fit["xy_model_mask"], dtype=bool),
            "yx_residual_rms": float(joint_fit["yx_residual_rms"]),
        },
        "relative_auto_ns": np.asarray(joint_fit["auto_delay_ns_by_pol"], dtype=float),
        "relative_suggested_ns": np.asarray(joint_fit["suggested_delay_ns_by_pol"], dtype=float),
    }


def _inband_diagnostic_for_antenna(
    scan: ScanAnalysis,
    ant: int,
    context: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Return cached or freshly-computed diagnostics for one antenna.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param ant: Zero-based antenna index.
    :type ant: int
    :param context: Optional shared per-scan diagnostic arrays.
    :type context: dict[str, Any] | None
    :returns: One-antenna diagnostic payload.
    :rtype: dict[str, Any]
    """

    if scan.delay_solution is None or not scan.raw:
        raise ValueError("In-band diagnostics require raw data and a delay solution.")
    ant_i = int(max(0, min(int(ant), scan.layout.nsolant - 1)))
    combined_key = _inband_diag_cache_key(scan)
    cache = scan.raw.get("residual_diagnostics_cache")
    if not cache or cache.get("key") != combined_key:
        cache = {"key": combined_key, "value": None, "antennas": {}}
        scan.raw["residual_diagnostics_cache"] = cache
    else:
        cache.setdefault("antennas", {})
    if cache.get("value"):
        payload = _extract_inband_diagnostic_antenna(cache["value"], ant_i)
        _set_relative_diag_arrays(scan, ant_i, payload)
        return payload
    ant_key = str(ant_i)
    if ant_key in cache["antennas"]:
        payload = cache["antennas"][ant_key]
        _set_relative_diag_arrays(scan, ant_i, payload)
        return payload
    if context is None:
        context = _inband_diag_context(scan)
    payload = _compute_inband_diagnostic_antenna(scan, ant_i, context)
    cache["antennas"][ant_key] = payload
    _set_relative_diag_arrays(scan, ant_i, payload)
    return payload


def _inband_cache_key(scan: ScanAnalysis, use_lobe: bool = False, refcal: Optional[ScanAnalysis] = None) -> str:
    """Build a compact cache key for overview payloads.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param use_lobe: Sum-phase lobe display toggle.
    :type use_lobe: bool
    :param refcal: Optional active refcal used by phacal displays.
    :type refcal: ScanAnalysis | None
    :returns: Cache signature string.
    :rtype: str
    """

    payload = {
        "scan_id": int(scan.scan_id),
        "scan_kind": scan.scan_kind,
        "use_lobe": bool(use_lobe),
        "dirty_inband": bool(scan.dirty_inband),
        "flagsum": int(np.nansum(scan.flags[: scan.layout.nsolant, :2])),
        "time_flags": len(scan.time_flag_groups),
        "delay_window": None if scan.delay_solution is None else scan.delay_solution.window_signature(),
        "relative_delay": None if scan.delay_solution is None else scan.delay_solution.relative_signature(),
        "active_ref": None if refcal is None or refcal.delay_solution is None else refcal.delay_solution.window_signature(),
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _inband_diagnostics(scan: ScanAnalysis) -> Dict[str, Any]:
    """Compute and cache shared in-band residual diagnostics for one scan.

    :param scan: Current scan with raw channel data and an in-band solution.
    :type scan: ScanAnalysis
    :returns: Cached diagnostic arrays and per-panel metadata.
    :rtype: dict[str, Any]
    """

    if scan.delay_solution is None or not scan.raw:
        raise ValueError("In-band diagnostics require raw data and a delay solution.")
    cache = scan.raw.get("residual_diagnostics_cache")
    combined_key = _inband_diag_cache_key(scan)
    if cache and cache.get("key") == combined_key and cache.get("value"):
        if scan.delay_solution is not None and cache.get("value"):
            cached_value = cache["value"]
            if "relative_auto_ns" in cached_value:
                scan.delay_solution.relative_auto_ns[:] = np.asarray(cached_value["relative_auto_ns"], dtype=float)
            if "relative_suggested_ns" in cached_value:
                scan.delay_solution.relative_suggested_ns[:] = np.asarray(cached_value["relative_suggested_ns"], dtype=float)
        return cache["value"]
    context = _inband_diag_context(scan)
    freq_ghz = np.asarray(context["freq_ghz"], dtype=float)
    band_values = np.asarray(context["band_values"], dtype=int)
    panel_meta: list[list[dict[str, Any]]] = [[], []]
    relative_auto_ns = np.zeros((scan.layout.nsolant, 2), dtype=float)
    relative_suggested_ns = np.zeros((scan.layout.nsolant, 2), dtype=float)
    for ant in range(scan.layout.nsolant):
        antenna_payload = _inband_diagnostic_for_antenna(scan, ant, context=context)
        panel_meta[0].append(antenna_payload["panels"][0])
        panel_meta[1].append(antenna_payload["panels"][1])
        relative_auto_ns[ant, :] = np.asarray(antenna_payload["relative_auto_ns"], dtype=float)
        relative_suggested_ns[ant, :] = np.asarray(antenna_payload["relative_suggested_ns"], dtype=float)
    xy_panel_meta: list[dict[str, Any]] = []
    yx_residual_rms = np.full(scan.layout.nsolant, np.nan, dtype=float)
    for ant in range(scan.layout.nsolant):
        antenna_payload = _inband_diagnostic_for_antenna(scan, ant, context=context)
        xy_panel_meta.append(antenna_payload["xy_panel"])
        yx_residual_rms[ant] = float(antenna_payload["xy_panel"].get("yx_residual_rms", np.nan))
    payload = {
        "freq_ghz": freq_ghz,
        "band_id": context["band_id"],
        "band_values": band_values,
        "band_edges": context["band_edges"],
        "band_colors": context["band_colors"],
        "panels": panel_meta,
        "xy_panels": xy_panel_meta,
        "relative_auto_ns": relative_auto_ns,
        "relative_suggested_ns": relative_suggested_ns,
        "yx_residual_rms": yx_residual_rms,
    }
    scan.delay_solution.relative_auto_ns[:] = relative_auto_ns
    scan.delay_solution.relative_suggested_ns[:] = relative_suggested_ns
    scan.raw["yx_residual_rms"] = np.asarray(yx_residual_rms, dtype=float)
    antenna_cache = {}
    if cache and cache.get("key") == combined_key:
        antenna_cache = cache.get("antennas", {})
    scan.raw["residual_diagnostics_cache"] = {"key": combined_key, "value": payload, "antennas": antenna_cache}
    return payload


def _sum_amp_vis(scan: ScanAnalysis) -> np.ndarray:
    """Return the band-averaged visibility cube used for sum-amplitude plots."""

    legacy = legacy_refcal_display_summary(scan)
    if legacy is not None and "x" in legacy:
        return np.asarray(legacy["x"], dtype=np.complex128)
    return np.asarray(scan.corrected_band_vis, dtype=np.complex128)


def _sum_phase_refcal_series(scan: ScanAnalysis, ant_idx: int, pol: int, good: np.ndarray, use_lobe: bool = False) -> Any:
    """Return v2-corrected refcal phase points for one antenna/pol.

    `scan.corrected_band_vis` already contains the active in-band delay
    correction. For the browser Sum Pha view, keep antenna 1 on the same
    legacy display path that the Tk widget uses, and show antennas 2+ relative
    to antenna 1 from the corrected product. The caller can enable the
    legacy-style `lobe(...)` remapping for those relative phases.
    """

    vis = np.asarray(scan.corrected_band_vis, dtype=np.complex128)
    legacy = legacy_refcal_display_summary(scan)
    legacy_vis = np.asarray(legacy["x"], dtype=np.complex128) if legacy is not None and "x" in legacy else None
    bands = scan.bands_band[good].astype(float)
    if ant_idx == 0:
        ref_vis = legacy_vis if legacy_vis is not None else vis
        phz = np.unwrap(np.angle(ref_vis[0, pol, good]))
        if phz.size:
            anchor_idx = min(scan.layout.nsolant + 1, phz.size - 1)
            phz = phz - np.round(phz[anchor_idx] / (2.0 * np.pi)) * 2.0 * np.pi
        return bands, phz
    if use_lobe and legacy_vis is not None:
        phz = lobe(np.unwrap(np.angle(legacy_vis[ant_idx, pol, good]) - np.angle(legacy_vis[0, pol, good])))
        return bands, phz
    ratio = np.divide(
        vis[ant_idx, pol, good],
        vis[0, pol, good],
        out=np.full(good.size, np.nan + 0j, dtype=np.complex128),
        where=np.abs(vis[0, pol, good]) > 0.0,
    )
    phz = np.unwrap(np.angle(ratio))
    return bands, lobe(phz) if use_lobe else phz


def _legacy_phacal_phase_series(scan: ScanAnalysis, ant_idx: int, pol: int, use_lobe: bool = False) -> Any:
    """Return legacy-style phacal phase points for one antenna/pol."""

    if scan.pdiff is None:
        return np.array([], dtype=float), np.array([], dtype=float)
    good = np.where(np.asarray(scan.flags[ant_idx, pol]) == 0)[0]
    if good.size == 0:
        return np.array([], dtype=float), np.array([], dtype=float)
    bands = scan.bands_band[good].astype(float)
    phz = np.unwrap(np.asarray(scan.pdiff[ant_idx, pol, good], dtype=float))
    if phz.size:
        if phz[0] < -np.pi:
            phz = phz + 2.0 * np.pi
        if phz[0] >= np.pi:
            phz = phz - 2.0 * np.pi
    return bands, lobe(phz) if use_lobe else phz


def sum_amp_payload(scan: Optional[ScanAnalysis]) -> Dict[str, Any]:
    """Serialize band-averaged amplitude for all antennas."""

    if scan is None:
        return _blank_payload("Select a scan.", "Sum Amp")
    good = np.where(scan.bands_band > 0)[0]
    if good.size == 0:
        return _blank_payload("No band-averaged data are available.", "Sum Amp")
    vis = _sum_amp_vis(scan)
    xvals = scan.bands_band[good].astype(float)
    panels = []
    for pol, pol_name in enumerate(("X", "Y")):
        row = []
        for ant_idx in range(scan.layout.nsolant):
            series = _finite_series(xvals, np.abs(vis[ant_idx, pol, good]))
            row.append(
                {
                    "title": "Ant {0:d}".format(ant_idx + 1),
                    "annotation": None,
                    "series": [{"label": "Amplitude", "mode": "points", "color": "#1f77b4", "x": series["x"], "y": series["y"]}],
                }
            )
        panels.append(row)
    payload = _panel_grid_payload(
        "Band-Averaged Amplitude",
        "Band",
        ["XX Amplitude", "YY Amplitude"],
        [float(np.nanmin(xvals)), float(np.nanmax(xvals))],
        _tick_values(xvals, max_ticks=4),
        [[0.0, 0.5], [0.0, 0.5]],
        panels,
    )
    payload["legend"] = []
    return payload


def sum_phase_payload(scan: Optional[ScanAnalysis], use_lobe: bool = False) -> Dict[str, Any]:
    """Serialize legacy-style phase summary for all antennas."""

    if scan is None:
        return _blank_payload("Select a scan.", "Sum Pha")
    good = np.where(scan.bands_band > 0)[0]
    if good.size == 0:
        return _blank_payload("No band-averaged data are available.", "Sum Pha")
    panels = []
    for pol in range(2):
        row = []
        for ant_idx in range(scan.layout.nsolant):
            if scan.scan_kind == "phacal":
                xvals, yvals = _legacy_phacal_phase_series(scan, ant_idx, pol, use_lobe=use_lobe)
            else:
                xvals, yvals = _sum_phase_refcal_series(scan, ant_idx, pol, good, use_lobe=use_lobe)
            series = _finite_series(xvals, yvals)
            row.append(
                {
                    "title": "Ant {0:d}".format(ant_idx + 1),
                    "annotation": None,
                    "series": [{"label": "Phase", "mode": "points", "color": "#ff7f0e", "x": series["x"], "y": series["y"]}],
                }
            )
        panels.append(row)
    row_labels = ["XX Phase (rad)", "YY Phase (rad)"] if scan.scan_kind != "phacal" else ["XX Phase Diff (rad)", "YY Phase Diff (rad)"]
    xvals = np.asarray(scan.bands_band[good], dtype=float)
    payload = _panel_grid_payload(
        "Band-Averaged Phase Difference" if scan.scan_kind == "phacal" else "Reference-Relative Band Phase",
        "Band",
        row_labels,
        [float(np.nanmin(xvals)), float(np.nanmax(xvals))],
        _tick_values(xvals, max_ticks=4),
        [[-np.pi, np.pi], [-np.pi, np.pi]],
        panels,
        auto_scale_rows=True,
    )
    payload["legend"] = []
    return payload


def inband_fit_payload(scan: Optional[ScanAnalysis]) -> Dict[str, Any]:
    """Serialize in-band fit diagnostics for all antennas."""

    if scan is None:
        return _blank_payload("Select a scan.", "Inband Fit")
    if scan.delay_solution is None:
        return _blank_payload("No in-band fit metadata are available.", "Inband Fit")
    if not scan.raw:
        return _blank_payload("No raw channel data are available for this scan.", "Inband Fit")
    diag = _inband_diagnostics(scan)
    freq_ghz = np.asarray(diag["freq_ghz"], dtype=float)
    panels = []
    all_x = freq_ghz[np.isfinite(freq_ghz)]
    for pol, pol_name in enumerate(("X", "Y")):
        row = []
        for ant_idx in range(scan.layout.nsolant):
            series_list = []
            panel_diag = diag["panels"][pol][ant_idx]
            included_bands = set(panel_diag["included_bands"])
            for band_data in panel_diag["raw_phase_by_band"]:
                series = _finite_series(band_data["x"], band_data["y"])
                series_list.append(
                    {
                        "label": "Raw phase",
                        "role": "raw",
                        "band": int(band_data["band"]),
                        "mode": "points",
                        "color": diag["band_colors"][int(band_data["band"])],
                        "opacity": 0.95 if int(band_data["band"]) in included_bands else 0.18,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            for band_fit in panel_diag["fit_phase_by_band"]:
                series = _finite_series(band_fit["x"], band_fit["y"])
                series_list.append(
                    {
                        "label": "Fit",
                        "role": "fit",
                        "band": int(band_fit["band"]),
                        "mode": "line",
                        "color": "#c43c35",
                        "opacity": 0.95 if int(band_fit["band"]) in included_bands else 0.25,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            row.append(
                {
                    "title": "Ant {0:d}".format(ant_idx + 1),
                    "annotation": "tau={0:.3f} ns | kept {1}".format(
                        scan.delay_solution.active_ns[ant_idx, pol],
                        _format_kept_ranges(panel_diag["kept_ranges"]),
                    ),
                    "kept_ranges": panel_diag["kept_ranges"],
                    "series": series_list,
                }
            )
        panels.append(row)
    payload = _panel_grid_payload(
        "In-Band Fit Diagnostics",
        "Frequency [GHz]",
        ["X Phase [rad]", "Y Phase [rad]"],
        [float(np.nanmin(all_x)), float(np.nanmax(all_x))],
        _tick_values(all_x, max_ticks=4),
        [[-3.4, 3.4], [-3.4, 3.4]],
        panels,
    )
    payload["legend"] = [{"label": "Fit", "color": "#c43c35", "mode": "line"}]
    payload["band_edges"] = diag["band_edges"]
    return payload


def inband_applied_payload(scan: Optional[ScanAnalysis], refcal: Optional[ScanAnalysis] = None) -> Dict[str, Any]:
    """Serialize phase after applying the active in-band delays."""

    if scan is None:
        return _blank_payload("Select a scan.", "Inband Applied")
    if not scan.raw:
        return _blank_payload("No raw channel data are available for this scan.", "Inband Applied")
    diag = _inband_diagnostics(scan)
    freq_ghz = np.asarray(diag["freq_ghz"], dtype=float)
    use_refdiff = bool(
        scan.scan_kind == "phacal"
        and refcal is not None
        and refcal.raw
        and refcal.corrected_channel_vis.shape[2] == scan.corrected_channel_vis.shape[2]
    )
    panels = []
    all_x = freq_ghz[np.isfinite(freq_ghz)]
    for pol, pol_name in enumerate(("X", "Y")):
        row = []
        for ant_idx in range(scan.layout.nsolant):
            included_bands = set(diag["panels"][pol][ant_idx]["included_bands"])
            if use_refdiff:
                scan_avg = np.nanmean(scan.corrected_channel_vis[ant_idx, :2], axis=2)
                ref_avg = np.nanmean(refcal.corrected_channel_vis[ant_idx, :2], axis=2)
                value = scan_avg[pol] / ref_avg[pol]
                band_id = np.asarray(scan.raw["channel_band"], dtype=int)
                series_source = []
                for band in diag["band_values"]:
                    idx = np.where(band_id == int(band))[0]
                    series_source.append({"band": int(band), "x": freq_ghz[idx], "y": np.angle(value[idx])})
            else:
                series_source = diag["panels"][pol][ant_idx]["applied_phase_by_band"]
            series_list = []
            for band_phase in series_source:
                phase_series = _finite_series(np.asarray(band_phase["x"], dtype=float), np.asarray(band_phase["y"], dtype=float))
                series_list.append(
                    {
                        "label": "Applied phase",
                        "role": "data",
                        "band": int(band_phase["band"]),
                        "mode": "points",
                        "color": diag["band_colors"][int(band_phase["band"])],
                        "opacity": 0.95 if int(band_phase["band"]) in included_bands else 0.18,
                        "x": phase_series["x"],
                        "y": phase_series["y"],
                    }
                )
            row.append(
                {
                    "title": "Ant {0:d}".format(ant_idx + 1),
                    "annotation": None,
                    "series": series_list,
                }
            )
        panels.append(row)
    row_labels = ["X Phase Diff [rad]", "Y Phase Diff [rad]"] if use_refdiff else ["X Phase [rad]", "Y Phase [rad]"]
    title = "Phacal Minus Refcal After In-Band Delay" if use_refdiff else "Phase After Active In-Band Delay"
    payload = _panel_grid_payload(
        title,
        "Frequency [GHz]",
        row_labels,
        [float(np.nanmin(all_x)), float(np.nanmax(all_x))],
        _tick_values(all_x, max_ticks=4),
        [[-3.4, 3.4], [-3.4, 3.4]],
        panels,
    )
    payload["legend"] = []
    payload["band_edges"] = diag["band_edges"]
    return payload


def inband_residual_phase_band_payload(scan: Optional[ScanAnalysis]) -> Dict[str, Any]:
    """Serialize per-band residual phase after removing each band's fitted line."""

    if scan is None:
        return _blank_payload("Select a scan.", "Per-Band Residual Phase")
    if scan.delay_solution is None or not scan.raw:
        return _blank_payload("No in-band residual phase data are available.", "Per-Band Residual Phase")
    diag = _inband_diagnostics(scan)
    panels = []
    all_x = np.asarray(diag["freq_ghz"], dtype=float)
    for pol, pol_name in enumerate(("X", "Y")):
        row = []
        for ant_idx in range(scan.layout.nsolant):
            panel_diag = diag["panels"][pol][ant_idx]
            included_bands = set(panel_diag["included_bands"])
            series_list = []
            for segment in panel_diag.get("residual_fit_segments", []):
                slope_series = _finite_series(segment["x"], segment["y"])
                series_list.append(
                    {
                        "label": "Multiband Fit",
                        "role": "fit",
                        "mode": "line",
                        "color": "#c43c35",
                        "opacity": 0.95,
                        "x": slope_series["x"],
                        "y": slope_series["y"],
                    }
                )
            for band_residual in panel_diag["residual_phase_by_band"]:
                band_value = int(band_residual["band"])
                series = _finite_series(band_residual["x"], band_residual["y"])
                series_list.append(
                    {
                        "label": "Residual phase",
                        "role": "data",
                        "band": band_value,
                        "mode": "points",
                        "color": diag["band_colors"][band_value],
                        "opacity": 0.95 if band_value in included_bands else 0.18,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
                band_fit = _per_band_phase_fit(band_residual["x"], band_residual["y"], band_value)
                for segment in band_fit.get("segments", []):
                    fit_series = _finite_series(segment["x"], segment["y"])
                    series_list.append(
                        {
                            "label": "Inband Fit",
                            "role": "fit",
                            "mode": "line",
                            "color": "#2a6fdb",
                            "opacity": 0.95 if band_value in included_bands else 0.18,
                            "x": fit_series["x"],
                            "y": fit_series["y"],
                        }
                    )
            row.append({"title": "Ant {0:d}".format(ant_idx + 1), "annotation": None, "series": series_list})
        panels.append(row)
    payload = _panel_grid_payload(
        "Per-Band Residual Phase",
        "Frequency [GHz]",
        ["XX residual [rad]", "YY residual [rad]"],
        [float(np.nanmin(all_x)), float(np.nanmax(all_x))],
        _tick_values(all_x, max_ticks=4),
        [[-3.4, 3.4], [-3.4, 3.4]],
        panels,
    )
    payload["legend"] = [
        {"label": "Multiband Fit", "color": "#c43c35", "mode": "line"},
        {"label": "Inband Fit", "color": "#2a6fdb", "mode": "line"},
    ]
    payload["band_edges"] = diag["band_edges"]
    payload["fit_method"] = "complex_poly_lowfreq"
    return payload


def inband_relative_phase_payload(scan: Optional[ScanAnalysis]) -> Dict[str, Any]:
    """Serialize corrected relative phase with fitted trends after in-band correction."""

    if scan is None:
        return _blank_payload("Select a scan.", "Relative Phase + Fit")
    if scan.delay_solution is None or not scan.raw:
        return _blank_payload("No relative-phase data are available.", "Relative Phase + Fit")
    diag = _inband_diagnostics(scan)
    manual_flagged = np.asarray(scan.delay_solution.manual_ant_flag_override, dtype=bool)
    auto_quality_flagged = _auto_quality_flagged_antennas(scan)
    panels = []
    all_x = np.asarray(diag["freq_ghz"], dtype=float)
    for pol in range(2):
        row = []
        for ant_idx in range(scan.layout.nsolant):
            panel_diag = diag["panels"][pol][ant_idx]
            included_bands = set(panel_diag["included_bands"])
            series_list = []
            for band_phase in panel_diag["relative_phase_by_band"]:
                series = _finite_series(band_phase["x"], band_phase["y"])
                series_list.append(
                    {
                        "label": "Relative phase",
                        "role": "data",
                        "band": int(band_phase["band"]),
                        "mode": "points",
                        "color": diag["band_colors"][int(band_phase["band"])],
                        "opacity": 0.95 if int(band_phase["band"]) in included_bands else 0.18,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            for segment in panel_diag.get("relative_fit_segments", []):
                series = _finite_series(segment["x"], segment["y"])
                series_list.append(
                    {
                        "label": "Fit",
                        "role": "fit",
                        "mode": "line",
                        "color": "#c43c35",
                        "opacity": 0.95,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            row.append(
                {
                    "title": panel_diag.get("relative_title", "Ant {0:d}".format(ant_idx + 1)),
                    "annotation": panel_diag["relative_fit_annotation"],
                    "kept_ranges": panel_diag["kept_ranges"],
                    "disabled": bool(manual_flagged[ant_idx] or auto_quality_flagged[ant_idx]),
                    "series": series_list,
                }
            )
        panels.append(row)
    xy_row = []
    for ant_idx in range(scan.layout.nsolant):
        panel_diag = diag["xy_panels"][ant_idx]
        included_bands = set(panel_diag["included_bands"])
        series_list = []
        for band_phase in panel_diag["phase_by_band"]:
            series = _finite_series(band_phase["x"], band_phase["y"])
            series_list.append(
                {
                    "label": "Y-X phase",
                    "role": "data",
                    "band": int(band_phase["band"]),
                    "mode": "points",
                    "color": diag["band_colors"][int(band_phase["band"])],
                    "opacity": 0.95 if int(band_phase["band"]) in included_bands else 0.18,
                    "x": series["x"],
                    "y": series["y"],
                }
            )
        for segment in panel_diag.get("fit_segments", []):
            series = _finite_series(segment["x"], segment["y"])
            series_list.append(
                {
                    "label": "Fit",
                    "role": "fit",
                    "mode": "line",
                    "color": "#c43c35",
                    "opacity": 0.95,
                    "x": series["x"],
                    "y": series["y"],
                }
            )
        xy_row.append(
            {
                "title": panel_diag["title"],
                "annotation": panel_diag["annotation"],
                "kept_ranges": panel_diag["kept_ranges"],
                "disabled": bool(manual_flagged[ant_idx] or auto_quality_flagged[ant_idx]),
                "series": series_list,
            }
        )
    panels.append(xy_row)
    payload = _panel_grid_payload(
        "Relative Phase + Fit",
        "Frequency [GHz]",
        ["XX Relative Phase [rad]", "YY Relative Phase [rad]", "Y-X Phase [rad]"],
        [float(np.nanmin(all_x)), float(np.nanmax(all_x))],
        _tick_values(all_x, max_ticks=4),
        [[-3.4, 3.4], [-3.4, 3.4], [-3.4, 3.4]],
        panels,
        auto_scale_rows=False,
    )
    payload["legend"] = [{"label": "Fit", "color": "#c43c35", "mode": "line"}]
    payload["band_edges"] = diag["band_edges"]
    payload["fit_method"] = "complex_poly_lowfreq"
    payload["column_controls"] = _shared_column_controls(scan)
    return payload


def inband_residual_delay_band_payload(scan: Optional[ScanAnalysis]) -> Dict[str, Any]:
    """Serialize per-band residual delay after applying the active mean delay."""

    if scan is None:
        return _blank_payload("Select a scan.", "Residual Delay Per Band")
    if scan.delay_solution is None or not scan.raw:
        return _blank_payload("No residual-delay data are available.", "Residual Delay Per Band")
    diag = _inband_diagnostics(scan)
    xvals = np.asarray([edge["x_center"] for edge in diag["band_edges"]], dtype=float)
    if xvals.size == 0:
        return _blank_payload("No residual-delay data are available.", "Residual Delay Per Band")
    panels = []
    for pol in range(2):
        row = []
        for ant_idx in range(scan.layout.nsolant):
            panel_diag = diag["panels"][pol][ant_idx]
            included_bands = set(panel_diag["included_bands"])
            series_list = []
            for band_idx, band_value in enumerate(diag["band_values"]):
                yval = panel_diag["residual_delay_per_band_ns"][band_idx]
                if not np.isfinite(yval):
                    continue
                center = next((edge["x_center"] for edge in diag["band_edges"] if int(edge["band"]) == int(band_value)), None)
                if center is None:
                    continue
                series_list.append(
                    {
                        "label": "Residual delay",
                        "role": "data",
                        "band": int(band_value),
                        "mode": "points",
                        "color": diag["band_colors"][int(band_value)],
                        "opacity": 0.95 if int(band_value) in included_bands else 0.18,
                        "x": [float(center)],
                        "y": [float(yval)],
                    }
                )
            if np.isfinite(panel_diag["all_residual_delay_ns"]):
                series_list.append(
                    {
                        "label": "All-band residual delay",
                        "role": "fit",
                        "mode": "line",
                        "color": "#c43c35",
                        "dasharray": "5 4",
                        "x": [float(np.nanmin(xvals)), float(np.nanmax(xvals))],
                        "y": [float(panel_diag["all_residual_delay_ns"]), float(panel_diag["all_residual_delay_ns"])],
                    }
                )
            row.append({"title": "Ant {0:d}".format(ant_idx + 1), "annotation": None, "series": series_list})
        panels.append(row)
    payload = _panel_grid_payload(
        "Residual Delay Per Band (After In-Band Correction)",
        "Band center [GHz]",
        ["XX residual delay [ns]", "YY residual delay [ns]"],
        [float(np.nanmin(xvals)), float(np.nanmax(xvals))],
        _tick_values(xvals, max_ticks=4),
        [[-1.0, 1.0], [-1.0, 1.0]],
        panels,
        auto_scale_rows=True,
    )
    payload["legend"] = [{"label": "All-band residual delay", "color": "#c43c35", "mode": "line"}]
    payload["band_edges"] = diag["band_edges"]
    return payload


def tab_payload(
    tab_name: str,
    scan: Optional[ScanAnalysis],
    refcal: Optional[ScanAnalysis] = None,
    use_lobe: bool = False,
) -> Dict[str, Any]:
    """Serialize one non-time-history tab for JS rendering."""

    if tab_name == "sum_amp":
        return sum_amp_payload(scan)
    if tab_name == "sum_pha":
        return sum_phase_payload(scan, use_lobe=use_lobe)
    if tab_name == "inband_fit":
        return inband_fit_payload(scan)
    if tab_name == "inband_applied":
        return inband_applied_payload(scan, refcal=refcal)
    return _blank_payload("Unknown tab: {0}".format(tab_name), "Plot")


def overview_payloads(
    scan: Optional[ScanAnalysis],
    refcal: Optional[ScanAnalysis] = None,
    use_lobe: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Return all always-visible overview payloads in one response."""

    if scan is not None and scan.raw:
        cache_key = _inband_cache_key(scan, use_lobe=use_lobe, refcal=refcal)
        cache = scan.raw.get("overview_payload_cache")
        if cache and cache.get("key") == cache_key:
            return cache["value"]
    payload = {
        "sum_amp": sum_amp_payload(scan),
        "sum_pha": sum_phase_payload(scan, use_lobe=use_lobe),
        "inband_fit": inband_fit_payload(scan),
        "inband_residual_phase_band": inband_residual_phase_band_payload(scan),
        "inband_relative_phase": inband_relative_phase_payload(scan),
        "inband_residual_delay_band": inband_residual_delay_band_payload(scan),
    }
    if scan is not None and scan.raw:
        scan.raw["overview_payload_cache"] = {"key": cache_key, "value": payload}
    return payload


def _normalize_sparse_antennas(scan: ScanAnalysis, antenna_indices: Sequence[int]) -> list[int]:
    """Return unique valid antenna indices for sparse panel refreshes.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param antenna_indices: Candidate antenna indices.
    :type antenna_indices: Sequence[int]
    :returns: Unique clamped antenna indices in ascending order.
    :rtype: list[int]
    """

    valid = {
        int(max(0, min(int(ant), scan.layout.nsolant - 1)))
        for ant in antenna_indices
        if ant is not None
    }
    return sorted(valid)


def _expand_reference_antenna_dependencies(scan: ScanAnalysis, antenna_indices: Sequence[int]) -> list[int]:
    """Expand sparse antenna updates when antenna 1 affects relative products.

    Antenna 1 is the reference antenna for relative-phase and sum-phase
    products. When its active in-band delay changes, all reference-relative
    panels depend on that updated row.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param antenna_indices: Candidate antenna indices.
    :type antenna_indices: Sequence[int]
    :returns: Normalized antenna list, expanded to all antennas when antenna 1
        is included.
    :rtype: list[int]
    """

    sparse_antennas = _normalize_sparse_antennas(scan, antenna_indices)
    if 0 in sparse_antennas:
        return list(range(scan.layout.nsolant))
    return sparse_antennas


def _sum_phase_partial_payload(
    scan: ScanAnalysis,
    use_lobe: bool,
    antenna_indices: Sequence[int],
) -> Dict[str, Any]:
    """Return sparse band-averaged phase payload for selected antennas.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param use_lobe: Whether to apply legacy lobe wrapping.
    :type use_lobe: bool
    :param antenna_indices: Zero-based antenna indices to refresh.
    :type antenna_indices: Sequence[int]
    :returns: Sparse sum-phase payload.
    :rtype: dict[str, Any]
    """

    sparse_antennas = _expand_reference_antenna_dependencies(scan, antenna_indices)
    if scan is None:
        return _blank_payload("Select a scan.", "Sum Pha")
    good = np.where(scan.bands_band > 0)[0]
    if good.size == 0:
        return _blank_payload("No band-averaged data are available.", "Sum Pha")
    panels = [[None] * scan.layout.nsolant for _ in range(2)]
    for pol in range(2):
        for ant_idx in sparse_antennas:
            if scan.scan_kind == "phacal":
                xvals, yvals = _legacy_phacal_phase_series(scan, ant_idx, pol, use_lobe=use_lobe)
            else:
                xvals, yvals = _sum_phase_refcal_series(scan, ant_idx, pol, good, use_lobe=use_lobe)
            series = _finite_series(xvals, yvals)
            panels[pol][ant_idx] = {
                "title": "Ant {0:d}".format(ant_idx + 1),
                "annotation": None,
                "series": [{"label": "Phase", "mode": "points", "color": "#ff7f0e", "x": series["x"], "y": series["y"]}],
            }
    row_labels = ["XX Phase (rad)", "YY Phase (rad)"] if scan.scan_kind != "phacal" else ["XX Phase Diff (rad)", "YY Phase Diff (rad)"]
    xvals = np.asarray(scan.bands_band[good], dtype=float)
    payload = _panel_grid_payload(
        "Band-Averaged Phase Difference" if scan.scan_kind == "phacal" else "Reference-Relative Band Phase",
        "Band",
        row_labels,
        [float(np.nanmin(xvals)), float(np.nanmax(xvals))],
        _tick_values(xvals, max_ticks=4),
        [[-np.pi, np.pi], [-np.pi, np.pi]],
        panels,
        auto_scale_rows=True,
    )
    payload["legend"] = []
    payload["sparse_antennas"] = sparse_antennas
    return payload


def _sum_amp_partial_payload(
    scan: ScanAnalysis,
    antenna_indices: Sequence[int],
) -> Dict[str, Any]:
    """Return sparse band-averaged amplitude payload for selected antennas.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :param antenna_indices: Zero-based antenna indices to refresh.
    :type antenna_indices: Sequence[int]
    :returns: Sparse sum-amplitude payload.
    :rtype: dict[str, Any]
    """

    sparse_antennas = _normalize_sparse_antennas(scan, antenna_indices)
    if scan is None:
        return _blank_payload("Select a scan.", "Sum Amp")
    good = np.where(scan.bands_band > 0)[0]
    if good.size == 0:
        return _blank_payload("No band-averaged data are available.", "Sum Amp")
    vis = _sum_amp_vis(scan)
    xvals = np.asarray(scan.bands_band[good], dtype=float)
    panels = [[None] * scan.layout.nsolant for _ in range(2)]
    for pol in range(2):
        for ant_idx in sparse_antennas:
            series = _finite_series(xvals, np.abs(vis[ant_idx, pol, good]))
            panels[pol][ant_idx] = {
                "title": "Ant {0:d}".format(ant_idx + 1),
                "annotation": None,
                "series": [{"label": "Amplitude", "mode": "points", "color": "#1f77b4", "x": series["x"], "y": series["y"]}],
            }
    payload = _panel_grid_payload(
        "Band-Averaged Amplitude",
        "Band",
        ["XX Amplitude", "YY Amplitude"],
        [float(np.nanmin(xvals)), float(np.nanmax(xvals))],
        _tick_values(xvals, max_ticks=4),
        [[0.0, 0.5], [0.0, 0.5]],
        panels,
    )
    payload["legend"] = []
    payload["sparse_antennas"] = sparse_antennas
    return payload


def _inband_window_partial_payloads(scan: ScanAnalysis, antenna_indices: Sequence[int]) -> Dict[str, Dict[str, Any]]:
    """Return sparse overview payloads for mask/window edits.

    :param scan: Current selected scan.
    :type scan: ScanAnalysis
    :param antenna_indices: Zero-based antenna indices to refresh.
    :type antenna_indices: Sequence[int]
    :returns: Sparse overview payload dictionary keyed by section id.
    :rtype: dict[str, dict[str, Any]]
    """

    sparse_antennas = _normalize_sparse_antennas(scan, antenna_indices)
    if not sparse_antennas:
        return inband_window_update_payloads(scan)
    relative_antennas = _expand_reference_antenna_dependencies(scan, sparse_antennas)
    context = _inband_diag_context(scan)
    band_colors = context["band_colors"]
    all_x = np.asarray(context["freq_ghz"], dtype=float)
    x_limits = [float(np.nanmin(all_x)), float(np.nanmax(all_x))]
    x_ticks = _tick_values(all_x, max_ticks=4)
    nsolant = scan.layout.nsolant

    inband_fit_panels = [[None] * nsolant for _ in range(2)]
    for ant_i in sparse_antennas:
        antenna_payload = _inband_diagnostic_for_antenna(scan, ant_i, context=context)
        for pol in range(2):
            panel_diag = antenna_payload["panels"][pol]
            included_bands = set(panel_diag["included_bands"])
            series_list = []
            for band_data in panel_diag["raw_phase_by_band"]:
                series = _finite_series(band_data["x"], band_data["y"])
                series_list.append(
                    {
                        "label": "Raw phase",
                        "role": "raw",
                        "band": int(band_data["band"]),
                        "mode": "points",
                        "color": band_colors[int(band_data["band"])],
                        "opacity": 0.95 if int(band_data["band"]) in included_bands else 0.18,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            for band_fit in panel_diag["fit_phase_by_band"]:
                series = _finite_series(band_fit["x"], band_fit["y"])
                series_list.append(
                    {
                        "label": "Fit",
                        "role": "fit",
                        "band": int(band_fit["band"]),
                        "mode": "line",
                        "color": "#c43c35",
                        "opacity": 0.95 if int(band_fit["band"]) in included_bands else 0.25,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            inband_fit_panels[pol][ant_i] = {
                "title": "Ant {0:d}".format(ant_i + 1),
                "annotation": "tau={0:.3f} ns | kept {1}".format(
                    scan.delay_solution.active_ns[ant_i, pol],
                    _format_kept_ranges(panel_diag["kept_ranges"]),
                ),
                "kept_ranges": panel_diag["kept_ranges"],
                "series": series_list,
            }
    inband_fit_payload_partial = _panel_grid_payload(
        "In-Band Fit Diagnostics",
        "Frequency [GHz]",
        ["X Phase [rad]", "Y Phase [rad]"],
        x_limits,
        x_ticks,
        [[-3.4, 3.4], [-3.4, 3.4]],
        inband_fit_panels,
    )
    inband_fit_payload_partial["legend"] = [{"label": "Fit", "color": "#c43c35", "mode": "line"}]
    inband_fit_payload_partial["band_edges"] = context["band_edges"]
    inband_fit_payload_partial["sparse_antennas"] = sparse_antennas

    relative_payloads = _relative_delay_partial_payloads(scan, relative_antennas)
    relative_payloads["inband_fit"] = inband_fit_payload_partial
    return relative_payloads


def inband_window_update_payloads(
    scan: Optional[ScanAnalysis],
    antenna_indices: Optional[Sequence[int]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return only the overview panels affected by kept-band mask updates."""

    if scan is not None and antenna_indices and scan.delay_solution is not None and scan.raw:
        return _inband_window_partial_payloads(scan, antenna_indices)
    return {
        "inband_fit": inband_fit_payload(scan),
        "inband_relative_phase": inband_relative_phase_payload(scan),
        "inband_residual_phase_band": inband_residual_phase_band_payload(scan),
        "inband_residual_delay_band": inband_residual_delay_band_payload(scan),
    }


def inband_delay_update_payloads(
    scan: Optional[ScanAnalysis],
    use_lobe: bool = False,
    antenna: Optional[int] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return overview panels affected by active in-band delay edits.

    :param scan: Current selected scan.
    :type scan: ScanAnalysis | None
    :param use_lobe: Whether Sum Pha should use lobe wrapping.
    :type use_lobe: bool
    :param antenna: Optional zero-based antenna index for sparse updates.
    :type antenna: int | None
    :returns: Partial overview payload dictionary.
    :rtype: dict[str, dict[str, Any]]
    """

    if scan is not None and antenna is not None and scan.delay_solution is not None and scan.raw:
        sparse = _inband_window_partial_payloads(scan, [antenna])
        sparse["sum_pha"] = _sum_phase_partial_payload(scan, use_lobe, [antenna])
        return sparse
    return {
        "sum_pha": sum_phase_payload(scan, use_lobe=use_lobe),
        "inband_fit": inband_fit_payload(scan),
        "inband_relative_phase": inband_relative_phase_payload(scan),
        "inband_residual_phase_band": inband_residual_phase_band_payload(scan),
        "inband_residual_delay_band": inband_residual_delay_band_payload(scan),
    }


def _relative_delay_partial_payloads(scan: ScanAnalysis, antenna_indices: Sequence[int]) -> Dict[str, Dict[str, Any]]:
    """Return sparse section payloads for relative-delay updates.

    :param scan: Current selected scan.
    :type scan: ScanAnalysis
    :param antenna_indices: Zero-based antenna indices to refresh.
    :type antenna_indices: Sequence[int]
    :returns: Partial overview payload dictionary keyed by section id.
    :rtype: dict[str, dict[str, Any]]
    """

    sparse_antennas = _normalize_sparse_antennas(scan, antenna_indices)
    if not sparse_antennas:
        return {
            "inband_relative_phase": inband_relative_phase_payload(scan),
            "inband_residual_phase_band": inband_residual_phase_band_payload(scan),
            "inband_residual_delay_band": inband_residual_delay_band_payload(scan),
        }
    context = _inband_diag_context(scan)
    band_colors = context["band_colors"]
    manual_flagged = np.asarray(scan.delay_solution.manual_ant_flag_override, dtype=bool)
    manual_kept = np.asarray(scan.delay_solution.manual_ant_keep_override, dtype=bool)
    all_x = np.asarray(context["freq_ghz"], dtype=float)
    x_limits = [float(np.nanmin(all_x)), float(np.nanmax(all_x))]
    x_ticks = _tick_values(all_x, max_ticks=4)
    nsolant = scan.layout.nsolant

    relative_panels = [[None] * nsolant for _ in range(3)]
    for ant_i in sparse_antennas:
        antenna_payload = _inband_diagnostic_for_antenna(scan, ant_i, context=context)
        antenna_auto_flagged = bool(
            np.isfinite(antenna_payload["xy_panel"].get("yx_residual_rms", np.nan))
            and float(antenna_payload["xy_panel"].get("yx_residual_rms", np.nan)) > yx_residual_threshold(scan)
            and not bool(manual_kept[ant_i])
        )
        for pol in range(2):
            panel_diag = antenna_payload["panels"][pol]
            included_bands = set(panel_diag["included_bands"])
            series_list = []
            for band_phase in panel_diag["relative_phase_by_band"]:
                series = _finite_series(band_phase["x"], band_phase["y"])
                series_list.append(
                    {
                        "label": "Relative phase",
                        "role": "data",
                        "band": int(band_phase["band"]),
                        "mode": "points",
                        "color": band_colors[int(band_phase["band"])],
                        "opacity": 0.95 if int(band_phase["band"]) in included_bands else 0.18,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            for segment in panel_diag.get("relative_fit_segments", []):
                series = _finite_series(segment["x"], segment["y"])
                series_list.append(
                    {
                        "label": "Fit",
                        "role": "fit",
                        "mode": "line",
                        "color": "#c43c35",
                        "opacity": 0.95,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
            relative_panels[pol][ant_i] = {
                "title": panel_diag.get("relative_title", "Ant {0:d}".format(ant_i + 1)),
                "annotation": panel_diag["relative_fit_annotation"],
                "kept_ranges": panel_diag["kept_ranges"],
                "disabled": bool(manual_flagged[ant_i] or antenna_auto_flagged),
                "series": series_list,
            }
        xy_panel_diag = antenna_payload["xy_panel"]
        xy_included_bands = set(xy_panel_diag["included_bands"])
        xy_series_list = []
        for band_phase in xy_panel_diag["phase_by_band"]:
            series = _finite_series(band_phase["x"], band_phase["y"])
            xy_series_list.append(
                {
                    "label": "Y-X phase",
                    "role": "data",
                    "band": int(band_phase["band"]),
                    "mode": "points",
                    "color": band_colors[int(band_phase["band"])],
                    "opacity": 0.95 if int(band_phase["band"]) in xy_included_bands else 0.18,
                    "x": series["x"],
                    "y": series["y"],
                }
            )
        for segment in xy_panel_diag.get("fit_segments", []):
            series = _finite_series(segment["x"], segment["y"])
            xy_series_list.append(
                {
                    "label": "Fit",
                    "role": "fit",
                    "mode": "line",
                    "color": "#c43c35",
                    "opacity": 0.95,
                    "x": series["x"],
                    "y": series["y"],
                }
            )
        relative_panels[2][ant_i] = {
            "title": xy_panel_diag["title"],
            "annotation": xy_panel_diag["annotation"],
            "kept_ranges": xy_panel_diag["kept_ranges"],
            "disabled": bool(manual_flagged[ant_i] or antenna_auto_flagged),
            "series": xy_series_list,
        }
    relative_payload = _panel_grid_payload(
        "Relative Phase + Fit",
        "Frequency [GHz]",
        ["XX Relative Phase [rad]", "YY Relative Phase [rad]", "Y-X Phase [rad]"],
        x_limits,
        x_ticks,
        [[-3.4, 3.4], [-3.4, 3.4], [-3.4, 3.4]],
        relative_panels,
        auto_scale_rows=False,
    )
    relative_payload["legend"] = [{"label": "Fit", "color": "#c43c35", "mode": "line"}]
    relative_payload["band_edges"] = context["band_edges"]
    relative_payload["fit_method"] = "complex_poly_lowfreq"
    relative_payload["sparse_antennas"] = sparse_antennas
    relative_payload["column_controls"] = _shared_column_controls(scan)

    residual_phase_panels = [[None] * nsolant for _ in range(2)]
    for ant_i in sparse_antennas:
        antenna_payload = _inband_diagnostic_for_antenna(scan, ant_i, context=context)
        for pol in range(2):
            panel_diag = antenna_payload["panels"][pol]
            included_bands = set(panel_diag["included_bands"])
            series_list = []
            for segment in panel_diag.get("residual_fit_segments", []):
                slope_series = _finite_series(segment["x"], segment["y"])
                series_list.append(
                    {
                        "label": "Multiband Fit",
                        "role": "fit",
                        "mode": "line",
                        "color": "#c43c35",
                        "opacity": 0.95,
                        "x": slope_series["x"],
                        "y": slope_series["y"],
                    }
                )
            for band_residual in panel_diag["residual_phase_by_band"]:
                band_value = int(band_residual["band"])
                series = _finite_series(band_residual["x"], band_residual["y"])
                series_list.append(
                    {
                        "label": "Residual phase",
                        "role": "data",
                        "band": band_value,
                        "mode": "points",
                        "color": band_colors[band_value],
                        "opacity": 0.95 if band_value in included_bands else 0.18,
                        "x": series["x"],
                        "y": series["y"],
                    }
                )
                band_fit = _per_band_phase_fit(band_residual["x"], band_residual["y"], band_value)
                for segment in band_fit.get("segments", []):
                    fit_series = _finite_series(segment["x"], segment["y"])
                    series_list.append(
                        {
                            "label": "Inband Fit",
                            "role": "fit",
                            "mode": "line",
                            "color": "#2a6fdb",
                            "opacity": 0.95 if band_value in included_bands else 0.18,
                            "x": fit_series["x"],
                            "y": fit_series["y"],
                        }
                    )
            residual_phase_panels[pol][ant_i] = {
                "title": "Ant {0:d}".format(ant_i + 1),
                "annotation": None,
                "series": series_list,
            }
    residual_phase_payload = _panel_grid_payload(
        "Per-Band Residual Phase",
        "Frequency [GHz]",
        ["XX residual [rad]", "YY residual [rad]"],
        x_limits,
        x_ticks,
        [[-3.4, 3.4], [-3.4, 3.4]],
        residual_phase_panels,
    )
    residual_phase_payload["legend"] = [
        {"label": "Multiband Fit", "color": "#c43c35", "mode": "line"},
        {"label": "Inband Fit", "color": "#2a6fdb", "mode": "line"},
    ]
    residual_phase_payload["band_edges"] = context["band_edges"]
    residual_phase_payload["fit_method"] = "complex_poly_lowfreq"
    residual_phase_payload["sparse_antennas"] = sparse_antennas

    band_centers = np.asarray([edge["x_center"] for edge in context["band_edges"]], dtype=float)
    residual_delay_panels = [[None] * nsolant for _ in range(2)]
    for ant_i in sparse_antennas:
        antenna_payload = _inband_diagnostic_for_antenna(scan, ant_i, context=context)
        for pol in range(2):
            panel_diag = antenna_payload["panels"][pol]
            included_bands = set(panel_diag["included_bands"])
            series_list = []
            for band_idx, band_value in enumerate(context["band_values"]):
                yval = panel_diag["residual_delay_per_band_ns"][band_idx]
                if not np.isfinite(yval):
                    continue
                center = next((edge["x_center"] for edge in context["band_edges"] if int(edge["band"]) == int(band_value)), None)
                if center is None:
                    continue
                series_list.append(
                    {
                        "label": "Residual delay",
                        "role": "data",
                        "band": int(band_value),
                        "mode": "points",
                        "color": band_colors[int(band_value)],
                        "opacity": 0.95 if int(band_value) in included_bands else 0.18,
                        "x": [float(center)],
                        "y": [float(yval)],
                    }
                )
            if np.isfinite(panel_diag["all_residual_delay_ns"]):
                series_list.append(
                    {
                        "label": "All-band residual delay",
                        "role": "fit",
                        "mode": "line",
                        "color": "#c43c35",
                        "dasharray": "5 4",
                        "x": [float(np.nanmin(band_centers)), float(np.nanmax(band_centers))],
                        "y": [float(panel_diag["all_residual_delay_ns"]), float(panel_diag["all_residual_delay_ns"])],
                    }
                )
            residual_delay_panels[pol][ant_i] = {
                "title": "Ant {0:d}".format(ant_i + 1),
                "annotation": None,
                "series": series_list,
            }
    residual_delay_payload = _panel_grid_payload(
        "Residual Delay Per Band (After In-Band Correction)",
        "Band center [GHz]",
        ["XX residual delay [ns]", "YY residual delay [ns]"],
        [float(np.nanmin(band_centers)), float(np.nanmax(band_centers))],
        _tick_values(band_centers, max_ticks=4),
        [[-1.0, 1.0], [-1.0, 1.0]],
        residual_delay_panels,
        auto_scale_rows=True,
    )
    residual_delay_payload["legend"] = [{"label": "All-band residual delay", "color": "#c43c35", "mode": "line"}]
    residual_delay_payload["band_edges"] = context["band_edges"]
    residual_delay_payload["sparse_antennas"] = sparse_antennas
    return {
        "inband_relative_phase": relative_payload,
        "inband_residual_phase_band": residual_phase_payload,
        "inband_residual_delay_band": residual_delay_payload,
    }


def relative_delay_update_payloads(scan: Optional[ScanAnalysis], antenna: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """Return only the panels affected by relative-delay edits.

    :param scan: Current selected scan.
    :type scan: ScanAnalysis | None
    :param antenna: Optional zero-based antenna index for sparse updates.
    :type antenna: int | None
    :returns: Partial overview payload dictionary.
    :rtype: dict[str, dict[str, Any]]
    """

    if scan is not None and antenna is not None and scan.delay_solution is not None and scan.raw:
        return _relative_delay_partial_payloads(scan, [antenna])
    return {
        "inband_relative_phase": inband_relative_phase_payload(scan),
        "inband_residual_phase_band": inband_residual_phase_band_payload(scan),
        "inband_residual_delay_band": inband_residual_delay_band_payload(scan),
    }


def time_flag_update_payloads(
    scan: Optional[ScanAnalysis],
    use_lobe: bool = False,
    antenna_indices: Optional[Sequence[int]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return overview payloads affected by time-mask edits.

    :param scan: Current selected scan.
    :type scan: ScanAnalysis | None
    :param use_lobe: Whether Sum Pha should use lobe wrapping.
    :type use_lobe: bool
    :param antenna_indices: Optional zero-based antenna indices to refresh.
    :type antenna_indices: Sequence[int] | None
    :returns: Partial overview payload dictionary.
    :rtype: dict[str, dict[str, Any]]
    """

    if scan is not None and antenna_indices:
        sparse = {
            "sum_amp": _sum_amp_partial_payload(scan, antenna_indices),
            "sum_pha": _sum_phase_partial_payload(scan, use_lobe, antenna_indices),
        }
        if scan.delay_solution is not None and scan.raw:
            sparse.update(_inband_window_partial_payloads(scan, antenna_indices))
        return sparse
    return overview_payloads(scan, use_lobe=use_lobe)


def relative_delay_editor_meta(scan: Optional[ScanAnalysis], ant: int) -> Dict[str, Any]:
    """Return selected-antenna metadata for the relative-delay editor.

    :param scan: Active refcal scan.
    :type scan: ScanAnalysis | None
    :param ant: Zero-based antenna index.
    :type ant: int
    :returns: Relative-delay editor metadata for one antenna.
    :rtype: dict[str, Any]
    """

    if scan is None or scan.delay_solution is None:
        return {}
    ant_i = int(max(0, min(int(ant), scan.layout.nsolant - 1)))
    antenna_diag = None
    if scan.raw:
        antenna_diag = _inband_diagnostic_for_antenna(scan, ant_i)
    auto = np.asarray(scan.delay_solution.relative_auto_ns[ant_i], dtype=float)
    suggested = np.asarray(scan.delay_solution.relative_suggested_ns[ant_i], dtype=float)
    applied = np.asarray(scan.delay_solution.relative_ns[ant_i], dtype=float)
    effective = auto + applied
    yx_rms = np.asarray(scan.raw.get("yx_residual_rms", np.full(scan.layout.nsolant, np.nan)), dtype=float)
    panel_meta = antenna_diag["panels"] if antenna_diag is not None else [{}, {}]
    return {
        "x_auto_relative_delay_ns": float(auto[0]),
        "y_auto_relative_delay_ns": float(auto[1]),
        "x_suggested_relative_delay_ns": float(suggested[0]),
        "y_suggested_relative_delay_ns": float(suggested[1]),
        "x_suggested_residual_inband_delay_ns": float(panel_meta[0].get("residual_inband_delay_ns", 0.0) or 0.0),
        "y_suggested_residual_inband_delay_ns": float(panel_meta[1].get("residual_inband_delay_ns", 0.0) or 0.0),
        "x_applied_relative_delay_ns": float(applied[0]),
        "y_applied_relative_delay_ns": float(applied[1]),
        "x_effective_relative_delay_ns": float(effective[0]),
        "y_effective_relative_delay_ns": float(effective[1]),
        "relative_undo_available": bool(scan.delay_solution.relative_prev_valid[ant_i]),
        "manual_ant_flagged": bool(scan.delay_solution.manual_ant_flag_override[ant_i]),
        "manual_ant_kept": bool(scan.delay_solution.manual_ant_keep_override[ant_i]),
        "yx_residual_rms": float(yx_rms[ant_i]) if ant_i < yx_rms.size else np.nan,
        "yx_residual_threshold_rad": float(yx_residual_threshold(scan)),
    }


def _band_mean_wrapped_phase(freq_ghz: np.ndarray, band_id: np.ndarray, band_values: np.ndarray, phase_rad: np.ndarray) -> np.ndarray:
    """Return one wrapped mean phase per used band.

    :param freq_ghz: Channel frequencies in GHz.
    :type freq_ghz: np.ndarray
    :param band_id: Band number for each channel.
    :type band_id: np.ndarray
    :param band_values: Used band numbers.
    :type band_values: np.ndarray
    :param phase_rad: Wrapped or unwrapped phase samples.
    :type phase_rad: np.ndarray
    :returns: Wrapped mean phase per band.
    :rtype: np.ndarray
    """

    del freq_ghz
    out = np.full(np.asarray(band_values, dtype=int).shape, np.nan, dtype=float)
    phase_arr = np.asarray(phase_rad, dtype=float)
    for band_idx, band_value in enumerate(np.asarray(band_values, dtype=int)):
        idx = np.where(np.asarray(band_id, dtype=int) == int(band_value))[0]
        if idx.size == 0:
            continue
        valid = np.isfinite(phase_arr[idx])
        if not np.any(valid):
            continue
        out[band_idx] = float(np.angle(np.nanmean(np.exp(1j * phase_arr[idx][valid]))))
    return out


def export_model_bundle_entry(scan: ScanAnalysis) -> Dict[str, Any]:
    """Build one NPZ-export bundle entry from the current tuned scan state.

    :param scan: Current analyzed scan.
    :type scan: ScanAnalysis
    :returns: Export-ready nested payload for one scan.
    :rtype: dict[str, Any]
    """

    legacy_flag = None
    legacy = legacy_refcal_display_summary(scan)
    if legacy is not None and "flags" in legacy:
        legacy_flag = np.asarray(legacy["flags"][: scan.layout.nsolant, :2], dtype=np.int32)
    elif scan.base_flags is not None:
        legacy_flag = np.asarray(scan.base_flags[: scan.layout.nsolant, :2], dtype=np.int32)
    else:
        legacy_flag = np.asarray(scan.flags[: scan.layout.nsolant, :2], dtype=np.int32)

    v2_flag = np.asarray(scan.flags[: scan.layout.nsolant, :2], dtype=np.int32)
    model_flag = np.asarray(v2_flag, dtype=np.int32).copy()
    manual_override = (
        np.asarray(scan.delay_solution.manual_ant_flag_override, dtype=bool)
        if scan.delay_solution is not None
        else np.zeros(scan.layout.nsolant, dtype=bool)
    )
    manual_keep_override = (
        np.asarray(scan.delay_solution.manual_ant_keep_override, dtype=bool)
        if scan.delay_solution is not None
        else np.zeros(scan.layout.nsolant, dtype=bool)
    )
    yx_rms = np.asarray(scan.raw.get("yx_residual_rms", np.full(scan.layout.nsolant, np.nan)), dtype=float)

    freq_fine = np.asarray(scan.raw["channel_freq_ghz"], dtype=float) if scan.raw else np.asarray([], dtype=float)
    band_id = np.asarray(scan.raw["channel_band"], dtype=int) if scan.raw else np.asarray([], dtype=int)
    model_phase_fine = np.full((scan.layout.nsolant, 2, freq_fine.size), np.nan, dtype=float)
    model_phase_band = np.full((scan.layout.nsolant, 2, scan.bands_band.size), np.nan, dtype=float)
    if scan.delay_solution is not None and scan.raw:
        context = _inband_diag_context(scan)
        for ant_i in range(scan.layout.nsolant):
            antenna_payload = _inband_diagnostic_for_antenna(scan, ant_i, context=context)
            for pol in range(2):
                model_phase = np.asarray(antenna_payload["panels"][pol]["relative_model_phase"], dtype=float)
                model_mask = np.asarray(antenna_payload["panels"][pol]["relative_model_mask"], dtype=bool)
                wrapped = np.angle(np.exp(1j * model_phase))
                wrapped[~model_mask] = np.nan
                model_phase_fine[ant_i, pol, :] = wrapped
                model_phase_band[ant_i, pol, :] = _band_mean_wrapped_phase(freq_fine, band_id, scan.delay_solution.band_values, wrapped)

    if scan_feed_kind(scan) != "hi":
        model_flag = np.asarray(v2_flag, dtype=np.int32).copy()

    return {
        "scan_id": int(scan.scan_id),
        "scan_kind": str(scan.scan_kind),
        "timestamp_iso": scan.timestamp.iso[:19],
        "source": str(scan.source),
        "feed_kind": scan_feed_kind(scan),
        "metadata_warning": str((scan.scan_meta or {}).get("metadata_warning", "")),
        "fine_frequency_ghz": freq_fine,
        "band_frequency_ghz": np.asarray(scan.fghz_band, dtype=float),
        "band_values": np.asarray(scan.bands_band, dtype=int),
        "model_phase_fine": model_phase_fine,
        "model_phase_band": model_phase_band,
        "legacy_flag": legacy_flag,
        "v2_flag": v2_flag,
        "model_flag": model_flag,
        "manual_ant_flag_override": manual_override,
        "manual_ant_keep_override": manual_keep_override,
        "yx_residual_rms": yx_rms,
        "yx_residual_threshold_rad": float(yx_residual_threshold(scan)),
        "scan_meta": dict(scan.scan_meta or {}),
    }


def _heatmap_2pol(fig_title: str, data_by_pol: np.ndarray, colorbar_label: str) -> Any:
    """Render a two-polarization heatmap panel."""

    fig, ax = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    for pol, pol_name in enumerate(("X", "Y")):
        im = ax[pol].imshow(data_by_pol[pol], origin="lower", aspect="auto", interpolation="nearest", cmap="coolwarm")
        ax[pol].set_ylabel("{0} Ant".format(pol_name))
        ax[pol].set_title("Pol {0}".format(pol_name))
        fig.colorbar(im, ax=ax[pol], fraction=0.046, pad=0.04, label=colorbar_label)
    ax[-1].set_xlabel("Band")
    fig.suptitle(fig_title)
    return fig


def save_refcal_report(v2: ScanAnalysis, legacy_sql: Dict[str, Any], outdir: str, ant: Optional[int] = None) -> Dict[str, Any]:
    """Save comparison plots for one refcal benchmark."""

    metrics = refcal_comparison_metrics(v2, legacy_sql)
    outpath = Path(outdir)
    outpath.mkdir(parents=True, exist_ok=True)
    legacy = sql_refcal_to_scan(legacy_sql)
    fig = _heatmap_2pol("Refcal Wrapped Phase Difference", np.transpose(metrics["phase_diff"], (1, 0, 2)), "rad")
    fig.savefig(outpath / "refcal_phase_diff.png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    fig = _heatmap_2pol("Refcal Amplitude Difference", np.transpose(metrics["amp_diff"], (1, 0, 2)), "arb")
    fig.savefig(outpath / "refcal_amp_diff.png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    if ant is not None:
        good = np.where(v2.bands_band > 0)[0]
        if good.size > 0:
            fig, ax = plt.subplots(2, 2, figsize=(12, 7), sharex=True)
            for pol, pol_name in enumerate(("X", "Y")):
                ax[pol, 0].plot(v2.bands_band[good], np.abs(v2.corrected_band_vis[ant, pol, good]), "o-", label="v2")
                ax[pol, 0].plot(legacy.bands_band[good], np.abs(legacy.corrected_band_vis[ant, pol, good]), "x--", label="legacy")
                ax[pol, 1].plot(v2.bands_band[good], metrics["phase_diff"][ant, pol, good], "o-")
                ax[pol, 0].set_ylabel("{0} amp".format(pol_name))
                ax[pol, 1].set_ylabel("{0} phase diff".format(pol_name))
                ax[pol, 0].legend(loc="best")
            ax[-1, 0].set_xlabel("Band")
            ax[-1, 1].set_xlabel("Band")
            fig.suptitle("Refcal Compare Ant {0:d}".format(ant + 1))
            fig.savefig(outpath / "refcal_ant_compare.png", dpi=120, bbox_inches="tight")
            plt.close(fig)
    return metrics


def save_phacal_report(v2: ScanAnalysis, legacy_sql: Dict[str, Any], outdir: str, ant: Optional[int] = None) -> Dict[str, Any]:
    """Save comparison plots for one phacal benchmark."""

    metrics = phacal_comparison_metrics(v2, legacy_sql)
    outpath = Path(outdir)
    outpath.mkdir(parents=True, exist_ok=True)
    fig = _heatmap_2pol("Phacal Wrapped Phase Difference", np.transpose(metrics["phase_diff"], (1, 0, 2)), "rad")
    fig.savefig(outpath / "phacal_phase_diff.png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    fig, ax = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    for pol, pol_name in enumerate(("X", "Y")):
        ax[pol].bar(np.arange(v2.layout.nsolant) + 1, metrics["mbd_diff"][: v2.layout.nsolant, pol])
        ax[pol].set_ylabel("{0} MBD diff [ns]".format(pol_name))
        ax[pol].grid(alpha=0.2)
    ax[-1].set_xlabel("Antenna")
    fig.suptitle("Phacal Multiband Delay Difference")
    fig.savefig(outpath / "phacal_mbd_diff.png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    if ant is not None:
        good = np.where(v2.bands_band > 0)[0]
        if good.size > 0:
            fig, ax = plt.subplots(2, 2, figsize=(12, 7), sharex=True)
            legacy = sql_phacal_to_scan(legacy_sql)
            for pol, pol_name in enumerate(("X", "Y")):
                ax[pol, 0].plot(v2.bands_band[good], np.abs(v2.corrected_band_vis[ant, pol, good]), "o-", label="v2")
                ax[pol, 0].plot(legacy.bands_band[good], np.abs(legacy.corrected_band_vis[ant, pol, good]), "x--", label="legacy")
                ax[pol, 1].plot(v2.bands_band[good], metrics["phase_diff"][ant, pol, good], "o-")
                ax[pol, 0].set_ylabel("{0} amp".format(pol_name))
                ax[pol, 1].set_ylabel("{0} phase diff".format(pol_name))
                ax[pol, 0].legend(loc="best")
            ax[-1, 0].set_xlabel("Band")
            ax[-1, 1].set_xlabel("Band")
            fig.suptitle("Phacal Compare Ant {0:d}".format(ant + 1))
            fig.savefig(outpath / "phacal_ant_compare.png", dpi=120, bbox_inches="tight")
            plt.close(fig)
    return metrics
