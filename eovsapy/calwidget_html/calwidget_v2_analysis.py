"""Reusable analysis helpers for the browser-based calibration widget.

This module is intentionally import-safe.  It lifts the legacy calibration
logic out of the Tk widget and extends it with in-band X/Y delay fitting and
application so that the web app and benchmark tools can share one code path.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import json
from pathlib import Path
import threading
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

import numpy as np

from eovsapy import cal_header as ch
from eovsapy import dbutil as db
from eovsapy.util import Time, extract, lin_phase_fit, lobe, nearest_val_idx


SIDECAR_DIR = Path("/common/webplots/phasecal")
TIME_FLAG_SCOPE_LABELS = {
    "selected": "Selected",
    "this_ant": "This Ant",
    "this_band": "This Band",
    "higher_bands": "Higher Bands",
    "all": "All",
    "migrated": "Migrated",
}
_LEGACY_BAND_TIME_LOCK = threading.Lock()
_FULL_FEED_ROTATION_END_MJD = Time("2025-07-15").mjd
_ANT4_PA_WRAP_FIX_START_MJD = Time("2025-08-08").mjd


class CalWidgetV2Error(RuntimeError):
    """Raised for recoverable calibration workflow errors."""


@dataclass
class LayoutInfo:
    """Array layout metadata derived from observation time."""

    mjd: float
    nsolant: int
    nant: int
    maxnbd: int
    ref_ant_index: int


@dataclass
class DelaySolution:
    """Fitted and active in-band delay metadata."""

    fitted_ns: np.ndarray
    active_ns: np.ndarray
    relative_ns: np.ndarray
    relative_auto_ns: np.ndarray
    relative_suggested_ns: np.ndarray
    relative_prev_ns: np.ndarray
    relative_prev_valid: np.ndarray
    fitted_std_ns: np.ndarray
    flag: np.ndarray
    per_band_delay_ns: np.ndarray
    per_band_std: np.ndarray
    per_band_phase0: np.ndarray
    band_values: np.ndarray
    band_centers_ghz: np.ndarray
    kept_band_mask: np.ndarray

    def reset_all(self) -> None:
        """Reset all active delays to the fitted solution."""

        self.active_ns[:] = self.fitted_ns
        if self.kept_band_mask.size:
            self.kept_band_mask[:] = True

    def reset_ant(self, ant: int) -> None:
        """Reset one antenna's active delays to the fitted solution."""

        self.active_ns[ant, :] = self.fitted_ns[ant, :]
        if self.kept_band_mask.size:
            self.kept_band_mask[ant, :] = True

    def reset_relative_all(self) -> None:
        """Reset all display-only relative-delay overrides."""

        self.relative_ns[:] = 0.0

    def reset_relative_ant(self, ant: int) -> None:
        """Reset one antenna's display-only relative-delay overrides."""

        self.relative_ns[ant, :] = 0.0

    def snapshot_relative_ant(self, ant: int) -> None:
        """Store one-step undo state for one antenna's relative-delay edits.

        :param ant: Zero-based antenna index.
        :type ant: int
        """

        ant_i = int(ant)
        self.relative_prev_ns[ant_i, :] = self.relative_ns[ant_i, :]
        self.relative_prev_valid[ant_i] = True

    def undo_relative_ant(self, ant: int) -> bool:
        """Restore one antenna's previous applied relative-delay correction.

        :param ant: Zero-based antenna index.
        :type ant: int
        :returns: ``True`` when an undo snapshot was available.
        :rtype: bool
        """

        ant_i = int(ant)
        if not bool(self.relative_prev_valid[ant_i]):
            return False
        self.relative_ns[ant_i, :] = self.relative_prev_ns[ant_i, :]
        self.relative_prev_valid[ant_i] = False
        return True

    def band_window(self, ant: int, pol: int) -> Tuple[int, int]:
        """Return the inclusive active band window for one antenna/polarization.

        :param ant: Zero-based antenna index.
        :type ant: int
        :param pol: Zero-based polarization index.
        :type pol: int
        :returns: Inclusive ``(start_band, end_band)`` window.
        :rtype: tuple[int, int]
        """

        if self.band_values.size == 0:
            return 0, 0
        mask = self.included_band_mask(ant, pol)
        if not np.any(mask):
            return int(self.band_values[0]), int(self.band_values[-1])
        kept = self.band_values[mask]
        return int(kept[0]), int(kept[-1])

    def included_band_mask(self, ant: int, pol: int) -> np.ndarray:
        """Return the active included-band mask for one antenna/polarization.

        :param ant: Zero-based antenna index.
        :type ant: int
        :param pol: Zero-based polarization index.
        :type pol: int
        :returns: Boolean mask over ``band_values``.
        :rtype: np.ndarray
        """

        if self.band_values.size == 0:
            return np.zeros(0, dtype=bool)
        mask = np.asarray(self.kept_band_mask[int(ant), int(pol)], dtype=bool)
        if mask.shape[0] != self.band_values.size:
            return np.ones(self.band_values.shape, dtype=bool)
        return mask.copy()

    def kept_band_ranges(self, ant: int, pol: int) -> List[Tuple[int, int]]:
        """Return contiguous kept-band ranges for one antenna/polarization."""

        mask = self.included_band_mask(ant, pol)
        if mask.size == 0 or not np.any(mask):
            return []
        bands = self.band_values[mask].astype(int)
        ranges: List[Tuple[int, int]] = []
        start = int(bands[0])
        previous = int(bands[0])
        for value in bands[1:]:
            current = int(value)
            if current != previous + 1:
                ranges.append((start, previous))
                start = current
            previous = current
        ranges.append((start, previous))
        return ranges

    def uses_full_window(self, ant: int, pol: int) -> bool:
        """Return whether one antenna/polarization still uses all fitted bands.

        :param ant: Zero-based antenna index.
        :type ant: int
        :param pol: Zero-based polarization index.
        :type pol: int
        :returns: ``True`` when the active window spans the full fitted range.
        :rtype: bool
        """

        if self.band_values.size == 0:
            return True
        return bool(np.all(self.included_band_mask(ant, pol)))

    def _recompute_active_delay(self, ant: int, pol: int) -> None:
        """Recompute one active mean delay from the kept per-band fits.

        :param ant: Zero-based antenna index.
        :type ant: int
        :param pol: Zero-based polarization index.
        :type pol: int
        """

        kept = self.included_band_mask(ant, pol)
        valid = (
            kept
            & np.isfinite(self.per_band_delay_ns[ant, pol])
            & np.isfinite(self.per_band_std[ant, pol])
            & (self.per_band_std[ant, pol] > 0.0)
        )
        if not np.any(valid):
            self.active_ns[ant, pol] = self.fitted_ns[ant, pol]
            return
        weights = 1.0 / np.square(self.per_band_std[ant, pol, valid])
        self.active_ns[ant, pol] = np.nansum(self.per_band_delay_ns[ant, pol, valid] * weights) / np.nansum(weights)

    def update_kept_band_mask(
        self,
        ant_indices: Sequence[int],
        pol_indices: Sequence[int],
        start_band: int,
        end_band: int,
        mode: str = "replace",
    ) -> None:
        """Update kept-band masks and recompute active mean delays.

        :param ant_indices: Zero-based antenna indices to update.
        :type ant_indices: Sequence[int]
        :param pol_indices: Zero-based polarization indices to update.
        :type pol_indices: Sequence[int]
        :param start_band: Inclusive start band number.
        :type start_band: int
        :param end_band: Inclusive end band number.
        :type end_band: int
        :param mode: Replace or exclude mode for the dragged band range.
        :type mode: str
        """

        if self.band_values.size == 0:
            return
        band_min = int(self.band_values[0])
        band_max = int(self.band_values[-1])
        lo = max(min(int(start_band), int(end_band)), band_min)
        hi = min(max(int(start_band), int(end_band)), band_max)
        selection = np.logical_and(self.band_values >= lo, self.band_values <= hi)
        for ant in ant_indices:
            for pol in pol_indices:
                ant_i = int(ant)
                pol_i = int(pol)
                current = self.included_band_mask(ant_i, pol_i)
                if mode == "replace":
                    candidate = selection.copy()
                elif mode == "exclude":
                    candidate = np.logical_and(current, ~selection)
                else:
                    raise CalWidgetV2Error("Unknown kept-band update mode {0}.".format(mode))
                if not np.any(candidate):
                    continue
                self.kept_band_mask[ant_i, pol_i] = candidate
                self._recompute_active_delay(ant_i, pol_i)

    def set_kept_band_ranges(self, ant: int, pol: int, ranges: Sequence[Tuple[int, int]]) -> None:
        """Set one target's kept-band mask from final contiguous ranges.

        :param ant: Zero-based antenna index.
        :type ant: int
        :param pol: Zero-based polarization index.
        :type pol: int
        :param ranges: Inclusive kept-band ranges.
        :type ranges: Sequence[tuple[int, int]]
        """

        if self.band_values.size == 0:
            return
        mask = np.zeros(self.band_values.shape, dtype=bool)
        band_min = int(self.band_values[0])
        band_max = int(self.band_values[-1])
        for start_band, end_band in ranges:
            lo = max(min(int(start_band), int(end_band)), band_min)
            hi = min(max(int(start_band), int(end_band)), band_max)
            mask |= np.logical_and(self.band_values >= lo, self.band_values <= hi)
        if not np.any(mask):
            return
        self.kept_band_mask[int(ant), int(pol)] = mask
        self._recompute_active_delay(int(ant), int(pol))

    def window_signature(self) -> str:
        """Return a compact signature for active window-dependent caches.

        :returns: Cache signature string.
        :rtype: str
        """

        if self.band_values.size == 0:
            return "empty"
        kept = self.kept_band_mask.astype(np.int8).ravel().tolist()
        active = np.round(self.active_ns.astype(np.float64), 6).ravel().tolist()
        return json.dumps({"kept": kept, "active": active}, separators=(",", ":"))

    def relative_signature(self) -> str:
        """Return a compact signature for display-only relative-delay edits.

        :returns: Relative-delay cache signature string.
        :rtype: str
        """

        relative = np.round(self.relative_ns.astype(np.float64), 6).ravel().tolist()
        return json.dumps({"relative": relative}, separators=(",", ":"))


@dataclass
class TimeFlagGroup:
    """One browser-native time-flag interval group.

    :param group_id: Stable session-local identifier for the interval group.
    :type group_id: str
    :param scope: Scope preset used when the interval was created.
    :type scope: str
    :param start_jd: Interval start time in Julian Date.
    :type start_jd: float
    :param end_jd: Interval end time in Julian Date.
    :type end_jd: float
    :param targets: Concrete ``(antenna, band)`` targets covered by the group.
    :type targets: list[tuple[int, int]]
    :param source: Provenance label, e.g. ``browser`` or ``legacy``.
    :type source: str
    """

    group_id: str
    scope: str
    start_jd: float
    end_jd: float
    targets: List[Tuple[int, int]]
    source: str = "browser"


@dataclass
class ScanAnalysis:
    """In-memory state for one analyzed scan."""

    scan_id: int
    scan_kind: str
    file: str
    source: str
    timestamp: Time
    t_bg: Time
    t_ed: Time
    duration_min: float
    layout: LayoutInfo
    raw: Dict[str, Any]
    corrected_channel_vis: np.ndarray
    corrected_band_vis: np.ndarray
    sigma: np.ndarray
    flags: np.ndarray
    fghz_band: np.ndarray
    bands_band: np.ndarray
    band_to_full_index: Dict[int, int]
    delay_solution: Optional[DelaySolution] = None
    tflags: Optional[np.ndarray] = None
    mbd: Optional[np.ndarray] = None
    mbd_flag: Optional[np.ndarray] = None
    offsets: Optional[np.ndarray] = None
    pdiff: Optional[np.ndarray] = None
    applied_ref_id: Optional[int] = None
    saved_to_sql: bool = False
    sidecar_path: Optional[str] = None
    dirty_inband: bool = False
    time_flag_groups: List[TimeFlagGroup] = field(default_factory=list)
    time_flag_groups_initialized: bool = False

    def to_refcal_sql(self, timestamp: Optional[Time] = None) -> Dict[str, Any]:
        """Build a legacy-compatible refcal SQL payload."""

        return {
            "timestamp": timestamp or self.timestamp,
            "t_bg": self.t_bg,
            "t_ed": self.t_ed,
            "flag": self.flags[:, :2],
            "vis": self.corrected_band_vis[:, :2],
            "sigma": self.sigma[:, :2],
            "fghz": self.fghz_band,
        }

    def to_phacal_sql(self) -> Dict[str, Any]:
        """Build a legacy-compatible phacal SQL payload."""

        if self.mbd is None or self.mbd_flag is None or self.offsets is None:
            raise CalWidgetV2Error("Phasecal has not been solved against a reference calibration.")
        phacal = {
            "flag": self.flags[:, :2],
            "sigma": self.sigma[:, :2],
            "fghz": self.fghz_band,
            "amp": np.abs(self.corrected_band_vis[:, :2]),
            "pha": np.angle(self.corrected_band_vis[:, :2]),
            "t_bg": self.t_bg,
            "t_ed": self.t_ed,
        }
        return {
            "phacal": phacal,
            "t_pha": self.timestamp,
            "t_bg": self.t_bg,
            "t_ed": self.t_ed,
            "poff": self.offsets,
            "pslope": self.mbd,
            "flag": self.mbd_flag,
        }


def _as_path(value: Any) -> Path:
    """Normalize a user-supplied path-like value."""

    return Path(str(value)).expanduser().resolve()


def _layout_for_mjd(mjd: float) -> LayoutInfo:
    """Return antenna and band layout metadata for an observation date."""

    if mjd > Time("2025-05-22").mjd:
        nsolant = 15
        nant = 16
    else:
        nsolant = 13
        nant = 15
    maxnbd = 52 if mjd > 58536 else 34
    return LayoutInfo(mjd=mjd, nsolant=nsolant, nant=nant, maxnbd=maxnbd, ref_ant_index=nsolant)


def _freq_to_band(freq_ghz: np.ndarray, mjd: float) -> np.ndarray:
    """Convert frequencies to legacy band numbers."""

    if mjd > 58536:
        from eovsapy.chan_util_52 import freq2bdname
    else:
        from eovsapy.chan_util_bc import freq2bdname
    return np.asarray(freq2bdname(freq_ghz), dtype=np.int32)


def _safe_nanmean(values: np.ndarray, axis: Optional[int] = None) -> np.ndarray:
    """np.nanmean without warnings on all-NaN slices."""

    values = np.asarray(values)
    with np.errstate(invalid="ignore"):
        return np.nanmean(values, axis=axis)


def _safe_nanstd(values: np.ndarray, axis: Optional[int] = None) -> np.ndarray:
    """np.nanstd without warnings on all-NaN slices."""

    values = np.asarray(values)
    with np.errstate(invalid="ignore"):
        return np.nanstd(values, axis=axis)


def _safe_complex_nanmean(values: np.ndarray, axis: Optional[int] = None) -> np.ndarray:
    """Complex nanmean without warnings on all-NaN slices."""

    values = np.asarray(values, dtype=np.complex128)
    with np.errstate(invalid="ignore"):
        return np.nanmean(values, axis=axis)


def find_phasecal_scans(trange: Time) -> Dict[str, Any]:
    """Identify PHASECAL scans for the provided time range."""

    from eovsapy import dump_tsys

    tstart, tend = trange.lv.astype(int).astype(str)
    cnxn, cursor = db.get_cursor()
    verstr = db.find_table_version(cursor, tstart, True)
    query = (
        "select Timestamp,Project,SourceID from hV{ver}_vD1 "
        "where left(Project,8) = 'PHASECAL' and Timestamp between {start} and {end} "
        "order by Timestamp"
    ).format(ver=verstr, start=tstart, end=tend)
    projdict, msg = db.do_query(cursor, query)
    cnxn.close()
    if msg != "Success":
        return {"msg": msg}
    if projdict == {}:
        return {"msg": "No PHASECAL scans for this day"}
    tsint = projdict["Timestamp"].astype(int)
    ufdb = dump_tsys.rd_ufdb(Time(int(tstart), format="lv"))
    mjd0 = int(Time(int(tstart), format="lv").mjd)
    mjdnow = int(Time.now().mjd)
    if mjd0 < mjdnow:
        try:
            ufdb2 = dump_tsys.rd_ufdb(Time(int(tstart) + 86400.0, format="lv"))
            for key in list(ufdb.keys()):
                ufdb.update({key: np.append(ufdb[key], ufdb2[key])})
        except Exception:
            pass
    ufdb_times = ufdb["ST_TS"].astype(float).astype(int)
    idx = nearest_val_idx(tsint, ufdb_times)
    fpath = "/data1/eovsa/fits/UDB/" + trange[0].iso[:4] + "/"
    dur = []
    filelist = []
    for i in idx:
        dur.append(((ufdb["EN_TS"].astype(float) - ufdb["ST_TS"].astype(float))[i]) / 60.0)
        filelist.append(fpath + ufdb["FILE"][i])
    srclist = np.array([str(item.replace("\x00", "")) for item in projdict["SourceID"]])
    return {
        "Timestamp": tsint,
        "SourceID": srclist,
        "duration": np.array(dur),
        "filelist": np.array(filelist),
        "msg": msg,
    }


def _sql_refcal_in_scan(scan_start: Time, scan_end: Time) -> Optional[Dict[str, Any]]:
    """Return a legacy SQL refcal if it overlaps the provided scan window."""

    try:
        xml, buf = ch.read_cal(8, t=scan_end)
        if not buf:
            return None
        t_beg = Time(extract(buf, xml["T_beg"]), format="lv")
        t_end = Time(extract(buf, xml["T_end"]), format="lv")
        refcal_time = Time((t_beg.lv + t_end.lv) / 2.0, format="lv")
        if (scan_start - refcal_time).jd < 0 and (scan_end - refcal_time).jd > 0:
            vis = extract(buf, xml["Refcal_Real"]) + 1j * extract(buf, xml["Refcal_Imag"])
            sigma = extract(buf, xml["Refcal_Sigma"])
            flags = extract(buf, xml["Refcal_Flag"])
            fghz = extract(buf, xml["Fghz"])
            return {
                "kind": "refcal",
                "sql_time": Time(extract(buf, xml["SQL_timestamp"]), format="lv"),
                "timestamp": refcal_time,
                "t_bg": t_beg,
                "t_ed": t_end,
                "x": vis,
                "sigma": sigma,
                "flags": flags,
                "fghz": fghz,
                "bands": _freq_to_band(fghz, refcal_time.mjd),
            }
    except Exception:
        return None
    return None


def _sql_phacal_in_scan(scan_start: Time, scan_end: Time) -> Optional[Dict[str, Any]]:
    """Return a legacy SQL phacal if it overlaps the provided scan window."""

    try:
        xml, buf = ch.read_cal(9, t=scan_end)
        if not buf:
            return None
        phacal_time = Time(extract(buf, xml["Timestamp"]), format="lv")
        if (scan_start - phacal_time).jd < 0 and (scan_end - phacal_time).jd > 0:
            amp = extract(buf, xml["Phacal_Amp"])
            pha = extract(buf, xml["Phacal_Pha"])
            sigma = extract(buf, xml["Phacal_Sigma"])
            flags = extract(buf, xml["Phacal_Flag"])
            fghz = extract(buf, xml["Fghz"])
            mbd = extract(buf, xml["MBD"])
            mbd_flag = extract(buf, xml["Flag"])
            return {
                "kind": "phacal",
                "sql_time": Time(extract(buf, xml["SQL_timestamp"]), format="lv"),
                "timestamp": phacal_time,
                "t_bg": Time(extract(buf, xml["T_beg"]), format="lv"),
                "t_ed": Time(extract(buf, xml["T_end"]), format="lv"),
                "x": amp * np.exp(1j * pha),
                "sigma": sigma,
                "flags": flags,
                "fghz": fghz,
                "bands": _freq_to_band(fghz, phacal_time.mjd),
                "mbd": mbd[:, :, 1],
                "offsets": mbd[:, :, 0],
                "mbd_flag": mbd_flag[:, :, 0],
                "t_ref": Time(extract(buf, xml["T_refcal"]), format="lv"),
            }
    except Exception:
        return None
    return None


def describe_day(date_text: str) -> Dict[str, Any]:
    """Load scan metadata and SQL status for one observing day."""

    mjd = Time(date_text).mjd
    trange = Time([mjd + 0.3, mjd + 1.3], format="mjd")
    scan_dict = find_phasecal_scans(trange)
    if scan_dict.get("msg") != "Success":
        return {"date": date_text, "entries": [], "scan_dict": scan_dict}
    entries = []
    for idx, ts in enumerate(scan_dict["Timestamp"]):
        start = Time(ts, format="lv")
        duration = float(scan_dict["duration"][idx])
        end = Time(ts + duration * 60.0, format="lv")
        sql_ref = _sql_refcal_in_scan(start, end)
        sql_pha = None if sql_ref else _sql_phacal_in_scan(start, end)
        status = "raw"
        sql_meta = None
        if sql_ref:
            status = "refcal"
            sql_meta = sql_ref
        elif sql_pha:
            status = "phacal"
            sql_meta = sql_pha
        color = "#fff"
        try:
            times, wscram, _avgwind = db.a14_wscram(Time([start.mjd, end.mjd], format="mjd"))
            if wscram is not None:
                nbad = int(np.sum(wscram))
                frac = nbad / max(len(wscram), 1)
                if frac > 0.2:
                    color = "#f88"
                elif nbad > 0:
                    color = "#ff8"
        except Exception:
            pass
        entries.append(
            {
                "scan_id": idx,
                "scan_time": start.iso[11:19],
                "source": str(scan_dict["SourceID"][idx]),
                "duration_min": duration,
                "file": str(scan_dict["filelist"][idx]),
                "timestamp_lv": int(ts),
                "status": status,
                "sql_time": sql_meta["sql_time"].iso[:19] if sql_meta else None,
                "color": color,
                "sql_meta": sql_meta,
            }
        )
    return {"date": date_text, "entries": entries, "scan_dict": scan_dict}


def _load_input_dict(
    file_or_npz: str,
    navg: int = 3,
    quackint: float = 120.0,
) -> Tuple[Dict[str, Any], str]:
    """Load either a raw UDB file or an NPZ saved by legacy pcal_anal."""

    from eovsapy.read_idb import read_idb, read_npz

    path = _as_path(file_or_npz)
    if path.suffix.lower() == ".npz":
        out = read_npz([str(path)])
        origin = "npz"
    else:
        out = read_idb([str(path)], navg=navg, quackint=quackint)
        origin = "udb"
    if not out:
        raise CalWidgetV2Error("No data were returned from the input file.")
    return out, origin


def _channel_calibration_terms(raw_out: Dict[str, Any], layout: LayoutInfo) -> Dict[str, Any]:
    """Build per-channel feed-rotation calibration terms."""

    xml, buf = ch.read_cal(11, Time(raw_out["time"][0], format="jd"))
    dph = extract(buf, xml["XYphase"])
    xi_rot = extract(buf, xml["Xi_Rot"])
    freq = extract(buf, xml["FGHz"])
    freq = freq[np.where(freq != 0)]
    cal_band = _freq_to_band(freq, layout.mjd)
    band_values, first_idx = np.unique(cal_band, return_index=True)
    last_idx = np.append(first_idx[1:], len(cal_band))
    band_dxy = np.zeros((layout.nsolant + 1, layout.maxnbd), dtype=np.float64)
    band_xi = np.zeros(layout.maxnbd, dtype=np.float64)
    for band_i, band_value in enumerate(band_values):
        sl = slice(first_idx[band_i], last_idx[band_i])
        band_xi[band_value - 1] = np.nanmean(xi_rot[sl])
        for ant in range(layout.nsolant + 1):
            band_dxy[ant, band_value - 1] = np.angle(np.sum(np.exp(1j * dph[ant, sl])))
    channel_band = _freq_to_band(raw_out["fghz"], layout.mjd)
    channel_dxy = np.zeros((layout.nsolant + 1, raw_out["fghz"].size), dtype=np.float64)
    channel_xi = np.zeros(raw_out["fghz"].size, dtype=np.float64)
    for chan, band_value in enumerate(channel_band):
        if band_value > 0:
            channel_dxy[:, chan] = band_dxy[:, band_value - 1]
            channel_xi[chan] = band_xi[band_value - 1]
    return {
        "channel_band": channel_band,
        "channel_dxy": channel_dxy,
        "channel_xi": channel_xi,
    }


def _apply_feed_rotation(
    raw_out: Dict[str, Any],
    layout: LayoutInfo,
    vis: np.ndarray,
) -> np.ndarray:
    """Apply the same date-dependent feed/parallactic corrections as calwidget.

    The legacy widget stopped applying the full X/Y feed-rotation plus
    parallactic-angle unrotation after 2025-07-15, when all solar antennas
    moved onto the same mount behavior. Starting 2025-08-08 it instead applies
    only the Ant 4-driven parallactic-angle wrap fix. The fine-channel browser
    path must follow the same cutoff logic as the legacy band-time path.
    """

    mjd = Time(raw_out["time"][0], format="jd").mjd
    needs_full_unrotation = mjd <= _FULL_FEED_ROTATION_END_MJD
    needs_ant4_wrap_fix = mjd >= _ANT4_PA_WRAP_FIX_START_MJD
    if not needs_full_unrotation and not needs_ant4_wrap_fix:
        return vis

    trange = Time(raw_out["time"][[0, -1]], format="jd")
    times, chi = db.get_chi(trange)
    if times is None or len(times) == 0:
        return vis
    tchi = times.jd
    idx = nearest_val_idx(raw_out["time"], tchi)
    pa = chi[idx]
    corrected = vis

    if needs_full_unrotation:
        cal = _channel_calibration_terms(raw_out, layout)
        channel_dxy = cal["channel_dxy"]
        channel_xi = cal["channel_xi"]
        if layout.nsolant == 13:
            pa[:, [8, 9, 10, 12]] = 0.0
        vis2 = deepcopy(corrected)
        ntime = raw_out["time"].size
        for ant in range(layout.nsolant):
            a1 = lobe(channel_dxy[ant] - channel_dxy[layout.nsolant])
            a2 = -channel_dxy[layout.nsolant] - channel_xi
            a3 = channel_dxy[ant] - channel_xi + np.pi
            phase_xx = np.exp(1j * a1)[:, None]
            phase_xy = np.exp(1j * a2)[:, None]
            phase_yx = np.exp(1j * a3)[:, None]
            vis2[ant, 1] *= phase_xx
            vis2[ant, 2] *= phase_xy
            vis2[ant, 3] *= phase_yx
        for itime in range(ntime):
            for ant in range(layout.nsolant):
                cos_pa = np.cos(pa[itime, ant])
                sin_pa = np.sin(pa[itime, ant])
                corrected[ant, 0, :, itime] = vis2[ant, 0, :, itime] * cos_pa + vis2[ant, 3, :, itime] * sin_pa
                corrected[ant, 2, :, itime] = vis2[ant, 2, :, itime] * cos_pa + vis2[ant, 1, :, itime] * sin_pa
                corrected[ant, 3, :, itime] = vis2[ant, 3, :, itime] * cos_pa - vis2[ant, 0, :, itime] * sin_pa
                corrected[ant, 1, :, itime] = vis2[ant, 1, :, itime] * cos_pa - vis2[ant, 2, :, itime] * sin_pa

    if needs_ant4_wrap_fix:
        pa_ant4 = pa[:, 3].astype(float)
        pa_adjust = np.zeros_like(pa_ant4)
        pa_pad = np.deg2rad(0.5)
        lim_hi = pa_ant4 > np.pi / 2.0 + pa_pad
        lim_lo = pa_ant4 < -np.pi / 2.0 + pa_pad
        pa_adjust[lim_hi] += np.pi
        pa_adjust[lim_lo] -= np.pi
        vis_adjust = np.exp(1j * pa_adjust)
        corrected[: layout.nsolant, :, :, :] *= vis_adjust[None, None, None, :]

    return corrected


def _apply_time_drift_correction(
    channel_vis: np.ndarray,
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    times_jd: np.ndarray,
    layout: LayoutInfo,
) -> np.ndarray:
    """Apply the legacy time-drift fix before in-band fitting."""

    vis = deepcopy(channel_vis)
    unique_bands = np.unique(band_id[band_id > 0])
    band_index_map = dict((int(b), np.where(band_id == b)[0]) for b in unique_bands)
    for ant in range(layout.nsolant):
        for pol in range(2):
            slopes = []
            for band_value in unique_bands:
                idx = band_index_map[int(band_value)]
                if idx.size == 0:
                    continue
                band_vis = _safe_nanmean(vis[ant, pol, idx], axis=0)
                pfit = lin_phase_fit(times_jd, np.angle(band_vis))
                if pfit[2] < 0.7:
                    slopes.append(pfit[1] / np.nanmean(freq_ghz[idx]))
            if not slopes:
                continue
            dpdt = np.nanmedian(slopes)
            for band_value in unique_bands:
                idx = band_index_map[int(band_value)]
                pfit = dpdt * np.nanmean(freq_ghz[idx]) * (times_jd - times_jd[times_jd.size // 2])
                correction = (np.cos(pfit) - 1j * np.sin(pfit))[None, :]
                vis[ant, :, idx, :] *= correction
    return vis


def prepare_channel_dataset(
    file_or_npz: str,
    scan_id: int = -1,
    scan_kind: str = "raw",
    navg: int = 3,
    quackint: float = 120.0,
    fix_drift: bool = True,
) -> Dict[str, Any]:
    """Read one scan and preserve corrected per-channel visibilities."""

    from eovsapy.read_idb import bl2ord

    raw_out, origin = _load_input_dict(file_or_npz, navg=navg, quackint=quackint)
    mjd = Time(raw_out["time"][0], format="jd").mjd
    layout = _layout_for_mjd(mjd)
    channel_band = _freq_to_band(raw_out["fghz"], mjd)
    vis = np.asarray(raw_out["x"][bl2ord[layout.ref_ant_index, : layout.nsolant]], dtype=np.complex128)
    vis = _apply_feed_rotation(raw_out, layout, vis)
    if fix_drift:
        vis = _apply_time_drift_correction(vis, np.asarray(raw_out["fghz"]), channel_band, np.asarray(raw_out["time"]), layout)
    duration_min = (float(raw_out["time"][-1]) - float(raw_out["time"][0])) * 24.0 * 60.0
    return {
        "scan_id": scan_id,
        "scan_kind": scan_kind,
        "file": str(file_or_npz),
        "source": str(raw_out.get("source", "")).replace("\x00", ""),
        "origin": origin,
        "fix_drift": bool(fix_drift),
        "raw": raw_out,
        "layout": layout,
        "channel_vis": vis,
        "channel_band": channel_band,
        "channel_freq_ghz": np.asarray(raw_out["fghz"], dtype=np.float64),
        "timestamp": Time(raw_out["time"][0], format="jd"),
        "t_bg": Time(raw_out["time"][0], format="jd"),
        "t_ed": Time(raw_out["time"][-1], format="jd"),
        "duration_min": duration_min,
    }


def _apply_legacy_band_time_drift(
    vis: np.ndarray,
    times_jd: np.ndarray,
    fghz_band: np.ndarray,
    layout: LayoutInfo,
) -> np.ndarray:
    """Apply the legacy band-domain time-drift correction."""

    corrected = deepcopy(vis)
    nant, npol, nband, _nt = corrected.shape
    for iant in range(nant):
        for ipol in range(min(npol, 2)):
            slopes = []
            for iband in range(nband):
                if fghz_band[iband] <= 0.0:
                    continue
                phz = np.angle(corrected[iant, ipol, iband])
                if np.sum(np.isfinite(phz)) <= 3:
                    continue
                pfit = lin_phase_fit(times_jd, phz)
                if pfit[2] < 0.7:
                    slopes.append(pfit[1] / fghz_band[iband])
            if not slopes:
                continue
            dpdt = np.nanmedian(slopes)
            for iband in range(nband):
                if fghz_band[iband] <= 0.0:
                    continue
                pfit = dpdt * fghz_band[iband] * (times_jd - times_jd[times_jd.size // 2])
                corrected[iant, ipol, iband] *= np.cos(pfit) - 1j * np.sin(pfit)
    return corrected


def _legacy_band_time_vis(raw_out: Dict[str, Any], layout: LayoutInfo, fix_drift: bool = True) -> Dict[str, Any]:
    """Reproduce the legacy band-time visibility product used by Tk time history."""

    from eovsapy.read_idb import bl2ord

    mjd = Time(raw_out["time"][0], format="jd").mjd
    maxnbd = layout.maxnbd
    nsolant = layout.nsolant
    nant = layout.nant
    nt = len(raw_out["time"])
    if "band" in raw_out:
        inband = np.asarray(raw_out["band"], dtype=np.int32)
    else:
        inband = _freq_to_band(np.asarray(raw_out["fghz"], dtype=np.float64), mjd)
    bds = np.unique(inband[inband > 0])
    vis = np.zeros((nant, 4, maxnbd, nt), dtype=np.complex128)
    fghz = np.zeros(maxnbd, dtype=np.float64)
    averaged = raw_out["x"][bl2ord[layout.ref_ant_index, : layout.nsolant]]
    for bd in bds:
        idx = np.where(inband == bd)[0]
        if idx.size == 0:
            continue
        fghz[bd - 1] = np.nanmean(raw_out["fghz"][idx])
        vis[:nsolant, :, bd - 1] = np.mean(averaged[:, :, idx], axis=2)

    xml, buf = ch.read_cal(11, Time(raw_out["time"][0], format="jd"))
    dph = extract(buf, xml["XYphase"])
    xi_rot = extract(buf, xml["Xi_Rot"])
    freq = extract(buf, xml["FGHz"])
    freq = freq[np.where(freq != 0)]
    cal_band = _freq_to_band(freq, mjd)
    band_values, sidx = np.unique(cal_band, return_index=True)
    eidx = np.append(sidx[1:], len(cal_band))
    dxy = np.zeros((nsolant + 1, maxnbd), dtype=np.float64)
    xi = np.zeros(maxnbd, dtype=np.float64)
    for b, band_value in enumerate(band_values):
        if band_value <= 0:
            continue
        sl = slice(sidx[b], eidx[b])
        xi[band_value - 1] = np.nanmean(xi_rot[sl])
        for ant in range(nsolant + 1):
            dxy[ant, band_value - 1] = np.angle(np.sum(np.exp(1j * dph[ant, sl])))
    if mjd <= Time("2025-07-15").mjd:
        trange = Time(raw_out["time"][[0, -1]], format="jd")
        times, chi = db.get_chi(trange)
        tchi = times.jd
        if len(raw_out["time"]) > 0:
            vis2 = deepcopy(vis)
            idx = nearest_val_idx(raw_out["time"], tchi)
            pa = chi[idx]
            if nsolant == 13:
                pa[:, [8, 9, 10, 12]] = 0.0
            a1 = lobe(dxy[:nsolant] - dxy[nsolant])
            a2 = -dxy[nsolant] - xi
            a3 = dxy[:nsolant] - xi + np.pi
            phase1 = np.exp(1j * a1)
            phase2 = np.exp(1j * a2)
            phase3 = np.exp(1j * a3)
            vis2[:nsolant, 1, :, :] *= phase1[:, :, None]
            vis2[:nsolant, 2, :, :] *= phase2[None, :, None]
            vis2[:nsolant, 3, :, :] *= phase3[:, :, None]
            pa_ant = pa[:, :nsolant].astype(float)
            cos_pa = np.cos(pa_ant).T[:, None, :]
            sin_pa = np.sin(pa_ant).T[:, None, :]
            v0 = vis2[:nsolant, 0, :, :]
            v1 = vis2[:nsolant, 1, :, :]
            v2 = vis2[:nsolant, 2, :, :]
            v3 = vis2[:nsolant, 3, :, :]
            vis[:nsolant, 0, :, :] = v0 * cos_pa + v3 * sin_pa
            vis[:nsolant, 2, :, :] = v2 * cos_pa + v1 * sin_pa
            vis[:nsolant, 3, :, :] = v3 * cos_pa - v0 * sin_pa
            vis[:nsolant, 1, :, :] = v1 * cos_pa - v2 * sin_pa
    if mjd >= Time("2025-08-08").mjd:
        trange = Time(raw_out["time"][[0, -1]], format="jd")
        times, chi = db.get_chi(trange)
        tchi = times.jd
        if len(raw_out["time"]) > 0:
            idx = nearest_val_idx(raw_out["time"], tchi)
            pa = chi[idx]
            pa_ant4 = pa[:, 3].astype(float)
            pa_adjust = np.zeros_like(pa_ant4)
            pa_pad = np.deg2rad(0.5)
            lim_hi = pa_ant4 > np.pi / 2.0 + pa_pad
            lim_lo = pa_ant4 < -np.pi / 2.0 + pa_pad
            pa_adjust[lim_hi] += np.pi
            pa_adjust[lim_lo] -= np.pi
            vis_adjust = np.exp(1j * pa_adjust)
            vis[:nsolant, :, :, :] *= vis_adjust[None, None, None, :]
    if fix_drift:
        vis = _apply_legacy_band_time_drift(vis, np.asarray(raw_out["time"], dtype=np.float64), fghz, layout)
    if maxnbd > 1 and fghz[1] < 1.0:
        fghz[1] = 1.9290
    return {"vis": vis, "times": np.asarray(raw_out["time"], dtype=np.float64), "fghz": fghz}


def _ensure_legacy_band_time(scan: ScanAnalysis) -> Dict[str, Any]:
    """Build the legacy band-time product at most once per scan.

    The legacy calibration reader in :mod:`eovsapy.cal_header` writes a shared
    temporary XML file under ``/tmp``. When the browser requests heatmap,
    time-history, and overview payloads in parallel, concurrent calls into that
    reader can race and briefly leave one worker parsing an empty file. Build
    the legacy band-time cache under a process-wide lock so one scan computes it
    once and all concurrent readers reuse the cached result.

    :param scan: Scan analysis object that owns the raw/cached products.
    :type scan: ScanAnalysis
    :returns: Cached legacy band-time visibility payload.
    :rtype: dict
    :raises CalWidgetV2Error: If the scan has no raw data or the legacy product
        cannot be built.
    """

    if not scan.raw or "raw" not in scan.raw:
        raise CalWidgetV2Error("No raw time history is available for this scan.")
    cached = scan.raw.get("legacy_band_time")
    if cached is not None:
        return cached
    with _LEGACY_BAND_TIME_LOCK:
        cached = scan.raw.get("legacy_band_time")
        if cached is not None:
            return cached
        try:
            cached = _legacy_band_time_vis(
                scan.raw["raw"],
                scan.layout,
                fix_drift=bool(scan.raw.get("fix_drift", True)),
            )
        except Exception as exc:
            raise CalWidgetV2Error(
                "Failed to build the legacy band-time display product: {0}".format(exc)
            ) from exc
        scan.raw["legacy_band_time"] = cached
        return cached


def legacy_time_history_payload(scan: Optional[ScanAnalysis], ant: int, band: int) -> Dict[str, Any]:
    """Return legacy-style time-history data for frontend JS plotting.

    :param scan: Current scan selection.
    :type scan: ScanAnalysis or None
    :param ant: Zero-based selected antenna index.
    :type ant: int
    :param band: Zero-based selected band index.
    :type band: int
    :returns: JSON-safe time history payload.
    :rtype: dict
    """

    if scan is None:
        return {"message": "No scan selected."}
    if not scan.raw:
        return {"message": "No raw time history is available for SQL-only results."}
    ant = int(max(0, min(ant, scan.layout.nsolant - 1)))
    band = int(max(0, min(band, scan.layout.maxnbd - 1)))
    try:
        legacy = _ensure_legacy_band_time(scan)
    except CalWidgetV2Error as exc:
        return {"message": str(exc)}
    vis = legacy["vis"]
    times_jd = legacy["times"]
    if band >= vis.shape[2]:
        return {"message": "Selected band is out of range."}
    amp_x = np.abs(vis[ant, 0, band])
    amp_y = np.abs(vis[ant, 1, band])
    # The Python 3 raw-reader path leaves the time-history visibilities in the
    # opposite overall sign convention from the legacy Tk display, which shows
    # up as a uniform -pi offset in phase. Shift only the display payload here
    # so the browser matches the legacy widget without changing the underlying
    # calibration products.
    pha_x = lobe(np.angle(vis[ant, 0, band]) + np.pi)
    pha_y = lobe(np.angle(vis[ant, 1, band]) + np.pi)
    offsets_min = (times_jd - times_jd[0]) * 24.0 * 60.0
    labels = [Time(t, format="jd").iso[11:16] for t in times_jd]
    tick_idx = np.unique(np.linspace(0, max(len(times_jd) - 1, 0), min(len(times_jd), 4), dtype=int))
    tick_offsets = offsets_min[tick_idx].tolist() if tick_idx.size else []
    tick_labels = [labels[idx] for idx in tick_idx] if tick_idx.size else []
    datamax = np.nanmax(np.abs(vis[ant, :2, band])) if np.any(np.isfinite(vis[ant, :2, band])) else 1.0
    groups = serialized_time_flag_groups_for_target(scan, ant, band, times_jd)
    return {
        "message": None,
        "title": "Ant {0:d}, Band {1:d}".format(ant + 1, band + 1),
        "amp_ylim": [1.0e-3, float(max(1.0, datamax))],
        "phase_ylim": [-4.0, 4.0],
        "start_jd": float(times_jd[0]),
        "end_jd": float(times_jd[-1]),
        "offset_min": offsets_min.tolist(),
        "tick_offsets": tick_offsets,
        "tick_labels": tick_labels,
        "series": [
            {"label": "X", "color": "#1f77b4", "amp": amp_x.tolist(), "phase": pha_x.tolist()},
            {"label": "Y", "color": "#ff7f0e", "amp": amp_y.tolist(), "phase": pha_y.tolist()},
        ],
        "interval_groups": groups,
    }


def _normalize_interval(start_jd: float, end_jd: float) -> Tuple[float, float]:
    """Return a sorted Julian-Date interval."""

    start = float(start_jd)
    end = float(end_jd)
    return (start, end) if start <= end else (end, start)


def _sorted_unique_targets(targets: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Return unique antenna-band targets in deterministic order."""

    return sorted(set((int(ant), int(band)) for ant, band in targets))


def _targets_for_scope(layout: LayoutInfo, ant: int, band: int, scope: str) -> List[Tuple[int, int]]:
    """Expand one scope preset into concrete antenna-band targets."""

    ant = int(max(0, min(ant, layout.nsolant - 1)))
    band = int(max(0, min(band, layout.maxnbd - 1)))
    if scope == "selected":
        return [(ant, band)]
    if scope == "this_ant":
        return [(ant, band_idx) for band_idx in range(layout.maxnbd)]
    if scope == "this_band":
        return [(ant_idx, band) for ant_idx in range(layout.nsolant)]
    if scope == "higher_bands":
        return [(ant, band_idx) for band_idx in range(band, layout.maxnbd)]
    if scope == "all":
        return [(ant_idx, band_idx) for ant_idx in range(layout.nsolant) for band_idx in range(layout.maxnbd)]
    raise CalWidgetV2Error("Unknown time-flag scope '{0}'.".format(scope))


def ensure_time_flag_groups(scan: ScanAnalysis) -> List[TimeFlagGroup]:
    """Initialize browser-native time-flag groups from legacy slots if needed.

    :param scan: Scan analysis object to inspect or update.
    :type scan: ScanAnalysis
    :returns: Mutable list of active interval groups for the scan.
    :rtype: list[TimeFlagGroup]
    """

    if scan.time_flag_groups_initialized:
        return scan.time_flag_groups
    scan.time_flag_groups_initialized = True
    if scan.tflags is None or np.ndim(scan.tflags) != 4:
        scan.time_flag_groups = []
        return scan.time_flag_groups
    grouped: Dict[Tuple[int, float, float], List[Tuple[int, int]]] = {}
    nslots = scan.tflags.shape[3]
    for ant in range(min(scan.layout.nsolant, scan.tflags.shape[0])):
        for band in range(min(scan.layout.maxnbd, scan.tflags.shape[1])):
            for slot in range(nslots):
                tflag = np.asarray(scan.tflags[ant, band, :, slot], dtype=np.float64)
                if tflag.size != 2 or not np.all(np.isfinite(tflag)) or np.any(np.isclose(tflag, 0.0)):
                    continue
                jdrange = Time(tflag, format="plot_date").jd
                start_jd, end_jd = _normalize_interval(jdrange[0], jdrange[1])
                key = (slot, round(start_jd, 12), round(end_jd, 12))
                grouped.setdefault(key, []).append((ant, band))
    groups: List[TimeFlagGroup] = []
    for idx, key in enumerate(sorted(grouped.keys())):
        slot, start_jd, end_jd = key
        groups.append(
            TimeFlagGroup(
                group_id="legacy-{0:d}-{1:d}".format(slot, idx),
                scope="migrated",
                start_jd=float(start_jd),
                end_jd=float(end_jd),
                targets=_sorted_unique_targets(grouped[key]),
                source="legacy",
            )
        )
    scan.time_flag_groups = groups
    return scan.time_flag_groups


def add_time_flag_group(scan: ScanAnalysis, ant: int, band: int, start_jd: float, end_jd: float, scope: str) -> TimeFlagGroup:
    """Append one browser-native interval group to the selected scan."""

    ensure_time_flag_groups(scan)
    start_norm, end_norm = _normalize_interval(start_jd, end_jd)
    if np.isclose(start_norm, end_norm, rtol=0.0, atol=1.0e-9):
        raise CalWidgetV2Error("Time-flag interval width is zero.")
    group = TimeFlagGroup(
        group_id=uuid4().hex,
        scope=str(scope),
        start_jd=start_norm,
        end_jd=end_norm,
        targets=_targets_for_scope(scan.layout, ant, band, str(scope)),
        source="browser",
    )
    scan.time_flag_groups.append(group)
    scan.time_flag_groups.sort(key=lambda item: (item.start_jd, item.end_jd, item.group_id))
    return group


def delete_time_flag_group(scan: ScanAnalysis, group_id: str) -> bool:
    """Delete one browser-native interval group by id."""

    ensure_time_flag_groups(scan)
    before = len(scan.time_flag_groups)
    scan.time_flag_groups = [group for group in scan.time_flag_groups if group.group_id != str(group_id)]
    return len(scan.time_flag_groups) != before


def serialized_time_flag_groups_for_target(scan: ScanAnalysis, ant: int, band: int, times_jd: np.ndarray) -> List[Dict[str, Any]]:
    """Return selected-cell interval groups in a frontend-ready format."""

    groups = []
    ant = int(max(0, min(ant, scan.layout.nsolant - 1)))
    band = int(max(0, min(band, scan.layout.maxnbd - 1)))
    ensure_time_flag_groups(scan)
    t0 = float(times_jd[0])
    for group in scan.time_flag_groups:
        if (ant, band) not in group.targets:
            continue
        groups.append(
            {
                "group_id": group.group_id,
                "scope": group.scope,
                "scope_label": TIME_FLAG_SCOPE_LABELS.get(group.scope, group.scope.title()),
                "source": group.source,
                "target_count": len(group.targets),
                "start_jd": float(group.start_jd),
                "end_jd": float(group.end_jd),
                "start_offset_min": float((group.start_jd - t0) * 24.0 * 60.0),
                "end_offset_min": float((group.end_jd - t0) * 24.0 * 60.0),
                "start_label": Time(group.start_jd, format="jd").iso[11:19],
                "end_label": Time(group.end_jd, format="jd").iso[11:19],
            }
        )
    groups.sort(key=lambda item: (item["start_jd"], item["end_jd"], item["group_id"]))
    return groups


def _time_flag_signature(scan: ScanAnalysis) -> str:
    """Return a stable signature for the current browser time-flag groups."""

    groups = ensure_time_flag_groups(scan)
    payload = [
        {
            "id": str(group.group_id),
            "scope": str(group.scope),
            "start": round(float(group.start_jd), 9),
            "end": round(float(group.end_jd), 9),
            "targets": [[int(ant), int(band)] for ant, band in sorted(group.targets)],
        }
        for group in groups
    ]
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _masked_time_combined_channel_vis(
    channel_vis: np.ndarray,
    channel_band: np.ndarray,
    layout: LayoutInfo,
    times_jd: np.ndarray,
    time_flag_groups: Optional[Sequence[TimeFlagGroup]] = None,
) -> np.ndarray:
    """Return time-combined channel vis with browser time flags applied.

    The browser inband diagnostics should operate on the same time-combined
    product implied by the Time History time mask, not on the full
    time-resolved visibility cube each time a panel refreshes.
    """

    vis = deepcopy(np.asarray(channel_vis[: layout.nsolant, :2], dtype=np.complex128))
    bands = np.asarray(channel_band, dtype=int)
    times = np.asarray(times_jd, dtype=np.float64)
    if time_flag_groups:
        for group in time_flag_groups:
            start_jd, end_jd = _normalize_interval(group.start_jd, group.end_jd)
            bad, = np.where(np.logical_and(times >= start_jd, times <= end_jd))
            if bad.size == 0:
                continue
            for ant, band_idx in group.targets:
                if ant < 0 or ant >= layout.nsolant or band_idx < 0 or band_idx >= layout.maxnbd:
                    continue
                chan_idx, = np.where(bands == int(band_idx) + 1)
                if chan_idx.size == 0:
                    continue
                for pol in range(min(vis.shape[1], 2)):
                    vis[ant, pol, chan_idx[:, None], bad] = np.nan
    return _safe_complex_nanmean(vis, axis=3)


def combined_channel_vis_with_time_flags(scan: ScanAnalysis) -> Tuple[np.ndarray, np.ndarray]:
    """Return cached raw/corrected time-combined channel vis for inband plots.

    The returned arrays are per-channel visibilities already averaged over
    time after applying the current browser-native time-mask state.
    """

    if not scan.raw:
        raise CalWidgetV2Error("Combined channel visibility cache requires raw scan data.")
    delay_signature = None if scan.delay_solution is None else scan.delay_solution.window_signature()
    cache_key = json.dumps(
        {
            "delay_window": delay_signature,
            "time_flags": _time_flag_signature(scan),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    cache = scan.raw.get("combined_channel_vis_cache")
    if cache and cache.get("key") == cache_key:
        return cache["raw"], cache["corrected"]
    groups = ensure_time_flag_groups(scan)
    times_jd = np.asarray(scan.raw["raw"]["time"], dtype=np.float64)
    raw_avg = _masked_time_combined_channel_vis(
        scan.raw["channel_vis"],
        scan.raw["channel_band"],
        scan.layout,
        times_jd,
        time_flag_groups=groups,
    )
    corrected_avg = _masked_time_combined_channel_vis(
        scan.corrected_channel_vis,
        scan.raw["channel_band"],
        scan.layout,
        times_jd,
        time_flag_groups=groups,
    )
    scan.raw["combined_channel_vis_cache"] = {
        "key": cache_key,
        "raw": raw_avg,
        "corrected": corrected_avg,
    }
    return raw_avg, corrected_avg


def legacy_refcal_display_summary(scan: Optional[ScanAnalysis]) -> Optional[Dict[str, np.ndarray]]:
    """Return the legacy refcal summary product used by the Tk widget display.

    The browser v2 analysis path fits and applies in-band delay before
    computing band-averaged visibility flags.  The legacy Tk sigma map for a
    refcal instead displays the pre-inband `rd_refcal() + fix_time_drift() +
    refcal_anal()` product.  Cache that legacy-equivalent summary here so the
    browser heatmap can match the legacy display without changing the v2
    calibration products used elsewhere.

    :param scan: Current scan selection.
    :type scan: ScanAnalysis or None
    :returns: Legacy-style refcal summary cache, or None when unavailable.
    :rtype: dict or None
    """

    if scan is None or scan.scan_kind != "refcal":
        return None
    if not scan.raw or "raw" not in scan.raw:
        return None
    cached = scan.raw.get("legacy_refcal_summary")
    if cached is not None:
        return cached
    try:
        legacy = _ensure_legacy_band_time(scan)
    except CalWidgetV2Error:
        return None
    groups = ensure_time_flag_groups(scan)
    vis_median, sigma, flags, _tflags = _summarize_band_vis(
        legacy["vis"],
        scan.layout,
        tflags=None,
        times_jd=np.asarray(legacy["times"], dtype=np.float64),
        time_flag_groups=groups,
    )
    cached = {
        "x": vis_median,
        "sigma": sigma,
        "flags": flags,
        "fghz": np.asarray(legacy["fghz"], dtype=np.float64),
    }
    scan.raw["legacy_refcal_summary"] = cached
    return cached


def _fit_uniform_inband_delay(
    channel_vis: np.ndarray,
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    layout: LayoutInfo,
) -> DelaySolution:
    """Fit one uniform X/Y in-band delay per antenna from per-band slopes.

    The weighted mean follows the notebook reference workflow in
    ``delay_cal_20260406.ipynb``: each band is weighted by the raw
    ``lin_phase_fit(...)[2]`` phase scatter. We intentionally do not convert
    that scatter into a delay-domain uncertainty, because the extra
    ``sigma_f^2`` factor changes the relative influence of wide high-frequency
    bands and was the main source of the calwidget/notebook inconsistency.
    """

    band_values = np.unique(band_id[band_id > 0])
    nbands_used = band_values.size
    per_band_delay_ns = np.full((layout.nsolant, 2, nbands_used), np.nan, dtype=np.float64)
    per_band_std = np.full((layout.nsolant, 2, nbands_used), np.nan, dtype=np.float64)
    per_band_phase0 = np.full((layout.nsolant, 2, nbands_used), np.nan, dtype=np.float64)
    fitted_ns = np.full((layout.nsolant, 2), np.nan, dtype=np.float64)
    fitted_std_ns = np.full((layout.nsolant, 2), np.nan, dtype=np.float64)
    delay_flag = np.ones((layout.nsolant, 2), dtype=np.float64)
    band_centers_ghz = np.array([np.nanmean(freq_ghz[band_id == band]) for band in band_values], dtype=np.float64)
    kept_band_mask = np.ones((layout.nsolant, 2, nbands_used), dtype=bool)
    chan_avg = _safe_nanmean(channel_vis[:, :2], axis=3)
    for ant in range(layout.nsolant):
        for pol in range(2):
            for band_idx, band_value in enumerate(band_values):
                idx = np.where(band_id == band_value)[0]
                if idx.size < 3:
                    continue
                pfit = lin_phase_fit(freq_ghz[idx], np.angle(chan_avg[ant, pol, idx]))
                per_band_delay_ns[ant, pol, band_idx] = pfit[1] / (2.0 * np.pi)
                per_band_phase0[ant, pol, band_idx] = pfit[0]
                # Keep the raw phase-fit scatter in radians so the weighted
                # mean matches the notebook's ``1 / pfit[2]^2`` rule.
                per_band_std[ant, pol, band_idx] = pfit[2]
            valid = np.isfinite(per_band_delay_ns[ant, pol]) & np.isfinite(per_band_std[ant, pol]) & (per_band_std[ant, pol] > 0)
            if np.count_nonzero(valid) == 0:
                continue
            weights = 1.0 / np.square(per_band_std[ant, pol, valid])
            fitted_ns[ant, pol] = np.nansum(per_band_delay_ns[ant, pol, valid] * weights) / np.nansum(weights)
            fitted_std_ns[ant, pol] = np.nanstd(per_band_delay_ns[ant, pol, valid])
            delay_flag[ant, pol] = 0.0
    return DelaySolution(
        fitted_ns=fitted_ns,
        active_ns=fitted_ns.copy(),
        relative_ns=np.zeros_like(fitted_ns),
        relative_auto_ns=np.zeros_like(fitted_ns),
        relative_suggested_ns=np.zeros_like(fitted_ns),
        relative_prev_ns=np.zeros_like(fitted_ns),
        relative_prev_valid=np.zeros(fitted_ns.shape[0], dtype=bool),
        fitted_std_ns=fitted_std_ns,
        flag=delay_flag,
        per_band_delay_ns=per_band_delay_ns,
        per_band_std=per_band_std,
        per_band_phase0=per_band_phase0,
        band_values=band_values,
        band_centers_ghz=band_centers_ghz,
        kept_band_mask=kept_band_mask,
    )


def _apply_uniform_delay(
    channel_vis: np.ndarray,
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    delay_ns: np.ndarray,
    layout: LayoutInfo,
) -> np.ndarray:
    """Apply one X/Y delay per antenna before band averaging."""

    corrected = deepcopy(channel_vis)
    for ant in range(layout.nsolant):
        for pol in range(2):
            tau_ns = delay_ns[ant, pol]
            if not np.isfinite(tau_ns):
                continue
            for band_value in np.unique(band_id[band_id > 0]):
                idx = np.where(band_id == band_value)[0]
                if idx.size == 0:
                    continue
                fmid = np.nanmean(freq_ghz[idx])
                phase = 2.0 * np.pi * freq_ghz[idx] * tau_ns - 2.0 * np.pi * fmid * tau_ns
                corr = np.exp(1j * phase)[:, None]
                corrected[ant, pol, idx, :] = corrected[ant, pol, idx, :] / corr
    return corrected


def solve_residual_delay_phi0(
    freq_hz: np.ndarray,
    vis: np.ndarray,
    weights: Optional[np.ndarray] = None,
    dly_max_s: float = 10e-9,
    step_s: float = 0.02e-9,
) -> Dict[str, Any]:
    """Estimate one residual delay and phase offset without phase unwrapping.

    :param freq_hz: Channel frequencies in Hz.
    :type freq_hz: np.ndarray
    :param vis: Complex, already in-band-corrected channel visibilities.
    :type vis: np.ndarray
    :param weights: Optional non-negative weights per channel.
    :type weights: np.ndarray | None
    :param dly_max_s: Maximum absolute residual delay to search.
    :type dly_max_s: float
    :param step_s: Delay-grid spacing in seconds.
    :type step_s: float
    :returns: Best-fit residual delay, phase offset, mask, and wrapped model.
    :rtype: dict[str, Any]
    """

    freq_hz = np.asarray(freq_hz, dtype=np.float64).reshape(-1)
    vis = np.asarray(vis, dtype=np.complex128).reshape(-1)
    if weights is None:
        weights = np.ones(freq_hz.shape, dtype=np.float64)
    else:
        weights = np.asarray(weights, dtype=np.float64).reshape(-1)
    valid = (
        np.isfinite(freq_hz)
        & np.isfinite(vis.real)
        & np.isfinite(vis.imag)
        & np.isfinite(weights)
        & (weights > 0.0)
    )
    if np.count_nonzero(valid) < 3:
        nan_phase = np.full(freq_hz.shape, np.nan, dtype=np.float64)
        return {
            "dly_res_s": np.nan,
            "phi0_rad": np.nan,
            "mask": valid,
            "phi_fit_wrapped": nan_phase,
            "coherence": np.array([], dtype=np.float64),
            "dly_grid_s": np.array([], dtype=np.float64),
        }
    grid = np.arange(-float(dly_max_s), float(dly_max_s) + 0.5 * float(step_s), float(step_s), dtype=np.float64)
    phase = -1j * 2.0 * np.pi * freq_hz[valid, None] * grid[None, :]
    rotated = np.sum((weights[valid] * vis[valid])[:, None] * np.exp(phase), axis=0)
    coherence = np.abs(rotated)
    best_idx = int(np.nanargmax(coherence))
    dly_res_s = float(grid[best_idx])
    phi0 = float(np.angle(rotated[best_idx]))
    phi_fit = np.full(freq_hz.shape, np.nan, dtype=np.float64)
    phi_fit[valid] = np.angle(np.exp(1j * (2.0 * np.pi * freq_hz[valid] * dly_res_s + phi0)))
    return {
        "dly_res_s": dly_res_s,
        "phi0_rad": phi0,
        "mask": valid,
        "phi_fit_wrapped": phi_fit,
        "coherence": coherence,
        "dly_grid_s": grid,
    }


def _band_average_channel_vis(
    channel_vis: np.ndarray,
    freq_ghz: np.ndarray,
    band_id: np.ndarray,
    layout: LayoutInfo,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Collapse corrected channel visibilities into legacy band products."""

    ntime = channel_vis.shape[-1]
    band_vis = np.full((layout.nant, 4, layout.maxnbd, ntime), np.nan + 1j * np.nan, dtype=np.complex128)
    fghz_band = np.zeros(layout.maxnbd, dtype=np.float64)
    used_band_ids = np.zeros(layout.maxnbd, dtype=np.int32)
    for band_value in np.unique(band_id[band_id > 0]):
        idx = np.where(band_id == band_value)[0]
        if idx.size == 0:
            continue
        full_index = int(band_value) - 1
        fghz_band[full_index] = np.nanmean(freq_ghz[idx])
        used_band_ids[full_index] = int(band_value)
        band_vis[: layout.nsolant, :, full_index, :] = _safe_nanmean(channel_vis[:, :, idx, :], axis=2)
    if layout.maxnbd > 1 and fghz_band[1] < 1.0:
        fghz_band[1] = 1.9290
    return band_vis, fghz_band, used_band_ids


def _summarize_band_vis(
    band_vis: np.ndarray,
    layout: LayoutInfo,
    tflags: Optional[np.ndarray] = None,
    times_jd: Optional[np.ndarray] = None,
    time_flag_groups: Optional[Sequence[TimeFlagGroup]] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Compute time-averaged products and flags from corrected band vis."""

    vis = deepcopy(band_vis)
    if tflags is None:
        tflags = np.zeros((layout.nant, layout.maxnbd, 2, 2), np.float64)
    if times_jd is not None:
        if time_flag_groups:
            for group in time_flag_groups:
                start_jd, end_jd = _normalize_interval(group.start_jd, group.end_jd)
                bad, = np.where(np.logical_and(times_jd >= start_jd, times_jd <= end_jd))
                if bad.size == 0:
                    continue
                for ant, band_idx in group.targets:
                    if ant < 0 or ant >= layout.nsolant or band_idx < 0 or band_idx >= layout.maxnbd:
                        continue
                    vis[ant, :, band_idx, bad] = np.nan
        else:
            for ant in range(layout.nsolant):
                for band_idx in range(layout.maxnbd):
                    for slot in range(2):
                        if tflags[ant, band_idx, 1, slot] == 0.0:
                            continue
                        jdrange = Time(tflags[ant, band_idx, :, slot], format="plot_date").jd
                        if jdrange[0] > jdrange[1]:
                            jdrange = jdrange[::-1]
                        bad, = np.where(np.logical_and(times_jd >= jdrange[0], times_jd <= jdrange[1]))
                        if bad.size > 0:
                            vis[ant, :, band_idx, bad] = np.nan
    sigma = _safe_nanstd(vis, axis=3)
    amp = np.abs(vis)
    vis_median = np.nanmedian(vis, axis=3)
    amp_median = np.nanmedian(amp, axis=3)
    snr = np.divide(amp_median, sigma, out=np.full_like(amp_median, np.nan), where=sigma != 0)
    flags = (snr < 1).astype(np.int32)
    flags[np.where(np.isnan(snr))] = 1
    return vis_median, sigma, flags, tflags


def _band_index_map(used_band_ids: np.ndarray) -> Dict[int, int]:
    """Map band number to the corresponding full band-array index."""

    out = {}
    for idx, band_value in enumerate(used_band_ids):
        if band_value > 0:
            out[int(band_value)] = idx
    return out


def analyze_refcal_input(
    file_or_npz: str,
    scan_id: int = -1,
    navg: int = 3,
    quackint: float = 120.0,
    fix_drift: bool = True,
) -> ScanAnalysis:
    """Analyze one raw scan as a v2 refcal product."""

    prepared = prepare_channel_dataset(
        file_or_npz=file_or_npz,
        scan_id=scan_id,
        scan_kind="refcal",
        navg=navg,
        quackint=quackint,
        fix_drift=fix_drift,
    )
    delay_solution = _fit_uniform_inband_delay(
        prepared["channel_vis"],
        prepared["channel_freq_ghz"],
        prepared["channel_band"],
        prepared["layout"],
    )
    corrected_channel_vis = _apply_uniform_delay(
        prepared["channel_vis"],
        prepared["channel_freq_ghz"],
        prepared["channel_band"],
        delay_solution.active_ns,
        prepared["layout"],
    )
    corrected_band_vis, fghz_band, used_band_ids = _band_average_channel_vis(
        corrected_channel_vis,
        prepared["channel_freq_ghz"],
        prepared["channel_band"],
        prepared["layout"],
    )
    vis_median, sigma, flags, tflags = _summarize_band_vis(
        corrected_band_vis,
        prepared["layout"],
        tflags=None,
        times_jd=np.asarray(prepared["raw"]["time"], dtype=np.float64),
    )
    return ScanAnalysis(
        scan_id=scan_id,
        scan_kind="refcal",
        file=prepared["file"],
        source=prepared["source"],
        timestamp=prepared["timestamp"],
        t_bg=prepared["t_bg"],
        t_ed=prepared["t_ed"],
        duration_min=prepared["duration_min"],
        layout=prepared["layout"],
        raw=prepared,
        corrected_channel_vis=corrected_channel_vis,
        corrected_band_vis=vis_median,
        sigma=sigma,
        flags=flags,
        fghz_band=fghz_band,
        bands_band=used_band_ids,
        band_to_full_index=_band_index_map(used_band_ids),
        delay_solution=delay_solution,
        tflags=tflags,
    )


def refresh_refcal_solution(scan: ScanAnalysis) -> None:
    """Reapply the active in-band delays after a manual edit."""

    if scan.delay_solution is None:
        raise CalWidgetV2Error("No in-band delay solution is attached to this refcal.")
    groups = ensure_time_flag_groups(scan)
    prepared = scan.raw
    corrected_channel_vis = _apply_uniform_delay(
        prepared["channel_vis"],
        prepared["channel_freq_ghz"],
        prepared["channel_band"],
        scan.delay_solution.active_ns,
        scan.layout,
    )
    band_vis, fghz_band, used_band_ids = _band_average_channel_vis(
        corrected_channel_vis,
        prepared["channel_freq_ghz"],
        prepared["channel_band"],
        scan.layout,
    )
    vis_median, sigma, flags, tflags = _summarize_band_vis(
        band_vis,
        scan.layout,
        tflags=None,
        times_jd=np.asarray(prepared["raw"]["time"], dtype=np.float64),
        time_flag_groups=groups,
    )
    scan.corrected_channel_vis = corrected_channel_vis
    scan.corrected_band_vis = vis_median
    scan.sigma = sigma
    scan.flags = flags
    scan.fghz_band = fghz_band
    scan.bands_band = used_band_ids
    scan.band_to_full_index = _band_index_map(used_band_ids)
    scan.tflags = tflags
    windows_are_full = all(
        scan.delay_solution.uses_full_window(ant, pol)
        for ant in range(scan.layout.nsolant)
        for pol in range(2)
    )
    scan.dirty_inband = (
        not np.allclose(
            scan.delay_solution.active_ns,
            scan.delay_solution.fitted_ns,
            equal_nan=True,
        )
        or not windows_are_full
    )
    if scan.raw:
        scan.raw.pop("legacy_refcal_summary", None)
        scan.raw.pop("overview_payload_cache", None)
        scan.raw.pop("residual_diagnostics_cache", None)
        scan.raw.pop("combined_channel_vis_cache", None)


def refresh_phacal_solution(scan: ScanAnalysis, refcal: ScanAnalysis) -> None:
    """Recompute one phacal summary and delay solve after time-flag edits.

    :param scan: Phase calibration analysis to refresh.
    :type scan: ScanAnalysis
    :param refcal: Active reference calibration used for the phacal solve.
    :type refcal: ScanAnalysis
    """

    if scan.scan_kind != "phacal":
        raise CalWidgetV2Error("refresh_phacal_solution expects a phacal scan.")
    if not scan.raw or "raw" not in scan.raw:
        raise CalWidgetV2Error("Selected phacal has no raw data available for time-flag recompute.")
    groups = ensure_time_flag_groups(scan)
    band_vis, fghz_band, used_band_ids = _band_average_channel_vis(
        scan.corrected_channel_vis,
        scan.raw["channel_freq_ghz"],
        scan.raw["channel_band"],
        scan.layout,
    )
    vis_median, sigma, flags, tflags = _summarize_band_vis(
        band_vis,
        scan.layout,
        tflags=None,
        times_jd=np.asarray(scan.raw["raw"]["time"], dtype=np.float64),
        time_flag_groups=groups,
    )
    scan.corrected_band_vis = vis_median
    scan.sigma = sigma
    scan.flags = flags
    scan.fghz_band = fghz_band
    scan.bands_band = used_band_ids
    scan.band_to_full_index = _band_index_map(used_band_ids)
    scan.tflags = tflags
    _phase_diff(scan, refcal)
    if scan.raw:
        scan.raw.pop("overview_payload_cache", None)
        scan.raw.pop("residual_diagnostics_cache", None)
        scan.raw.pop("combined_channel_vis_cache", None)


def _phase_diff(phacal: ScanAnalysis, refcal: ScanAnalysis) -> None:
    """Legacy multi-band delay solve with v2-corrected band-averaged inputs."""

    from scipy.optimize import curve_fit

    def mbdfunc0(freq_ghz: np.ndarray, mbd: float) -> np.ndarray:
        return 2.0 * np.pi * freq_ghz * mbd

    def coarse_delay(freq_ghz: np.ndarray, phase_rad: np.ndarray) -> float:
        pfit = lin_phase_fit(freq_ghz, phase_rad)
        return pfit[1] / (2.0 * np.pi)

    if phacal.fghz_band.size != refcal.fghz_band.size:
        raise CalWidgetV2Error("Phasecal and refcal have different band definitions.")
    dpha = np.angle(phacal.corrected_band_vis[:, :2]) - np.angle(refcal.corrected_band_vis[:, :2])
    flags = np.logical_or(phacal.flags[:, :2], refcal.flags[:, :2]).astype(np.int32)
    amp_pc = np.abs(phacal.corrected_band_vis[:, :2])
    amp_rc = np.abs(refcal.corrected_band_vis[:, :2])
    sigma = np.sqrt(
        np.square(np.divide(phacal.sigma[:, :2], amp_pc, out=np.zeros_like(phacal.sigma[:, :2]), where=amp_pc != 0))
        + np.square(np.divide(refcal.sigma[:, :2], amp_rc, out=np.zeros_like(refcal.sigma[:, :2]), where=amp_rc != 0))
    )
    slopes = np.zeros((phacal.layout.nant, 2), np.float64)
    offsets = np.zeros((phacal.layout.nant, 2), np.float64)
    flag = np.ones((phacal.layout.nant, 2), np.float64)
    for ant in range(phacal.layout.nsolant):
        for pol in range(2):
            good, = np.where(flags[ant, pol] == 0)
            if good.size <= 3:
                continue
            x = phacal.fghz_band[good]
            t0 = coarse_delay(x, dpha[ant, pol, good])
            y = np.unwrap(lobe(dpha[ant, pol, good] - 2.0 * np.pi * t0 * x))
            fit, _pcov = curve_fit(
                mbdfunc0,
                x,
                y,
                p0=[0.0],
                sigma=sigma[ant, pol, good],
                absolute_sigma=False,
            )
            slopes[ant, pol] = fit[0] + t0
            flag[ant, pol] = 0.0
    phacal.mbd = slopes
    phacal.mbd_flag = flag
    phacal.offsets = offsets
    phacal.pdiff = dpha
    phacal.flags[:, :2] = flags


def analyze_phacal_input(
    file_or_npz: str,
    refcal: ScanAnalysis,
    scan_id: int = -1,
    navg: int = 3,
    quackint: float = 120.0,
    fix_drift: bool = True,
) -> ScanAnalysis:
    """Analyze one raw scan as a v2 phacal product against a refcal."""

    if refcal.delay_solution is None:
        raise CalWidgetV2Error("Reference calibration is missing in-band delay metadata.")
    prepared = prepare_channel_dataset(
        file_or_npz=file_or_npz,
        scan_id=scan_id,
        scan_kind="phacal",
        navg=navg,
        quackint=quackint,
        fix_drift=fix_drift,
    )
    corrected_channel_vis = _apply_uniform_delay(
        prepared["channel_vis"],
        prepared["channel_freq_ghz"],
        prepared["channel_band"],
        refcal.delay_solution.active_ns,
        prepared["layout"],
    )
    band_vis, fghz_band, used_band_ids = _band_average_channel_vis(
        corrected_channel_vis,
        prepared["channel_freq_ghz"],
        prepared["channel_band"],
        prepared["layout"],
    )
    vis_median, sigma, flags, tflags = _summarize_band_vis(
        band_vis,
        prepared["layout"],
        tflags=None,
        times_jd=np.asarray(prepared["raw"]["time"], dtype=np.float64),
    )
    phacal = ScanAnalysis(
        scan_id=scan_id,
        scan_kind="phacal",
        file=prepared["file"],
        source=prepared["source"],
        timestamp=prepared["timestamp"],
        t_bg=prepared["t_bg"],
        t_ed=prepared["t_ed"],
        duration_min=prepared["duration_min"],
        layout=prepared["layout"],
        raw=prepared,
        corrected_channel_vis=corrected_channel_vis,
        corrected_band_vis=vis_median,
        sigma=sigma,
        flags=flags,
        fghz_band=fghz_band,
        bands_band=used_band_ids,
        band_to_full_index=_band_index_map(used_band_ids),
        delay_solution=refcal.delay_solution,
        tflags=tflags,
        applied_ref_id=refcal.scan_id,
    )
    _phase_diff(phacal, refcal)
    return phacal


def combine_refcals(refcal_a: ScanAnalysis, refcal_b: ScanAnalysis) -> ScanAnalysis:
    """Legacy LO/HI refcal combine path using already-corrected band products."""

    if refcal_a.layout.mjd != refcal_b.layout.mjd:
        raise CalWidgetV2Error("LO/HI refcals come from incompatible array layouts.")
    result = deepcopy(refcal_b)
    layout = result.layout
    nflagged_a = int(np.sum(np.asarray(refcal_a.flags[: layout.nsolant, :2]).astype(int)))
    nflagged_b = int(np.sum(np.asarray(refcal_b.flags[: layout.nsolant, :2]).astype(int)))
    lo = None
    hi = None
    if nflagged_a > 80 * layout.nsolant:
        lo = refcal_a
    else:
        hi = refcal_a
    if nflagged_b > 80 * layout.nsolant:
        lo = refcal_b
    else:
        hi = refcal_b
    if lo is None or hi is None:
        raise CalWidgetV2Error("Selected scans do not form a LO/HI refcal pair.")
    fghz = lo.fghz_band
    plo = np.angle(lo.corrected_band_vis)
    phi = np.angle(hi.corrected_band_vis)
    lobands, = np.where(fghz < 3.0)
    overlap, = np.where(np.logical_and(fghz > 3.0, fghz < 6.0))
    merged = deepcopy(hi)
    merged.corrected_band_vis[:, :, lobands] = lo.corrected_band_vis[:, :, lobands]
    merged.flags[:, :, lobands] = lo.flags[:, :, lobands]
    merged.sigma[:, :, lobands] = lo.sigma[:, :, lobands]
    for ant in range(layout.nsolant - 1):
        for pol in range(2):
            pcal = lobe(plo[0, pol, overlap] - phi[0, pol, overlap])
            ph = lobe(plo[ant + 1, pol, overlap] - phi[ant + 1, pol, overlap] - pcal)
            pfit = lin_phase_fit(fghz[overlap], ph)
            if pfit[2] >= 0.7:
                continue
            coeffs = pfit[[1, 0]]
            pcor = np.polyval(coeffs, fghz[lobands])
            merged.corrected_band_vis[ant + 1, pol, lobands] = lo.corrected_band_vis[ant + 1, pol, lobands] * (
                np.cos(pcor) - 1j * np.sin(pcor)
            )
            for ibd in overlap:
                pcor_single = np.polyval(coeffs, fghz[ibd])
                if merged.flags[ant + 1, pol, ibd]:
                    merged.corrected_band_vis[ant + 1, pol, ibd] = lo.corrected_band_vis[ant + 1, pol, ibd] * (
                        np.cos(pcor_single) - 1j * np.sin(pcor_single)
                    )
    if hi.delay_solution is not None:
        merged.delay_solution = deepcopy(hi.delay_solution)
    return merged


def sidecar_path_for_scan(scan: ScanAnalysis) -> Path:
    """Return the standard sidecar path for a refcal."""

    source = scan.source.replace("/", "_").replace(" ", "_")
    return SIDECAR_DIR / "{0}_{1}_inband_v2.npz".format(scan.timestamp.iso[:19].replace("-", "").replace(":", "").replace(" ", ""), source)


def write_sidecar(scan: ScanAnalysis) -> str:
    """Persist fitted and active in-band delays for later reuse."""

    if scan.delay_solution is None:
        raise CalWidgetV2Error("Only refcals with an in-band solution can write a sidecar.")
    path = sidecar_path_for_scan(scan)
    path.parent.mkdir(parents=True, exist_ok=True)
    active_band_start = np.zeros((scan.layout.nsolant, 2), dtype=np.int32)
    active_band_end = np.zeros((scan.layout.nsolant, 2), dtype=np.int32)
    for ant in range(scan.layout.nsolant):
        for pol in range(2):
            start_band, end_band = scan.delay_solution.band_window(ant, pol)
            active_band_start[ant, pol] = int(start_band)
            active_band_end[ant, pol] = int(end_band)
    payload = {
        "file": scan.file,
        "source": scan.source,
        "timestamp_iso": scan.timestamp.iso[:19],
        "timestamp_lv": int(scan.timestamp.lv),
        "t_bg_lv": int(scan.t_bg.lv),
        "t_ed_lv": int(scan.t_ed.lv),
        "fitted_ns": scan.delay_solution.fitted_ns,
        "active_ns": scan.delay_solution.active_ns,
        "relative_ns": scan.delay_solution.relative_ns,
        "fitted_std_ns": scan.delay_solution.fitted_std_ns,
        "flag": scan.delay_solution.flag,
        "per_band_delay_ns": scan.delay_solution.per_band_delay_ns,
        "per_band_std": scan.delay_solution.per_band_std,
        "per_band_phase0": scan.delay_solution.per_band_phase0,
        "band_values": scan.delay_solution.band_values,
        "band_centers_ghz": scan.delay_solution.band_centers_ghz,
        "kept_band_mask": scan.delay_solution.kept_band_mask.astype(np.uint8),
        "active_band_start": active_band_start,
        "active_band_end": active_band_end,
        "fghz_band": scan.fghz_band,
        "bands_band": scan.bands_band,
    }
    np.savez_compressed(str(path), payload=payload)
    scan.sidecar_path = str(path)
    return str(path)


def load_sidecar(path: str) -> Dict[str, Any]:
    """Read a v2 sidecar."""

    data = np.load(path, allow_pickle=True)
    return data["payload"].item()


def find_sidecar_by_timestamp(timestamp: Time, root: Optional[Path] = None) -> Optional[str]:
    """Locate a sidecar by SQL/refcal timestamp prefix."""

    root = root or SIDECAR_DIR
    if not root.exists():
        return None
    prefix = timestamp.iso[:19].replace("-", "").replace(":", "").replace(" ", "")
    matches = sorted(root.glob(prefix + "*_inband_v2.npz"))
    if not matches:
        return None
    return str(matches[0])


def attach_sidecar_delay(scan: ScanAnalysis, sidecar: Dict[str, Any]) -> None:
    """Attach saved delay metadata to a scan."""

    band_values = np.asarray(sidecar["band_values"], dtype=np.int32)
    nsolant = int(np.asarray(sidecar["fitted_ns"], dtype=np.float64).shape[0])
    full_start = int(band_values[0]) if band_values.size else 0
    full_end = int(band_values[-1]) if band_values.size else 0
    kept_band_mask = sidecar.get("kept_band_mask")
    if kept_band_mask is None:
        starts = np.asarray(
            sidecar.get("active_band_start", np.full((nsolant, 2), full_start, dtype=np.int32)),
            dtype=np.int32,
        )
        ends = np.asarray(
            sidecar.get("active_band_end", np.full((nsolant, 2), full_end, dtype=np.int32)),
            dtype=np.int32,
        )
        kept_band_mask = np.zeros((nsolant, 2, band_values.size), dtype=bool)
        for ant in range(nsolant):
            for pol in range(2):
                lo = int(min(starts[ant, pol], ends[ant, pol]))
                hi = int(max(starts[ant, pol], ends[ant, pol]))
                kept_band_mask[ant, pol] = np.logical_and(band_values >= lo, band_values <= hi)
    kept_band_mask = np.asarray(kept_band_mask, dtype=bool)
    if kept_band_mask.shape != (nsolant, 2, band_values.size):
        kept_band_mask = np.ones((nsolant, 2, band_values.size), dtype=bool)
    delay_solution = DelaySolution(
        fitted_ns=np.asarray(sidecar["fitted_ns"], dtype=np.float64),
        active_ns=np.asarray(sidecar["active_ns"], dtype=np.float64),
        relative_ns=np.asarray(sidecar.get("relative_ns", np.zeros((nsolant, 2), dtype=np.float64)), dtype=np.float64),
        relative_auto_ns=np.zeros((nsolant, 2), dtype=np.float64),
        relative_suggested_ns=np.zeros((nsolant, 2), dtype=np.float64),
        relative_prev_ns=np.zeros((nsolant, 2), dtype=np.float64),
        relative_prev_valid=np.zeros(nsolant, dtype=bool),
        fitted_std_ns=np.asarray(sidecar["fitted_std_ns"], dtype=np.float64),
        flag=np.asarray(sidecar["flag"], dtype=np.float64),
        per_band_delay_ns=np.asarray(sidecar["per_band_delay_ns"], dtype=np.float64),
        per_band_std=np.asarray(sidecar["per_band_std"], dtype=np.float64),
        per_band_phase0=np.asarray(sidecar["per_band_phase0"], dtype=np.float64),
        band_values=band_values,
        band_centers_ghz=np.asarray(sidecar["band_centers_ghz"], dtype=np.float64),
        kept_band_mask=kept_band_mask,
    )
    scan.delay_solution = delay_solution
    scan.sidecar_path = scan.sidecar_path or ""


def sql2refcalX(trange: Time, *args: Any, **kwargs: Any) -> Any:
    """Load legacy SQL refcal products."""

    xml, bufs = ch.read_calX(8, t=trange, *args, **kwargs)
    if isinstance(bufs, np.ndarray):
        out = []
        for buf in bufs:
            ref = extract(buf, xml["Refcal_Real"]) + extract(buf, xml["Refcal_Imag"]) * 1j
            out.append(
                {
                    "pha": np.angle(ref),
                    "amp": np.abs(ref),
                    "flag": extract(buf, xml["Refcal_Flag"]),
                    "fghz": extract(buf, xml["Fghz"]),
                    "sigma": extract(buf, xml["Refcal_Sigma"]),
                    "timestamp": Time(extract(buf, xml["Timestamp"]), format="lv"),
                    "t_bg": Time(extract(buf, xml["T_beg"]), format="lv"),
                    "t_ed": Time(extract(buf, xml["T_end"]), format="lv"),
                }
            )
        return out
    if isinstance(bufs, bytes):
        refcal = extract(bufs, xml["Refcal_Real"]) + extract(bufs, xml["Refcal_Imag"]) * 1j
        return {
            "pha": np.angle(refcal),
            "amp": np.abs(refcal),
            "flag": extract(bufs, xml["Refcal_Flag"]),
            "fghz": extract(bufs, xml["Fghz"]),
            "sigma": extract(bufs, xml["Refcal_Sigma"]),
            "timestamp": Time(extract(bufs, xml["Timestamp"]), format="lv"),
            "t_bg": Time(extract(bufs, xml["T_beg"]), format="lv"),
            "t_ed": Time(extract(bufs, xml["T_end"]), format="lv"),
        }
    return None


def sql2phacalX(trange: Time, *args: Any, **kwargs: Any) -> Any:
    """Load legacy SQL phacal products."""

    xml, bufs = ch.read_calX(9, t=trange, *args, **kwargs)
    if isinstance(bufs, np.ndarray):
        out = []
        for buf in bufs:
            tmp = extract(buf, xml["MBD"])
            out.append(
                {
                    "pslope": tmp[:, :, 1],
                    "t_pha": Time(extract(buf, xml["Timestamp"]), format="lv"),
                    "flag": extract(buf, xml["Flag"])[:, :, 0],
                    "poff": tmp[:, :, 0],
                    "t_ref": Time(extract(buf, xml["T_refcal"]), format="lv"),
                    "phacal": {
                        "pha": extract(buf, xml["Phacal_Pha"]),
                        "amp": extract(buf, xml["Phacal_Amp"]),
                        "flag": extract(buf, xml["Phacal_Flag"]),
                        "fghz": extract(buf, xml["Fghz"]),
                        "sigma": extract(buf, xml["Phacal_Sigma"]),
                        "timestamp": Time(extract(buf, xml["Timestamp"]), format="lv"),
                        "t_bg": Time(extract(buf, xml["T_beg"]), format="lv"),
                        "t_ed": Time(extract(buf, xml["T_end"]), format="lv"),
                    },
                }
            )
        return out
    if isinstance(bufs, bytes):
        tmp = extract(bufs, xml["MBD"])
        return {
            "pslope": tmp[:, :, 1],
            "t_pha": Time(extract(bufs, xml["Timestamp"]), format="lv"),
            "flag": extract(bufs, xml["Flag"])[:, :, 0],
            "poff": tmp[:, :, 0],
            "t_ref": Time(extract(bufs, xml["T_refcal"]), format="lv"),
            "phacal": {
                "pha": extract(bufs, xml["Phacal_Pha"]),
                "amp": extract(bufs, xml["Phacal_Amp"]),
                "flag": extract(bufs, xml["Phacal_Flag"]),
                "fghz": extract(bufs, xml["Fghz"]),
                "sigma": extract(bufs, xml["Phacal_Sigma"]),
                "timestamp": Time(extract(bufs, xml["Timestamp"]), format="lv"),
                "t_bg": Time(extract(bufs, xml["T_beg"]), format="lv"),
                "t_ed": Time(extract(bufs, xml["T_end"]), format="lv"),
            },
        }
    return None


def sql_refcal_to_scan(sql_refcal: Dict[str, Any], scan_id: int = -1) -> ScanAnalysis:
    """Convert a legacy SQL refcal into a ScanAnalysis-like object."""

    layout = _layout_for_mjd(sql_refcal["timestamp"].mjd)
    x = np.full((layout.nant, 4, layout.maxnbd), np.nan + 1j * np.nan, dtype=np.complex128)
    sigma = np.full((layout.nant, 4, layout.maxnbd), np.nan, dtype=np.float64)
    flags = np.ones((layout.nant, 4, layout.maxnbd), dtype=np.int32)
    vis = sql_refcal.get("x")
    if vis is None:
        vis = sql_refcal["amp"] * np.exp(1j * sql_refcal["pha"])
    flag_in = sql_refcal.get("flag", sql_refcal.get("flags"))
    x[:, :2, : vis.shape[-1]] = vis
    sigma[:, :2, : sql_refcal["sigma"].shape[-1]] = sql_refcal["sigma"]
    if flag_in is not None:
        flags[:, :2, : flag_in.shape[-1]] = flag_in
    fghz_band = np.zeros(layout.maxnbd, dtype=np.float64)
    fghz_band[: sql_refcal["fghz"].shape[0]] = sql_refcal["fghz"]
    bands_band = _freq_to_band(fghz_band, sql_refcal["timestamp"].mjd)
    return ScanAnalysis(
        scan_id=scan_id,
        scan_kind="refcal",
        file="",
        source="SQL refcal",
        timestamp=sql_refcal["timestamp"],
        t_bg=sql_refcal["t_bg"],
        t_ed=sql_refcal["t_ed"],
        duration_min=(sql_refcal["t_ed"].mjd - sql_refcal["t_bg"].mjd) * 24.0 * 60.0,
        layout=layout,
        raw={},
        corrected_channel_vis=np.empty((layout.nsolant, 4, 0, 0), dtype=np.complex128),
        corrected_band_vis=x,
        sigma=sigma,
        flags=flags,
        fghz_band=fghz_band,
        bands_band=bands_band,
        band_to_full_index=_band_index_map(bands_band),
    )


def sql_phacal_to_scan(sql_phacal: Dict[str, Any], scan_id: int = -1) -> ScanAnalysis:
    """Convert a legacy SQL phacal into a ScanAnalysis-like object."""

    phacal = sql_phacal.get("phacal", sql_phacal)
    layout = _layout_for_mjd(phacal["timestamp"].mjd)
    x = np.full((layout.nant, 4, layout.maxnbd), np.nan + 1j * np.nan, dtype=np.complex128)
    sigma = np.full((layout.nant, 4, layout.maxnbd), np.nan, dtype=np.float64)
    flags = np.ones((layout.nant, 4, layout.maxnbd), dtype=np.int32)
    vis = phacal.get("x")
    if vis is None:
        vis = phacal["amp"] * np.exp(1j * phacal["pha"])
    flag_in = phacal.get("flag", phacal.get("flags"))
    x[:, :2, : vis.shape[-1]] = vis
    sigma[:, :2, : phacal["sigma"].shape[-1]] = phacal["sigma"]
    if flag_in is not None:
        flags[:, :2, : flag_in.shape[-1]] = flag_in
    fghz_band = np.zeros(layout.maxnbd, dtype=np.float64)
    fghz_band[: phacal["fghz"].shape[0]] = phacal["fghz"]
    bands_band = _freq_to_band(fghz_band, phacal["timestamp"].mjd)
    return ScanAnalysis(
        scan_id=scan_id,
        scan_kind="phacal",
        file="",
        source="SQL phacal",
        timestamp=phacal["timestamp"],
        t_bg=phacal["t_bg"],
        t_ed=phacal["t_ed"],
        duration_min=(phacal["t_ed"].mjd - phacal["t_bg"].mjd) * 24.0 * 60.0,
        layout=layout,
        raw={},
        corrected_channel_vis=np.empty((layout.nsolant, 4, 0, 0), dtype=np.complex128),
        corrected_band_vis=x,
        sigma=sigma,
        flags=flags,
        fghz_band=fghz_band,
        bands_band=bands_band,
        band_to_full_index=_band_index_map(bands_band),
        mbd=sql_phacal.get("pslope", sql_phacal.get("mbd")),
        mbd_flag=sql_phacal.get("flag", sql_phacal.get("mbd_flag")),
        offsets=sql_phacal.get("poff", sql_phacal.get("offsets")),
    )


def wrapped_phase_diff(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Return the wrapped phase difference between two complex products."""

    return np.angle(np.exp(1j * (np.angle(a) - np.angle(b))))


def refcal_comparison_metrics(v2: ScanAnalysis, legacy_sql: Dict[str, Any]) -> Dict[str, Any]:
    """Compute compact comparison metrics for refcal validation."""

    legacy = sql_refcal_to_scan(legacy_sql)
    amp_v2 = np.abs(v2.corrected_band_vis[:, :2])
    amp_legacy = np.abs(legacy.corrected_band_vis[:, :2])
    phase_diff = wrapped_phase_diff(v2.corrected_band_vis[:, :2], legacy.corrected_band_vis[:, :2])
    sigma_diff = v2.sigma[:, :2] - legacy.sigma[:, :2]
    flag_agree = (v2.flags[:, :2] == legacy.flags[:, :2]).astype(float)
    return {
        "amp_mean_abs_diff": np.nanmean(np.abs(amp_v2 - amp_legacy), axis=2),
        "phase_wrapped_rms": np.sqrt(np.nanmean(np.square(phase_diff), axis=2)),
        "sigma_mean_abs_diff": np.nanmean(np.abs(sigma_diff), axis=2),
        "flag_agreement": np.nanmean(flag_agree, axis=2),
        "phase_diff": phase_diff,
        "amp_diff": amp_v2 - amp_legacy,
        "sigma_diff": sigma_diff,
    }


def phacal_comparison_metrics(v2: ScanAnalysis, legacy_sql: Dict[str, Any]) -> Dict[str, Any]:
    """Compute compact comparison metrics for phacal validation."""

    legacy = sql_phacal_to_scan(legacy_sql)
    amp_v2 = np.abs(v2.corrected_band_vis[:, :2])
    amp_legacy = np.abs(legacy.corrected_band_vis[:, :2])
    phase_diff = wrapped_phase_diff(v2.corrected_band_vis[:, :2], legacy.corrected_band_vis[:, :2])
    sigma_diff = v2.sigma[:, :2] - legacy.sigma[:, :2]
    flag_agree = (v2.flags[:, :2] == legacy.flags[:, :2]).astype(float)
    mbd_diff = v2.mbd - legacy_sql["pslope"]
    poff_diff = v2.offsets - legacy_sql["poff"]
    return {
        "amp_mean_abs_diff": np.nanmean(np.abs(amp_v2 - amp_legacy), axis=2),
        "phase_wrapped_rms": np.sqrt(np.nanmean(np.square(phase_diff), axis=2)),
        "sigma_mean_abs_diff": np.nanmean(np.abs(sigma_diff), axis=2),
        "flag_agreement": np.nanmean(flag_agree, axis=2),
        "mbd_diff": mbd_diff,
        "poff_diff": poff_diff,
        "phase_diff": phase_diff,
        "amp_diff": amp_v2 - amp_legacy,
        "sigma_diff": sigma_diff,
    }


def metrics_to_jsonable(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Convert comparison metrics to plain JSON-serializable values."""

    out = {}
    for key, value in metrics.items():
        if isinstance(value, np.ndarray):
            out[key] = np.asarray(value).tolist()
        else:
            out[key] = value
    return out


def save_metrics_json(metrics: Dict[str, Any], path: str) -> None:
    """Write metrics to disk."""

    Path(path).write_text(json.dumps(metrics_to_jsonable(metrics), indent=2, sort_keys=True))
