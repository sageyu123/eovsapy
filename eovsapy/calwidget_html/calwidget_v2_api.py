"""FastAPI application for the browser-based phase calibration widget."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

from eovsapy.util import Time

from .calwidget_v2_analysis import (
    CalWidgetV2Error,
    ScanAnalysis,
    add_time_flag_group,
    analyze_phacal_input,
    analyze_refcal_input,
    attach_sidecar_delay,
    combine_refcals,
    delete_time_flag_group,
    describe_day,
    ensure_time_flag_groups,
    find_sidecar_by_timestamp,
    legacy_time_history_payload,
    load_sidecar,
    refresh_phacal_solution,
    refresh_refcal_solution,
    sidecar_path_for_scan,
    sql2phacalX,
    sql2refcalX,
    sql_phacal_to_scan,
    sql_refcal_to_scan,
    write_sidecar,
)
from .calwidget_v2_plots import (
    TAB_NAMES,
    heatmap_payload,
    heatmap_plot_meta,
    inband_delay_update_payloads,
    relative_delay_update_payloads,
    inband_window_update_payloads,
    overview_payloads,
    relative_delay_editor_meta,
    render_heatmap,
    render_tab,
    tab_payload,
)


STATIC_DIR = Path(__file__).resolve().parent / "calwidget_v2_frontend"


class SessionRequest(BaseModel):
    """Only the session id is needed."""

    session_id: str


class ScanRequest(SessionRequest):
    """Requests that operate on one scan."""

    scan_id: int


class CombineRequest(SessionRequest):
    """Combine two analyzed refcals."""

    scan_ids: List[int]


class SelectionRequest(SessionRequest):
    """Update selected antenna and/or band."""

    antenna: Optional[int] = None
    band: Optional[int] = None


class FixDriftRequest(SessionRequest):
    """Toggle the legacy time-drift correction."""

    fix_drift: bool


class LobeRequest(SessionRequest):
    """Toggle lobe wrapping for the Sum Pha overview."""

    use_lobe: bool


class UpdateInbandRequest(SessionRequest):
    """Update active X/Y in-band delays for one antenna."""

    antenna: int
    x_delay_ns: Optional[float] = None
    y_delay_ns: Optional[float] = None


class UpdateRelativeDelayRequest(SessionRequest):
    """Update display-only X/Y residual delays for one antenna."""

    antenna: int
    x_delay_ns: Optional[float] = None
    y_delay_ns: Optional[float] = None


class RelativeDelayAntennaRequest(SessionRequest):
    """Operate on one antenna's display-only relative-delay state."""

    antenna: int


class ResetInbandRequest(SessionRequest):
    """Reset one antenna or all antennas."""

    antenna: Optional[int] = None


class InbandWindowRequest(SessionRequest):
    """Update the kept-band window used for the active mean in-band delay."""

    start_band: int
    end_band: int
    mode: str
    antenna_scope: str
    polarization_scope: str
    source_polarization: int
    source_antenna: Optional[int] = None


class InbandWindowOperation(BaseModel):
    """One staged kept-band mask edit."""

    start_band: int
    end_band: int
    mode: str
    antenna_scope: str
    polarization_scope: str
    source_polarization: int
    source_antenna: Optional[int] = None


class InbandWindowBatchRequest(SessionRequest):
    """Apply multiple staged kept-band mask edits in one refresh."""

    operations: List[InbandWindowOperation]


class InbandBandRange(BaseModel):
    """One inclusive kept-band range."""

    start_band: int
    end_band: int


class InbandMaskTarget(BaseModel):
    """Final kept-band mask for one antenna/polarization target."""

    antenna: int
    polarization: int
    kept_ranges: List[InbandBandRange]


class InbandMaskBatchRequest(SessionRequest):
    """Apply final staged kept-band masks in one refresh."""

    targets: List[InbandMaskTarget]


class SaveSqlRequest(SessionRequest):
    """Save the selected or provided scan to SQL."""

    scan_id: Optional[int] = None
    timestamp_iso: Optional[str] = None


class TimeFlagAddRequest(SessionRequest):
    """Add one browser-native time-flag interval group."""

    start_jd: float
    end_jd: float
    scope: str


class TimeFlagDeleteRequest(SessionRequest):
    """Delete one browser-native time-flag interval group."""

    group_id: str


@dataclass
class WidgetSession:
    """State for one browser session."""

    session_id: str
    day: Optional[Dict] = None
    entries: List[Dict] = field(default_factory=list)
    analyses: Dict[int, ScanAnalysis] = field(default_factory=dict)
    selected_scan_id: Optional[int] = None
    ref_scan_id: Optional[int] = None
    selected_ant: int = 0
    selected_band: int = 0
    status_message: str = "Ready."
    fix_drift: bool = True
    use_lobe: bool = True

    def load_date(self, date_text: str) -> None:
        """Load the scan list for one day and reset the working set."""

        previous_date = None if self.day is None else self.day.get("date")
        previous_selected = self.selected_scan_id
        previous_ref = self.ref_scan_id
        previous_ant = self.selected_ant
        previous_band = self.selected_band
        previous_analyses = self.analyses
        self.day = describe_day(date_text)
        self.entries = self.day["entries"]
        valid_ids = set(int(entry["scan_id"]) for entry in self.entries)
        if previous_date == date_text:
            self.analyses = dict(
                (scan_id, analysis)
                for scan_id, analysis in previous_analyses.items()
                if int(scan_id) in valid_ids
            )
            self.selected_scan_id = previous_selected if previous_selected in valid_ids else None
            self.ref_scan_id = previous_ref if previous_ref in valid_ids else None
            self.selected_ant = previous_ant
            self.selected_band = previous_band
        else:
            self.analyses = {}
            self.selected_scan_id = None
            self.ref_scan_id = None
            self.selected_ant = 0
            self.selected_band = 0
        if self.entries:
            if self.selected_scan_id is None:
                self.selected_scan_id = self.entries[0]["scan_id"]
            if previous_date == date_text:
                self.status_message = "Reloaded {0:d} scans for {1} and kept current session state.".format(
                    len(self.entries), date_text
                )
            else:
                self.status_message = "Loaded {0:d} scans for {1}.".format(len(self.entries), date_text)
        else:
            self.status_message = self.day["scan_dict"].get("msg", "No scans found.")

    def _entry(self, scan_id: int) -> Dict:
        """Return one scan-list entry."""

        for entry in self.entries:
            if int(entry["scan_id"]) == int(scan_id):
                return entry
        raise CalWidgetV2Error("Unknown scan id {0}".format(scan_id))

    def _current_result(self) -> Optional[ScanAnalysis]:
        """Return the currently selected scan result if available."""

        if self.selected_scan_id is None:
            return None
        return self.get_scan_result(self.selected_scan_id)

    def _serialize_entry(self, entry: Dict) -> Dict:
        """Convert one scan entry to JSON-safe metadata."""

        analysis = self.analyses.get(int(entry["scan_id"]))
        status = entry["status"]
        saved = status in ("refcal", "phacal")
        if analysis is not None:
            status = analysis.scan_kind
            saved = analysis.saved_to_sql
        return {
            "scan_id": int(entry["scan_id"]),
            "scan_time": entry["scan_time"],
            "source": entry["source"],
            "duration_min": entry["duration_min"],
            "file": entry["file"],
            "status": status,
            "sql_time": entry["sql_time"],
            "color": entry["color"],
            "selected": self.selected_scan_id == int(entry["scan_id"]),
            "is_refcal": self.ref_scan_id == int(entry["scan_id"]),
            "saved_to_sql": saved,
            "analyzed": analysis is not None,
        }

    def get_scan_result(self, scan_id: int) -> Optional[ScanAnalysis]:
        """Return the best available representation of one scan."""

        if int(scan_id) in self.analyses:
            return self.analyses[int(scan_id)]
        entry = self._entry(scan_id)
        sql_meta = entry.get("sql_meta")
        if not sql_meta:
            return None
        if sql_meta["kind"] == "refcal":
            result = sql_refcal_to_scan(sql_meta, scan_id=scan_id)
            sidecar_file = find_sidecar_by_timestamp(sql_meta["timestamp"])
            if sidecar_file:
                attach_sidecar_delay(result, load_sidecar(sidecar_file))
                result.sidecar_path = sidecar_file
            return result
        result = sql_phacal_to_scan(sql_meta, scan_id=scan_id)
        return result

    def _ensure_refcal_analysis(self, scan_id: int) -> ScanAnalysis:
        """Ensure that the reference calibration has raw v2 analysis attached."""

        if scan_id in self.analyses and self.analyses[scan_id].scan_kind == "refcal":
            return self.analyses[scan_id]
        entry = self._entry(scan_id)
        result = analyze_refcal_input(entry["file"], scan_id=scan_id, fix_drift=self.fix_drift)
        try:
            result.sidecar_path = write_sidecar(result)
        except Exception:
            result.sidecar_path = str(sidecar_path_for_scan(result))
        self.analyses[scan_id] = result
        entry["status"] = "refcal"
        self.status_message = "Analyzed refcal scan {0}.".format(scan_id)
        return result

    def _invalidate_dependent_phacals(self, ref_scan_id: int) -> None:
        """Drop phacal analyses that depend on the edited refcal."""

        to_drop = []
        for scan_id, analysis in self.analyses.items():
            if analysis.scan_kind == "phacal" and analysis.applied_ref_id == ref_scan_id:
                to_drop.append(scan_id)
        for scan_id in to_drop:
            del self.analyses[scan_id]

    def select_scan(self, scan_id: int) -> None:
        """Select one scan."""

        self.selected_scan_id = int(scan_id)
        result = self.get_scan_result(scan_id)
        if result is not None:
            self.selected_ant = min(self.selected_ant, max(result.layout.nsolant - 1, 0))
            self.selected_band = min(self.selected_band, max(result.layout.maxnbd - 1, 0))
        self.status_message = "Selected scan {0}.".format(scan_id)

    def set_fix_drift(self, fix_drift: bool) -> None:
        """Update the fix-drift setting for future analyses."""

        self.fix_drift = bool(fix_drift)
        self.status_message = "Fix-drift set to {0}.".format(self.fix_drift)

    def set_use_lobe(self, use_lobe: bool) -> None:
        """Update lobe wrapping for Sum Pha display."""

        self.use_lobe = bool(use_lobe)
        self.status_message = "Sum Pha lobe set to {0}.".format(self.use_lobe)

    def analyze_refcal(self, scan_id: int) -> None:
        """Analyze the requested scan as a refcal."""

        result = self._ensure_refcal_analysis(scan_id)
        result.saved_to_sql = False
        self.selected_scan_id = scan_id
        self.status_message = "Analyzed refcal {0}.".format(scan_id)

    def set_refcal(self, scan_id: int) -> None:
        """Mark one scan as the active refcal."""

        result = self._ensure_refcal_analysis(scan_id)
        self.ref_scan_id = scan_id
        self.selected_scan_id = scan_id
        self.selected_ant = min(self.selected_ant, max(result.layout.nsolant - 1, 0))
        self.status_message = "Set scan {0} as active refcal.".format(scan_id)

    def combine_refcal_pair(self, scan_ids: List[int]) -> None:
        """Combine two analyzed refcals into one LO/HI result."""

        if len(scan_ids) != 2:
            raise CalWidgetV2Error("Exactly two scan ids are required for refcal combine.")
        ref_a = self._ensure_refcal_analysis(int(scan_ids[0]))
        ref_b = self._ensure_refcal_analysis(int(scan_ids[1]))
        merged = combine_refcals(ref_a, ref_b)
        for scan_id in scan_ids:
            self.analyses[int(scan_id)] = deepcopy(merged)
            self.analyses[int(scan_id)].scan_id = int(scan_id)
            self.analyses[int(scan_id)].saved_to_sql = False
            self._entry(int(scan_id))["status"] = "refcal"
        self.ref_scan_id = int(scan_ids[-1])
        self.selected_scan_id = int(scan_ids[-1])
        self.status_message = "Combined scans {0} and {1} as one refcal.".format(scan_ids[0], scan_ids[1])

    def analyze_phacal(self, scan_id: int) -> None:
        """Analyze one scan as a phacal using the active refcal."""

        if self.ref_scan_id is None:
            raise CalWidgetV2Error("Select a refcal before analyzing a phacal.")
        refcal = self._ensure_refcal_analysis(self.ref_scan_id)
        entry = self._entry(scan_id)
        result = analyze_phacal_input(entry["file"], refcal, scan_id=scan_id, fix_drift=self.fix_drift)
        result.saved_to_sql = False
        self.analyses[scan_id] = result
        entry["status"] = "phacal"
        self.selected_scan_id = scan_id
        self.status_message = "Analyzed phacal {0} against refcal {1}.".format(scan_id, self.ref_scan_id)

    def select_antenna_band(self, antenna: Optional[int] = None, band: Optional[int] = None) -> None:
        """Update the selected antenna and/or band."""

        result = self._current_result()
        if result is None:
            return
        if antenna is not None:
            self.selected_ant = int(max(0, min(antenna, result.layout.nsolant - 1)))
        if band is not None:
            self.selected_band = int(max(0, min(band, result.layout.maxnbd - 1)))
        self.status_message = "Selected antenna {0:d}, band {1:d}.".format(self.selected_ant + 1, self.selected_band + 1)

    def _selected_editable_scan(self) -> ScanAnalysis:
        """Return the selected analyzed scan that can accept browser edits."""

        if self.selected_scan_id is None or self.selected_scan_id not in self.analyses:
            raise CalWidgetV2Error("Analyze the selected scan before editing browser time flags.")
        scan = self.analyses[self.selected_scan_id]
        if not scan.raw or "raw" not in scan.raw:
            raise CalWidgetV2Error("Selected scan has no raw data available for time-flag editing.")
        ensure_time_flag_groups(scan)
        return scan

    def add_time_flag(self, start_jd: float, end_jd: float, scope: str) -> None:
        """Add one browser-native time-flag interval group and live-recompute."""

        scan = self._selected_editable_scan()
        group = add_time_flag_group(scan, self.selected_ant, self.selected_band, start_jd, end_jd, scope)
        if scan.scan_kind == "refcal":
            refresh_refcal_solution(scan)
            scan.saved_to_sql = False
            self._invalidate_dependent_phacals(scan.scan_id)
        elif scan.scan_kind == "phacal":
            ref_id = scan.applied_ref_id if scan.applied_ref_id is not None else self.ref_scan_id
            if ref_id is None:
                raise CalWidgetV2Error("Active refcal is required to recompute time-flagged phacal data.")
            refcal = self._ensure_refcal_analysis(int(ref_id))
            refresh_phacal_solution(scan, refcal)
            scan.saved_to_sql = False
        else:
            raise CalWidgetV2Error("Time-flag editing is only available for analyzed refcal or phacal scans.")
        self.status_message = "Added {0} time flag {1}-{2} for {3}.".format(
            group.scope,
            Time(group.start_jd, format="jd").iso[11:19],
            Time(group.end_jd, format="jd").iso[11:19],
            scan.scan_kind,
        )

    def delete_time_flag(self, group_id: str) -> None:
        """Delete one browser-native time-flag interval group and live-recompute."""

        scan = self._selected_editable_scan()
        removed = delete_time_flag_group(scan, group_id)
        if not removed:
            raise CalWidgetV2Error("Requested time-flag interval was not found.")
        if scan.scan_kind == "refcal":
            refresh_refcal_solution(scan)
            scan.saved_to_sql = False
            self._invalidate_dependent_phacals(scan.scan_id)
        elif scan.scan_kind == "phacal":
            ref_id = scan.applied_ref_id if scan.applied_ref_id is not None else self.ref_scan_id
            if ref_id is None:
                raise CalWidgetV2Error("Active refcal is required to recompute time-flagged phacal data.")
            refcal = self._ensure_refcal_analysis(int(ref_id))
            refresh_phacal_solution(scan, refcal)
            scan.saved_to_sql = False
        else:
            raise CalWidgetV2Error("Time-flag editing is only available for analyzed refcal or phacal scans.")
        self.status_message = "Deleted one time-flag interval from {0}.".format(scan.scan_kind)

    def _editable_inband_refcal(self) -> tuple[int, ScanAnalysis]:
        """Return the refcal analysis that should receive in-band edits.

        If no active refcal has been explicitly set yet, allow the currently
        selected analyzed refcal to be edited directly so browser inband-range
        selection works immediately after `Analyze Refcal`.
        """

        if self.ref_scan_id is not None:
            ref_id = int(self.ref_scan_id)
            refcal = self._ensure_refcal_analysis(ref_id)
            if refcal.delay_solution is None:
                raise CalWidgetV2Error("The active refcal has no in-band solution.")
            return ref_id, refcal
        if self.selected_scan_id is not None and self.selected_scan_id in self.analyses:
            scan = self.analyses[int(self.selected_scan_id)]
            if scan.scan_kind == "refcal" and scan.delay_solution is not None:
                self.ref_scan_id = int(scan.scan_id)
                return int(scan.scan_id), scan
        raise CalWidgetV2Error("No active refcal is selected.")

    def update_inband(self, antenna: int, x_delay_ns: Optional[float], y_delay_ns: Optional[float]) -> None:
        """Apply a manual in-band delay edit to the active refcal."""

        ref_id, refcal = self._editable_inband_refcal()
        ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
        if x_delay_ns is not None:
            refcal.delay_solution.active_ns[ant, 0] = float(x_delay_ns)
        if y_delay_ns is not None:
            refcal.delay_solution.active_ns[ant, 1] = float(y_delay_ns)
        refresh_refcal_solution(refcal)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.selected_ant = ant
        self.status_message = "Updated active in-band delays for antenna {0:d}.".format(ant + 1)

    def update_relative_delay(self, antenna: int, x_delay_ns: Optional[float], y_delay_ns: Optional[float]) -> None:
        """Apply one display-only residual delay edit to the active refcal.

        :param antenna: Zero-based antenna index.
        :type antenna: int
        :param x_delay_ns: Residual X delay override in ns.
        :type x_delay_ns: float | None
        :param y_delay_ns: Residual Y delay override in ns.
        :type y_delay_ns: float | None
        """

        _ref_id, refcal = self._editable_inband_refcal()
        ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
        refcal.delay_solution.snapshot_relative_ant(ant)
        if x_delay_ns is not None:
            refcal.delay_solution.relative_ns[ant, 0] = float(x_delay_ns)
        if y_delay_ns is not None:
            refcal.delay_solution.relative_ns[ant, 1] = float(y_delay_ns)
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        self.selected_ant = ant
        self.status_message = "Updated relative-phase residual delays for antenna {0:d}.".format(ant + 1)

    def apply_relative_delay_suggestion(self, antenna: int) -> None:
        """Apply one antenna's current residual-guided relative-delay suggestion.

        :param antenna: Zero-based antenna index.
        :type antenna: int
        """

        _ref_id, refcal = self._editable_inband_refcal()
        ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
        meta = relative_delay_editor_meta(refcal, ant)
        x_suggest = float(meta.get("x_suggested_relative_delay_ns", 0.0) or 0.0)
        y_suggest = float(meta.get("y_suggested_relative_delay_ns", 0.0) or 0.0)
        refcal.delay_solution.snapshot_relative_ant(ant)
        refcal.delay_solution.relative_ns[ant, 0] = float(refcal.delay_solution.relative_ns[ant, 0] + x_suggest)
        refcal.delay_solution.relative_ns[ant, 1] = float(refcal.delay_solution.relative_ns[ant, 1] + y_suggest)
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        self.selected_ant = ant
        self.status_message = "Applied residual-guided relative-phase suggestion for antenna {0:d}.".format(ant + 1)

    def undo_relative_delay(self, antenna: int) -> None:
        """Undo the last applied relative-delay edit for one antenna.

        :param antenna: Zero-based antenna index.
        :type antenna: int
        """

        _ref_id, refcal = self._editable_inband_refcal()
        ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
        if not refcal.delay_solution.undo_relative_ant(ant):
            raise CalWidgetV2Error("No relative-phase residual edit is available to undo for this antenna.")
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        self.selected_ant = ant
        self.status_message = "Undid the last relative-phase residual edit for antenna {0:d}.".format(ant + 1)

    def update_inband_window(
        self,
        start_band: int,
        end_band: int,
        mode: str,
        antenna_scope: str,
        polarization_scope: str,
        source_polarization: int,
        source_antenna: Optional[int] = None,
    ) -> None:
        """Update the kept-band mask for the active refcal and refresh products."""

        self.update_inband_window_batch(
            [
                InbandWindowOperation(
                    start_band=start_band,
                    end_band=end_band,
                    mode=mode,
                    antenna_scope=antenna_scope,
                    polarization_scope=polarization_scope,
                    source_polarization=source_polarization,
                    source_antenna=source_antenna,
                )
            ]
        )

    def update_inband_window_batch(self, operations: List[InbandWindowOperation]) -> None:
        """Apply staged kept-band mask edits, then refresh once.

        :param operations: Ordered staged edits to apply.
        :type operations: list[InbandWindowOperation]
        """

        if not operations:
            return
        ref_id, refcal = self._editable_inband_refcal()
        last_ant = self.selected_ant
        last_start = 0
        last_end = 0
        last_mode = "replace"
        last_polarization_scope = "selected"
        last_ant_count = 0
        for operation in operations:
            start_band = int(operation.start_band)
            end_band = int(operation.end_band)
            antenna_scope = str(operation.antenna_scope)
            polarization_scope = str(operation.polarization_scope)
            source_polarization = int(operation.source_polarization)
            source_antenna = operation.source_antenna
            ant0 = self.selected_ant if source_antenna is None else int(source_antenna)
            ant0 = int(max(0, min(ant0, refcal.layout.nsolant - 1)))
            pol0 = int(max(0, min(source_polarization, 1)))
            if antenna_scope == "all":
                ant_indices = list(range(refcal.layout.nsolant))
            elif antenna_scope == "selected":
                ant_indices = [ant0]
            else:
                raise CalWidgetV2Error("Unknown in-band antenna scope {0}.".format(antenna_scope))
            if polarization_scope == "all":
                pol_indices = [0, 1]
            elif polarization_scope == "selected":
                pol_indices = [pol0]
            else:
                raise CalWidgetV2Error("Unknown in-band polarization scope {0}.".format(polarization_scope))
            refcal.delay_solution.update_kept_band_mask(ant_indices, pol_indices, start_band, end_band, mode=str(operation.mode))
            last_ant = ant0
            last_start = min(start_band, end_band)
            last_end = max(start_band, end_band)
            last_mode = str(operation.mode)
            last_polarization_scope = polarization_scope
            last_ant_count = len(ant_indices)
        refresh_refcal_solution(refcal)
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.selected_ant = last_ant
        if len(operations) == 1:
            action = "Replaced" if last_mode == "replace" else "Excluded"
            self.status_message = "{0} kept in-band bands {1:d}-{2:d} for {3} antenna(s), {4} polarization scope.".format(
                action,
                last_start,
                last_end,
                last_ant_count,
                last_polarization_scope,
            )
        else:
            self.status_message = "Applied {0:d} staged in-band mask edits.".format(len(operations))

    def apply_inband_mask_targets(self, targets: List[InbandMaskTarget]) -> None:
        """Apply final kept-band masks for selected antenna/polarization targets.

        :param targets: Final masks keyed by antenna/polarization.
        :type targets: list[InbandMaskTarget]
        """

        if not targets:
            return
        ref_id, refcal = self._editable_inband_refcal()
        last_ant = self.selected_ant
        for target in targets:
            ant = int(max(0, min(int(target.antenna), refcal.layout.nsolant - 1)))
            pol = int(max(0, min(int(target.polarization), 1)))
            ranges = [(int(item.start_band), int(item.end_band)) for item in target.kept_ranges]
            refcal.delay_solution.set_kept_band_ranges(ant, pol, ranges)
            last_ant = ant
        refresh_refcal_solution(refcal)
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.selected_ant = last_ant
        self.status_message = "Applied {0:d} staged in-band masks.".format(len(targets))

    def reset_inband(self, antenna: Optional[int] = None) -> None:
        """Reset active in-band delays."""

        ref_id, refcal = self._editable_inband_refcal()
        if antenna is None:
            refcal.delay_solution.reset_all()
            message = "Reset all active in-band delays."
        else:
            ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
            refcal.delay_solution.reset_ant(ant)
            message = "Reset antenna {0:d} in-band delays.".format(ant + 1)
        refresh_refcal_solution(refcal)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.status_message = message

    def reset_relative_delay(self, antenna: Optional[int] = None) -> None:
        """Reset display-only residual delays.

        :param antenna: Optional zero-based antenna index.
        :type antenna: int | None
        """

        _ref_id, refcal = self._editable_inband_refcal()
        if antenna is None:
            for ant in range(refcal.layout.nsolant):
                refcal.delay_solution.snapshot_relative_ant(ant)
            refcal.delay_solution.reset_relative_all()
            message = "Reset all relative-phase residual delays."
        else:
            ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
            refcal.delay_solution.snapshot_relative_ant(ant)
            refcal.delay_solution.reset_relative_ant(ant)
            self.selected_ant = ant
            message = "Reset antenna {0:d} relative-phase residual delays.".format(ant + 1)
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        self.status_message = message

    def save_sql(self, scan_id: Optional[int], timestamp_iso: Optional[str] = None) -> None:
        """Save one analyzed scan to the legacy SQL schema."""

        target_id = self.selected_scan_id if scan_id is None else int(scan_id)
        if target_id is None or target_id not in self.analyses:
            raise CalWidgetV2Error("Analyze a scan before saving it to SQL.")
        result = self.analyses[target_id]
        timestamp = Time(timestamp_iso) if timestamp_iso else None
        if result.scan_kind == "refcal":
            from eovsapy import cal_header as ch

            ch.refcal2sql(result.to_refcal_sql(timestamp=timestamp), timestamp=timestamp)
        elif result.scan_kind == "phacal":
            from eovsapy import cal_header as ch

            payload = result.to_phacal_sql()
            if self.ref_scan_id is not None:
                payload["t_ref"] = self._ensure_refcal_analysis(self.ref_scan_id).timestamp
            ch.phacal2sql(payload, timestamp=timestamp)
        else:
            raise CalWidgetV2Error("Only refcal or phacal products can be saved.")
        result.saved_to_sql = True
        self.status_message = "Saved scan {0} to SQL.".format(target_id)

    def state(self) -> Dict:
        """Serialize session state for the frontend."""

        current = self._current_result()
        current_entry = None
        if self.selected_scan_id is not None:
            try:
                current_entry = self._entry(self.selected_scan_id)
            except CalWidgetV2Error:
                current_entry = None
        current_layout = None
        current_meta = None
        if current is not None:
            current_layout = {
                "nsolant": current.layout.nsolant,
                "nant": current.layout.nant,
                "maxnbd": current.layout.maxnbd,
            }
            current_meta = {
                "scan_id": current.scan_id,
                "kind": current.scan_kind,
                "timestamp_iso": current.timestamp.iso[:19],
                "scan_time": current_entry["scan_time"] if current_entry else current.t_bg.iso[11:19],
                "source": current.source,
                "saved_to_sql": current.saved_to_sql,
                "sidecar_path": current.sidecar_path,
            }
        ref_meta = None
        ref_scan_id = self.ref_scan_id
        refcal = None
        if ref_scan_id is not None:
            refcal = self.get_scan_result(ref_scan_id)
        elif self.selected_scan_id is not None and self.selected_scan_id in self.analyses:
            candidate = self.analyses[int(self.selected_scan_id)]
            if candidate.scan_kind == "refcal" and candidate.delay_solution is not None:
                ref_scan_id = int(candidate.scan_id)
                refcal = candidate
        if ref_scan_id is not None and refcal is not None:
            editor_meta = relative_delay_editor_meta(refcal, self.selected_ant)
            ref_entry = None
            try:
                ref_entry = self._entry(ref_scan_id)
            except CalWidgetV2Error:
                ref_entry = None
            ref_meta = {
                "scan_id": ref_scan_id,
                "timestamp_iso": refcal.timestamp.iso[:19],
                "scan_time": ref_entry["scan_time"] if ref_entry else refcal.t_bg.iso[11:19],
                "source": refcal.source,
                "dirty_inband": refcal.dirty_inband,
                "sidecar_path": refcal.sidecar_path,
                "x_delay_ns": None if refcal.delay_solution is None else float(refcal.delay_solution.active_ns[self.selected_ant, 0]),
                "y_delay_ns": None if refcal.delay_solution is None else float(refcal.delay_solution.active_ns[self.selected_ant, 1]),
                "x_relative_delay_ns": None if refcal.delay_solution is None else float(refcal.delay_solution.relative_ns[self.selected_ant, 0]),
                "y_relative_delay_ns": None if refcal.delay_solution is None else float(refcal.delay_solution.relative_ns[self.selected_ant, 1]),
                "x_window": None if refcal.delay_solution is None else list(refcal.delay_solution.band_window(self.selected_ant, 0)),
                "y_window": None if refcal.delay_solution is None else list(refcal.delay_solution.band_window(self.selected_ant, 1)),
                **editor_meta,
            }
        return {
            "session_id": self.session_id,
            "date": None if self.day is None else self.day["date"],
            "status_message": self.status_message,
            "fix_drift": self.fix_drift,
            "use_lobe": self.use_lobe,
            "selected_scan_id": self.selected_scan_id,
            "ref_scan_id": self.ref_scan_id,
            "selected_ant": self.selected_ant,
            "selected_band": self.selected_band,
            "tabs": list(TAB_NAMES),
            "current_layout": current_layout,
            "heatmap_meta": heatmap_plot_meta(current),
            "current_scan": current_meta,
            "active_refcal": ref_meta,
            "scans": [self._serialize_entry(entry) for entry in self.entries],
        }


SESSIONS: Dict[str, WidgetSession] = {}


def _get_session(session_id: str) -> WidgetSession:
    """Fetch a live session or fail with 404."""

    if session_id not in SESSIONS:
        raise HTTPException(status_code=404, detail="Unknown session id.")
    return SESSIONS[session_id]


def _current_scan_label(session: WidgetSession) -> Optional[str]:
    """Return the selected scan label used by the legacy widget."""

    if session.selected_scan_id is None:
        return None
    try:
        return session._entry(session.selected_scan_id)["scan_time"]
    except CalWidgetV2Error:
        return None


def _resolve_tab_context(session: WidgetSession, tab: str) -> tuple[Optional[ScanAnalysis], Optional[ScanAnalysis]]:
    """Resolve scan inputs for one tab, promoting SQL refcals only when required."""

    scan = session._current_result()
    if scan is not None and tab in ("inband_fit", "inband_applied") and scan.scan_kind == "refcal" and not scan.raw:
        try:
            scan = session._ensure_refcal_analysis(session.selected_scan_id)
        except CalWidgetV2Error:
            scan = session._current_result()
    refcal = None
    if session.ref_scan_id is not None and tab == "inband_applied":
        try:
            refcal = session._ensure_refcal_analysis(session.ref_scan_id)
        except CalWidgetV2Error:
            refcal = None
    return scan, refcal


def _overview_context(session: WidgetSession) -> tuple[Optional[ScanAnalysis], Optional[ScanAnalysis]]:
    """Resolve the current scan/refcal pair used by always-visible overview panels."""

    scan = session._current_result()
    refcal = None
    if scan is not None and scan.scan_kind == "phacal":
        ref_id = scan.applied_ref_id if scan.applied_ref_id is not None else session.ref_scan_id
        if ref_id is not None:
            refcal = session._ensure_refcal_analysis(int(ref_id))
    return scan, refcal


def build_app() -> FastAPI:
    """Create the FastAPI app."""

    app = FastAPI(title="calwidget_v2", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    def index() -> FileResponse:
        """Serve the browser app."""

        index_file = STATIC_DIR / "index.html"
        if not index_file.exists():
            raise HTTPException(status_code=500, detail="Frontend assets are missing.")
        return FileResponse(str(index_file))

    @app.post("/api/session")
    def create_session() -> Dict:
        """Create a new browser session."""

        session_id = uuid4().hex
        SESSIONS[session_id] = WidgetSession(session_id=session_id)
        return {"session_id": session_id}

    @app.get("/api/state")
    def get_state(session_id: str) -> Dict:
        """Return the current frontend state."""

        return _get_session(session_id).state()

    @app.get("/api/scans")
    def load_scans(session_id: str, date: str) -> Dict:
        """Load one observing day."""

        session = _get_session(session_id)
        try:
            session.load_date(date)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/select-scan")
    def select_scan(payload: ScanRequest) -> Dict:
        """Select a scan row."""

        session = _get_session(payload.session_id)
        try:
            session.select_scan(payload.scan_id)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/settings/fix-drift")
    def set_fix_drift(payload: FixDriftRequest) -> Dict:
        """Toggle the legacy drift correction."""

        session = _get_session(payload.session_id)
        session.set_fix_drift(payload.fix_drift)
        return session.state()

    @app.post("/api/settings/lobe")
    def set_use_lobe(payload: LobeRequest) -> Dict:
        """Toggle lobe wrapping for Sum Pha."""

        session = _get_session(payload.session_id)
        session.set_use_lobe(payload.use_lobe)
        return session.state()

    @app.post("/api/refcal/analyze")
    def analyze_refcal(payload: ScanRequest) -> Dict:
        """Analyze one scan as refcal."""

        session = _get_session(payload.session_id)
        try:
            session.analyze_refcal(payload.scan_id)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/refcal/select")
    def select_refcal(payload: ScanRequest) -> Dict:
        """Set the active refcal."""

        session = _get_session(payload.session_id)
        try:
            session.set_refcal(payload.scan_id)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/refcal/combine")
    def combine_refcal(payload: CombineRequest) -> Dict:
        """Combine two refcals."""

        session = _get_session(payload.session_id)
        try:
            session.combine_refcal_pair(payload.scan_ids)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/phacal/analyze")
    def analyze_phacal(payload: ScanRequest) -> Dict:
        """Analyze one scan as phacal."""

        session = _get_session(payload.session_id)
        try:
            session.analyze_phacal(payload.scan_id)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/select-band")
    def select_band(payload: SelectionRequest) -> Dict:
        """Update selected antenna/band."""

        session = _get_session(payload.session_id)
        session.select_antenna_band(payload.antenna, payload.band)
        return session.state()

    @app.post("/api/time-flags/add")
    def add_time_flag(payload: TimeFlagAddRequest) -> Dict:
        """Add one browser-native time-flag interval group."""

        session = _get_session(payload.session_id)
        try:
            session.add_time_flag(payload.start_jd, payload.end_jd, payload.scope)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/time-flags/delete")
    def delete_time_flag(payload: TimeFlagDeleteRequest) -> Dict:
        """Delete one browser-native time-flag interval group."""

        session = _get_session(payload.session_id)
        try:
            session.delete_time_flag(payload.group_id)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.post("/api/inband/update")
    def update_inband(payload: UpdateInbandRequest) -> Dict:
        """Apply one manual in-band delay edit."""

        session = _get_session(payload.session_id)
        try:
            session.update_inband(payload.antenna, payload.x_delay_ns, payload.y_delay_ns)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": inband_delay_update_payloads(scan, use_lobe=session.use_lobe),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-delay/update")
    def update_relative_delay(payload: UpdateRelativeDelayRequest) -> Dict:
        """Apply one display-only residual delay edit."""

        session = _get_session(payload.session_id)
        try:
            session.update_relative_delay(payload.antenna, payload.x_delay_ns, payload.y_delay_ns)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-delay/apply-suggestion")
    def apply_relative_delay_suggestion(payload: RelativeDelayAntennaRequest) -> Dict:
        """Apply one antenna's current residual-guided relative-delay suggestion."""

        session = _get_session(payload.session_id)
        try:
            session.apply_relative_delay_suggestion(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-delay/undo")
    def undo_relative_delay(payload: RelativeDelayAntennaRequest) -> Dict:
        """Undo the last relative-delay edit for one antenna."""

        session = _get_session(payload.session_id)
        try:
            session.undo_relative_delay(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/inband/window")
    def update_inband_window(payload: InbandWindowRequest) -> Dict:
        """Update the kept-band window used for the active in-band mean delay."""

        session = _get_session(payload.session_id)
        try:
            session.update_inband_window(
                payload.start_band,
                payload.end_band,
                payload.mode,
                payload.antenna_scope,
                payload.polarization_scope,
                payload.source_polarization,
                source_antenna=payload.source_antenna,
            )
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": inband_window_update_payloads(scan),
        }

    @app.post("/api/inband/window/batch")
    def update_inband_window_batch(payload: InbandWindowBatchRequest) -> Dict:
        """Apply staged kept-band window edits in one refresh."""

        session = _get_session(payload.session_id)
        try:
            session.update_inband_window_batch(payload.operations)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": inband_window_update_payloads(scan),
        }

    @app.post("/api/inband/mask/batch")
    def apply_inband_mask_batch(payload: InbandMaskBatchRequest) -> Dict:
        """Apply final staged in-band masks in one refresh."""

        session = _get_session(payload.session_id)
        try:
            session.apply_inband_mask_targets(payload.targets)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": inband_window_update_payloads(scan),
        }

    @app.post("/api/inband/reset")
    def reset_inband(payload: ResetInbandRequest) -> Dict:
        """Reset active delays."""

        session = _get_session(payload.session_id)
        try:
            session.reset_inband(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": inband_delay_update_payloads(scan, use_lobe=session.use_lobe),
            "updated_antenna": None if payload.antenna is None else int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-delay/reset")
    def reset_relative_delay(payload: ResetInbandRequest) -> Dict:
        """Reset display-only residual delays."""

        session = _get_session(payload.session_id)
        try:
            session.reset_relative_delay(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan),
            "updated_antenna": None if payload.antenna is None else int(max(0, payload.antenna)),
        }

    @app.post("/api/save/sql")
    def save_sql(payload: SaveSqlRequest) -> Dict:
        """Write the current scan to SQL."""

        session = _get_session(payload.session_id)
        try:
            session.save_sql(payload.scan_id, timestamp_iso=payload.timestamp_iso)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return session.state()

    @app.get("/api/plot/heatmap.png")
    def heatmap_png(session_id: str) -> Response:
        """Render the heatmap image."""

        session = _get_session(session_id)
        current = session._current_result()
        scan_label = _current_scan_label(session)
        png = render_heatmap(current, session.selected_ant, session.selected_band, scan_label=scan_label)
        return Response(content=png, media_type="image/png")

    @app.get("/api/plot/heatmap-data")
    def heatmap_data(session_id: str) -> JSONResponse:
        """Return heatmap data for JS rendering."""

        session = _get_session(session_id)
        payload = heatmap_payload(
            session._current_result(),
            session.selected_ant,
            session.selected_band,
            scan_label=_current_scan_label(session),
        )
        return JSONResponse(content=payload)

    @app.get("/api/plot/tab.png")
    def tab_png(session_id: str, tab: str) -> Response:
        """Render a tab image."""

        if tab not in TAB_NAMES:
            raise HTTPException(status_code=400, detail="Unknown tab.")
        session = _get_session(session_id)
        scan, refcal = _resolve_tab_context(session, tab)
        png = render_tab(tab, scan, session.selected_ant, session.selected_band, refcal=refcal)
        return Response(content=png, media_type="image/png")

    @app.get("/api/plot/tab-data")
    def tab_data(session_id: str, tab: str) -> JSONResponse:
        """Return non-time-history tab data for JS rendering."""

        if tab not in TAB_NAMES:
            raise HTTPException(status_code=400, detail="Unknown tab.")
        if tab == "time_history":
            raise HTTPException(status_code=400, detail="Use /api/plot/time-history for that tab.")
        session = _get_session(session_id)
        scan, refcal = _resolve_tab_context(session, tab)
        return JSONResponse(content=tab_payload(tab, scan, refcal=refcal, use_lobe=session.use_lobe))

    @app.get("/api/plot/overview-data")
    def overview_data(session_id: str) -> JSONResponse:
        """Return all always-visible overview payloads in one response."""

        session = _get_session(session_id)
        scan, refcal = _overview_context(session)
        return JSONResponse(content=overview_payloads(scan, refcal=refcal, use_lobe=session.use_lobe))

    @app.get("/api/plot/time-history")
    def time_history_json(session_id: str) -> JSONResponse:
        """Return legacy-style time-history data for JS rendering."""

        session = _get_session(session_id)
        payload = legacy_time_history_payload(session._current_result(), session.selected_ant, session.selected_band)
        return JSONResponse(content=payload)

    return app


app = build_app()


def main() -> None:
    """Run the FastAPI app."""

    uvicorn.run("eovsapy.calwidget_html.calwidget_v2_api:app", host="127.0.0.1", port=8765, reload=False)


if __name__ == "__main__":
    main()
