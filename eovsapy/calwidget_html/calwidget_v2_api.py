"""FastAPI application for the browser-based phase calibration widget."""

from __future__ import annotations

import json
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
import numpy as np
import uvicorn

from eovsapy.util import Time

from .calwidget_v2_analysis import (
    _solve_phacal_against_anchor,
    CalWidgetV2Error,
    SIDECAR_DIR,
    ScanAnalysis,
    YX_RESIDUAL_THRESHOLD_RAD,
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
    scan_feed_kind,
    sidecar_path_for_scan,
    sql2phacalX,
    sql2refcalX,
    sql_phacal_to_scan,
    sql_refcal_to_scan,
    ensure_phacal_solve_state,
    write_sidecar,
    yx_residual_threshold,
)
from .calwidget_v2_plots import (
    TAB_NAMES,
    _auto_quality_flagged_antennas,
    _effective_disabled_antennas,
    _expand_reference_antenna_dependencies,
    _relative_delay_partial_payloads,
    export_model_bundle_entry,
    heatmap_payload,
    heatmap_plot_meta,
    inband_fit_payload,
    inband_delay_update_payloads,
    inband_relative_phase_payload,
    inband_residual_phase_band_payload,
    residual_inband_apply_payloads,
    residual_mask_update_payloads,
    relative_delay_update_payloads,
    inband_window_update_payloads,
    overview_payloads,
    phacal_anchor_phase_payload,
    phacal_delay_editor_meta,
    phacal_multiband_residual_payload,
    phacal_phase_compare_payload,
    phacal_per_band_residual_payload,
    phacal_residual_inband_suggestions,
    _phacal_residual_panel_meta,
    refresh_model_flag_state,
    refcal_compare_payload,
    relative_delay_editor_meta,
    render_heatmap,
    render_tab,
    tab_payload,
    time_flag_update_payloads,
)


STATIC_DIR = Path(__file__).resolve().parent / "calwidget_v2_frontend"


class SessionRequest(BaseModel):
    """Only the session id is needed."""

    session_id: str


class ScanRequest(SessionRequest):
    """Requests that operate on one scan."""

    scan_id: int


class OptionalScanRequest(SessionRequest):
    """Requests that optionally operate on one scan."""

    scan_id: Optional[int] = None


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
    ant1_manual_dxy_corr_rad: Optional[float] = None
    ant1_dip_center_ghz: Optional[float] = None
    ant1_dip_width_ghz: Optional[float] = None
    ant1_dip_depth_rad: Optional[float] = None
    ant1_lowfreq_weight_power: Optional[float] = None


class PreviewAnt1ShapeRequest(SessionRequest):
    """Preview staged Ant 1 multiband-shape tuning without committing it."""

    dip_center_ghz: float
    dip_width_ghz: float
    dip_depth_rad: float
    lowfreq_weight_power: float


class PreviewAnt1DxyRequest(SessionRequest):
    """Preview staged Ant 1 ``Δ(Y-X)`` tuning without committing it."""

    manual_dxy_corr_rad: float


class UpdatePhacalSolveRequest(SessionRequest):
    """Update one phacal antenna's manual delay/offset corrections."""

    antenna: int
    x_delay_ns: Optional[float] = None
    y_delay_ns: Optional[float] = None
    x_offset_rad: Optional[float] = None
    y_offset_rad: Optional[float] = None


class RelativeDelayAntennaRequest(SessionRequest):
    """Operate on one antenna's display-only relative-delay state."""

    antenna: int


class PhacalSeedFromSlopeRequest(SessionRequest):
    """Seed the phacal solver for one antenna from a Shift+click slope gesture."""

    antenna: int
    seed_delay_ns: float
    seed_offset_rad: float


class PhacalFallbackRequest(SessionRequest):
    """Toggle one phacal antenna's temporary fallback path."""

    antenna: int
    enabled: bool


class DonorPatchCandidateRequest(SessionRequest):
    """Stage one donor-patch candidate antenna for compare mode."""

    antenna: int
    selected: bool


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


class MultibandFitKindRequest(SessionRequest):
    """Set the per-antenna multiband fit kind for the active refcal."""

    antenna: int
    kind: str


class SaveSqlRequest(SessionRequest):
    """Save the selected or provided scan to SQL."""

    scan_id: Optional[int] = None
    timestamp_iso: Optional[str] = None


class SaveNpzRequest(SessionRequest):
    """Save one daily v2/model NPZ bundle."""

    scan_ids: Optional[List[int]] = None


class ManualAntennaFlagRequest(SessionRequest):
    """Update one antenna's manual keep/flag override."""

    antenna: int
    flagged: bool


class YXResidualThresholdRequest(SessionRequest):
    """Update the active refcal Y-X residual RMS auto-keep threshold."""

    value: float


class ResidualBandThresholdRequest(SessionRequest):
    """Update the active refcal residual bad-band scatter threshold."""

    value: float


class ResidualInbandFitRequest(SessionRequest):
    """Apply residual in-band correction after committing residual masks."""

    targets: List[InbandMaskTarget] = []
    antenna: Optional[int] = None


class SectionMaskPreviewRequest(SessionRequest):
    """Preview one masked section without committing state."""

    section_id: str
    targets: List[InbandMaskTarget] = []


class TimeFlagAddRequest(SessionRequest):
    """Add one browser-native time-flag interval group."""

    start_jd: float
    end_jd: float
    scope: str


class TimeFlagInterval(BaseModel):
    """One staged browser-native time-flag interval."""

    antenna: int
    band: int
    start_jd: float
    end_jd: float
    scope: str


class TimeFlagBatchRequest(SessionRequest):
    """Apply multiple staged browser-native time-flag intervals."""

    intervals: List[TimeFlagInterval]


class TimeFlagDeleteRequest(SessionRequest):
    """Delete one browser-native time-flag interval group."""

    group_id: str


def _phacal_mask_signature(targets: List[InbandMaskTarget]) -> str:
    """Return a stable signature for a set of staged in-band mask targets.

    Used to determine whether a Commit can short-circuit the re-solve by
    reusing the cached preview result. The signature is independent of
    target order so a Preview followed immediately by a Commit on the same
    selection always matches.
    """

    parts = []
    for target in (targets or []):
        ranges = sorted(
            (int(item.start_band), int(item.end_band))
            for item in (target.kept_ranges or [])
        )
        parts.append((int(target.antenna), int(target.polarization), tuple(ranges)))
    parts.sort()
    return json.dumps(parts, separators=(",", ":"))


@dataclass
class WidgetSession:
    """State for one browser session."""

    session_id: str
    day: Optional[Dict] = None
    entries: List[Dict] = field(default_factory=list)
    analyses: Dict[int, ScanAnalysis] = field(default_factory=dict)
    selected_scan_id: Optional[int] = None
    ref_scan_id: Optional[int] = None
    secondary_ref_scan_id: Optional[int] = None
    compare_ref_scan_ids: List[int] = field(default_factory=list)
    compare_feed_kind: Optional[str] = None
    staged_secondary_patch_antennas: List[int] = field(default_factory=list)
    secondary_patch_antennas: List[int] = field(default_factory=list)
    selected_ant: int = 0
    selected_band: int = 0
    status_message: str = "Ready."
    fix_drift: bool = True
    use_lobe: bool = True
    residual_panel_history: List[Dict] = field(default_factory=list)

    def load_date(self, date_text: str) -> None:
        """Load the scan list for one day and reset the working set."""

        previous_date = None if self.day is None else self.day.get("date")
        previous_selected = self.selected_scan_id
        previous_ref = self.ref_scan_id
        previous_secondary = self.secondary_ref_scan_id
        previous_compare = list(self.compare_ref_scan_ids)
        previous_compare_feed = self.compare_feed_kind
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
            self.secondary_ref_scan_id = previous_secondary if previous_secondary in valid_ids else None
            self.compare_ref_scan_ids = [int(scan_id) for scan_id in previous_compare if int(scan_id) in valid_ids]
            if len(self.compare_ref_scan_ids) != 2:
                self.compare_ref_scan_ids = []
                self.compare_feed_kind = None
            else:
                self.compare_feed_kind = previous_compare_feed
            valid_ant_ids = set(range(15))
            self.staged_secondary_patch_antennas = [
                int(ant) for ant in self.staged_secondary_patch_antennas if int(ant) in valid_ant_ids
            ]
            self.secondary_patch_antennas = [
                int(ant) for ant in self.secondary_patch_antennas if int(ant) in valid_ant_ids
            ]
            self.selected_ant = previous_ant
            self.selected_band = previous_band
        else:
            self.analyses = {}
            self.selected_scan_id = None
            self.ref_scan_id = None
            self.secondary_ref_scan_id = None
            self.compare_ref_scan_ids = []
            self.compare_feed_kind = None
            self.staged_secondary_patch_antennas = []
            self.secondary_patch_antennas = []
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
            "project": entry.get("project"),
            "fseqfile": entry.get("fseqfile"),
            "feed_kind": entry.get("feed_kind", "unknown"),
            "metadata_warning": entry.get("metadata_warning", ""),
            "status": status,
            "sql_time": entry["sql_time"],
            "color": entry["color"],
            "selected": self.selected_scan_id == int(entry["scan_id"]),
            "is_refcal": self.ref_scan_id == int(entry["scan_id"]),
            "is_secondary_anchor": self.secondary_ref_scan_id == int(entry["scan_id"]),
            "saved_to_sql": saved,
            "analyzed": analysis is not None,
        }

    def _secondary_refcal(self) -> Optional[ScanAnalysis]:
        """Return the active secondary same-feed refcal."""

        if self.secondary_ref_scan_id is None:
            return None
        return self._ensure_refcal_analysis(int(self.secondary_ref_scan_id))

    def _invalidate_analyzed_phacals(self) -> None:
        """Drop analyzed phacals after an anchor change."""

        to_drop = [scan_id for scan_id, analysis in self.analyses.items() if analysis.scan_kind == "phacal"]
        for scan_id in to_drop:
            del self.analyses[scan_id]

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
            result.scan_meta.update(
                {
                    "project": entry.get("project", ""),
                    "fseqfile": entry.get("fseqfile", ""),
                    "feed_kind": entry.get("feed_kind", "unknown"),
                    "metadata_warning": entry.get("metadata_warning", ""),
                }
            )
            sidecar_file = find_sidecar_by_timestamp(sql_meta["timestamp"])
            if sidecar_file:
                attach_sidecar_delay(result, load_sidecar(sidecar_file))
                result.sidecar_path = sidecar_file
            return result
        result = sql_phacal_to_scan(sql_meta, scan_id=scan_id)
        result.scan_meta.update(
            {
                "project": entry.get("project", ""),
                "fseqfile": entry.get("fseqfile", ""),
                "feed_kind": entry.get("feed_kind", "unknown"),
                "metadata_warning": entry.get("metadata_warning", ""),
            }
        )
        return result

    def _ensure_refcal_analysis(self, scan_id: int) -> ScanAnalysis:
        """Ensure that the reference calibration has raw v2 analysis attached."""

        if scan_id in self.analyses and self.analyses[scan_id].scan_kind == "refcal":
            return self.analyses[scan_id]
        entry = self._entry(scan_id)
        result = analyze_refcal_input(entry["file"], scan_id=scan_id, fix_drift=self.fix_drift)
        result.scan_meta.update(
            {
                "project": entry.get("project", ""),
                "fseqfile": entry.get("fseqfile", ""),
                "feed_kind": entry.get("feed_kind", "unknown"),
                "metadata_warning": entry.get("metadata_warning", ""),
            }
        )
        refresh_model_flag_state(result)
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

        self.use_lobe = True
        self.status_message = "Sum Pha lobe remains enabled."

    def analyze_refcal(self, scan_id: int) -> None:
        """Analyze the requested scan as a refcal."""

        result = self._ensure_refcal_analysis(scan_id)
        result.saved_to_sql = False
        self.selected_scan_id = scan_id
        self.status_message = "Analyzed refcal {0}.".format(scan_id)

    def set_refcal(self, scan_id: int) -> None:
        """Mark one scan as the active refcal."""

        result = self._ensure_refcal_analysis(scan_id)
        if self.secondary_ref_scan_id == int(scan_id):
            self.secondary_ref_scan_id = None
        self.staged_secondary_patch_antennas = []
        self.secondary_patch_antennas = []
        self._invalidate_analyzed_phacals()
        self.ref_scan_id = scan_id
        self.selected_scan_id = scan_id
        self.selected_ant = min(self.selected_ant, max(result.layout.nsolant - 1, 0))
        self.status_message = "Set scan {0} as canonical anchor refcal.".format(scan_id)

    def set_secondary_refcal(self, scan_id: Optional[int]) -> None:
        """Set or clear the optional same-feed secondary anchor."""

        if scan_id is None:
            self.secondary_ref_scan_id = None
            self.staged_secondary_patch_antennas = []
            self.secondary_patch_antennas = []
            self._invalidate_analyzed_phacals()
            self.status_message = "Cleared the secondary anchor refcal."
            return
        if self.ref_scan_id is None:
            raise CalWidgetV2Error("Set a canonical anchor refcal before selecting a secondary anchor.")
        canonical = self._ensure_refcal_analysis(int(self.ref_scan_id))
        secondary = self._ensure_refcal_analysis(int(scan_id))
        if int(scan_id) == int(self.ref_scan_id):
            raise CalWidgetV2Error("Canonical and secondary anchors must be different scans.")
        if scan_feed_kind(canonical) != scan_feed_kind(secondary):
            raise CalWidgetV2Error("Secondary anchor must have the same feed as the canonical anchor.")
        self.secondary_ref_scan_id = int(scan_id)
        self.staged_secondary_patch_antennas = []
        self.secondary_patch_antennas = []
        self._invalidate_analyzed_phacals()
        self.status_message = "Set scan {0} as the secondary anchor refcal.".format(int(scan_id))

    def compare_refcal_pair(self, scan_ids: List[int]) -> None:
        """Enable side-by-side same-feed anchor compare mode."""

        if len(scan_ids) != 2:
            raise CalWidgetV2Error("Exactly two refcals are required for same-feed compare.")
        left = self._ensure_refcal_analysis(int(scan_ids[0]))
        right = self._ensure_refcal_analysis(int(scan_ids[1]))
        left_feed = scan_feed_kind(left)
        right_feed = scan_feed_kind(right)
        if left_feed != right_feed:
            raise CalWidgetV2Error("Compare 2 Anchors requires two refcals of the same feed.")
        self.compare_ref_scan_ids = [int(scan_ids[0]), int(scan_ids[1])]
        self.compare_feed_kind = str(left_feed)
        compare_set = {int(scan_ids[0]), int(scan_ids[1])}
        if self.ref_scan_id not in compare_set:
            self.staged_secondary_patch_antennas = []
            self.secondary_patch_antennas = []
        elif self.secondary_ref_scan_id is not None and int(self.secondary_ref_scan_id) not in compare_set:
            self.staged_secondary_patch_antennas = []
            self.secondary_patch_antennas = []
        self.status_message = "Comparing {0} anchors {1} and {2}.".format(
            str(left_feed).upper(),
            int(scan_ids[0]),
            int(scan_ids[1]),
        )

    def clear_refcal_compare(self) -> None:
        """Clear same-feed anchor compare mode."""

        self.compare_ref_scan_ids = []
        self.compare_feed_kind = None
        self.staged_secondary_patch_antennas = []
        self.status_message = "Cleared the anchor compare view."

    def set_donor_patch_candidate(self, antenna: int, selected: bool) -> None:
        """Stage one donor-patch antenna for the current compare pair."""

        if len(self.compare_ref_scan_ids) != 2:
            raise CalWidgetV2Error("Compare two same-feed refcals before staging donor patches.")
        if self.ref_scan_id is None or self.secondary_ref_scan_id is None:
            raise CalWidgetV2Error("Set canonical and secondary anchors before staging donor patches.")
        compare_set = {int(scan_id) for scan_id in self.compare_ref_scan_ids}
        if int(self.ref_scan_id) not in compare_set or int(self.secondary_ref_scan_id) not in compare_set:
            raise CalWidgetV2Error("Canonical and secondary anchors must both be part of the current compare view.")
        ant = int(max(0, min(int(antenna), 14)))
        staged = set(int(value) for value in self.staged_secondary_patch_antennas)
        if selected:
            staged.add(ant)
        else:
            staged.discard(ant)
        self.staged_secondary_patch_antennas = sorted(staged)
        self.status_message = (
            "Staged donor patch for antenna {0:d}.".format(ant + 1)
            if selected
            else "Removed staged donor patch for antenna {0:d}.".format(ant + 1)
        )

    def apply_donor_patch_selection(self) -> None:
        """Commit the staged donor-patch antenna list."""

        if self.ref_scan_id is None or self.secondary_ref_scan_id is None:
            raise CalWidgetV2Error("Canonical and secondary anchors are required before applying donor patches.")
        self.secondary_patch_antennas = sorted(int(value) for value in self.staged_secondary_patch_antennas)
        self._invalidate_analyzed_phacals()
        if self.secondary_patch_antennas:
            labels = ", ".join("Ant {0:d}".format(int(ant) + 1) for ant in self.secondary_patch_antennas)
            self.status_message = "Applied donor-patch candidates from the secondary anchor: {0}.".format(labels)
        else:
            self.status_message = "Cleared donor-patch candidates from the secondary anchor."

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
        secondary_refcal = self._secondary_refcal()
        entry = self._entry(scan_id)
        result = analyze_phacal_input(
            entry["file"],
            refcal,
            scan_id=scan_id,
            fix_drift=self.fix_drift,
            secondary_refcal=secondary_refcal,
            donor_patch_antennas=self.secondary_patch_antennas,
        )
        result.scan_meta.update(
            {
                "project": entry.get("project", ""),
                "fseqfile": entry.get("fseqfile", ""),
                "feed_kind": entry.get("feed_kind", "unknown"),
                "metadata_warning": entry.get("metadata_warning", ""),
            }
        )
        result.saved_to_sql = False
        self.analyses[scan_id] = result
        entry["status"] = "phacal"
        self.selected_scan_id = scan_id
        self.status_message = "Analyzed phacal {0} against refcal {1}.".format(scan_id, self.ref_scan_id)

    def _editable_phacal_scan(self) -> tuple[int, ScanAnalysis, ScanAnalysis]:
        """Return the selected analyzed phacal and its active anchor refcal."""

        if self.selected_scan_id is None or self.selected_scan_id not in self.analyses:
            raise CalWidgetV2Error("Analyze a phacal before editing phasecal solve values.")
        scan = self.analyses[int(self.selected_scan_id)]
        if scan.scan_kind != "phacal":
            raise CalWidgetV2Error("Select an analyzed phacal to edit phasecal solve values.")
        ref_id_value = scan.applied_ref_id if scan.applied_ref_id is not None else self.ref_scan_id
        if ref_id_value is None:
            raise CalWidgetV2Error("An active anchor refcal is required for phacal editing.")
        ref_id = int(ref_id_value)
        refcal = self._ensure_refcal_analysis(ref_id)
        ensure_phacal_solve_state(scan)
        return int(scan.scan_id), scan, refcal

    def update_phacal_solve(
        self,
        antenna: int,
        x_delay_ns: Optional[float],
        y_delay_ns: Optional[float],
        x_offset_rad: Optional[float],
        y_offset_rad: Optional[float],
    ) -> None:
        """Apply one phacal antenna's manual delay/offset correction."""

        _scan_id, scan, refcal = self._editable_phacal_scan()
        ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
        state = ensure_phacal_solve_state(scan)
        state.snapshot_ant(ant)
        if x_delay_ns is not None:
            state.applied_delay_ns[ant, 0] = float(x_delay_ns)
        if y_delay_ns is not None:
            state.applied_delay_ns[ant, 1] = float(y_delay_ns)
        if x_offset_rad is not None:
            state.applied_offset_rad[ant, 0] = float(x_offset_rad)
        if y_offset_rad is not None:
            state.applied_offset_rad[ant, 1] = float(y_offset_rad)
        _solve_phacal_against_anchor(scan, refcal)
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        scan.saved_to_sql = False
        self.selected_ant = ant
        self.status_message = "Updated phacal multiband solve values for antenna {0:d}.".format(ant + 1)

    def apply_phacal_suggestion(self, antenna: int) -> None:
        """Apply one phacal antenna's current suggested correction."""

        _scan_id, scan, refcal = self._editable_phacal_scan()
        ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
        state = ensure_phacal_solve_state(scan)
        meta = phacal_delay_editor_meta(scan, refcal, ant)
        state.snapshot_ant(ant)
        state.applied_delay_ns[ant, 0] = float(state.applied_delay_ns[ant, 0] + float(meta.get("x_suggested_delay_ns", 0.0) or 0.0))
        state.applied_delay_ns[ant, 1] = float(state.applied_delay_ns[ant, 1] + float(meta.get("y_suggested_delay_ns", 0.0) or 0.0))
        state.applied_offset_rad[ant, 0] = float(state.applied_offset_rad[ant, 0] + float(meta.get("x_suggested_offset_rad", 0.0) or 0.0))
        state.applied_offset_rad[ant, 1] = float(state.applied_offset_rad[ant, 1] + float(meta.get("y_suggested_offset_rad", 0.0) or 0.0))
        _solve_phacal_against_anchor(scan, refcal)
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        scan.saved_to_sql = False
        self.selected_ant = ant
        self.status_message = "Applied the phacal suggested correction for antenna {0:d}.".format(ant + 1)

    def seed_phacal_from_slope(
        self,
        antenna: int,
        seed_delay_ns: float,
        seed_offset_rad: float,
    ) -> None:
        """Re-solve one phacal antenna with a user-supplied slope as the seed.

        Used by the Anchor-Ref. Phase Shift+click gesture: the two clicked
        points give ``(seed_delay_ns, seed_offset_rad)``; the solver runs a
        narrow ±1 ns coherence search centered on the seed for both pols.
        ``applied_delay_ns`` and per-band I-corrections are untouched so M,
        Apply Manual, and Reset continue to compose naturally.
        """

        _scan_id, scan, refcal = self._editable_phacal_scan()
        ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
        state = ensure_phacal_solve_state(scan)
        state.snapshot_ant(ant)
        _solve_phacal_against_anchor(
            scan,
            refcal,
            seed_ant=ant,
            seed_delay_ns=float(seed_delay_ns),
            seed_offset_rad=float(seed_offset_rad),
        )
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        scan.saved_to_sql = False
        self.selected_ant = ant
        self.status_message = (
            "Seeded phacal solve from gesture for antenna {0:d}.".format(ant + 1)
        )

    def undo_phacal_solve(self, antenna: int) -> None:
        """Undo the last applied phacal manual correction for one antenna."""

        _scan_id, scan, refcal = self._editable_phacal_scan()
        ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
        state = ensure_phacal_solve_state(scan)
        if not state.undo_ant(ant):
            raise CalWidgetV2Error("No phacal manual correction is available to undo for this antenna.")
        _solve_phacal_against_anchor(scan, refcal)
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        scan.saved_to_sql = False
        self.selected_ant = ant
        self.status_message = "Undid the last phacal manual correction for antenna {0:d}.".format(ant + 1)

    def reset_phacal_solve(self, antenna: int) -> None:
        """Reset one phacal antenna back to the automatic multiband solve."""

        _scan_id, scan, refcal = self._editable_phacal_scan()
        ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
        state = ensure_phacal_solve_state(scan)
        state.snapshot_ant(ant)
        state.applied_delay_ns[ant, :] = 0.0
        state.applied_offset_rad[ant, :] = 0.0
        _solve_phacal_against_anchor(scan, refcal)
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        scan.saved_to_sql = False
        self.selected_ant = ant
        self.status_message = "Reset phacal manual corrections for antenna {0:d}.".format(ant + 1)

    def set_phacal_manual_antenna_flag(self, antenna: int, flagged: bool) -> None:
        """Update one phacal antenna's internal-only keep/skip state."""

        _scan_id, scan, _refcal = self._editable_phacal_scan()
        ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
        state = ensure_phacal_solve_state(scan)
        state.manual_skip_override[ant] = bool(flagged)
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        self.selected_ant = ant
        self.status_message = (
            "Skipped phacal diagnostics for antenna {0:d}.".format(ant + 1)
            if flagged
            else "Restored phacal diagnostics for antenna {0:d}.".format(ant + 1)
        )

    def set_phacal_anchor_fallback(self, antenna: int, enabled: bool) -> None:
        """Toggle one phacal antenna's temporary anchor fallback."""

        _scan_id, scan, refcal = self._editable_phacal_scan()
        ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
        state = ensure_phacal_solve_state(scan)
        state.manual_anchor_fallback_override[ant] = bool(enabled)
        _solve_phacal_against_anchor(scan, refcal)
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        self.selected_ant = ant
        self.status_message = (
            "Enabled temporary phacal fallback for antenna {0:d}.".format(ant + 1)
            if enabled
            else "Cleared temporary phacal fallback for antenna {0:d}.".format(ant + 1)
        )

    def _preview_scan_pair(self) -> tuple[ScanAnalysis, Optional[ScanAnalysis]]:
        """Return deep-copied scan/refcal objects for preview-only section recompute."""

        current = self._current_result()
        if current is None:
            raise CalWidgetV2Error("Select an analyzed scan before previewing masked diagnostics.")
        scan = deepcopy(current)
        refcal = None
        if scan.scan_kind == "phacal":
            ref_id = scan.applied_ref_id if scan.applied_ref_id is not None else self.ref_scan_id
            if ref_id is None:
                raise CalWidgetV2Error("Active anchor refcal is required for phacal preview.")
            refcal = deepcopy(self._ensure_refcal_analysis(int(ref_id)))
        if scan.raw:
            scan.raw.pop("overview_payload_cache", None)
            scan.raw.pop("residual_diagnostics_cache", None)
            scan.raw.pop("preview_solve_cache", None)
        if refcal is not None and refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        return scan, refcal

    def preview_refcal_ant1_shape(
        self,
        dip_center_ghz: float,
        dip_width_ghz: float,
        dip_depth_rad: float,
        lowfreq_weight_power: float,
    ) -> Dict[str, Dict]:
        """Preview Ant 1 multiband-shape tuning without mutating session state."""

        _ref_id, refcal = self._editable_inband_refcal()
        preview = deepcopy(refcal)
        if preview.delay_solution is None:
            raise CalWidgetV2Error("The active refcal has no multiband fit to preview.")
        if preview.raw:
            preview.raw.pop("overview_payload_cache", None)
            preview.raw.pop("residual_diagnostics_cache", None)
        preview.delay_solution.set_ant1_multiband_shape(
            dip_center_ghz,
            dip_width_ghz,
            dip_depth_rad,
            lowfreq_weight_power,
        )
        refresh_model_flag_state(preview, antenna_indices=[0])
        return relative_delay_update_payloads(preview, antenna=0)

    def preview_refcal_ant1_dxy(self, manual_dxy_corr_rad: float) -> Dict[str, Dict]:
        """Preview Ant 1 manual ``Δ(Y-X)`` tuning without mutating session state."""

        _ref_id, refcal = self._editable_inband_refcal()
        preview = deepcopy(refcal)
        if preview.delay_solution is None:
            raise CalWidgetV2Error("The active refcal has no multiband fit to preview.")
        if preview.raw:
            preview.raw.pop("overview_payload_cache", None)
            preview.raw.pop("residual_diagnostics_cache", None)
        preview.delay_solution.set_ant1_manual_dxy_corr(float(manual_dxy_corr_rad))
        refresh_model_flag_state(preview, antenna_indices=[0])
        return relative_delay_update_payloads(preview, antenna=0)

    def preview_inband_mask_section(self, section_id: str, targets: List[InbandMaskTarget]) -> Dict[str, Dict]:
        """Preview one kept-band-masked section without mutating session state."""

        scan, refcal = self._preview_scan_pair()
        if scan.delay_solution is None:
            raise CalWidgetV2Error("No in-band solution is available for preview.")
        changed_ants = set()
        for target in targets:
            ant = int(max(0, min(int(target.antenna), scan.layout.nsolant - 1)))
            pol = int(target.polarization)
            ranges = [(int(item.start_band), int(item.end_band)) for item in target.kept_ranges]
            if pol == 2:
                scan.delay_solution.set_xy_kept_band_ranges(ant, ranges)
            else:
                scan.delay_solution.set_kept_band_ranges(ant, int(max(0, min(pol, 1))), ranges)
            changed_ants.add(ant)
        sparse_antennas = sorted(changed_ants) if changed_ants else None
        if scan.scan_kind == "phacal":
            if refcal is None:
                raise CalWidgetV2Error("Active anchor refcal is required for phacal preview.")
            _solve_phacal_against_anchor(scan, refcal)
            if section_id not in ("inband_relative_phase", "inband_fit"):
                raise CalWidgetV2Error("Unsupported kept-mask preview section {0}.".format(section_id))
            # Snapshot the previewed solver outputs onto the actual scan so a
            # subsequent Commit on the same staged mask can skip the (slow)
            # re-solve and just restore these values.
            actual_current = self._current_result()
            if actual_current is not None and actual_current.scan_kind == "phacal" and actual_current.raw is not None:
                preview_state = ensure_phacal_solve_state(scan)
                actual_current.raw["preview_solve_cache"] = {
                    "mask_signature": _phacal_mask_signature(targets),
                    "auto_delay_ns": np.asarray(preview_state.auto_delay_ns, dtype=np.float64).copy(),
                    "auto_offset_rad": np.asarray(preview_state.auto_offset_rad, dtype=np.float64).copy(),
                    "suggested_delay_ns": np.asarray(preview_state.suggested_delay_ns, dtype=np.float64).copy(),
                    "suggested_offset_rad": np.asarray(preview_state.suggested_offset_rad, dtype=np.float64).copy(),
                    "fallback_used": np.asarray(preview_state.fallback_used, dtype=bool).copy(),
                    "ant1_self_reference_used": np.asarray(preview_state.ant1_self_reference_used, dtype=bool).copy(),
                    "phacal_inband_per_band_delay_ns": np.asarray(preview_state.phacal_inband_per_band_delay_ns, dtype=np.float64).copy(),
                    "phacal_inband_delay_ns": np.asarray(preview_state.phacal_inband_delay_ns, dtype=np.float64).copy(),
                    "missing_in_phacal": np.asarray(preview_state.missing_in_phacal, dtype=bool).copy(),
                    "missing_in_refcal": np.asarray(preview_state.missing_in_refcal, dtype=bool).copy(),
                }
            return {
                "inband_fit": phacal_anchor_phase_payload(scan, refcal, antenna_indices=sparse_antennas),
                "inband_relative_phase": phacal_multiband_residual_payload(scan, refcal, antenna_indices=sparse_antennas),
            }
        refresh_refcal_solution(scan, antenna_indices=sparse_antennas, invalidate_legacy_summary=False)
        if section_id == "inband_fit":
            return {"inband_fit": inband_fit_payload(scan)}
        if section_id == "inband_relative_phase":
            if sparse_antennas:
                only_delta = all(int(t.polarization) == 2 for t in targets)
                effective_antennas = (
                    sparse_antennas if only_delta
                    else _expand_reference_antenna_dependencies(scan, sparse_antennas)
                )
                partial = _relative_delay_partial_payloads(scan, effective_antennas)
                return {"inband_relative_phase": partial["inband_relative_phase"]}
            return {"inband_relative_phase": inband_relative_phase_payload(scan)}
        raise CalWidgetV2Error("Unsupported kept-mask preview section {0}.".format(section_id))

    def preview_residual_mask_section(self, targets: List[InbandMaskTarget]) -> Dict[str, Dict]:
        """Preview the residual panel with staged residual masks only."""

        scan, refcal = self._preview_scan_pair()
        if scan.delay_solution is None:
            raise CalWidgetV2Error("No in-band solution is available for residual preview.")
        sparse_antennas = []
        for target in targets:
            ant = int(max(0, min(int(target.antenna), scan.layout.nsolant - 1)))
            pol = int(max(0, min(int(target.polarization), 1)))
            ranges = [(int(item.start_band), int(item.end_band)) for item in target.kept_ranges]
            scan.delay_solution.set_residual_kept_band_ranges(ant, pol, ranges)
            sparse_antennas.append(ant)
        sparse_antennas = sorted(set(sparse_antennas)) or None
        if scan.scan_kind == "phacal":
            return {
                "inband_residual_phase_band": phacal_per_band_residual_payload(scan, refcal, antenna_indices=sparse_antennas)
            }
        return {"inband_residual_phase_band": inband_residual_phase_band_payload(scan)}

    def apply_residual_multiband_fit(self, antenna: int) -> List[int]:
        """Apply the residual-panel multiband fit for one selected antenna."""

        current = self._current_result()
        if current is not None and current.scan_kind == "phacal":
            _scan_id, scan, refcal = self._editable_phacal_scan()
            ant = int(max(0, min(int(antenna), scan.layout.nsolant - 1)))
            disabled = _effective_disabled_antennas(scan)
            if ant < disabled.size and disabled[ant]:
                raise CalWidgetV2Error("Selected phacal antenna is flagged or has no usable data.")
            meta = phacal_delay_editor_meta(scan, refcal, ant)
            if all(
                abs(float(meta.get(key, 0.0) or 0.0)) <= 0.0
                for key in (
                    "x_suggested_delay_ns",
                    "y_suggested_delay_ns",
                    "x_suggested_offset_rad",
                    "y_suggested_offset_rad",
                )
            ):
                raise CalWidgetV2Error("Selected phacal antenna has no multiband suggestion to apply.")
            self.apply_phacal_suggestion(ant)
            self.residual_panel_history.append(
                {"kind": "multiband", "scan_kind": "phacal", "scan_id": int(scan.scan_id), "antenna": int(ant)}
            )
            return [int(ant)]
        _ref_id, refcal = self._editable_inband_refcal()
        ant = int(max(0, min(int(antenna), refcal.layout.nsolant - 1)))
        disabled = _effective_disabled_antennas(refcal)
        if ant < disabled.size and disabled[ant]:
            raise CalWidgetV2Error("Selected antenna is flagged or has no usable data.")
        meta = relative_delay_editor_meta(refcal, ant)
        if all(
            abs(float(meta.get(key, 0.0) or 0.0)) <= 0.0
            for key in ("x_suggested_relative_delay_ns", "y_suggested_relative_delay_ns")
        ):
            raise CalWidgetV2Error("Selected antenna has no multiband suggestion to apply.")
        self.apply_relative_delay_suggestion(ant)
        self.residual_panel_history.append(
            {"kind": "multiband", "scan_kind": "refcal", "scan_id": int(refcal.scan_id), "antenna": int(ant)}
        )
        return [int(ant)]

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

    def add_time_flag(self, start_jd: float, end_jd: float, scope: str) -> List[int]:
        """Add one browser-native time-flag interval group and live-recompute."""

        scan = self._selected_editable_scan()
        group = add_time_flag_group(scan, self.selected_ant, self.selected_band, start_jd, end_jd, scope)
        touched_ants = sorted({int(target[0]) for target in group.targets})
        self._refresh_time_flag_scan(scan, antenna_indices=touched_ants)
        self.status_message = "Added {0} time flag {1}-{2} for {3}.".format(
            group.scope,
            Time(group.start_jd, format="jd").iso[11:19],
            Time(group.end_jd, format="jd").iso[11:19],
            scan.scan_kind,
        )
        return [int(ant) for ant in touched_ants]

    def add_time_flags(self, intervals: List[TimeFlagInterval]) -> List[int]:
        """Add multiple browser-native time-flag interval groups and refresh once."""

        if not intervals:
            return []
        scan = self._selected_editable_scan()
        groups = []
        touched_ants = set()
        for interval in intervals:
            group = add_time_flag_group(
                scan,
                int(interval.antenna),
                int(interval.band),
                float(interval.start_jd),
                float(interval.end_jd),
                str(interval.scope),
            )
            groups.append(group)
            touched_ants.update(int(target[0]) for target in group.targets)
        self._refresh_time_flag_scan(scan, antenna_indices=sorted(touched_ants))
        self.status_message = "Applied {0:d} staged time-flag interval(s) for {1}.".format(len(groups), scan.scan_kind)
        return sorted(int(ant) for ant in touched_ants)

    def _refresh_time_flag_scan(self, scan: ScanAnalysis, antenna_indices: Optional[List[int]] = None) -> None:
        """Recompute products after browser-native time-flag edits."""

        if scan.scan_kind == "refcal":
            refresh_refcal_solution(scan, antenna_indices=antenna_indices, invalidate_legacy_summary=False)
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

    def set_manual_antenna_flag(self, antenna: int, flagged: bool) -> None:
        """Update one antenna's manual tuned v2/model keep/flag override.

        :param antenna: Zero-based antenna index.
        :type antenna: int
        :param flagged: Whether the whole antenna should be manually flagged.
        :type flagged: bool
        """

        ref_id, refcal = self._editable_inband_refcal()
        ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
        refcal.delay_solution.manual_ant_flag_override[ant] = bool(flagged)
        auto_flagged_mask = _auto_quality_flagged_antennas(refcal)
        auto_flagged = bool(ant < auto_flagged_mask.size and auto_flagged_mask[ant])
        refcal.delay_solution.manual_ant_keep_override[ant] = bool((not flagged) and auto_flagged)
        refresh_model_flag_state(refcal, antenna_indices=[ant])
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.selected_ant = ant
        if flagged:
            self.status_message = "Excluded antenna {0:d} from the tuned v2/model solution.".format(ant + 1)
        elif auto_flagged:
            self.status_message = "Forced antenna {0:d} back into the tuned v2/model solution.".format(ant + 1)
        else:
            self.status_message = "Cleared the manual tuned v2/model flag for antenna {0:d}.".format(ant + 1)

    def update_yx_residual_threshold(self, value: float) -> None:
        """Update the active refcal Y-X residual RMS auto-keep threshold.

        :param value: Threshold in radians.
        :type value: float
        """

        _ref_id, refcal = self._editable_inband_refcal()
        threshold = max(float(value), 0.0)
        refcal.scan_meta["yx_residual_threshold_rad"] = threshold
        refresh_model_flag_state(refcal)
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        refcal.saved_to_sql = False
        self.status_message = "Set the Y-X residual RMS threshold to {0:.2f} rad.".format(threshold)

    def update_residual_band_threshold(self, value: float) -> None:
        """Update the active refcal residual bad-band scatter threshold.

        :param value: Threshold in radians.
        :type value: float
        """

        _ref_id, refcal = self._editable_inband_refcal()
        threshold = max(float(value), 0.0)
        refcal.scan_meta["residual_band_threshold_rad"] = threshold
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        residual_mask_update_payloads(refcal)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        refcal.saved_to_sql = False
        self.status_message = "Set the residual bad-band threshold to {0:.2f} rad.".format(threshold)

    def delete_time_flag(self, group_id: str) -> List[int]:
        """Delete one browser-native time-flag interval group and live-recompute."""

        scan = self._selected_editable_scan()
        removed = delete_time_flag_group(scan, group_id)
        if not removed:
            raise CalWidgetV2Error("Requested time-flag interval was not found.")
        touched_ants = sorted({int(target[0]) for target in removed.targets}) if hasattr(removed, "targets") else None
        self._refresh_time_flag_scan(scan, antenna_indices=touched_ants)
        self.status_message = "Deleted one time-flag interval from {0}.".format(scan.scan_kind)
        return touched_ants or []

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
        refresh_refcal_solution(refcal, antenna_indices=[ant], invalidate_legacy_summary=False)
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.selected_ant = ant
        self.status_message = "Updated active in-band delays for antenna {0:d}.".format(ant + 1)

    def update_relative_delay(
        self,
        antenna: int,
        x_delay_ns: Optional[float],
        y_delay_ns: Optional[float],
        ant1_manual_dxy_corr_rad: Optional[float] = None,
        ant1_dip_center_ghz: Optional[float] = None,
        ant1_dip_width_ghz: Optional[float] = None,
        ant1_dip_depth_rad: Optional[float] = None,
        ant1_lowfreq_weight_power: Optional[float] = None,
    ) -> None:
        """Apply one display-only residual delay edit to the active refcal.

        :param antenna: Zero-based antenna index.
        :type antenna: int
        :param x_delay_ns: Residual X delay override in ns.
        :type x_delay_ns: float | None
        :param y_delay_ns: Residual Y delay override in ns.
        :type y_delay_ns: float | None
        :param ant1_manual_dxy_corr_rad: Optional Ant 1 additive ``Δ(Y-X)``
            correction in radians.
        :type ant1_manual_dxy_corr_rad: float | None
        """

        _ref_id, refcal = self._editable_inband_refcal()
        ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
        refcal.delay_solution.snapshot_relative_ant(ant)
        if x_delay_ns is not None:
            refcal.delay_solution.relative_ns[ant, 0] = float(x_delay_ns)
        if y_delay_ns is not None:
            refcal.delay_solution.relative_ns[ant, 1] = float(y_delay_ns)
        if ant == 0 and ant1_manual_dxy_corr_rad is not None:
            refcal.delay_solution.set_ant1_manual_dxy_corr(float(ant1_manual_dxy_corr_rad))
        if ant == 0 and all(
            value is not None
            for value in (
                ant1_dip_center_ghz,
                ant1_dip_width_ghz,
                ant1_dip_depth_rad,
                ant1_lowfreq_weight_power,
            )
        ):
            refcal.delay_solution.set_ant1_multiband_shape(
                float(ant1_dip_center_ghz),
                float(ant1_dip_width_ghz),
                float(ant1_dip_depth_rad),
                float(ant1_lowfreq_weight_power),
            )
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        refresh_model_flag_state(refcal, antenna_indices=[ant])
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
        refresh_model_flag_state(refcal, antenna_indices=[ant])
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        self.selected_ant = ant
        self.status_message = "Applied residual-guided relative-phase suggestion for antenna {0:d}.".format(ant + 1)

    def apply_residual_inband_fit(
        self,
        targets: Optional[List[InbandMaskTarget]] = None,
        antenna: Optional[int] = None,
    ) -> List[int]:
        """Apply residual in-band delay suggestions for all antennas/pols.

        :param targets: Optional committed residual kept-band masks.
        :type targets: list[InbandMaskTarget] | None
        :param antenna: Optional zero-based antenna index. When provided,
            the correction is only applied to that one antenna.
        :type antenna: int | None
        :returns: Zero-based antenna indices whose active delays were refreshed.
        :rtype: list[int]
        """

        current = self._current_result()
        if current is not None and current.scan_kind == "phacal":
            _scan_id, scan, refcal = self._editable_phacal_scan()
            state = ensure_phacal_solve_state(scan)
            if targets and scan.delay_solution is not None:
                for target in targets:
                    ant = int(max(0, min(int(target.antenna), scan.layout.nsolant - 1)))
                    pol = int(max(0, min(int(target.polarization), 1)))
                    ranges = [(int(item.start_band), int(item.end_band)) for item in target.kept_ranges]
                    scan.delay_solution.set_residual_kept_band_ranges(ant, pol, ranges)
            if scan.delay_solution is None:
                self.status_message = "No phacal in-band residual correction was applied (no delay solution)."
                return []
            disabled_antennas = _effective_disabled_antennas(scan)
            nbands = int(scan.delay_solution.band_values.size)
            band_values = np.asarray(scan.delay_solution.band_values, dtype=int)
            # Ensure the per-band correction array has the right shape for
            # the current scan's band count (may have grown from default 0).
            if state.phacal_applied_inband_correction_ns.shape != (scan.layout.nsolant, 2, nbands):
                old = state.phacal_applied_inband_correction_ns
                new = np.zeros((scan.layout.nsolant, 2, nbands), dtype=np.float64)
                if old.ndim == 3 and old.shape[0] == scan.layout.nsolant and old.shape[1] == 2:
                    copy_n = min(old.shape[2], nbands)
                    new[:, :, :copy_n] = old[:, :, :copy_n]
                state.phacal_applied_inband_correction_ns = new
            before_corr = state.phacal_applied_inband_correction_ns.copy()
            effective_delay = np.asarray(state.auto_delay_ns + state.applied_delay_ns, dtype=float)
            explicit_antenna = antenna is not None
            ant_range = (
                [int(max(0, min(int(antenna), scan.layout.nsolant - 1)))]
                if explicit_antenna
                else list(range(scan.layout.nsolant))
            )
            updated_ants: List[int] = []
            for ant in ant_range:
                if ant < disabled_antennas.size and disabled_antennas[ant]:
                    continue
                # Only apply the "must have a multiband fit" guard in the
                # all-antennas auto-apply mode. When the user explicitly
                # clicks the per-antenna I button, respect their choice —
                # an in-band correction is meaningful even if the multiband
                # delay is zero.
                if not explicit_antenna and np.allclose(effective_delay[ant], 0.0, atol=0.0):
                    continue
                ant_changed = False
                for pol in range(2):
                    meta = _phacal_residual_panel_meta(scan, refcal, ant, pol)
                    band_fits = meta.get("residual_band_fits", [])
                    kept_mask = scan.delay_solution.included_residual_band_mask(ant, pol)
                    for band_idx, band_value in enumerate(band_values):
                        if band_idx >= len(band_fits) or band_idx >= kept_mask.size:
                            continue
                        if not bool(kept_mask[band_idx]):
                            continue
                        band_fit = band_fits[band_idx] or {}
                        delay = float(band_fit.get("delay_ns", 0.0) or 0.0)
                        if not np.isfinite(delay) or delay == 0.0:
                            continue
                        state.phacal_applied_inband_correction_ns[ant, pol, band_idx] = float(
                            state.phacal_applied_inband_correction_ns[ant, pol, band_idx] + delay
                        )
                        ant_changed = True
                if ant_changed:
                    updated_ants.append(int(ant))
            if updated_ants:
                self.residual_panel_history.append(
                    {
                        "kind": "inband_perband",
                        "scan_kind": "phacal",
                        "scan_id": int(scan.scan_id),
                        "antennas": [int(ant) for ant in updated_ants],
                        "phacal_applied_inband_correction_ns": before_corr,
                    }
                )
            _solve_phacal_against_anchor(scan, refcal)
            if scan.raw:
                scan.raw.pop("overview_payload_cache", None)
                scan.raw.pop("residual_diagnostics_cache", None)
                scan.raw.pop("preview_solve_cache", None)
            scan.saved_to_sql = False
            if updated_ants:
                diff = state.phacal_applied_inband_correction_ns - before_corr
                n_bands_changed = int(np.count_nonzero(np.isfinite(diff) & (np.abs(diff) > 0.0)))
                if antenna is not None:
                    self.status_message = "Applied in-band residual correction for phacal Ant {0:d} ({1:d} per-band rotations).".format(int(antenna) + 1, n_bands_changed)
                else:
                    self.status_message = "Applied in-band residual fit for {0:d} antennas ({1:d} per-band rotations).".format(len(updated_ants), n_bands_changed)
            else:
                if antenna is not None:
                    self.status_message = "No in-band correction applied to Ant {0:d}: no kept bands had a finite per-band fit.".format(int(antenna) + 1)
                else:
                    self.status_message = "No phacal in-band residual correction was applied."
            return updated_ants

        ref_id, refcal = self._editable_inband_refcal()
        if targets:
            for target in targets:
                ant = int(max(0, min(int(target.antenna), refcal.layout.nsolant - 1)))
                pol = int(max(0, min(int(target.polarization), 1)))
                ranges = [(int(item.start_band), int(item.end_band)) for item in target.kept_ranges]
                refcal.delay_solution.set_residual_kept_band_ranges(ant, pol, ranges)
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        residual_mask_update_payloads(refcal)
        disabled_antennas = _effective_disabled_antennas(refcal)
        updated_ants = []
        before_active = np.asarray(refcal.delay_solution.active_ns, dtype=float).copy()
        for ant in range(refcal.layout.nsolant):
            if ant < disabled_antennas.size and disabled_antennas[ant]:
                continue
            meta = relative_delay_editor_meta(refcal, ant)
            effective_relative = np.asarray(
                [
                    float(meta.get("x_effective_relative_delay_ns", 0.0) or 0.0),
                    float(meta.get("y_effective_relative_delay_ns", 0.0) or 0.0),
                ],
                dtype=float,
            )
            if np.allclose(effective_relative, 0.0, atol=0.0):
                continue
            x_suggest = float(meta.get("x_suggested_residual_inband_delay_ns", 0.0) or 0.0)
            y_suggest = float(meta.get("y_suggested_residual_inband_delay_ns", 0.0) or 0.0)
            ant_changed = False
            if abs(x_suggest) > 0.0:
                refcal.delay_solution.active_ns[ant, 0] = float(refcal.delay_solution.active_ns[ant, 0] + x_suggest)
                ant_changed = True
            if abs(y_suggest) > 0.0:
                refcal.delay_solution.active_ns[ant, 1] = float(refcal.delay_solution.active_ns[ant, 1] + y_suggest)
                ant_changed = True
            if ant_changed:
                updated_ants.append(int(ant))
        if updated_ants:
            self.residual_panel_history.append(
                {
                    "kind": "inband_global",
                    "scan_kind": "refcal",
                    "scan_id": int(refcal.scan_id),
                    "antennas": [int(ant) for ant in updated_ants],
                    "active_ns": before_active,
                }
            )
            refresh_refcal_solution(
                refcal,
                antenna_indices=updated_ants,
                invalidate_legacy_summary=False,
            )
        try:
            refcal.sidecar_path = write_sidecar(refcal)
        except Exception:
            pass
        if updated_ants:
            refcal.saved_to_sql = False
            self._invalidate_dependent_phacals(ref_id)
            self.status_message = "Applied residual in-band delay correction for unflagged antennas."
        else:
            self.status_message = "No residual in-band correction was applied because all antennas are flagged."
        return updated_ants

    def undo_residual_panel_action(self) -> List[int]:
        """Undo the most recent residual-panel apply action."""

        if not self.residual_panel_history:
            raise CalWidgetV2Error("No residual-panel action is available to undo.")
        action = self.residual_panel_history.pop()
        kind = str(action.get("kind", ""))
        scan_kind = str(action.get("scan_kind", ""))
        antenna = int(action.get("antenna", self.selected_ant))
        if kind == "multiband":
            if scan_kind == "phacal":
                self.undo_phacal_solve(antenna)
            else:
                self.undo_relative_delay(antenna)
            return [antenna]
        if kind == "inband_global" and scan_kind == "phacal":
            _scan_id, scan, refcal = self._editable_phacal_scan()
            state = ensure_phacal_solve_state(scan)
            state.applied_delay_ns[:, :] = np.asarray(action.get("applied_delay_ns", state.applied_delay_ns), dtype=float)
            _solve_phacal_against_anchor(scan, refcal)
            if scan.raw:
                scan.raw.pop("overview_payload_cache", None)
                scan.raw.pop("residual_diagnostics_cache", None)
                scan.raw.pop("preview_solve_cache", None)
            self.status_message = "Undid the last phacal in-band residual apply action."
            return [int(ant) for ant in action.get("antennas", [])]
        if kind == "inband_perband" and scan_kind == "phacal":
            _scan_id, scan, refcal = self._editable_phacal_scan()
            state = ensure_phacal_solve_state(scan)
            saved = action.get("phacal_applied_inband_correction_ns")
            if saved is not None:
                saved = np.asarray(saved, dtype=float)
                if saved.shape == state.phacal_applied_inband_correction_ns.shape:
                    state.phacal_applied_inband_correction_ns[:, :, :] = saved
                else:
                    state.phacal_applied_inband_correction_ns = saved.copy()
            _solve_phacal_against_anchor(scan, refcal)
            if scan.raw:
                scan.raw.pop("overview_payload_cache", None)
                scan.raw.pop("residual_diagnostics_cache", None)
                scan.raw.pop("preview_solve_cache", None)
            self.status_message = "Undid the last phacal per-band in-band correction."
            return [int(ant) for ant in action.get("antennas", [])]
        if kind == "inband_global":
            ref_id, refcal = self._editable_inband_refcal()
            refcal.delay_solution.active_ns[:, :] = np.asarray(action.get("active_ns", refcal.delay_solution.active_ns), dtype=float)
            changed = [int(ant) for ant in action.get("antennas", [])]
            refresh_refcal_solution(
                refcal,
                antenna_indices=changed or None,
                invalidate_legacy_summary=False,
            )
            refcal.saved_to_sql = False
            self._invalidate_dependent_phacals(ref_id)
            self.status_message = "Undid the last in-band residual apply action."
            return changed
        raise CalWidgetV2Error("Residual-panel undo does not recognize the last action.")

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
        refresh_model_flag_state(refcal, antenna_indices=[ant])
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
        changed_ants = set()
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
            changed_ants.update(int(ant) for ant in ant_indices)
            last_ant = ant0
            last_start = min(start_band, end_band)
            last_end = max(start_band, end_band)
            last_mode = str(operation.mode)
            last_polarization_scope = polarization_scope
            last_ant_count = len(ant_indices)
        refresh_refcal_solution(
            refcal,
            antenna_indices=None if len(changed_ants) >= refcal.layout.nsolant else sorted(changed_ants),
            invalidate_legacy_summary=False,
        )
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
        current = self._current_result()
        if current is not None and current.scan_kind == "phacal":
            _scan_id, scan, refcal = self._editable_phacal_scan()
            last_ant = self.selected_ant
            changed_ants = set()
            for target in targets:
                ant = int(max(0, min(int(target.antenna), scan.layout.nsolant - 1)))
                pol = int(target.polarization)
                ranges = [(int(item.start_band), int(item.end_band)) for item in target.kept_ranges]
                if pol == 2:
                    scan.delay_solution.set_xy_kept_band_ranges(ant, ranges)
                else:
                    scan.delay_solution.set_kept_band_ranges(ant, int(max(0, min(pol, 1))), ranges)
                changed_ants.add(ant)
                last_ant = ant
            # Fast path: if the user just previewed this exact mask, the solver
            # output is already cached on the scan and re-running it would
            # produce the same arrays. Restore from cache and skip the solve.
            cache = scan.raw.get("preview_solve_cache") if scan.raw else None
            commit_signature = _phacal_mask_signature(targets)
            cache_hit = bool(
                cache
                and isinstance(cache, dict)
                and cache.get("mask_signature") == commit_signature
            )
            if cache_hit:
                state = ensure_phacal_solve_state(scan)
                for key in (
                    "auto_delay_ns",
                    "auto_offset_rad",
                    "suggested_delay_ns",
                    "suggested_offset_rad",
                    "fallback_used",
                    "ant1_self_reference_used",
                    "phacal_inband_per_band_delay_ns",
                    "phacal_inband_delay_ns",
                    "missing_in_phacal",
                    "missing_in_refcal",
                ):
                    if key in cache:
                        target_arr = getattr(state, key, None)
                        cached_arr = np.asarray(cache[key])
                        if target_arr is not None and target_arr.shape == cached_arr.shape:
                            target_arr[...] = cached_arr
                        else:
                            setattr(state, key, cached_arr.copy())
            else:
                _solve_phacal_against_anchor(scan, refcal)
            if scan.raw:
                scan.raw.pop("overview_payload_cache", None)
                scan.raw.pop("residual_diagnostics_cache", None)
                scan.raw.pop("preview_solve_cache", None)
                # The cache has now been consumed (or was a miss). Either way,
                # invalidate it so any subsequent state mutation can't hit a
                # stale entry.
                scan.raw.pop("preview_solve_cache", None)
            scan.saved_to_sql = False
            self.selected_ant = last_ant
            self.status_message = "Applied {0:d} staged phasecal masks.{1}".format(
                len(targets), " (used preview cache)" if cache_hit else "",
            )
            return
        ref_id, refcal = self._editable_inband_refcal()
        last_ant = self.selected_ant
        changed_ants = set()
        for target in targets:
            ant = int(max(0, min(int(target.antenna), refcal.layout.nsolant - 1)))
            pol = int(target.polarization)
            ranges = [(int(item.start_band), int(item.end_band)) for item in target.kept_ranges]
            if pol == 2:
                refcal.delay_solution.set_xy_kept_band_ranges(ant, ranges)
            else:
                refcal.delay_solution.set_kept_band_ranges(ant, int(max(0, min(pol, 1))), ranges)
            changed_ants.add(ant)
            last_ant = ant
        refresh_refcal_solution(
            refcal,
            antenna_indices=None if len(changed_ants) >= refcal.layout.nsolant else sorted(changed_ants),
            invalidate_legacy_summary=False,
        )
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.selected_ant = last_ant
        self.status_message = "Applied {0:d} staged in-band masks.".format(len(targets))

    def set_multiband_fit_kind(self, antenna: int, kind: str) -> None:
        """Set the per-antenna multiband fit kind on the active refcal."""

        ref_id, refcal = self._editable_inband_refcal()
        if refcal.delay_solution is None:
            raise CalWidgetV2Error("No in-band delay solution is available.")
        ant = int(max(0, min(int(antenna), refcal.layout.nsolant - 1)))
        valid = {"linear", "poly2", "poly3"}
        kind_str = str(kind)
        if kind_str not in valid:
            raise CalWidgetV2Error("Multiband fit kind must be one of linear, poly2, poly3.")
        refcal.delay_solution.set_multiband_fit_kind(ant, kind_str)
        if refcal.raw:
            refcal.raw.pop("overview_payload_cache", None)
            refcal.raw.pop("residual_diagnostics_cache", None)
        refcal.saved_to_sql = False
        self._invalidate_dependent_phacals(ref_id)
        self.selected_ant = ant
        self.status_message = "Set multiband fit kind to {0} for antenna {1:d}.".format(kind_str, ant + 1)

    def reset_inband(self, antenna: Optional[int] = None) -> None:
        """Reset active in-band delays."""

        ref_id, refcal = self._editable_inband_refcal()
        if antenna is None:
            refcal.delay_solution.reset_all()
            message = "Reset all active in-band delays."
            refresh_antennas = None
        else:
            ant = int(max(0, min(antenna, refcal.layout.nsolant - 1)))
            refcal.delay_solution.reset_ant(ant)
            message = "Reset antenna {0:d} in-band delays.".format(ant + 1)
            refresh_antennas = [ant]
        refresh_refcal_solution(refcal, antenna_indices=refresh_antennas, invalidate_legacy_summary=False)
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
        refresh_model_flag_state(refcal, antenna_indices=None if antenna is None else [ant])
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

    def save_npz_bundle(self, scan_ids: Optional[List[int]] = None) -> str:
        """Save one daily v2/model bundle under ``/common/webplots/phasecal``.

        :param scan_ids: Optional explicit scan ids to include.
        :type scan_ids: list[int] | None
        :returns: Absolute output path.
        :rtype: str
        """

        if self.day is None:
            raise CalWidgetV2Error("Load a day before saving an NPZ bundle.")
        chosen_ids = [int(scan_id) for scan_id in (scan_ids or [])]
        if not chosen_ids:
            chosen_ids = sorted(int(scan_id) for scan_id in self.analyses.keys())
        if self.ref_scan_id is not None and int(self.ref_scan_id) not in chosen_ids:
            chosen_ids = [int(self.ref_scan_id)] + chosen_ids
        chosen_ids = sorted(set(chosen_ids))
        prior_selected_scan = self.selected_scan_id
        prior_status = self.status_message
        for scan_id in chosen_ids:
            if scan_id in self.analyses:
                continue
            try:
                entry = self._entry(scan_id)
            except CalWidgetV2Error:
                continue
            sql_meta = entry.get("sql_meta") or {}
            try:
                if sql_meta.get("kind") == "refcal":
                    self._ensure_refcal_analysis(scan_id)
                elif sql_meta.get("kind") == "phacal" and self.ref_scan_id is not None:
                    self.analyze_phacal(scan_id)
            except CalWidgetV2Error:
                continue
        self.selected_scan_id = prior_selected_scan
        self.status_message = prior_status

        products: Dict[str, Dict] = {}
        phacal_ids: List[int] = []
        for scan_id in chosen_ids:
            if scan_id not in self.analyses:
                continue
            analysis = self.analyses[scan_id]
            if analysis.scan_kind == "phacal":
                phacal_ids.append(int(scan_id))
            products[str(scan_id)] = export_model_bundle_entry(analysis)
        if not products:
            raise CalWidgetV2Error("Analyze at least one scan before saving the NPZ bundle.")

        canonical_ref = self.ref_scan_id if self.ref_scan_id is not None else None
        same_feed_ref_candidates = [
            int(scan_id)
            for scan_id in chosen_ids
            if scan_id in self.analyses
            and self.analyses[scan_id].scan_kind == "refcal"
            and (
                canonical_ref is None
                or scan_feed_kind(self.analyses[scan_id]) == scan_feed_kind(self.analyses[int(canonical_ref)])
            )
        ]
        secondary_ref = self.secondary_ref_scan_id if self.secondary_ref_scan_id is not None else None
        if secondary_ref is not None and secondary_ref not in same_feed_ref_candidates:
            secondary_ref = None
        bundle_threshold = float(YX_RESIDUAL_THRESHOLD_RAD)
        if canonical_ref is not None and int(canonical_ref) in self.analyses:
            bundle_threshold = float(yx_residual_threshold(self.analyses[int(canonical_ref)]))

        saved_at_iso = Time.now().iso[:19]
        outpath = SIDECAR_DIR / "{0}_calwidget_v2_daily.npz".format(self.day["date"].replace("-", ""))
        outpath.parent.mkdir(parents=True, exist_ok=True)
        archive: Dict[str, np.ndarray] = {
            "bundle_schema_version": np.asarray(2, dtype=np.int32),
            "date": np.asarray(self.day["date"]),
            "saved_at_iso": np.asarray(saved_at_iso),
            "canonical_anchor_scan_id": np.asarray(-1 if canonical_ref is None else int(canonical_ref), dtype=np.int32),
            "secondary_anchor_scan_id": np.asarray(-1 if secondary_ref is None else int(secondary_ref), dtype=np.int32),
            "single_anchor_mode": np.asarray(bool(secondary_ref is None), dtype=np.uint8),
            "selected_phacal_ids": np.asarray(phacal_ids, dtype=np.int32),
            "yx_residual_threshold_rad": np.asarray(bundle_threshold, dtype=np.float64),
            "product_scan_ids": np.asarray(sorted(int(scan_id) for scan_id in products.keys()), dtype=np.int32),
            "product_manifest_json": np.asarray(
                json.dumps(
                    {
                        "canonical_anchor_scan_id": None if canonical_ref is None else int(canonical_ref),
                        "secondary_anchor_scan_id": None if secondary_ref is None else int(secondary_ref),
                        "single_anchor_mode": bool(secondary_ref is None),
                        "selected_phacal_ids": phacal_ids,
                    },
                    separators=(",", ":"),
                )
            ),
        }
        for scan_id, product in products.items():
            prefix = "scan_{0}".format(scan_id)
            archive[prefix + "__scan_id"] = np.asarray(int(scan_id), dtype=np.int32)
            archive[prefix + "__scan_kind"] = np.asarray(product.get("scan_kind", ""))
            archive[prefix + "__timestamp_iso"] = np.asarray(product.get("timestamp_iso", ""))
            archive[prefix + "__source"] = np.asarray(product.get("source", ""))
            archive[prefix + "__feed_kind"] = np.asarray(product.get("feed_kind", "unknown"))
            archive[prefix + "__metadata_warning"] = np.asarray(product.get("metadata_warning", ""))
            archive[prefix + "__fine_frequency_ghz"] = np.asarray(product.get("fine_frequency_ghz", []), dtype=np.float64)
            archive[prefix + "__band_frequency_ghz"] = np.asarray(product.get("band_frequency_ghz", []), dtype=np.float64)
            archive[prefix + "__band_values"] = np.asarray(product.get("band_values", []), dtype=np.int32)
            archive[prefix + "__model_phase_fine"] = np.asarray(product.get("model_phase_fine", []), dtype=np.float64)
            archive[prefix + "__model_phase_band"] = np.asarray(product.get("model_phase_band", []), dtype=np.float64)
            archive[prefix + "__legacy_flag"] = np.asarray(product.get("legacy_flag", []), dtype=np.int32)
            archive[prefix + "__v2_flag"] = np.asarray(product.get("v2_flag", []), dtype=np.int32)
            archive[prefix + "__model_flag"] = np.asarray(product.get("model_flag", []), dtype=np.int32)
            archive[prefix + "__manual_ant_flag_override"] = np.asarray(
                product.get("manual_ant_flag_override", []),
                dtype=np.uint8,
            )
            archive[prefix + "__manual_ant_keep_override"] = np.asarray(
                product.get("manual_ant_keep_override", []),
                dtype=np.uint8,
            )
            archive[prefix + "__yx_residual_rms"] = np.asarray(product.get("yx_residual_rms", []), dtype=np.float64)
            archive[prefix + "__yx_residual_threshold_rad"] = np.asarray(
                product.get("yx_residual_threshold_rad", bundle_threshold),
                dtype=np.float64,
            )
            archive[prefix + "__scan_meta_json"] = np.asarray(
                json.dumps(product.get("scan_meta", {}) or {}, separators=(",", ":"))
            )
        np.savez_compressed(str(outpath), **archive)
        self.status_message = "Saved daily v2/model NPZ bundle to {0}.".format(outpath)
        return str(outpath)

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
                "feed_kind": scan_feed_kind(current),
                "saved_to_sql": current.saved_to_sql,
                "sidecar_path": current.sidecar_path,
            }
        ref_meta = None
        phacal_meta = None
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
                "feed_kind": scan_feed_kind(refcal),
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
        if current is not None and current.scan_kind == "phacal":
            phacal_ref_id = current.applied_ref_id if current.applied_ref_id is not None else ref_scan_id
            phacal_ref = None
            phacal_ref_entry = None
            if phacal_ref_id is not None:
                try:
                    phacal_ref = self._ensure_refcal_analysis(int(phacal_ref_id))
                except CalWidgetV2Error:
                    phacal_ref = None
                try:
                    phacal_ref_entry = self._entry(int(phacal_ref_id))
                except CalWidgetV2Error:
                    phacal_ref_entry = None
            editor_meta = phacal_delay_editor_meta(current, phacal_ref, self.selected_ant)
            phacal_entry = None
            try:
                phacal_entry = self._entry(current.scan_id)
            except CalWidgetV2Error:
                phacal_entry = None
            phacal_meta = {
                "scan_id": int(current.scan_id),
                "timestamp_iso": current.timestamp.iso[:19],
                "scan_time": phacal_entry["scan_time"] if phacal_entry else current.t_bg.iso[11:19],
                "source": current.source,
                "anchor_scan_id": None if phacal_ref_id is None else int(phacal_ref_id),
                "anchor_scan_time": None if phacal_ref is None else (phacal_ref_entry["scan_time"] if phacal_ref_entry else phacal_ref.t_bg.iso[11:19]),
                "secondary_anchor_scan_id": (current.scan_meta or {}).get("secondary_refcal_scan_id"),
                "secondary_anchor_scan_time": (current.scan_meta or {}).get("secondary_refcal_time"),
                "patched_antennas": list((current.scan_meta or {}).get("patched_antennas", [])),
                "patch_method": list((current.scan_meta or {}).get("patch_method", [])),
                "manual_anchor_fallback_override": list(
                    (current.scan_meta or {}).get("manual_anchor_fallback_override", [])
                ),
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
            "secondary_ref_scan_id": self.secondary_ref_scan_id,
            "compare_ref_scan_ids": [int(scan_id) for scan_id in self.compare_ref_scan_ids],
            "compare_feed_kind": self.compare_feed_kind,
            "staged_secondary_patch_antennas": [int(ant) for ant in self.staged_secondary_patch_antennas],
            "secondary_patch_antennas": [int(ant) for ant in self.secondary_patch_antennas],
            "selected_ant": self.selected_ant,
            "selected_band": self.selected_band,
            "tabs": list(TAB_NAMES),
            "current_layout": current_layout,
            "heatmap_meta": heatmap_plot_meta(current),
            "current_scan": current_meta,
            "active_refcal": ref_meta,
            "active_phacal": phacal_meta,
            "residual_panel_undo_available": bool(self.residual_panel_history),
            "scan_metadata_warnings": sorted(
                {
                    str(entry.get("metadata_warning", "")).strip()
                    for entry in self.entries
                    if str(entry.get("metadata_warning", "")).strip()
                }
            ),
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
        response = {"state": session.state()}
        current = session._current_result()
        if current is not None and current.scan_kind == "phacal":
            scan, refcal = _overview_context(session)
            response["overview"] = overview_payloads(scan, refcal=refcal, use_lobe=session.use_lobe)
        return response

    @app.post("/api/refcal/secondary")
    def select_secondary_refcal(payload: OptionalScanRequest) -> Dict:
        """Set or clear the active secondary same-feed anchor refcal."""

        session = _get_session(payload.session_id)
        try:
            session.set_secondary_refcal(payload.scan_id)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"state": session.state()}

    @app.post("/api/refcal/compare")
    def compare_refcals(payload: CombineRequest) -> Dict:
        """Enable side-by-side same-feed anchor compare mode."""

        session = _get_session(payload.session_id)
        try:
            session.compare_refcal_pair(payload.scan_ids)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"state": session.state()}

    @app.post("/api/refcal/compare/clear")
    def clear_refcal_compare(payload: SessionRequest) -> Dict:
        """Clear side-by-side same-feed anchor compare mode."""

        session = _get_session(payload.session_id)
        session.clear_refcal_compare()
        return {"state": session.state()}

    @app.post("/api/refcal/donor-patch/candidate")
    def set_donor_patch_candidate(payload: DonorPatchCandidateRequest) -> Dict:
        """Stage one donor-patch candidate antenna in compare mode."""

        session = _get_session(payload.session_id)
        try:
            session.set_donor_patch_candidate(payload.antenna, payload.selected)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"state": session.state()}

    @app.post("/api/refcal/donor-patch/apply")
    def apply_donor_patch_selection(payload: SessionRequest) -> Dict:
        """Commit the staged donor-patch antenna selection."""

        session = _get_session(payload.session_id)
        try:
            session.apply_donor_patch_selection()
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"state": session.state()}

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

    @app.post("/api/phacal/anchor-fallback")
    def set_phacal_anchor_fallback(payload: PhacalFallbackRequest) -> Dict:
        """Toggle one phacal antenna's temporary anchor fallback."""

        session = _get_session(payload.session_id)
        try:
            session.set_phacal_anchor_fallback(payload.antenna, payload.enabled)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

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
            touched_ants = session.add_time_flag(payload.start_jd, payload.end_jd, payload.scope)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": time_flag_update_payloads(
                scan,
                use_lobe=session.use_lobe,
                antenna_indices=touched_ants,
                refcal=refcal,
            ),
            "heatmap": heatmap_payload(
                scan,
                session.selected_ant,
                session.selected_band,
                scan_label=_current_scan_label(session),
            ),
            "time_history": legacy_time_history_payload(scan, session.selected_ant, session.selected_band),
        }

    @app.post("/api/time-flags/add-batch")
    def add_time_flags(payload: TimeFlagBatchRequest) -> Dict:
        """Add multiple browser-native time-flag interval groups."""

        session = _get_session(payload.session_id)
        try:
            touched_ants = session.add_time_flags(payload.intervals)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": time_flag_update_payloads(
                scan,
                use_lobe=session.use_lobe,
                antenna_indices=touched_ants,
                refcal=refcal,
            ),
            "heatmap": heatmap_payload(
                scan,
                session.selected_ant,
                session.selected_band,
                scan_label=_current_scan_label(session),
            ),
            "time_history": legacy_time_history_payload(scan, session.selected_ant, session.selected_band),
        }

    @app.post("/api/time-flags/delete")
    def delete_time_flag(payload: TimeFlagDeleteRequest) -> Dict:
        """Delete one browser-native time-flag interval group."""

        session = _get_session(payload.session_id)
        try:
            touched_ants = session.delete_time_flag(payload.group_id)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": time_flag_update_payloads(
                scan,
                use_lobe=session.use_lobe,
                antenna_indices=touched_ants,
                refcal=refcal,
            ),
            "heatmap": heatmap_payload(
                scan,
                session.selected_ant,
                session.selected_band,
                scan_label=_current_scan_label(session),
            ),
            "time_history": legacy_time_history_payload(scan, session.selected_ant, session.selected_band),
        }

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
            "overview_updates": inband_delay_update_payloads(
                scan,
                use_lobe=session.use_lobe,
                antenna=payload.antenna,
            ),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-delay/update")
    def update_relative_delay(payload: UpdateRelativeDelayRequest) -> Dict:
        """Apply one display-only residual delay edit."""

        session = _get_session(payload.session_id)
        try:
            session.update_relative_delay(
                payload.antenna,
                payload.x_delay_ns,
                payload.y_delay_ns,
                ant1_manual_dxy_corr_rad=payload.ant1_manual_dxy_corr_rad,
                ant1_dip_center_ghz=payload.ant1_dip_center_ghz,
                ant1_dip_width_ghz=payload.ant1_dip_width_ghz,
                ant1_dip_depth_rad=payload.ant1_dip_depth_rad,
                ant1_lowfreq_weight_power=payload.ant1_lowfreq_weight_power,
            )
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-delay/ant1-shape/preview")
    def preview_ant1_shape(payload: PreviewAnt1ShapeRequest) -> Dict:
        """Preview staged Ant 1 multiband-shape tuning."""

        session = _get_session(payload.session_id)
        try:
            overview_updates = session.preview_refcal_ant1_shape(
                payload.dip_center_ghz,
                payload.dip_width_ghz,
                payload.dip_depth_rad,
                payload.lowfreq_weight_power,
            )
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"overview_updates": overview_updates, "updated_antenna": 0}

    @app.post("/api/relative-delay/ant1-dxy/preview")
    def preview_ant1_dxy(payload: PreviewAnt1DxyRequest) -> Dict:
        """Preview staged Ant 1 ``Δ(Y-X)`` tuning."""

        session = _get_session(payload.session_id)
        try:
            overview_updates = session.preview_refcal_ant1_dxy(payload.manual_dxy_corr_rad)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"overview_updates": overview_updates, "updated_antenna": 0}

    @app.post("/api/phacal/solve/update")
    def update_phacal_solve(payload: UpdatePhacalSolveRequest) -> Dict:
        """Apply one phacal manual delay/offset edit."""

        session = _get_session(payload.session_id)
        try:
            session.update_phacal_solve(
                payload.antenna,
                payload.x_delay_ns,
                payload.y_delay_ns,
                payload.x_offset_rad,
                payload.y_offset_rad,
            )
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
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
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/phacal/solve/apply-suggestion")
    def apply_phacal_suggestion(payload: RelativeDelayAntennaRequest) -> Dict:
        """Apply one phacal antenna's current suggested correction."""

        session = _get_session(payload.session_id)
        try:
            session.apply_phacal_suggestion(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/phacal/solve/seed-from-slope")
    def seed_phacal_from_slope(payload: PhacalSeedFromSlopeRequest) -> Dict:
        """Seed the phacal solver from a Shift+click slope gesture."""

        session = _get_session(payload.session_id)
        try:
            session.seed_phacal_from_slope(
                payload.antenna,
                payload.seed_delay_ns,
                payload.seed_offset_rad,
            )
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/inband/apply-residual-fit")
    def apply_residual_inband_fit(payload: ResidualInbandFitRequest) -> Dict:
        """Apply residual in-band correction after committing residual masks.

        When ``payload.antenna`` is provided, the correction is applied to
        that single antenna only (per-antenna apply, mirrors the per-antenna
        Multiband Fit M button). Otherwise it applies to all eligible
        antennas as before.
        """

        session = _get_session(payload.session_id)
        try:
            updated_antennas = session.apply_residual_inband_fit(payload.targets, antenna=payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        if scan is not None and scan.scan_kind == "phacal":
            overview_updates = {
                "inband_fit": phacal_anchor_phase_payload(scan, _refcal, antenna_indices=updated_antennas),
                "inband_relative_phase": phacal_multiband_residual_payload(scan, _refcal, antenna_indices=updated_antennas),
                "inband_residual_phase_band": phacal_per_band_residual_payload(scan, _refcal, antenna_indices=updated_antennas),
            }
        else:
            overview_updates = residual_inband_apply_payloads(
                scan,
                use_lobe=session.use_lobe,
                antenna_indices=updated_antennas,
            )
        return {
            "state": session.state(),
            "overview_updates": overview_updates,
        }

    @app.post("/api/inband/residual-threshold")
    def set_residual_band_threshold(payload: ResidualBandThresholdRequest) -> Dict:
        """Update the active refcal residual bad-band threshold."""

        session = _get_session(payload.session_id)
        try:
            session.update_residual_band_threshold(payload.value)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": residual_mask_update_payloads(scan),
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
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/phacal/solve/undo")
    def undo_phacal_solve(payload: RelativeDelayAntennaRequest) -> Dict:
        """Undo the last phacal manual edit for one antenna."""

        session = _get_session(payload.session_id)
        try:
            session.undo_phacal_solve(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/phacal/solve/reset")
    def reset_phacal_solve(payload: RelativeDelayAntennaRequest) -> Dict:
        """Reset one phacal antenna back to its automatic solve."""

        session = _get_session(payload.session_id)
        try:
            session.reset_phacal_solve(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-phase/antenna-flag")
    def set_manual_antenna_flag(payload: ManualAntennaFlagRequest) -> Dict:
        """Update one antenna's manual tuned-solution keep/flag state."""

        session = _get_session(payload.session_id)
        try:
            session.set_manual_antenna_flag(payload.antenna, payload.flagged)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "heatmap": heatmap_payload(
                session._current_result(),
                session.selected_ant,
                session.selected_band,
                scan_label=_current_scan_label(session),
            ),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/phacal/antenna-flag")
    def set_phacal_antenna_flag(payload: ManualAntennaFlagRequest) -> Dict:
        """Update one phacal antenna's internal-only keep/skip state."""

        session = _get_session(payload.session_id)
        try:
            session.set_phacal_manual_antenna_flag(payload.antenna, payload.flagged)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
        }

    @app.post("/api/relative-phase/yx-threshold")
    def set_yx_residual_threshold(payload: YXResidualThresholdRequest) -> Dict:
        """Update the active refcal Y-X residual RMS threshold."""

        session = _get_session(payload.session_id)
        try:
            session.update_yx_residual_threshold(payload.value)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview": overview_payloads(scan, refcal=refcal, use_lobe=session.use_lobe),
            "heatmap": heatmap_payload(
                session._current_result(),
                session.selected_ant,
                session.selected_band,
                scan_label=_current_scan_label(session),
            ),
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
            "overview_updates": inband_window_update_payloads(
                scan,
                antenna_indices=[int(target.antenna) for target in payload.targets],
                refcal=_refcal,
            ),
        }

    @app.post("/api/refcal/multiband-fit-kind")
    def set_multiband_fit_kind(payload: MultibandFitKindRequest) -> Dict:
        """Set per-antenna multiband fit kind on the active refcal."""

        session = _get_session(payload.session_id)
        try:
            session.set_multiband_fit_kind(payload.antenna, payload.kind)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": inband_window_update_payloads(
                scan,
                antenna_indices=[int(payload.antenna)],
                refcal=_refcal,
            ),
        }

    @app.post("/api/inband/mask/preview")
    def preview_inband_mask(payload: SectionMaskPreviewRequest) -> Dict:
        """Preview one kept-band-masked section without committing state."""

        session = _get_session(payload.session_id)
        try:
            overview_updates = session.preview_inband_mask_section(payload.section_id, payload.targets)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"overview_updates": overview_updates}

    @app.post("/api/inband/residual-mask/preview")
    def preview_residual_mask(payload: SectionMaskPreviewRequest) -> Dict:
        """Preview the residual panel with staged residual masks only."""

        session = _get_session(payload.session_id)
        try:
            overview_updates = session.preview_residual_mask_section(payload.targets)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"overview_updates": overview_updates}

    @app.post("/api/residual-panel/apply-multiband-fit")
    def apply_residual_panel_multiband_fit(payload: RelativeDelayAntennaRequest) -> Dict:
        """Apply the residual-panel multiband fit for the selected antenna."""

        session = _get_session(payload.session_id)
        try:
            updated_antennas = session.apply_residual_multiband_fit(payload.antenna)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        return {
            "state": session.state(),
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
            "updated_antenna": int(max(0, payload.antenna)),
            "updated_antennas": updated_antennas,
        }

    @app.post("/api/residual-panel/undo")
    def undo_residual_panel_action(payload: SessionRequest) -> Dict:
        """Undo the last residual-panel apply action."""

        session = _get_session(payload.session_id)
        try:
            updated_antennas = session.undo_residual_panel_action()
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        scan, _refcal = _overview_context(session)
        if scan is not None and scan.scan_kind == "phacal":
            overview_updates = (
                relative_delay_update_payloads(scan, antenna=updated_antennas[0], refcal=_refcal)
                if len(updated_antennas) == 1
                else relative_delay_update_payloads(scan, refcal=_refcal)
            )
        else:
            overview_updates = residual_inband_apply_payloads(
                scan,
                use_lobe=session.use_lobe,
                antenna_indices=updated_antennas or None,
            )
        return {
            "state": session.state(),
            "overview_updates": overview_updates,
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
            "overview_updates": inband_delay_update_payloads(
                scan,
                use_lobe=session.use_lobe,
                antenna=payload.antenna,
            ),
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
            "overview_updates": relative_delay_update_payloads(scan, antenna=payload.antenna, refcal=_refcal),
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

    @app.post("/api/save/npz")
    def save_npz(payload: SaveNpzRequest) -> Dict:
        """Write the current day's tuned v2/model bundle to NPZ."""

        session = _get_session(payload.session_id)
        try:
            outpath = session.save_npz_bundle(payload.scan_ids)
        except CalWidgetV2Error as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"state": session.state(), "path": outpath}

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

    @app.get("/api/plot/refcal-compare")
    def refcal_compare_data(session_id: str) -> JSONResponse:
        """Return same-feed refcal compare payloads."""

        session = _get_session(session_id)
        compare_ids = [int(scan_id) for scan_id in session.compare_ref_scan_ids]
        if len(compare_ids) != 2:
            raise HTTPException(status_code=400, detail="Select exactly two same-feed refcals to compare.")
        left = session._ensure_refcal_analysis(compare_ids[0])
        right = session._ensure_refcal_analysis(compare_ids[1])
        return JSONResponse(
            content=refcal_compare_payload(
                left,
                right,
                use_lobe=session.use_lobe,
                canonical_scan_id=session.ref_scan_id,
                secondary_scan_id=session.secondary_ref_scan_id,
                staged_patch_antennas=session.staged_secondary_patch_antennas,
                applied_patch_antennas=session.secondary_patch_antennas,
            )
        )

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
