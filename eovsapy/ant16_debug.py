"""Live Ant 16 command/health checks from ACC stateframes.

This module reads the live ACC stateframe and summarizes whether Ant 16
(`Ant A`, the 27-m antenna) is:

- reachable via the parser/controller path
- changing state in response to commands
- free of obvious controller, servo, and cryostat faults

It is intentionally read-only. It does not send commands to the antenna.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any, Iterable, List, Mapping, Optional, Sequence

from .telemetry import FullFrameTelemetryNormalizer, LiveStateframeReader
from .util import Time

ANT_INDEX = 15
ANT_NUMBER = 16

POWER_SWITCH_LABELS = {
    0: "OFF",
    1: "ON",
}

RUN_CONTROL_LABELS = {
    0: "STANDBY",
    1: "OPERATE",
}

RUN_MODE_LABELS = {
    0: "STOP",
    1: "POSITION",
    2: "VELOCITY",
    4: "TRACK",
}

DATA_MODE_LABELS = {
    0: "AZ-EL",
    1: "RA-DEC",
}

RX_LABELS = {
    1: "LO",
    2: "HI",
}


@dataclass(frozen=True)
class AxisStatus:
    """Decoded axis-drive status bits."""

    tripped: bool
    inactive: bool
    local: bool
    brake_engaged: bool
    low_soft_limit: bool
    high_soft_limit: bool
    low_hard_limit: bool
    high_hard_limit: bool
    drive_enabled: bool
    permit: bool
    speed_demand_limited: bool
    brake_alarm: bool
    brake_disabled: bool


@dataclass(frozen=True)
class CentralStatus:
    """Decoded central-controller status bits."""

    operate: bool
    standby: bool
    remote: bool
    local: bool
    clock_ok: bool
    sntp_ok: bool
    timeout_enabled: bool
    has_track_points: bool
    stow: bool
    stowing: bool
    ra_dec_mode: bool
    track_mode_enabled: bool
    track_array_ok: bool
    elevation_online: bool
    azimuth_online: bool
    motion_mode: str
    offsets_on: bool
    ra_offset_on: bool
    az_offset_on: bool
    correction_mode: str


@dataclass(frozen=True)
class ServoStatus:
    """Decoded FRM servo status for one motor."""

    amplifier_fault: bool
    minus_limit: bool
    plus_limit: bool
    motor_current: Optional[float]
    position: Optional[float]
    position_error: Optional[float]
    position_offset: Optional[float]


@dataclass(frozen=True)
class HealthIssue:
    """One health finding for the current snapshot."""

    level: str
    message: str


@dataclass(frozen=True)
class Ant16Snapshot:
    """One parsed Ant 16 stateframe snapshot."""

    timestamp_iso: Optional[str]
    parser_command: str
    parser_comm_err: Optional[int]
    power_switch: str
    run_control: str
    run_mode: str
    data_mode: str
    command_remote: bool
    motion_mode: str
    cRIO_time_error_s: Optional[float]
    controller_time_error_s: Optional[float]
    requested_ha_deg: Optional[float]
    requested_dec_deg: Optional[float]
    actual_ha_deg: Optional[float]
    actual_dec_deg: Optional[float]
    delta_ha_deg: Optional[float]
    delta_dec_deg: Optional[float]
    azimuth_status: AxisStatus
    elevation_status: AxisStatus
    central_status: CentralStatus
    azimuth_trip_code: Optional[int]
    elevation_trip_code: Optional[int]
    azimuth_trip_name: Optional[str]
    elevation_trip_name: Optional[str]
    bright_scram: bool
    wind_scram: bool
    fem_temperature_c: Optional[float]
    cryo_second_stage_k: Optional[float]
    selected_rx: Optional[str]
    homed: bool
    rx_select: ServoStatus
    zfocus: ServoStatus
    position_angle: ServoStatus


def decode_axis_status(value: int) -> AxisStatus:
    """Decode the axis status bits using the same positions as ``sf_display``."""

    bits = [(value >> index) & 1 for index in range(24)]
    return AxisStatus(
        tripped=bool(bits[0]),
        inactive=bool(bits[1]),
        local=bool(bits[2]),
        brake_engaged=bool(bits[3]),
        low_soft_limit=bool(bits[4]),
        high_soft_limit=bool(bits[5]),
        low_hard_limit=bool(bits[6]),
        high_hard_limit=bool(bits[7]),
        drive_enabled=bool(bits[16]),
        permit=bool(bits[17]),
        speed_demand_limited=bool(bits[19]),
        brake_alarm=bool(bits[21]),
        brake_disabled=bool(bits[22]),
    )


def decode_central_status(value: int) -> CentralStatus:
    """Decode the central status bits using the same semantics as ``sf_display``."""

    bits = [(value >> index) & 1 for index in range(32)]
    bits[1], bits[2] = bits[2], bits[1]

    track_mode_value = bits[29] * 2 + bits[28]
    motion_mode = {
        0: "STOP",
        1: "POSITION",
        2: "VELOCITY",
    }.get(track_mode_value, "UNKNOWN")

    correction_mode_value = bits[23] * 2 + bits[22]
    correction_mode = {
        0: "REFR+PNT",
        1: "REFR_OFF",
        2: "PNT_OFF",
        3: "ALL_OFF",
    }.get(correction_mode_value, "UNKNOWN")

    return CentralStatus(
        operate=not bool(bits[1]),
        standby=bool(bits[1]),
        remote=not bool(bits[2]),
        local=bool(bits[2]),
        clock_ok=not bool(bits[3]),
        sntp_ok=not bool(bits[4]),
        timeout_enabled=not bool(bits[12]),
        has_track_points=not bool(bits[6]),
        stow=bool(bits[7]),
        stowing=bool(bits[8]),
        ra_dec_mode=bool(bits[9]),
        track_mode_enabled=bool(bits[10]),
        track_array_ok=not bool(bits[16]),
        elevation_online=bool(bits[18]),
        azimuth_online=bool(bits[20]),
        motion_mode=motion_mode,
        offsets_on=bool(bits[24]),
        ra_offset_on=bool(bits[26]),
        az_offset_on=bool(bits[27]),
        correction_mode=correction_mode,
    )


def decode_servo_status(node: Mapping[str, Any]) -> ServoStatus:
    """Decode one FRM servo status block."""

    return ServoStatus(
        amplifier_fault=bool(node.get("AmplifierFault", 0)),
        minus_limit=bool(node.get("MinusLimit", 0)),
        plus_limit=bool(node.get("PlusLimit", 0)),
        motor_current=_as_float(node.get("MotorCurrent")),
        position=_as_float(node.get("Position")),
        position_error=_as_float(node.get("PositionError")),
        position_offset=_as_float(node.get("PositionOffset")),
    )


def lookup_trip_info(trip_code: Optional[int], trip_info_path: Path) -> Optional[str]:
    """Return the short trip label for one CT trip code."""

    if trip_code is None or not trip_info_path.is_file():
        return None
    lines = trip_info_path.read_text(encoding="utf-8", errors="replace").splitlines()
    blank_lines = [index for index, line in enumerate(lines) if not line.strip()]
    for blank_index in blank_lines[:-3]:
        code_line = lines[blank_index + 2].strip()
        code_values = [value.strip() for value in code_line.split(",") if value.strip()]
        if str(trip_code) in code_values:
            return lines[blank_index + 1].strip() or None
    return None


def build_snapshot(
    record: Mapping[str, Any],
    *,
    trip_info_path: Optional[Path] = None,
) -> Ant16Snapshot:
    """Build an Ant 16 snapshot from one full-frame telemetry record."""

    data = record["data"]
    antenna = data["Antenna"][ANT_INDEX]
    controller = antenna["Controller"]
    frontend = antenna["Frontend"]
    parser = antenna.get("Parser", {})
    fema = data.get("FEMA", {})
    frm_servo = fema.get("FRMServo", {})

    timestamp_lv = record["timestamp"].get("labview")
    sf_time = Time(timestamp_lv, format="lv") if timestamp_lv is not None else None
    sf_mjd = sf_time.mjd if sf_time is not None else None

    cRIO_time_error_s = None
    controller_time_error_s = None
    if sf_mjd is not None:
        cRIO_clock_ms = _as_float(controller.get("cRIOClockms"))
        controller_mjd_day = _as_float(controller.get("SystemClockMJDay"))
        controller_clock_ms = _as_float(controller.get("SystemClockms"))
        if cRIO_clock_ms is not None:
            cRIO_mjd = int(sf_mjd) + cRIO_clock_ms / 86400000.0
            cRIO_time_error_s = (cRIO_mjd - sf_mjd) * 86400.0
        if controller_mjd_day is not None and controller_clock_ms is not None:
            controller_mjd = min(controller_mjd_day, int(sf_mjd)) + controller_clock_ms / 86400000.0
            controller_time_error_s = (controller_mjd - sf_mjd) * 86400.0

    run_mode_value = _as_int(controller.get("RunMode"), default=-1)
    requested_ha_deg, requested_dec_deg = _requested_axes(controller, run_mode_value)
    actual_ha_deg, actual_dec_deg, delta_ha_deg, delta_dec_deg = _actual_axes(
        controller,
        run_mode_value,
        requested_ha_deg,
        requested_dec_deg,
    )

    azimuth_trip_code = _as_int(controller.get("AzimuthTrip0"))
    elevation_trip_code = _as_int(controller.get("ElevationTrip0"))
    trip_info_path = trip_info_path or default_trip_info_path()

    return Ant16Snapshot(
        timestamp_iso=record["timestamp"].get("iso_utc"),
        parser_command=str(parser.get("Command", "") or "").strip(),
        parser_comm_err=_as_int(parser.get("CommErr")),
        power_switch=_label_for_value(POWER_SWITCH_LABELS, _as_int(controller.get("PowerSwitch"), default=-1)),
        run_control=_label_for_value(RUN_CONTROL_LABELS, _as_int(controller.get("RunControl"), default=-1)),
        run_mode=_label_for_value(RUN_MODE_LABELS, run_mode_value),
        data_mode=_label_for_value(DATA_MODE_LABELS, _as_int(controller.get("DataMode"), default=-1)),
        command_remote=not bool(_as_int(controller.get("RemoteControl"), default=0))
        if controller.get("RemoteControl") is not None
        else decode_central_status(_as_int(controller.get("CentralStatus"), default=0)).remote,
        motion_mode=decode_central_status(_as_int(controller.get("CentralStatus"), default=0)).motion_mode,
        cRIO_time_error_s=cRIO_time_error_s,
        controller_time_error_s=controller_time_error_s,
        requested_ha_deg=requested_ha_deg,
        requested_dec_deg=requested_dec_deg,
        actual_ha_deg=actual_ha_deg,
        actual_dec_deg=actual_dec_deg,
        delta_ha_deg=delta_ha_deg,
        delta_dec_deg=delta_dec_deg,
        azimuth_status=decode_axis_status(_as_int(controller.get("AzimuthMasterStatus"), default=0)),
        elevation_status=decode_axis_status(_as_int(controller.get("ElevationStatus"), default=0)),
        central_status=decode_central_status(_as_int(controller.get("CentralStatus"), default=0)),
        azimuth_trip_code=azimuth_trip_code,
        elevation_trip_code=elevation_trip_code,
        azimuth_trip_name=lookup_trip_info(azimuth_trip_code, trip_info_path),
        elevation_trip_name=lookup_trip_info(elevation_trip_code, trip_info_path),
        bright_scram=bool(frontend.get("BrightScram", {}).get("State", 0)),
        wind_scram=bool(frontend.get("WindScram", {}).get("State", 0)),
        fem_temperature_c=_as_float(frontend.get("FEM", {}).get("Temperature")),
        cryo_second_stage_k=_as_float(fema.get("Thermal", {}).get("SecondStageTemp")),
        selected_rx=RX_LABELS.get(_as_int(frm_servo.get("SelectedRx"))),
        homed=bool(frm_servo.get("Homed", 0)),
        rx_select=decode_servo_status(frm_servo.get("RxSelect", {})),
        zfocus=decode_servo_status(frm_servo.get("ZFocus", {})),
        position_angle=decode_servo_status(frm_servo.get("PositionAngle", {})),
    )


def evaluate_snapshot(snapshot: Ant16Snapshot) -> List[HealthIssue]:
    """Evaluate one snapshot and return health findings."""

    issues: List[HealthIssue] = []

    if snapshot.parser_comm_err not in (None, 0):
        issues.append(HealthIssue("fail", f"parser comm error is {snapshot.parser_comm_err}"))
    if snapshot.central_status.local:
        issues.append(HealthIssue("fail", "controller is in LOCAL mode"))
    if snapshot.azimuth_status.local or snapshot.elevation_status.local:
        issues.append(HealthIssue("fail", "one or more axis drives report LOCAL mode"))
    if snapshot.azimuth_status.tripped:
        issues.append(
            HealthIssue(
                "fail",
                f"azimuth drive tripped ({_trip_message(snapshot.azimuth_trip_code, snapshot.azimuth_trip_name)})",
            )
        )
    if snapshot.elevation_status.tripped:
        issues.append(
            HealthIssue(
                "fail",
                f"elevation drive tripped ({_trip_message(snapshot.elevation_trip_code, snapshot.elevation_trip_name)})",
            )
        )
    if snapshot.bright_scram:
        issues.append(HealthIssue("fail", "bright scram is active"))
    if snapshot.wind_scram:
        issues.append(HealthIssue("fail", "wind scram is active"))
    if _abs_over(snapshot.cRIO_time_error_s, 1.0):
        issues.append(HealthIssue("fail", f"cRIO clock offset is {snapshot.cRIO_time_error_s:.3f} s"))
    if _abs_over(snapshot.controller_time_error_s, 1.0):
        issues.append(
            HealthIssue("fail", f"controller clock offset is {snapshot.controller_time_error_s:.3f} s")
        )
    if not snapshot.central_status.clock_ok:
        issues.append(HealthIssue("fail", "controller clock is not initialized"))
    if not snapshot.central_status.sntp_ok:
        issues.append(HealthIssue("fail", "controller reports SNTP dead"))
    if not snapshot.central_status.azimuth_online:
        issues.append(HealthIssue("fail", "azimuth drive is offline"))
    if not snapshot.central_status.elevation_online:
        issues.append(HealthIssue("fail", "elevation drive is offline"))

    for name, axis_status in (("azimuth", snapshot.azimuth_status), ("elevation", snapshot.elevation_status)):
        if axis_status.low_soft_limit or axis_status.high_soft_limit:
            issues.append(HealthIssue("fail", f"{name} axis is at a soft limit"))
        if axis_status.low_hard_limit or axis_status.high_hard_limit:
            issues.append(HealthIssue("fail", f"{name} axis is at a hard limit"))
        if axis_status.brake_alarm:
            issues.append(HealthIssue("fail", f"{name} axis brake alarm is active"))
        if not axis_status.drive_enabled and snapshot.central_status.operate:
            issues.append(HealthIssue("fail", f"{name} axis is not drive-enabled while OPERATE is set"))
        if not axis_status.permit and snapshot.central_status.operate:
            issues.append(HealthIssue("fail", f"{name} axis run permit is missing while OPERATE is set"))

    for name, servo in (
        ("FRM RX select", snapshot.rx_select),
        ("FRM Z focus", snapshot.zfocus),
        ("FRM position angle", snapshot.position_angle),
    ):
        if servo.amplifier_fault:
            issues.append(HealthIssue("fail", f"{name} amplifier fault is active"))
        if servo.minus_limit or servo.plus_limit:
            issues.append(HealthIssue("fail", f"{name} is sitting on a limit switch"))

    if snapshot.cryo_second_stage_k is not None and not 10.0 <= snapshot.cryo_second_stage_k <= 50.0:
        issues.append(
            HealthIssue(
                "fail",
                f"cryo second-stage temperature is {snapshot.cryo_second_stage_k:.1f} K",
            )
        )
    if snapshot.fem_temperature_c is not None and not 20.0 <= snapshot.fem_temperature_c <= 30.0:
        issues.append(
            HealthIssue("warn", f"FEM temperature is {snapshot.fem_temperature_c:.1f} C")
        )
    if not snapshot.homed:
        issues.append(HealthIssue("warn", "FRM servo is not homed"))
    if snapshot.run_control == "OPERATE" and snapshot.run_mode == "TRACK":
        if _abs_over(snapshot.delta_ha_deg, 0.02) or _abs_over(snapshot.delta_dec_deg, 0.02):
            issues.append(
                HealthIssue(
                    "fail",
                    "tracking error exceeds 0.02 deg in HA/Dec",
                )
            )
        elif _abs_over(snapshot.delta_ha_deg, 0.005) or _abs_over(snapshot.delta_dec_deg, 0.005):
            issues.append(
                HealthIssue(
                    "warn",
                    "tracking error exceeds 0.005 deg in HA/Dec",
                )
            )
    return issues


def summarize_observation(
    snapshots: Sequence[Ant16Snapshot],
    *,
    expect_command: Optional[str] = None,
) -> List[HealthIssue]:
    """Aggregate findings across the observation window."""

    issues: List[HealthIssue] = []
    if not snapshots:
        return [HealthIssue("fail", "no stateframes were read")]

    parser_commands = [_normalize_command(snapshot.parser_command) for snapshot in snapshots if snapshot.parser_command]
    if len(set(parser_commands)) > 1:
        issues.append(HealthIssue("info", "parser command changed during the watch window"))
    elif parser_commands:
        issues.append(HealthIssue("info", f"parser command stayed at {parser_commands[-1]!r}"))

    if expect_command:
        normalized = _normalize_command(expect_command)
        if any(normalized in command for command in parser_commands):
            issues.append(HealthIssue("info", f"expected command {normalized!r} was seen"))
        else:
            issues.append(HealthIssue("fail", f"expected command {normalized!r} was not seen"))

    request_changed = _float_series_changed(
        [snapshot.requested_ha_deg for snapshot in snapshots]
    ) or _float_series_changed([snapshot.requested_dec_deg for snapshot in snapshots])
    if request_changed:
        issues.append(HealthIssue("info", "requested HA/Dec changed during the watch window"))

    actual_changed = _float_series_changed(
        [snapshot.actual_ha_deg for snapshot in snapshots]
    ) or _float_series_changed([snapshot.actual_dec_deg for snapshot in snapshots])
    if actual_changed:
        issues.append(HealthIssue("info", "actual HA/Dec changed during the watch window"))

    latest_issues = evaluate_snapshot(snapshots[-1])
    issues.extend(latest_issues)
    return issues


def format_snapshot(snapshot: Ant16Snapshot) -> str:
    """Render one single-line snapshot summary."""

    return (
        f"{snapshot.timestamp_iso or 'unknown-time'} "
        f"cmd={snapshot.parser_command or '<empty>'!r} "
        f"cmderr={_fmt_int(snapshot.parser_comm_err)} "
        f"pwr={snapshot.power_switch} ctrl={snapshot.run_control} "
        f"run={snapshot.run_mode} data={snapshot.data_mode} "
        f"remote={'yes' if snapshot.central_status.remote else 'no'} "
        f"motion={snapshot.central_status.motion_mode} "
        f"ha(req/act/del)={_fmt_float(snapshot.requested_ha_deg)}/"
        f"{_fmt_float(snapshot.actual_ha_deg)}/{_fmt_float(snapshot.delta_ha_deg)} "
        f"dec(req/act/del)={_fmt_float(snapshot.requested_dec_deg)}/"
        f"{_fmt_float(snapshot.actual_dec_deg)}/{_fmt_float(snapshot.delta_dec_deg)} "
        f"cryo={_fmt_float(snapshot.cryo_second_stage_k)}K "
        f"rx={snapshot.selected_rx or '?'} homed={'yes' if snapshot.homed else 'no'}"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the Ant 16 live-check CLI."""

    parser = argparse.ArgumentParser(
        description=(
            "Read live ACC stateframes and report whether Ant 16 is seeing commands "
            "and appears healthy."
        )
    )
    parser.add_argument(
        "--acc-ini",
        default=str(default_acc_ini_path()),
        help="Path to acc.ini used to bootstrap the live ACC socket connection.",
    )
    parser.add_argument(
        "--xml",
        default=str(default_xml_path()),
        help="Path to the matching stateframe XML file.",
    )
    parser.add_argument(
        "--host",
        default="acc.solar.pvt",
        help="ACC hostname.",
    )
    parser.add_argument(
        "--frames",
        type=int,
        default=10,
        help="Number of frames to sample.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between live reads.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=0.5,
        help="Socket timeout in seconds.",
    )
    parser.add_argument(
        "--expect-command",
        default=None,
        help="Optional parser command substring to look for, for example TRACK or FRM-RX-SEL.",
    )
    parser.add_argument(
        "--max-read-errors",
        type=int,
        default=5,
        help="Abort after this many consecutive socket read failures.",
    )
    args = parser.parse_args(argv)

    snapshots: List[Ant16Snapshot] = []
    trip_info_path = default_trip_info_path()
    reader = LiveStateframeReader.from_acc_ini(
        args.acc_ini,
        xml_path=args.xml,
        host=args.host,
        timeout=args.timeout,
    )
    normalizer = FullFrameTelemetryNormalizer(frame_kind="stateframe")
    read_errors = 0
    try:
        while len(snapshots) < args.frames:
            try:
                frame = reader.read_frame()
            except (TimeoutError, ConnectionError) as exc:
                read_errors += 1
                print(f"READ-ERROR: {exc}")
                if read_errors >= args.max_read_errors:
                    break
                if args.poll_interval > 0:
                    time.sleep(args.poll_interval)
                continue
            read_errors = 0
            record = normalizer.normalize_frame(frame).record
            snapshot = build_snapshot(record, trip_info_path=trip_info_path)
            snapshots.append(snapshot)
            print(format_snapshot(snapshot))
            if args.poll_interval > 0 and len(snapshots) < args.frames:
                time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        pass

    issues = summarize_observation(snapshots, expect_command=args.expect_command)
    failures = [issue.message for issue in issues if issue.level == "fail"]
    warnings = [issue.message for issue in issues if issue.level == "warn"]
    infos = [issue.message for issue in issues if issue.level == "info"]

    print("")
    print("Observation summary")
    for message in infos:
        print(f"INFO: {message}")
    for message in warnings:
        print(f"WARN: {message}")
    for message in failures:
        print(f"FAIL: {message}")
    if not failures and not warnings:
        print("PASS: Ant 16 looks healthy in the last sampled frame.")
    return 1 if failures else 0


def default_acc_ini_path() -> Path:
    """Return the repo-local default ``acc.ini`` path."""

    return repo_repos_path() / "eovsa" / "acc.ini"


def default_xml_path() -> Path:
    """Return the repo-local default stateframe XML path."""

    return repo_repos_path() / "eovsa" / "stateframe.xml"


def default_trip_info_path() -> Path:
    """Return the repo-local CT trip info table path."""

    return repo_repos_path() / "eovsa" / "CT_Trip_Info.txt"


def repo_repos_path() -> Path:
    """Return the shared ``repos`` directory in this workspace."""

    return Path(__file__).resolve().parents[2]


def _requested_axes(controller: Mapping[str, Any], run_mode_value: int) -> tuple[Optional[float], Optional[float]]:
    if run_mode_value == 4:
        return (
            _scaled(controller.get("AzimuthVirtualAxis")),
            _scaled(controller.get("ElevationVirtualAxis")),
        )
    return (
        _scaled(controller.get("AzimuthPosition")),
        _scaled(controller.get("ElevationPosition")),
    )


def _actual_axes(
    controller: Mapping[str, Any],
    run_mode_value: int,
    requested_ha_deg: Optional[float],
    requested_dec_deg: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    az1 = _scaled(controller.get("Azimuth1"))
    el1 = _scaled(controller.get("Elevation1"))
    az_corr = _scaled(controller.get("AzimuthPositionCorrected"))
    el_corr = _scaled(controller.get("ElevationPositionCorrected"))

    if run_mode_value == 1:
        delta_ha_deg = _subtract(az1, az_corr)
        delta_dec_deg = _subtract(el1, el_corr)
        actual_ha_deg = _add(requested_ha_deg, delta_ha_deg)
        actual_dec_deg = _add(requested_dec_deg, delta_dec_deg)
    else:
        actual_ha_deg = az1
        actual_dec_deg = el1
        delta_ha_deg = _subtract(actual_ha_deg, requested_ha_deg)
        delta_dec_deg = _subtract(actual_dec_deg, requested_dec_deg)
    return actual_ha_deg, actual_dec_deg, delta_ha_deg, delta_dec_deg


def _trip_message(trip_code: Optional[int], trip_name: Optional[str]) -> str:
    if trip_code is None:
        return "trip code unavailable"
    if trip_name:
        return f"{trip_name}, code {trip_code}"
    return f"code {trip_code}"


def _normalize_command(command: str) -> str:
    return " ".join(command.upper().split())


def _float_series_changed(values: Iterable[Optional[float]], *, tolerance: float = 1e-4) -> bool:
    series = [value for value in values if value is not None]
    if len(series) < 2:
        return False
    return max(series) - min(series) > tolerance


def _fmt_float(value: Optional[float]) -> str:
    if value is None:
        return "None"
    return f"{value:.4f}"


def _fmt_int(value: Optional[int]) -> str:
    if value is None:
        return "None"
    return str(value)


def _label_for_value(mapping: Mapping[int, str], value: int) -> str:
    return mapping.get(value, f"UNKNOWN({value})")


def _scaled(value: Any, scale: float = 10000.0) -> Optional[float]:
    raw = _as_float(value)
    if raw is None:
        return None
    return raw / scale


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any, *, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _abs_over(value: Optional[float], limit: float) -> bool:
    return value is not None and abs(value) > limit


def _subtract(left: Optional[float], right: Optional[float]) -> Optional[float]:
    if left is None or right is None:
        return None
    return left - right


def _add(left: Optional[float], right: Optional[float]) -> Optional[float]:
    if left is None or right is None:
        return None
    return left + right


if __name__ == "__main__":
    raise SystemExit(main())
