"""Expose structured ACC telemetry for Telegraf and debugging.

This service keeps the ACC-specific parsing logic in :mod:`eovsapy.telemetry`
and provides a thin HTTP adapter on top:

- ``/healthz``: health/staleness metadata
- ``/snapshot``: latest full structured telemetry document
- ``/records``: latest entity-oriented measurement records as JSON
- ``/telegraf``: latest entity-oriented measurement records in Influx line
  protocol for Telegraf's ``inputs.http`` plugin with ``data_format = "influx"``
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time
from typing import Any, Dict, List, Mapping, Optional

from .external_sources import (
    ControlRoomTempAdapter,
    ControlRoomTempConfig,
    ExternalSourceManager,
    RoachSensorAdapter,
    RoachSensorConfig,
    WeatherStationAdapter,
    WeatherStationConfig,
    parse_external_station_specs,
    parse_roach_hosts,
)
from .telemetry import build_entity_records, iter_live_full_frame_telemetry

DEFAULT_ACC_INI = "/common/python/runtime-cache/acc.ini"
DEFAULT_STATEFRAME_XML = "/common/python/runtime-cache/stateframe.xml"


def _enabled(value: str) -> bool:
    """Interpret one environment-style boolean."""
    return str(value).strip().lower() not in ("0", "false", "no", "off", "")


def _build_external_manager(args: argparse.Namespace) -> ExternalSourceManager:
    """Build the configured external source adapter set."""
    adapters = []
    if args.enable_weather:
        adapters.append(
            WeatherStationAdapter(
                WeatherStationConfig(timeout=args.weather_timeout, stale_after_s=args.weather_stale_after),
                poll_interval=args.weather_poll_interval,
            )
        )
    if args.enable_control_room:
        adapters.append(
            ControlRoomTempAdapter(
                ControlRoomTempConfig(timeout=args.control_room_timeout, stale_after_s=args.control_room_stale_after),
                poll_interval=args.control_room_poll_interval,
            )
        )
    adapters.extend(
        ExternalSourceManager.from_solar_station_specs(
            parse_external_station_specs(args.solar_stations),
            poll_interval=args.solar_poll_interval,
            timeout=args.solar_timeout,
            stale_after_s=args.solar_stale_after,
        ).adapters
    )
    if args.enable_roach_sensors:
        adapters.extend(
            [
                RoachSensorAdapter(
                    RoachSensorConfig(host=host, timeout=args.roach_sensor_timeout),
                    poll_interval=args.roach_sensor_poll_interval,
                )
                for host in parse_roach_hosts(args.roach_hosts)
            ]
        )
    return ExternalSourceManager(adapters=adapters)


def _utc_now() -> datetime:
    """Return the current UTC time."""
    return datetime.now(timezone.utc)


def _parse_timestamp_ns(timestamp_iso: Optional[str]) -> Optional[int]:
    """Convert one ISO timestamp into Unix nanoseconds."""
    if not timestamp_iso:
        return None
    try:
        moment = datetime.fromisoformat(str(timestamp_iso).replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1_000_000_000)


def _escape_measurement(text: str) -> str:
    """Escape a line-protocol measurement or tag component."""
    return str(text).replace("\\", "\\\\").replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")


def _escape_field_key(text: str) -> str:
    """Escape a line-protocol field key."""
    return _escape_measurement(text)


def _format_field_value(value: Any) -> Optional[str]:
    """Format one line-protocol field value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value}i"
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return repr(value)
    text = str(value).replace("\\", "\\\\").replace('"', r"\"")
    return f'"{text}"'


def entity_records_to_line_protocol(records: List[Mapping[str, Any]]) -> str:
    """Render entity-oriented records as Influx line protocol.

    :param records: Entity-oriented record documents.
    :type records: list[Mapping[str, Any]]
    :returns: Line protocol body suitable for Telegraf ``inputs.http`` with
        ``data_format = "influx"``.
    :rtype: str
    """
    lines: List[str] = []
    for record in records:
        measurement = _escape_measurement(record.get("measurement", "eovsa_stateframe"))
        tags = record.get("tags", {})
        fields = record.get("fields", {})
        if not isinstance(tags, Mapping) or not isinstance(fields, Mapping):
            continue
        rendered_tags = [
            f"{_escape_measurement(key)}={_escape_measurement(value)}"
            for key, value in sorted(tags.items())
            if value not in (None, "")
        ]
        rendered_fields = []
        for key, value in sorted(fields.items()):
            rendered = _format_field_value(value)
            if rendered is None:
                continue
            rendered_fields.append(f"{_escape_field_key(key)}={rendered}")
        if not rendered_fields:
            continue
        line = measurement
        if rendered_tags:
            line += "," + ",".join(rendered_tags)
        line += " " + ",".join(rendered_fields)
        timestamp_ns = _parse_timestamp_ns(record.get("time"))
        if timestamp_ns is not None:
            line += f" {timestamp_ns}"
        lines.append(line)
    return "\n".join(lines) + ("\n" if lines else "")


class ExportState:
    """Thread-safe in-memory cache of the latest exported telemetry."""

    def __init__(self, *, stale_after: float) -> None:
        self.stale_after = stale_after
        self.lock = threading.Lock()
        self.latest_snapshot: Optional[Dict[str, Any]] = None
        self.latest_records: List[Dict[str, Any]] = []
        self.latest_line_protocol = ""
        self.last_success_utc: Optional[str] = None
        self.last_error: Optional[str] = None
        self.error_count = 0
        self.record_count = 0

    def update(self, snapshot: Dict[str, Any], records: List[Dict[str, Any]], line_protocol: str) -> None:
        """Store one successful export cycle."""
        with self.lock:
            self.latest_snapshot = snapshot
            self.latest_records = records
            self.latest_line_protocol = line_protocol
            self.last_success_utc = _utc_now().isoformat()
            self.last_error = None
            self.record_count += 1

    def update_error(self, exc: Exception) -> None:
        """Store one polling error."""
        with self.lock:
            self.last_error = str(exc)
            self.error_count += 1

    def health_document(self) -> Dict[str, Any]:
        """Return a JSON-serializable health document."""
        with self.lock:
            last_success = self.last_success_utc
            stale = True
            if last_success:
                try:
                    moment = datetime.fromisoformat(last_success)
                    stale = (_utc_now() - moment).total_seconds() > self.stale_after
                except Exception:
                    stale = True
            return {
                "status": "stale" if stale else "ok",
                "last_success_utc": last_success,
                "last_error": self.last_error,
                "error_count": self.error_count,
                "record_count": self.record_count,
                "stale_after_s": self.stale_after,
            }


def _run_poll_loop(args: argparse.Namespace, state: ExportState) -> None:
    """Run the live exporter polling loop forever."""
    records = iter_live_full_frame_telemetry(
        args.acc_ini,
        xml_path=args.xml_path,
        host=args.host,
        sf_num=args.sf_num,
        timeout=args.timeout,
        poll_interval=args.poll_interval,
    )
    external_manager = _build_external_manager(args)
    while True:
        try:
            snapshot = next(records)
            acc_records = build_entity_records(snapshot, measurement_prefix=args.measurement_prefix)
            emitted_external_records = external_manager.poll()
            latest_external_records = external_manager.latest_records()
            latest_external_snapshot = external_manager.snapshot()
            display_records = acc_records + latest_external_records
            line_protocol = entity_records_to_line_protocol(acc_records + emitted_external_records)
            merged_snapshot = dict(snapshot)
            if latest_external_snapshot:
                merged_snapshot["external_sources"] = latest_external_snapshot
            state.update(merged_snapshot, display_records, line_protocol)
        except Exception as exc:
            state.update_error(exc)
            time.sleep(max(args.poll_interval, 0.1))


def _make_handler(state: ExportState):
    """Create a request handler bound to one exporter state object."""

    class ExportHandler(BaseHTTPRequestHandler):
        """Serve exporter state over simple JSON and text endpoints."""

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                self._write_json(200, state.health_document())
                return
            if self.path == "/snapshot":
                with state.lock:
                    snapshot = state.latest_snapshot
                if snapshot is None:
                    self._write_json(503, {"error": "no snapshot available yet"})
                    return
                self._write_json(200, snapshot)
                return
            if self.path == "/records":
                with state.lock:
                    records = state.latest_records
                self._write_json(200, records)
                return
            if self.path == "/telegraf":
                with state.lock:
                    payload = state.latest_line_protocol
                if not payload:
                    self._write_text(503, "# no records available yet\n", "text/plain; charset=utf-8")
                    return
                self._write_text(200, payload, "text/plain; charset=utf-8")
                return
            self._write_json(404, {"error": "not found"})

        def log_message(self, format: str, *args: Any) -> None:
            """Suppress noisy default request logging."""
            return

        def _write_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_text(self, status: int, payload: str, content_type: str) -> None:
            body = payload.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return ExportHandler


def _build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the ACC exporter."""
    parser = argparse.ArgumentParser(description="Serve full ACC telemetry over HTTP for Telegraf and debugging.")
    parser.add_argument("--bind-host", default=os.environ.get("EOVSA_EXPORTER_BIND_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("EOVSA_EXPORTER_PORT", "9108")))
    parser.add_argument("--acc-ini", default=os.environ.get("EOVSA_ACC_INI", DEFAULT_ACC_INI))
    parser.add_argument("--xml-path", default=os.environ.get("EOVSA_STATEFRAME_XML", DEFAULT_STATEFRAME_XML))
    parser.add_argument("--host", default=os.environ.get("EOVSA_ACC_HOST", "acc.solar.pvt"))
    parser.add_argument("--sf-num", type=int, default=int(os.environ.get("EOVSA_SF_NUM", "1")))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("EOVSA_ACC_TIMEOUT", "0.5")))
    parser.add_argument("--poll-interval", type=float, default=float(os.environ.get("EOVSA_POLL_INTERVAL", "1.0")))
    parser.add_argument(
        "--measurement-prefix",
        default=os.environ.get("EOVSA_EXPORTER_MEASUREMENT_PREFIX", "eovsa_stateframe"),
    )
    parser.add_argument(
        "--stale-after",
        type=float,
        default=float(os.environ.get("EOVSA_EXPORTER_STALE_AFTER", "10.0")),
        help="Mark the exporter stale when no successful refresh occurs for this many seconds.",
    )
    parser.add_argument(
        "--solar-stations",
        default=os.environ.get("EOVSA_SOLAR_STATIONS", "MW5127:Ant12,MW5241:Ant13"),
        help="Comma-separated external solar station specs as ID:Name entries.",
    )
    parser.add_argument(
        "--solar-poll-interval",
        type=float,
        default=float(os.environ.get("EOVSA_SOLAR_POLL_INTERVAL", "60.0")),
        help="Polling cadence in seconds for external solar station sources.",
    )
    parser.add_argument(
        "--solar-timeout",
        type=float,
        default=float(os.environ.get("EOVSA_SOLAR_TIMEOUT", "10.0")),
        help="HTTP timeout in seconds for external solar station fetches.",
    )
    parser.add_argument(
        "--solar-stale-after",
        type=float,
        default=float(os.environ.get("EOVSA_SOLAR_STALE_AFTER", "300.0")),
        help="Mark external solar samples stale after this source-reported age.",
    )
    parser.add_argument(
        "--enable-weather",
        type=_enabled,
        default=_enabled(os.environ.get("EOVSA_ENABLE_EXTERNAL_WEATHER", "1")),
        help="Enable the external weather station adapter.",
    )
    parser.add_argument(
        "--weather-poll-interval",
        type=float,
        default=float(os.environ.get("EOVSA_WEATHER_POLL_INTERVAL", "15.0")),
        help="Polling cadence in seconds for the external weather source.",
    )
    parser.add_argument(
        "--weather-timeout",
        type=float,
        default=float(os.environ.get("EOVSA_WEATHER_TIMEOUT", "2.0")),
        help="HTTP timeout in seconds for the external weather source.",
    )
    parser.add_argument(
        "--weather-stale-after",
        type=float,
        default=float(os.environ.get("EOVSA_WEATHER_STALE_AFTER", "300.0")),
        help="Mark weather samples stale after this many seconds past the source sample time.",
    )
    parser.add_argument(
        "--enable-control-room",
        type=_enabled,
        default=_enabled(os.environ.get("EOVSA_ENABLE_EXTERNAL_CONTROL_ROOM", "1")),
        help="Enable the external control-room temperature adapter.",
    )
    parser.add_argument(
        "--control-room-poll-interval",
        type=float,
        default=float(os.environ.get("EOVSA_CONTROL_ROOM_POLL_INTERVAL", "30.0")),
        help="Polling cadence in seconds for the control-room temperature source.",
    )
    parser.add_argument(
        "--control-room-timeout",
        type=float,
        default=float(os.environ.get("EOVSA_CONTROL_ROOM_TIMEOUT", "2.0")),
        help="HTTP timeout in seconds for the control-room temperature source.",
    )
    parser.add_argument(
        "--control-room-stale-after",
        type=float,
        default=float(os.environ.get("EOVSA_CONTROL_ROOM_STALE_AFTER", "300.0")),
        help="Mark control-room samples stale after this many seconds without a valid reading.",
    )
    parser.add_argument(
        "--enable-roach-sensors",
        type=_enabled,
        default=_enabled(os.environ.get("EOVSA_ENABLE_EXTERNAL_ROACH_SENSORS", "1")),
        help="Enable direct ROACH sensor polling adapters.",
    )
    parser.add_argument(
        "--roach-hosts",
        default=os.environ.get(
            "EOVSA_ROACH_HOSTS",
            "roach1.solar.pvt,roach2.solar.pvt,roach3.solar.pvt,roach4.solar.pvt,"
            "roach5.solar.pvt,roach6.solar.pvt,roach7.solar.pvt,roach8.solar.pvt",
        ),
        help="Comma-separated ROACH host list for direct sensor polling.",
    )
    parser.add_argument(
        "--roach-sensor-poll-interval",
        type=float,
        default=float(os.environ.get("EOVSA_ROACH_SENSOR_POLL_INTERVAL", "60.0")),
        help="Polling cadence in seconds for direct ROACH sensor reads.",
    )
    parser.add_argument(
        "--roach-sensor-timeout",
        type=float,
        default=float(os.environ.get("EOVSA_ROACH_SENSOR_TIMEOUT", "3.0")),
        help="Connection timeout in seconds for direct ROACH sensor reads.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Run the ACC exporter HTTP service."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    state = ExportState(stale_after=max(args.stale_after, args.poll_interval * 2.0))
    worker = threading.Thread(target=_run_poll_loop, args=(args, state), daemon=True)
    worker.start()

    server = ThreadingHTTPServer((args.bind_host, args.port), _make_handler(state))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
