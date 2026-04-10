"""Helpers for replaying and exporting EOVSA ACC telemetry.

This module provides Python 3 readers for saved and live ACC binary frame
streams together with two export layers:

- a narrow operational subset normalizer kept for compatibility with the
  current direct InfluxDB writer
- a full structured export path that preserves the complete parsed pointer tree
  and can be converted into entity-oriented measurement records
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
import re
import socket
import struct
import time
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union
import urllib.request

import numpy as np

from .read_xml2 import xml_ptrs
from .util import Time as EOVSATime
from .util import extract as extract_value

STATEFRAME_LOG_RE = re.compile(r"^(sf|sh)_\d{8}_v(?P<version>\d+(?:\.\d+)?)\.log$")

PathElement = Union[str, int]
PointerTree = Union[Mapping[str, Any], Sequence[Any]]


def _lv_to_datetime(timestamp_lv: Optional[float]) -> Optional[datetime]:
    """Convert a LabVIEW timestamp into a UTC datetime."""
    if timestamp_lv is None:
        return None
    try:
        return EOVSATime(float(timestamp_lv), format="lv").datetime
    except Exception:
        return None


def _decode_text(value: Any) -> Any:
    """Decode legacy byte strings and trim trailing null padding."""
    if isinstance(value, bytes):
        return value.split(b"\x00", 1)[0].decode("utf-8", errors="replace").strip()
    return value


def _pythonize(value: Any) -> Any:
    """Convert numpy-heavy values into plain Python structures."""
    value = _decode_text(value)
    if isinstance(value, np.ndarray):
        return [_pythonize(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, list):
        return [_pythonize(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_pythonize(item) for item in value)
    if isinstance(value, dict):
        return {key: _pythonize(val) for key, val in value.items()}
    return value


def _lookup(tree: PointerTree, path: Sequence[PathElement]) -> Any:
    """Look up a nested value in a mixed dict/list pointer tree."""
    node: Any = tree
    for element in path:
        if isinstance(node, Mapping):
            node = node[element]
        else:
            node = node[element]
    return node


def _safe_lookup(tree: PointerTree, path: Sequence[PathElement]) -> Any:
    """Look up a nested value and return ``None`` if the path is absent."""
    try:
        return _lookup(tree, path)
    except (KeyError, IndexError, TypeError):
        return None


def _extract_path(definition: "FrameDefinition", payload: bytes, path: Sequence[PathElement]) -> Any:
    """Extract one value from a frame payload using the XML pointer tree."""
    pointer = _safe_lookup(definition.pointers, path)
    if pointer is None:
        return None
    try:
        return _pythonize(extract_value(payload, pointer))
    except Exception:
        return None


def _string_or_none(value: Any) -> Optional[str]:
    value = _pythonize(value)
    if value in (None, ""):
        return None
    return str(value)


def _number_or_none(value: Any, scale: float = 1.0) -> Optional[float]:
    value = _pythonize(value)
    if value is None:
        return None
    try:
        return float(value) / scale
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> Optional[bool]:
    value = _pythonize(value)
    if value is None:
        return None
    return bool(value)


def _infer_xml_name(log_path: str, version: float) -> str:
    """Infer the companion XML filename from an ``sf_*`` or ``sh_*`` log."""
    base = os.path.basename(log_path)
    match = STATEFRAME_LOG_RE.match(base)
    if not match:
        raise ValueError(f"Unsupported log filename format: {base}")
    prefix = base.split("_", 1)[0]
    stem = "stateframe" if prefix == "sf" else "scanheader"
    return os.path.join(os.path.dirname(log_path), f"{stem}_v{int(version)}.00.xml")


@dataclass(frozen=True)
class FrameDefinition:
    """Versioned XML pointer tree for one binary frame layout.

    :param pointers: Nested pointer tree from :func:`eovsapy.read_xml2.xml_ptrs`.
    :type pointers: Mapping[str, Any]
    :param version: Stateframe or scanheader version carried by the XML.
    :type version: float
    :param xml_path: Source XML path, if known.
    :type xml_path: str | None
    """

    pointers: Mapping[str, Any]
    version: float
    xml_path: Optional[str] = None

    @classmethod
    def from_xml(cls, xml_path: str) -> "FrameDefinition":
        """Load a frame definition from a versioned XML file.

        :param xml_path: Path to the stateframe or scanheader XML file.
        :type xml_path: str
        :returns: Loaded frame definition.
        :rtype: FrameDefinition
        """
        pointers, version = xml_ptrs(xml_path)
        return cls(pointers=pointers, version=float(version), xml_path=xml_path)


@dataclass(frozen=True)
class BinaryFrame:
    """One raw binary log record plus source metadata."""

    payload: bytes
    timestamp_lv: float
    timestamp_utc: Optional[datetime]
    embedded_version: float
    record_size: int
    frame_index: int
    source_path: str
    definition: FrameDefinition


@dataclass(frozen=True)
class ACCStateframeConfig:
    """Connection/bootstrap details for live ACC stateframe reads."""

    host: str
    sfport: int
    binsize: int
    xml_path: Optional[str] = None
    scdport: Optional[int] = None
    scdsfport: Optional[int] = None
    version_hint: Optional[float] = None

    @classmethod
    def from_acc_ini(
        cls,
        acc_ini_path: str,
        *,
        xml_path: Optional[str] = None,
        host: str = "acc.solar.pvt",
    ) -> "ACCStateframeConfig":
        """Load live reader settings from an ACC.ini file.

        :param acc_ini_path: Local path or URL to ``acc.ini``.
        :type acc_ini_path: str
        :param xml_path: Explicit stateframe XML path. If omitted and the INI
            contains a template path, that value is preserved as a hint.
        :type xml_path: str | None
        :param host: Hostname to use for the live TCP connection.
        :type host: str
        :returns: Parsed live reader configuration.
        :rtype: ACCStateframeConfig
        """
        values = _parse_acc_ini_text(_read_text_from_path_or_url(acc_ini_path))
        return cls(
            host=host,
            sfport=values["sfport"],
            binsize=values["binsize"],
            xml_path=xml_path or values.get("xmlpath"),
            scdport=values.get("scdport"),
            scdsfport=values.get("scdsfport"),
        )


@dataclass(frozen=True)
class OperationalTelemetryRecord:
    """Normalized stateframe subset for operational telemetry export."""

    frame: BinaryFrame
    record: Dict[str, Any]


@dataclass(frozen=True)
class StructuredTelemetryRecord:
    """Full structured export for one parsed frame.

    :param frame: Raw frame metadata and payload envelope.
    :type frame: BinaryFrame
    :param record: Structured telemetry document with source/schema/timestamp
        metadata plus the parsed ACC payload in ``data``.
    :type record: dict
    """

    frame: BinaryFrame
    record: Dict[str, Any]


class BinaryFrameLogReader:
    """Iterate replayable/tailable binary frame logs.

    The reader is generic over ``sf_*.log`` and ``sh_*.log`` files. It only
    understands the binary record envelope and delegates field interpretation
    to the XML pointer tree.
    """

    def __init__(
        self,
        log_path: str,
        definition: FrameDefinition,
        *,
        follow: bool = False,
        poll_interval: float = 1.0,
    ) -> None:
        self.log_path = log_path
        self.definition = definition
        self.follow = follow
        self.poll_interval = poll_interval
        self._record_size: Optional[int] = None

    @classmethod
    def from_log(
        cls,
        log_path: str,
        *,
        xml_path: Optional[str] = None,
        follow: bool = False,
        poll_interval: float = 1.0,
    ) -> "BinaryFrameLogReader":
        """Construct a reader by loading or inferring the companion XML.

        :param log_path: Path to an ``sf_*.log`` or ``sh_*.log`` file.
        :type log_path: str
        :param xml_path: Explicit companion XML path. If omitted, infer it from
            the log filename and embedded version.
        :type xml_path: str | None
        :param follow: If ``True``, wait for appended records at EOF.
        :type follow: bool
        :param poll_interval: Poll interval in seconds when ``follow=True``.
        :type poll_interval: float
        :returns: Configured log reader.
        :rtype: BinaryFrameLogReader
        """
        version = cls.peek_embedded_version(log_path)
        if xml_path is None:
            xml_path = _infer_xml_name(log_path, version)
        definition = FrameDefinition.from_xml(xml_path)
        return cls(log_path, definition, follow=follow, poll_interval=poll_interval)

    @staticmethod
    def peek_embedded_version(log_path: str) -> float:
        """Read the embedded version from the first log record.

        :param log_path: Path to a binary frame log.
        :type log_path: str
        :returns: Embedded record version.
        :rtype: float
        """
        with open(log_path, "rb") as handle:
            header = handle.read(24)
        if len(header) < 16:
            raise ValueError(f"Log header is too short: {log_path}")
        return float(struct.unpack_from("<d", header, 8)[0])

    @property
    def record_size(self) -> int:
        """Return the fixed record size for this log."""
        if self._record_size is None:
            with open(self.log_path, "rb") as handle:
                header = handle.read(24)
            if len(header) < 20:
                raise ValueError(f"Log header is too short: {self.log_path}")
            self._record_size = int(struct.unpack_from("<i", header, 16)[0])
        return self._record_size

    def iter_frames(self) -> Iterator[BinaryFrame]:
        """Yield binary frames from the log.

        :returns: Iterator of raw frame records.
        :rtype: Iterator[BinaryFrame]
        """
        frame_index = 0
        record_size = self.record_size
        with open(self.log_path, "rb") as handle:
            while True:
                payload = handle.read(record_size)
                if len(payload) == record_size:
                    timestamp_lv = float(struct.unpack_from("<d", payload, 0)[0])
                    embedded_version = float(struct.unpack_from("<d", payload, 8)[0])
                    yield BinaryFrame(
                        payload=payload,
                        timestamp_lv=timestamp_lv,
                        timestamp_utc=_lv_to_datetime(timestamp_lv),
                        embedded_version=embedded_version,
                        record_size=record_size,
                        frame_index=frame_index,
                        source_path=self.log_path,
                        definition=self.definition,
                    )
                    frame_index += 1
                    continue
                if len(payload) == 0 and self.follow:
                    time.sleep(self.poll_interval)
                    continue
                if len(payload) != 0:
                    raise ValueError(
                        f"Short trailing record in {self.log_path}: "
                        f"expected {record_size} bytes, got {len(payload)}"
                    )
                break


class LiveStateframeReader:
    """Read live stateframe frames directly from the ACC stateframe socket."""

    def __init__(
        self,
        config: ACCStateframeConfig,
        definition: FrameDefinition,
        *,
        sf_num: int = 1,
        timeout: float = 0.5,
    ) -> None:
        self.config = config
        self.definition = definition
        self.sf_num = sf_num
        self.timeout = timeout
        self._frame_index = 0

    @classmethod
    def from_acc_ini(
        cls,
        acc_ini_path: str,
        *,
        xml_path: Optional[str] = None,
        host: str = "acc.solar.pvt",
        sf_num: int = 1,
        timeout: float = 0.5,
    ) -> "LiveStateframeReader":
        """Construct a live reader from ``acc.ini`` and a versioned XML file."""
        config = ACCStateframeConfig.from_acc_ini(acc_ini_path, xml_path=xml_path, host=host)
        if not config.xml_path:
            raise ValueError("xml_path is required for live reading when acc.ini does not provide a usable path")
        definition = FrameDefinition.from_xml(config.xml_path)
        return cls(config, definition, sf_num=sf_num, timeout=timeout)

    def read_frame(self) -> BinaryFrame:
        """Read one live stateframe record from the ACC socket.

        :returns: One raw live frame.
        :rtype: BinaryFrame
        :raises TimeoutError: If the ACC socket read times out.
        :raises ConnectionError: If the socket cannot be read completely.
        """
        payload = self._read_payload()
        timestamp_lv = float(struct.unpack_from("<d", payload, 0)[0])
        embedded_version = float(struct.unpack_from("<d", payload, 8)[0])
        frame = BinaryFrame(
            payload=payload,
            timestamp_lv=timestamp_lv,
            timestamp_utc=_lv_to_datetime(timestamp_lv),
            embedded_version=embedded_version,
            record_size=self.config.binsize,
            frame_index=self._frame_index,
            source_path=f"tcp://{self.config.host}:{self.config.sfport}",
            definition=self.definition,
        )
        self._frame_index += 1
        return frame

    def iter_frames(self, *, poll_interval: float = 1.0) -> Iterator[BinaryFrame]:
        """Yield live frames forever, sleeping between polls."""
        while True:
            try:
                yield self.read_frame()
            except (TimeoutError, ConnectionError):
                if poll_interval > 0:
                    time.sleep(poll_interval)
                continue
            if poll_interval > 0:
                time.sleep(poll_interval)

    def _read_payload(self) -> bytes:
        expected = self.config.binsize
        request = struct.pack(">i", self.sf_num)
        chunks: List[bytes] = []
        total = 0
        try:
            with socket.create_connection(
                (self.config.host, self.config.sfport),
                timeout=self.timeout,
            ) as conn:
                conn.settimeout(self.timeout)
                conn.sendall(request)
                while total < expected:
                    chunk = conn.recv(expected - total)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)
        except socket.timeout as exc:
            raise TimeoutError(
                f"Timed out reading stateframe from {self.config.host}:{self.config.sfport}"
            ) from exc
        except OSError as exc:
            raise ConnectionError(
                f"Cannot read stateframe from {self.config.host}:{self.config.sfport}"
            ) from exc

        if total != expected:
            raise ConnectionError(
                f"Short stateframe read from {self.config.host}:{self.config.sfport}: "
                f"expected {expected} bytes, got {total}"
            )
        return b"".join(chunks)


class StateframeOperationalTelemetryNormalizer:
    """Normalize a selected operational telemetry subset from stateframe frames."""

    def normalize_frame(self, frame: BinaryFrame) -> OperationalTelemetryRecord:
        """Normalize one raw stateframe frame.

        :param frame: Raw frame from :class:`BinaryFrameLogReader`.
        :type frame: BinaryFrame
        :returns: Normalized operational telemetry record.
        :rtype: OperationalTelemetryRecord
        """
        definition = frame.definition
        payload = frame.payload
        antennas = self._extract_antennas(definition, payload)
        record = {
            "source": {
                "kind": "live_acc_stateframe" if frame.source_path.startswith("tcp://") else "stateframe_log",
                "source_path": frame.source_path,
                "xml_path": definition.xml_path,
                "frame_index": frame.frame_index,
            },
            "schema": {
                "stateframe_version": frame.embedded_version,
                "definition_version": definition.version,
                "record_size_bytes": frame.record_size,
            },
            "timestamp": {
                "labview": frame.timestamp_lv,
                "iso_utc": frame.timestamp_utc.isoformat() if frame.timestamp_utc else None,
            },
            "schedule": {
                "task": _string_or_none(_extract_path(definition, payload, ("Schedule", "Task"))),
                "scan_state": _pythonize(_extract_path(definition, payload, ("Schedule", "Data", "ScanState"))),
                "phase_tracking": _pythonize(
                    _extract_path(definition, payload, ("Schedule", "Data", "PhaseTracking"))
                ),
                "subarray1": _pythonize(_extract_path(definition, payload, ("LODM", "Subarray1"))),
                "subarray2": _pythonize(_extract_path(definition, payload, ("LODM", "Subarray2"))),
                "run_mode_summary": self._run_mode_summary(antennas),
            },
            "weather": self._extract_weather(definition, payload),
            "lo": self._extract_lo(definition, payload),
            "fem_banks": self._extract_fem_banks(definition, payload),
            "power": self._extract_power(definition, payload),
            "antennas": antennas,
        }
        return OperationalTelemetryRecord(frame=frame, record=record)

    def normalize_frames(self, frames: Iterable[BinaryFrame]) -> Iterator[OperationalTelemetryRecord]:
        """Normalize an iterable of raw frames."""
        for frame in frames:
            yield self.normalize_frame(frame)

    def _extract_weather(self, definition: FrameDefinition, payload: bytes) -> Dict[str, Any]:
        weather_path = ("Schedule", "Data", "Weather")
        fields = [
            "Wind",
            "WindDirection",
            "AvgWind",
            "AvgWindDirection",
            "AvgWindGust",
            "Temperature",
            "Pressure",
            "Humidity",
            "RainRate",
            "RainToday",
        ]
        return {
            field.lower(): _pythonize(_extract_path(definition, payload, weather_path + (field,)))
            for field in fields
        }

    def _extract_lo(self, definition: FrameDefinition, payload: bytes) -> Dict[str, Any]:
        def _block(base: str) -> Dict[str, Any]:
            return {
                "stb": _pythonize(_extract_path(definition, payload, ("LODM", base, "STB"))),
                "esr": _pythonize(_extract_path(definition, payload, ("LODM", base, "ESR"))),
                "sweep_status": _pythonize(
                    _extract_path(definition, payload, ("LODM", base, "SweepStatus"))
                ),
                "err": _pythonize(_extract_path(definition, payload, ("LODM", base, "ERR"))),
                "fseqfile": _string_or_none(
                    _extract_path(definition, payload, ("LODM", base, "FSeqFile"))
                ),
                "comm_err": _pythonize(_extract_path(definition, payload, ("LODM", base, "CommErr"))),
            }

        return {
            "lo1a": _block("LO1A"),
            "lo1b": _block("LO1B"),
            "lo2_lock": _pythonize(_extract_path(definition, payload, ("LODM", "LO2_Lock"))),
            "comm_err": _pythonize(_extract_path(definition, payload, ("LODM", "CommErr"))),
            "version": _pythonize(_extract_path(definition, payload, ("LODM", "Version"))),
        }

    def _extract_fem_banks(self, definition: FrameDefinition, payload: bytes) -> Dict[str, Any]:
        def _bank(name: str) -> Dict[str, Any]:
            prefix = (name,)
            return {
                "timestamp_labview": _pythonize(_extract_path(definition, payload, prefix + ("Timestamp",))),
                "version": _pythonize(_extract_path(definition, payload, prefix + ("Version",))),
                "power_strip": {
                    "rf_switch_status": _pythonize(
                        _extract_path(definition, payload, prefix + ("PowerStrip", "RFSwitchStatus"))
                    ),
                    "computer_status": _pythonize(
                        _extract_path(definition, payload, prefix + ("PowerStrip", "ComputerStatus"))
                    ),
                    "volts": _pythonize(_extract_path(definition, payload, prefix + ("PowerStrip", "Volts"))),
                    "current": _pythonize(_extract_path(definition, payload, prefix + ("PowerStrip", "Current"))),
                },
                "thermal": {
                    "first_stage_temp": _pythonize(
                        _extract_path(definition, payload, prefix + ("Thermal", "FirstStageTemp"))
                    ),
                    "second_stage_temp": _pythonize(
                        _extract_path(definition, payload, prefix + ("Thermal", "SecondStageTemp"))
                    ),
                    "focus_box_temp": _pythonize(
                        _extract_path(definition, payload, prefix + ("Thermal", "FocusBoxTemp"))
                    ),
                    "radiation_shield_temp": _pythonize(
                        _extract_path(definition, payload, prefix + ("Thermal", "RadiationShieldTemp"))
                    ),
                },
                "receiver": {
                    "lo_freq_enabled": _pythonize(
                        _extract_path(definition, payload, prefix + ("Receiver", "LoFreqEnabled"))
                    ),
                    "hi_freq_enabled": _pythonize(
                        _extract_path(definition, payload, prefix + ("Receiver", "HiFreqEnabled"))
                    ),
                    "noise_diode_enabled": _pythonize(
                        _extract_path(definition, payload, prefix + ("Receiver", "NoiseDiodeEnabled"))
                    ),
                },
                "frm_servo": {
                    "homed": _pythonize(_extract_path(definition, payload, prefix + ("FRMServo", "Homed"))),
                    "selected_rx": _pythonize(
                        _extract_path(definition, payload, prefix + ("FRMServo", "SelectedRx"))
                    ),
                    "position_angle": _pythonize(
                        _extract_path(definition, payload, prefix + ("FRMServo", "PositionAngle"))
                    ),
                },
            }

        return {"fema": _bank("FEMA"), "femb": _bank("FEMB")}

    def _extract_power(self, definition: FrameDefinition, payload: bytes) -> Dict[str, Any]:
        solar_power = []
        solar_path = _safe_lookup(definition.pointers, ("Schedule", "Data", "SolarPower"))
        if isinstance(solar_path, list):
            for index in range(len(solar_path)):
                prefix = ("Schedule", "Data", "SolarPower", index)
                solar_power.append(
                    {
                        "source_index": index,
                        "timestamp_labview": _pythonize(
                            _extract_path(definition, payload, prefix + ("Timestamp",))
                        ),
                        "charge": _pythonize(_extract_path(definition, payload, prefix + ("Charge",))),
                        "volts": _pythonize(_extract_path(definition, payload, prefix + ("Volts",))),
                        "amps": _pythonize(_extract_path(definition, payload, prefix + ("Amps",))),
                        "battery_temp": _pythonize(
                            _extract_path(definition, payload, prefix + ("BatteryTemp",))
                        ),
                    }
                )

        roach = []
        roach_path = _safe_lookup(definition.pointers, ("Schedule", "Data", "Roach"))
        if isinstance(roach_path, list):
            for index in range(len(roach_path)):
                prefix = ("Schedule", "Data", "Roach", index)
                roach.append(
                    {
                        "roach_index": index,
                        "status": _pythonize(_extract_path(definition, payload, prefix + ("Status",))),
                        "temp_ambient": _pythonize(
                            _extract_path(definition, payload, prefix + ("Temp.ambient",))
                        ),
                        "temp_fpga": _pythonize(_extract_path(definition, payload, prefix + ("Temp.fpga",))),
                        "voltage_12v": _pythonize(
                            _extract_path(definition, payload, prefix + ("Voltage.12v",))
                        ),
                        "current_12v": _pythonize(
                            _extract_path(definition, payload, prefix + ("Current.12v",))
                        ),
                    }
                )

        return {"solar_power": solar_power, "roach": roach}

    def _extract_antennas(self, definition: FrameDefinition, payload: bytes) -> List[Dict[str, Any]]:
        antenna_defs = _safe_lookup(definition.pointers, ("Antenna",))
        schedule_az = _extract_path(definition, payload, ("Schedule", "Data", "Azimuth"))
        schedule_el = _extract_path(definition, payload, ("Schedule", "Data", "Elevation"))
        schedule_track = _extract_path(definition, payload, ("Schedule", "Data", "TrackFlag"))
        if not isinstance(antenna_defs, list):
            return []

        dcm_defs = _safe_lookup(definition.pointers, ("DCM",))
        antennas: List[Dict[str, Any]] = []
        for index in range(len(antenna_defs)):
            ctrl = ("Antenna", index, "Controller")
            frontend = ("Antenna", index, "Frontend")
            requested_az = self._array_value(schedule_az, index)
            requested_el = self._array_value(schedule_el, index)
            actual_az = _number_or_none(
                _extract_path(definition, payload, ctrl + ("Azimuth1",)), scale=10000.0
            )
            actual_el = _number_or_none(
                _extract_path(definition, payload, ctrl + ("Elevation1",)), scale=10000.0
            )
            track_flag = self._array_value(schedule_track, index)
            dcm_prefix = ("DCM", index) if isinstance(dcm_defs, list) and index < len(dcm_defs) else None
            antennas.append(
                {
                    "antenna": index + 1,
                    "run_mode": _pythonize(_extract_path(definition, payload, ctrl + ("RunMode",))),
                    "requested_azimuth_deg": requested_az,
                    "requested_elevation_deg": requested_el,
                    "actual_azimuth_deg": actual_az,
                    "actual_elevation_deg": actual_el,
                    "delta_azimuth_deg": self._delta(actual_az, requested_az),
                    "delta_elevation_deg": self._delta(actual_el, requested_el),
                    "track_flag": _pythonize(track_flag),
                    "track_source_flag": self._track_source_flag(definition, payload, ctrl),
                    "frontend": {
                        "fem_temperature": _pythonize(
                            _extract_path(definition, payload, frontend + ("FEM", "Temperature"))
                        ),
                        "fem_comm_err": _pythonize(
                            _extract_path(definition, payload, frontend + ("FEM", "CommErr"))
                        ),
                        "tec_temperature": _pythonize(
                            _extract_path(definition, payload, frontend + ("TEC", "Temperature"))
                        ),
                        "tec_input_voltage": _pythonize(
                            _extract_path(definition, payload, frontend + ("TEC", "InputVoltage"))
                        ),
                        "tec_main_current": _pythonize(
                            _extract_path(definition, payload, frontend + ("TEC", "MainCurrent"))
                        ),
                        "tec_alarm": _pythonize(
                            _extract_path(definition, payload, frontend + ("TEC", "Alarm"))
                        ),
                        "tec_error": _pythonize(
                            _extract_path(definition, payload, frontend + ("TEC", "Error"))
                        ),
                        "bright_scram_active": _pythonize(
                            _extract_path(definition, payload, frontend + ("BrightScram", "Active"))
                        ),
                        "bright_scram_state": _pythonize(
                            _extract_path(definition, payload, frontend + ("BrightScram", "State"))
                        ),
                        "wind_scram_active": _pythonize(
                            _extract_path(definition, payload, frontend + ("WindScram", "Active"))
                        ),
                        "wind_scram_state": _pythonize(
                            _extract_path(definition, payload, frontend + ("WindScram", "State"))
                        ),
                    },
                    "dcm": None
                    if dcm_prefix is None
                    else {
                        "mode": _pythonize(_extract_path(definition, payload, dcm_prefix + ("Mode",))),
                        "slot": _pythonize(_extract_path(definition, payload, dcm_prefix + ("Slot",))),
                        "offset_attn": _pythonize(
                            _extract_path(definition, payload, dcm_prefix + ("Offset_Attn",))
                        ),
                        "vpol": _pythonize(_extract_path(definition, payload, dcm_prefix + ("VPol",))),
                        "hpol": _pythonize(_extract_path(definition, payload, dcm_prefix + ("HPol",))),
                        "comm_err": _pythonize(_extract_path(definition, payload, dcm_prefix + ("CommErr",))),
                    },
                }
            )
        return antennas

    @staticmethod
    def _array_value(values: Any, index: int) -> Any:
        values = _pythonize(values)
        if not isinstance(values, list) or index >= len(values):
            return None
        return values[index]

    @staticmethod
    def _delta(actual: Optional[float], requested: Optional[float]) -> Optional[float]:
        if actual is None or requested is None:
            return None
        return actual - requested

    @staticmethod
    def _run_mode_summary(antennas: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
        summary: Dict[str, int] = {}
        for antenna in antennas:
            key = str(antenna.get("run_mode"))
            summary[key] = summary.get(key, 0) + 1
        return summary

    def _track_source_flag(
        self,
        definition: FrameDefinition,
        payload: bytes,
        controller_prefix: Sequence[PathElement],
    ) -> Optional[bool]:
        offsets = []
        for field in ("RAOffset", "DecOffset", "ElOffset", "AzOffset"):
            offsets.append(_number_or_none(_extract_path(definition, payload, tuple(controller_prefix) + (field,))))
        if any(value is None for value in offsets):
            return None
        return bool(sum(offsets) == 0.0)


def _is_pointer_leaf(node: Any) -> bool:
    """Return whether a pointer-tree node is a leaf pointer definition."""
    return (
        isinstance(node, (list, tuple))
        and len(node) >= 2
        and isinstance(node[0], str)
        and isinstance(node[1], int)
    )


def _extract_pointer_tree(node: Any, payload: bytes) -> Any:
    """Recursively extract a full parsed subtree from one pointer-tree node.

    :param node: Pointer-tree node from :func:`eovsapy.read_xml2.xml_ptrs`.
    :type node: Any
    :param payload: Raw frame payload.
    :type payload: bytes
    :returns: Parsed Python structure for the node.
    :rtype: Any
    """
    if _is_pointer_leaf(node):
        try:
            return _pythonize(extract_value(payload, node))
        except Exception:
            return None
    if isinstance(node, Mapping):
        return {key: _extract_pointer_tree(value, payload) for key, value in node.items()}
    if isinstance(node, list):
        return [_extract_pointer_tree(item, payload) for item in node]
    if isinstance(node, tuple):
        return tuple(_extract_pointer_tree(item, payload) for item in node)
    return None


class FullFrameTelemetryNormalizer:
    """Normalize one frame into a full structured telemetry document.

    This exporter preserves the complete parsed payload behind a stable
    envelope so newer collector paths can build entity-oriented measurements
    without changing the live reader API.

    :param frame_kind: Logical frame kind for the export. This defaults to
        ``"stateframe"`` for the current live reader path.
    :type frame_kind: str
    """

    def __init__(self, frame_kind: str = "stateframe") -> None:
        self.frame_kind = frame_kind

    def normalize_frame(self, frame: BinaryFrame) -> StructuredTelemetryRecord:
        """Normalize one raw frame into the full structured export shape.

        :param frame: Raw frame from :class:`BinaryFrameLogReader` or
            :class:`LiveStateframeReader`.
        :type frame: BinaryFrame
        :returns: Structured full-tree export document.
        :rtype: StructuredTelemetryRecord
        """
        definition = frame.definition
        record = {
            "source": {
                "kind": "live_acc_stateframe" if frame.source_path.startswith("tcp://") else "stateframe_log",
                "frame_kind": self.frame_kind,
                "source_path": frame.source_path,
                "xml_path": definition.xml_path,
                "frame_index": frame.frame_index,
            },
            "schema": {
                "stateframe_version": frame.embedded_version,
                "definition_version": definition.version,
                "record_size_bytes": frame.record_size,
            },
            "timestamp": {
                "labview": frame.timestamp_lv,
                "iso_utc": frame.timestamp_utc.isoformat() if frame.timestamp_utc else None,
            },
            "data": _extract_pointer_tree(definition.pointers, frame.payload),
        }
        return StructuredTelemetryRecord(frame=frame, record=record)

    def normalize_frames(self, frames: Iterable[BinaryFrame]) -> Iterator[StructuredTelemetryRecord]:
        """Normalize an iterable of raw frames into full structured exports.

        :param frames: Iterable of raw frames.
        :type frames: Iterable[BinaryFrame]
        :returns: Iterator of structured records.
        :rtype: Iterator[StructuredTelemetryRecord]
        """
        for frame in frames:
            yield self.normalize_frame(frame)


def _sanitize_metric_name(name: Any) -> str:
    """Normalize a measurement/tag/field component into ASCII-ish form."""
    text = re.sub(r"[^0-9A-Za-z]+", "_", str(name)).strip("_").lower()
    return text or "value"


def _entity_tag_name(path_segment: str) -> str:
    """Return the tag key used for indexed entities under one list path."""
    return _sanitize_metric_name(path_segment)


def _measurement_for_path(prefix: str, path: Sequence[str]) -> str:
    """Build one entity measurement name from a parsed path."""
    if not path:
        return prefix
    return prefix + "_" + "_".join(_sanitize_metric_name(segment) for segment in path)


def _entity_field_value(value: Any) -> Optional[Any]:
    """Convert one parsed value into a measurement field candidate."""
    value = _pythonize(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value):
            return None
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _is_mapping_sequence(value: Any) -> bool:
    """Return whether a value is a non-empty sequence of mappings."""
    return isinstance(value, list) and bool(value) and all(isinstance(item, Mapping) for item in value)


def _collect_entity_records(
    node: Mapping[str, Any],
    *,
    path: Sequence[str],
    measurement_prefix: str,
    timestamp_iso: Optional[str],
    common_tags: Mapping[str, str],
    tags: Mapping[str, str],
    records: List[Dict[str, Any]],
) -> None:
    """Recursively build entity-oriented measurement records from one mapping."""
    scalar_fields: Dict[str, Any] = {}
    for key, value in node.items():
        if isinstance(value, Mapping):
            _collect_entity_records(
                value,
                path=tuple(path) + (str(key),),
                measurement_prefix=measurement_prefix,
                timestamp_iso=timestamp_iso,
                common_tags=common_tags,
                tags=tags,
                records=records,
            )
            continue
        if _is_mapping_sequence(value):
            tag_key = _entity_tag_name(str(key))
            for index, item in enumerate(value, start=1):
                child_tags = dict(tags)
                child_tags[tag_key] = str(index)
                _collect_entity_records(
                    item,
                    path=tuple(path) + (str(key),),
                    measurement_prefix=measurement_prefix,
                    timestamp_iso=timestamp_iso,
                    common_tags=common_tags,
                    tags=child_tags,
                    records=records,
                )
            continue
        normalized = _entity_field_value(value)
        if normalized is not None:
            scalar_fields[_sanitize_metric_name(key)] = normalized

    # Skip emitting a root-level catchall measurement. The supported Influx
    # layout is entity-oriented (`eovsa_stateframe_*`), while top-level scalar
    # metadata is already available through subsystem measurements and tags.
    if scalar_fields and path:
        record_tags = dict(common_tags)
        record_tags.update(tags)
        records.append(
            {
                "measurement": _measurement_for_path(measurement_prefix, path),
                "time": timestamp_iso,
                "tags": record_tags,
                "fields": scalar_fields,
            }
        )


def build_entity_records(
    record: Mapping[str, Any],
    *,
    measurement_prefix: str = "eovsa_stateframe",
) -> List[Dict[str, Any]]:
    """Build entity-oriented measurement records from a full structured export.

    :param record: Structured telemetry record produced by
        :class:`FullFrameTelemetryNormalizer`.
    :type record: Mapping[str, Any]
    :param measurement_prefix: Prefix for generated measurement names.
    :type measurement_prefix: str
    :returns: List of entity-oriented measurement documents.
    :rtype: list[dict]
    """
    data = record.get("data")
    if not isinstance(data, Mapping):
        return []
    source = record.get("source", {})
    schema = record.get("schema", {})
    timestamp = record.get("timestamp", {})
    common_tags = {
        "source_kind": str(source.get("kind", "")),
        "source_path": str(source.get("source_path", "")),
        "frame_kind": str(source.get("frame_kind", "")),
        "stateframe_version": str(schema.get("stateframe_version", "")),
        "definition_version": str(schema.get("definition_version", "")),
    }
    records: List[Dict[str, Any]] = []
    _collect_entity_records(
        data,
        path=(),
        measurement_prefix=measurement_prefix,
        timestamp_iso=timestamp.get("iso_utc") if isinstance(timestamp, Mapping) else None,
        common_tags=common_tags,
        tags={},
        records=records,
    )
    return records


def iter_log_frames(
    log_path: str,
    *,
    xml_path: Optional[str] = None,
    follow: bool = False,
    poll_interval: float = 1.0,
) -> Iterator[BinaryFrame]:
    """Convenience wrapper for iterating raw binary log frames."""
    reader = BinaryFrameLogReader.from_log(
        log_path,
        xml_path=xml_path,
        follow=follow,
        poll_interval=poll_interval,
    )
    yield from reader.iter_frames()


def iter_live_frames(
    acc_ini_path: str,
    *,
    xml_path: Optional[str] = None,
    host: str = "acc.solar.pvt",
    sf_num: int = 1,
    timeout: float = 0.5,
    poll_interval: float = 1.0,
) -> Iterator[BinaryFrame]:
    """Convenience wrapper for iterating live ACC stateframe frames."""
    reader = LiveStateframeReader.from_acc_ini(
        acc_ini_path,
        xml_path=xml_path,
        host=host,
        sf_num=sf_num,
        timeout=timeout,
    )
    yield from reader.iter_frames(poll_interval=poll_interval)


def iter_operational_telemetry(
    log_path: str,
    *,
    xml_path: Optional[str] = None,
    follow: bool = False,
    poll_interval: float = 1.0,
) -> Iterator[Dict[str, Any]]:
    """Iterate normalized operational telemetry records from a stateframe log.

    :param log_path: Path to an ``sf_*.log`` file.
    :type log_path: str
    :param xml_path: Explicit stateframe XML path. If omitted, infer it from
        the log filename and embedded version.
    :type xml_path: str | None
    :param follow: If ``True``, keep tailing for appended records.
    :type follow: bool
    :param poll_interval: Poll interval in seconds when ``follow=True``.
    :type poll_interval: float
    :returns: Iterator of normalized telemetry dictionaries.
    :rtype: Iterator[dict]
    """
    normalizer = StateframeOperationalTelemetryNormalizer()
    for frame in iter_log_frames(
        log_path,
        xml_path=xml_path,
        follow=follow,
        poll_interval=poll_interval,
    ):
        yield normalizer.normalize_frame(frame).record


def iter_live_operational_telemetry(
    acc_ini_path: str,
    *,
    xml_path: Optional[str] = None,
    host: str = "acc.solar.pvt",
    sf_num: int = 1,
    timeout: float = 0.5,
    poll_interval: float = 1.0,
) -> Iterator[Dict[str, Any]]:
    """Iterate normalized operational telemetry records from live ACC reads."""
    normalizer = StateframeOperationalTelemetryNormalizer()
    for frame in iter_live_frames(
        acc_ini_path,
        xml_path=xml_path,
        host=host,
        sf_num=sf_num,
        timeout=timeout,
        poll_interval=poll_interval,
    ):
        yield normalizer.normalize_frame(frame).record


def iter_full_frame_telemetry(
    log_path: str,
    *,
    xml_path: Optional[str] = None,
    follow: bool = False,
    poll_interval: float = 1.0,
    frame_kind: str = "stateframe",
) -> Iterator[Dict[str, Any]]:
    """Iterate full structured telemetry records from a frame log.

    :param log_path: Path to an ``sf_*.log`` file.
    :type log_path: str
    :param xml_path: Explicit stateframe XML path.
    :type xml_path: str | None
    :param follow: If ``True``, keep tailing for appended records.
    :type follow: bool
    :param poll_interval: Poll interval in seconds when ``follow=True``.
    :type poll_interval: float
    :param frame_kind: Logical frame kind label stored in the envelope.
    :type frame_kind: str
    :returns: Iterator of full structured telemetry dictionaries.
    :rtype: Iterator[dict]
    """
    normalizer = FullFrameTelemetryNormalizer(frame_kind=frame_kind)
    for frame in iter_log_frames(
        log_path,
        xml_path=xml_path,
        follow=follow,
        poll_interval=poll_interval,
    ):
        yield normalizer.normalize_frame(frame).record


def iter_live_full_frame_telemetry(
    acc_ini_path: str,
    *,
    xml_path: Optional[str] = None,
    host: str = "acc.solar.pvt",
    sf_num: int = 1,
    timeout: float = 0.5,
    poll_interval: float = 1.0,
    frame_kind: str = "stateframe",
) -> Iterator[Dict[str, Any]]:
    """Iterate full structured telemetry records from live ACC reads.

    :param acc_ini_path: Path or URL to ``acc.ini``.
    :type acc_ini_path: str
    :param xml_path: Explicit stateframe XML path.
    :type xml_path: str | None
    :param host: ACC hostname for the TCP socket.
    :type host: str
    :param sf_num: Requested stateframe number.
    :type sf_num: int
    :param timeout: Socket connect/read timeout in seconds.
    :type timeout: float
    :param poll_interval: Time between read attempts.
    :type poll_interval: float
    :param frame_kind: Logical frame kind label stored in the envelope.
    :type frame_kind: str
    :returns: Iterator of full structured telemetry dictionaries.
    :rtype: Iterator[dict]
    """
    normalizer = FullFrameTelemetryNormalizer(frame_kind=frame_kind)
    for frame in iter_live_frames(
        acc_ini_path,
        xml_path=xml_path,
        host=host,
        sf_num=sf_num,
        timeout=timeout,
        poll_interval=poll_interval,
    ):
        yield normalizer.normalize_frame(frame).record


def _read_text_from_path_or_url(path_or_url: str) -> str:
    """Read UTF-8-ish text from a local path or URL."""
    if re.match(r"^[a-z]+://", path_or_url):
        with urllib.request.urlopen(path_or_url, timeout=1.0) as response:
            return response.read().decode("utf-8", errors="replace")
    with open(path_or_url, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _parse_acc_ini_text(text: str) -> Dict[str, Any]:
    """Parse the small subset of ACC.ini needed for live stateframe reads."""
    section = None
    values: Dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(";") or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line
            continue
        if "=" not in line:
            continue
        key, value = [item.strip() for item in line.split("=", 1)]
        if section == "[Stateframe]":
            if key.lower() == "bin size":
                values["binsize"] = int(value)
            elif key.lower() == "template path":
                values["xmlpath"] = value
        elif section == "[Network]":
            if key == "TCP.schedule.port":
                values["scdport"] = int(value)
            elif key == "TCP.stateframe.port":
                values["sfport"] = int(value)
            elif key == "TCP.schedule.stateframe.port":
                values["scdsfport"] = int(value)
    if "binsize" not in values or "sfport" not in values:
        raise ValueError("ACC.ini did not contain the required Stateframe/Network entries")
    return values


__all__ = [
    "ACCStateframeConfig",
    "BinaryFrame",
    "BinaryFrameLogReader",
    "FrameDefinition",
    "FullFrameTelemetryNormalizer",
    "LiveStateframeReader",
    "OperationalTelemetryRecord",
    "StateframeOperationalTelemetryNormalizer",
    "StructuredTelemetryRecord",
    "build_entity_records",
    "iter_full_frame_telemetry",
    "iter_live_full_frame_telemetry",
    "iter_live_frames",
    "iter_live_operational_telemetry",
    "iter_log_frames",
    "iter_operational_telemetry",
]
