"""External telemetry adapters for sources that do not originate in ACC."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import html
import http.client
import json
import re
import subprocess
import time
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse
import urllib.request
import xml.etree.ElementTree as ET


def _utc_now() -> datetime:
    """Return the current UTC time."""
    return datetime.now(timezone.utc)


def _coerce_float(value: Any) -> Optional[float]:
    """Convert a scalar-like value into a float when possible."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    """Convert a scalar-like value into an int when possible."""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_celsius(text: Any) -> Optional[float]:
    """Extract the Celsius value from a Magnum temperature string."""
    if text in (None, ""):
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*(?:°|&deg;)C", html.unescape(str(text)))
    if not match:
        return None
    return _coerce_float(match.group(1))


def _parse_magnum_timestamp(packet_date_local: Any, timezone_name: Any) -> Optional[str]:
    """Parse the Magnum packet timestamp into ISO UTC."""
    if packet_date_local in (None, ""):
        return None
    timestamp_text = str(packet_date_local).strip()
    try:
        moment = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    tz_name = str(timezone_name or "").strip().upper()
    if tz_name == "UTC":
        moment = moment.replace(tzinfo=timezone.utc)
    else:
        # Preserve a useful timestamp even if the provider stops labelling UTC.
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.isoformat()


def _load_json(url: str, *, timeout: float) -> Mapping[str, Any]:
    """Fetch one JSON document from a URL."""
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "eovsapy-external-source/1.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _load_text(url: str, *, timeout: float) -> str:
    """Fetch one text document from a URL."""
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "eovsapy-external-source/1.0",
            "Accept": "application/xml,text/xml,text/plain,*/*",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except http.client.BadStatusLine:
        return _load_text_via_curl(url, timeout=timeout)


def _load_text_via_curl(url: str, *, timeout: float) -> str:
    """Fetch one text document using curl for broken HTTP endpoints."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise RuntimeError(f"curl fallback only supports HTTP(S) URLs: {url}")
    completed = subprocess.run(
        # Some legacy device endpoints return useful XML bodies with malformed
        # HTTP framing. Accept the body and let the XML parser validate it.
        ["curl", "--http0.9", "-sS", url],
        capture_output=True,
        text=True,
        timeout=max(timeout, 1.0) + 1.0,
    )
    if completed.stdout:
        return completed.stdout
    raise RuntimeError(
        f"curl fallback returned no body for {url} "
        f"(exit={completed.returncode}, stderr={completed.stderr.strip()!r})"
    )


def _parse_weather_timestamp(value: Any) -> Optional[str]:
    """Parse the legacy weather station timestamp into UTC ISO format."""
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            moment = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return moment.isoformat()
        except ValueError:
            continue
    return None


def _extract_first_float(text: Any) -> Optional[float]:
    """Extract the first float-like token from arbitrary text."""
    if text in (None, ""):
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", str(text))
    if not match:
        return None
    return _coerce_float(match.group(0))


def _find_first_numeric_xml_value(root: ET.Element, tags: Sequence[str]) -> Optional[float]:
    """Return the first numeric text value found among a list of XML tags."""
    for tag in tags:
        element = root.find(tag)
        if element is None:
            continue
        value = _coerce_float((element.text or "").strip())
        if value is not None:
            return value
    return None


class ExternalSourceAdapter(ABC):
    """Base class for non-ACC telemetry source adapters."""

    group_name: str = "external"

    def __init__(self, *, poll_interval: float) -> None:
        self.poll_interval = max(float(poll_interval), 0.0)
        self._next_poll_monotonic = 0.0
        self._latest_snapshot: Dict[str, Any] = {"status": "never_polled"}
        self._latest_record: Optional[Dict[str, Any]] = None

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return a stable source identifier."""

    @property
    def latest_snapshot(self) -> Dict[str, Any]:
        """Return the latest adapter snapshot."""
        return self._latest_snapshot

    @property
    def latest_record(self) -> Optional[Dict[str, Any]]:
        """Return the latest emitted-style record for display/state purposes."""
        return self._latest_record

    def poll(self, *, now_monotonic: Optional[float] = None) -> List[Dict[str, Any]]:
        """Poll the source if due and return newly-emitted records."""
        current = time.monotonic() if now_monotonic is None else float(now_monotonic)
        if current < self._next_poll_monotonic:
            return []
        self._next_poll_monotonic = current + self.poll_interval
        return self._poll_impl()

    @abstractmethod
    def _poll_impl(self) -> List[Dict[str, Any]]:
        """Implement one source-specific poll cycle."""


@dataclass(frozen=True)
class SolarPowerStationConfig:
    """Configuration for one external Magnum solar power station."""

    station_id: str
    station_name: str
    json_url: str
    page_url: str
    timeout: float = 10.0
    stale_after_s: float = 300.0


@dataclass(frozen=True)
class WeatherStationConfig:
    """Configuration for the OVRO external weather source."""

    xml_url: str = "http://wx.cm.pvt/latestsampledata.xml"
    timeout: float = 2.0
    stale_after_s: float = 300.0


@dataclass(frozen=True)
class ControlRoomTempConfig:
    """Configuration for the external control-room temperature source."""

    xml_url: str = "http://192.168.24.233/state.xml"
    timeout: float = 2.0
    stale_after_s: float = 300.0


@dataclass(frozen=True)
class RoachSensorConfig:
    """Configuration for one ROACH sensor adapter."""

    host: str
    timeout: float = 3.0


class SolarPowerStationAdapter(ExternalSourceAdapter):
    """Fetch and normalize one Magnum Energy solar power station feed."""

    group_name = "solar_power"

    def __init__(
        self,
        config: SolarPowerStationConfig,
        *,
        poll_interval: float = 60.0,
        fetch_json: Optional[Callable[[str, float], Mapping[str, Any]]] = None,
    ) -> None:
        super().__init__(poll_interval=poll_interval)
        self.config = config
        self._fetch_json = fetch_json or (lambda url, timeout: _load_json(url, timeout=timeout))
        self._last_emitted_signature: Optional[Tuple[Optional[str], Optional[int], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[str], bool]] = None

    @property
    def source_name(self) -> str:
        """Return the stable station identifier."""
        return self.config.station_id

    def _poll_impl(self) -> List[Dict[str, Any]]:
        fetched_at = _utc_now()
        try:
            payload = self._fetch_json(self.config.json_url, self.config.timeout)
            record, snapshot = self._normalize_payload(payload, fetched_at=fetched_at)
            self._latest_snapshot = snapshot
            self._latest_record = record
            signature = self._record_signature(record, snapshot)
            if signature == self._last_emitted_signature:
                return []
            self._last_emitted_signature = signature
            return [record]
        except Exception as exc:
            self._latest_snapshot = {
                "status": "error",
                "station_id": self.config.station_id,
                "station_name": self.config.station_name,
                "page_url": self.config.page_url,
                "json_url": self.config.json_url,
                "poll_interval_s": self.poll_interval,
                "fetched_at_utc": fetched_at.isoformat(),
                "error": str(exc),
            }
            return []

    def _normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        fetched_at: datetime,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        source_timestamp = _parse_magnum_timestamp(payload.get("packet_date_local"), payload.get("timeZone"))
        source_age_s = _coerce_float(payload.get("ageSeconds"))
        stale = bool(source_age_s is not None and source_age_s > self.config.stale_after_s)
        fields = {
            "charge_pct": _coerce_int(payload.get("b_state_of_charge")),
            "dc_volts": _coerce_float(payload.get("b_dc_volts")),
            "dc_amps": _coerce_float(payload.get("b_dc_amps")),
            "amp_hours_net": _coerce_float(payload.get("b_amph_in_out")),
            "amp_hours_trip": _coerce_float(payload.get("b_amph_trip")),
            "amp_hours_cumulative_k": _coerce_float(payload.get("b_amph_cumulative")),
            "battery_temp_c": _extract_celsius(payload.get("i_temp_battery_C")),
            "transformer_temp_c": _extract_celsius(payload.get("i_temp_transformer")),
            "fet_temp_c": _extract_celsius(payload.get("i_temp_fet")),
            "source_age_s": source_age_s,
            "stale": stale,
            "inverter_status": payload.get("i_status"),
        }
        record = {
            "measurement": "eovsa_external_solar_power",
            "time": source_timestamp or fetched_at.isoformat(),
            "tags": {
                "source_kind": "external_solar_power",
                "source_path": self.config.json_url,
                "vendor": "magnum_energy",
                "station_id": self.config.station_id,
                "station_name": self.config.station_name,
            },
            "fields": {
                key: value
                for key, value in fields.items()
                if value not in (None, "")
            },
        }
        snapshot = {
            "status": "ok",
            "station_id": self.config.station_id,
            "station_name": self.config.station_name,
            "page_url": self.config.page_url,
            "json_url": self.config.json_url,
            "poll_interval_s": self.poll_interval,
            "fetched_at_utc": fetched_at.isoformat(),
            "source_timestamp_utc": source_timestamp,
            "source_age_s": source_age_s,
            "stale": stale,
            "fields": record["fields"],
        }
        return record, snapshot

    def _record_signature(
        self,
        record: Mapping[str, Any],
        snapshot: Mapping[str, Any],
    ) -> Tuple[Optional[str], Optional[int], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[str], bool]:
        fields = record.get("fields", {})
        if not isinstance(fields, Mapping):
            fields = {}
        return (
            record.get("time"),
            _coerce_int(fields.get("charge_pct")),
            _coerce_float(fields.get("dc_volts")),
            _coerce_float(fields.get("dc_amps")),
            _coerce_float(fields.get("amp_hours_net")),
            _coerce_float(fields.get("battery_temp_c")),
            _coerce_float(fields.get("transformer_temp_c")),
            _coerce_float(fields.get("fet_temp_c")),
            str(fields.get("inverter_status")) if fields.get("inverter_status") not in (None, "") else None,
            bool(snapshot.get("stale")),
        )


class ExternalSourceManager:
    """Poll and aggregate multiple external-source adapters."""

    def __init__(self, adapters: Optional[Sequence[ExternalSourceAdapter]] = None) -> None:
        self.adapters = list(adapters or [])

    @classmethod
    def from_solar_station_specs(
        cls,
        specs: Iterable[Tuple[str, str]],
        *,
        poll_interval: float = 60.0,
        timeout: float = 10.0,
        stale_after_s: float = 300.0,
    ) -> "ExternalSourceManager":
        """Build a manager from a list of solar station identifiers and names."""
        adapters: List[ExternalSourceAdapter] = []
        for station_id, station_name in specs:
            clean_id = str(station_id).strip()
            if not clean_id:
                continue
            adapters.append(
                SolarPowerStationAdapter(
                    SolarPowerStationConfig(
                        station_id=clean_id,
                        station_name=str(station_name).strip() or clean_id,
                        json_url=f"http://data.magnumenergy.com/mw/json.php?station_id={clean_id}&hours=24",
                        page_url=f"http://data.magnumenergy.com/{clean_id}",
                        timeout=timeout,
                        stale_after_s=stale_after_s,
                    ),
                    poll_interval=poll_interval,
                )
            )
        return cls(adapters=adapters)

    def poll(self) -> List[Dict[str, Any]]:
        """Poll all due adapters and return newly emitted records."""
        emitted: List[Dict[str, Any]] = []
        for adapter in self.adapters:
            emitted.extend(adapter.poll())
        return emitted

    def latest_records(self) -> List[Dict[str, Any]]:
        """Return the latest known records for all adapters."""
        return [record for record in (adapter.latest_record for adapter in self.adapters) if record is not None]

    def snapshot(self) -> Dict[str, Any]:
        """Return grouped snapshot state for all adapters."""
        grouped: Dict[str, Dict[str, Any]] = {}
        for adapter in self.adapters:
            grouped.setdefault(adapter.group_name, {})[adapter.source_name] = adapter.latest_snapshot
        return grouped


class WeatherStationAdapter(ExternalSourceAdapter):
    """Fetch and normalize the OVRO weather station feed."""

    group_name = "weather"

    def __init__(
        self,
        config: WeatherStationConfig,
        *,
        poll_interval: float = 15.0,
        fetch_text: Optional[Callable[[str, float], str]] = None,
    ) -> None:
        super().__init__(poll_interval=poll_interval)
        self.config = config
        self._fetch_text = fetch_text or (lambda url, timeout: _load_text(url, timeout=timeout))
        self._last_emitted_signature: Optional[Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float], bool]] = None

    @property
    def source_name(self) -> str:
        """Return a stable weather source identifier."""
        return "ovro_weather"

    def _poll_impl(self) -> List[Dict[str, Any]]:
        fetched_at = _utc_now()
        try:
            payload = self._fetch_text(self.config.xml_url, self.config.timeout)
            record, snapshot = self._normalize_payload(payload, fetched_at=fetched_at)
            self._latest_snapshot = snapshot
            self._latest_record = record
            signature = self._record_signature(record, snapshot)
            if signature == self._last_emitted_signature:
                return []
            self._last_emitted_signature = signature
            return [record]
        except Exception as exc:
            self._latest_snapshot = {
                "status": "error",
                "xml_url": self.config.xml_url,
                "poll_interval_s": self.poll_interval,
                "fetched_at_utc": fetched_at.isoformat(),
                "error": str(exc),
            }
            return []

    def _normalize_payload(self, payload: str, *, fetched_at: datetime) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        root = ET.fromstring(payload.strip())
        raw_fields: Dict[str, str] = {}
        for element in root.findall("meas"):
            name = element.get("name")
            if not name:
                continue
            raw_fields[name] = (element.text or "").strip()
        pressure_mbar = _coerce_float(raw_fields.get("mtRawBaromPress"))
        if pressure_mbar is not None:
            pressure_mbar *= 33.8637526
        source_timestamp = _parse_weather_timestamp(raw_fields.get("mtSampTime"))
        stale = True
        if source_timestamp:
            moment = datetime.fromisoformat(source_timestamp)
            stale = (fetched_at - moment).total_seconds() > self.config.stale_after_s
        fields = {
            "wind_mph": _coerce_float(raw_fields.get("mt2MinRollAvgWindSpeed")),
            "wind_direction_deg": _coerce_float(raw_fields.get("mtWindDirection"))
            if raw_fields.get("mtWindDirection") not in (None, "")
            else _coerce_float(raw_fields.get("mt2MinRollAvgWindDir")),
            "wind_gust_mph": _coerce_float(raw_fields.get("mtPeakWindSpeed"))
            if raw_fields.get("mtPeakWindSpeed") not in (None, "")
            else _coerce_float(raw_fields.get("mt2MinWindGustSpeed")),
            "temperature_f": _coerce_float(raw_fields.get("mtOutdoorTemp"))
            if raw_fields.get("mtOutdoorTemp") not in (None, "")
            else _coerce_float(raw_fields.get("mtTemp1")),
            "humidity_pct": _coerce_float(raw_fields.get("mtOutdoorHumidity"))
            if raw_fields.get("mtOutdoorHumidity") not in (None, "")
            else _coerce_float(raw_fields.get("mtRelHumidity")),
            "pressure_mbar": pressure_mbar,
            "rain_rate_in_hr": _coerce_float(raw_fields.get("mtRainRate")),
            "rain_today_in": _coerce_float(raw_fields.get("mtDailyRain"))
            if raw_fields.get("mtDailyRain") not in (None, "")
            else _coerce_float(raw_fields.get("mtRainToday")),
            "stale": stale,
        }
        record = {
            "measurement": "eovsa_external_weather",
            "time": source_timestamp or fetched_at.isoformat(),
            "tags": {
                "source_kind": "external_weather",
                "source_path": self.config.xml_url,
                "station_id": "ovro_weather",
            },
            "fields": {key: value for key, value in fields.items() if value not in (None, "")},
        }
        snapshot = {
            "status": "ok",
            "xml_url": self.config.xml_url,
            "poll_interval_s": self.poll_interval,
            "fetched_at_utc": fetched_at.isoformat(),
            "source_timestamp_utc": source_timestamp,
            "stale": stale,
            "fields": record["fields"],
            "raw_fields": raw_fields,
        }
        return record, snapshot

    def _record_signature(
        self,
        record: Mapping[str, Any],
        snapshot: Mapping[str, Any],
    ) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float], bool]:
        fields = record.get("fields", {})
        if not isinstance(fields, Mapping):
            fields = {}
        return (
            record.get("time"),
            _coerce_float(fields.get("wind_mph")),
            _coerce_float(fields.get("wind_direction_deg")),
            _coerce_float(fields.get("temperature_f")),
            _coerce_float(fields.get("pressure_mbar")),
            bool(snapshot.get("stale")),
        )


class ControlRoomTempAdapter(ExternalSourceAdapter):
    """Fetch and normalize the control-room temperature sensor feed."""

    group_name = "control_room"

    def __init__(
        self,
        config: ControlRoomTempConfig,
        *,
        poll_interval: float = 30.0,
        fetch_text: Optional[Callable[[str, float], str]] = None,
    ) -> None:
        super().__init__(poll_interval=poll_interval)
        self.config = config
        self._fetch_text = fetch_text or (lambda url, timeout: _load_text(url, timeout=timeout))
        self._last_emitted_signature: Optional[Tuple[Optional[float], bool]] = None

    @property
    def source_name(self) -> str:
        """Return a stable control-room source identifier."""
        return "control_room_temperature"

    def _poll_impl(self) -> List[Dict[str, Any]]:
        fetched_at = _utc_now()
        try:
            payload = self._fetch_text(self.config.xml_url, self.config.timeout)
            record, snapshot = self._normalize_payload(payload, fetched_at=fetched_at)
            self._latest_snapshot = snapshot
            self._latest_record = record
            signature = self._record_signature(record, snapshot)
            if signature == self._last_emitted_signature:
                return []
            self._last_emitted_signature = signature
            return [record]
        except Exception as exc:
            self._latest_snapshot = {
                "status": "error",
                "xml_url": self.config.xml_url,
                "poll_interval_s": self.poll_interval,
                "fetched_at_utc": fetched_at.isoformat(),
                "error": str(exc),
            }
            return []

    def _normalize_payload(self, payload: str, *, fetched_at: datetime) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        root = ET.fromstring(payload.strip())
        units = (root.findtext("units") or "").strip().upper()
        sensor_value = _find_first_numeric_xml_value(root, ("sensor1temp", "sensor1", "temperature"))
        temp_f = None
        temp_c = None
        if sensor_value is not None:
            if units == "C":
                temp_c = sensor_value
                temp_f = round(sensor_value * 9.0 / 5.0 + 32.0, 1)
            else:
                temp_f = sensor_value
                temp_c = round((sensor_value - 32.0) * 5.0 / 9.0, 1)
        stale = temp_c is None
        record = {
            "measurement": "eovsa_external_control_room",
            "time": fetched_at.isoformat(),
            "tags": {
                "source_kind": "external_control_room",
                "source_path": self.config.xml_url,
                "sensor": "ambient_temperature",
            },
            "fields": {
                key: value
                for key, value in {
                    "temperature_c": temp_c,
                    "temperature_f": temp_f,
                    "units": units or None,
                    "stale": stale,
                }.items()
                if value not in (None, "")
            },
        }
        snapshot = {
            "status": "ok",
            "xml_url": self.config.xml_url,
            "poll_interval_s": self.poll_interval,
            "fetched_at_utc": fetched_at.isoformat(),
            "stale": stale,
            "fields": record["fields"],
        }
        return record, snapshot

    def _record_signature(
        self,
        record: Mapping[str, Any],
        snapshot: Mapping[str, Any],
    ) -> Tuple[Optional[float], bool]:
        fields = record.get("fields", {})
        if not isinstance(fields, Mapping):
            fields = {}
        return (_coerce_float(fields.get("temperature_c")), bool(snapshot.get("stale")))


class RoachSensorAdapter(ExternalSourceAdapter):
    """Fetch and normalize one ROACH board's sensor dictionary."""

    group_name = "roach_sensors"

    def __init__(
        self,
        config: RoachSensorConfig,
        *,
        poll_interval: float = 60.0,
        fetch_sensors: Optional[Callable[[str, float], Mapping[str, Any]]] = None,
    ) -> None:
        super().__init__(poll_interval=poll_interval)
        self.config = config
        self._fetch_sensors = fetch_sensors or _fetch_roach_sensor_dict
        self._last_emitted_signature: Optional[Tuple[Tuple[str, str], ...]] = None

    @property
    def source_name(self) -> str:
        """Return a stable ROACH source identifier."""
        return self.config.host

    def _poll_impl(self) -> List[Dict[str, Any]]:
        fetched_at = _utc_now()
        try:
            sensors = dict(self._fetch_sensors(self.config.host, self.config.timeout))
            fields = {
                _sanitize_field_name(key): value
                for key, value in sensors.items()
                if value not in (None, "")
            }
            record = {
                "measurement": "eovsa_external_roach_sensors",
                "time": fetched_at.isoformat(),
                "tags": {
                    "source_kind": "external_roach_sensors",
                    "source_path": self.config.host,
                    "roach_host": self.config.host,
                },
                "fields": fields,
            }
            snapshot = {
                "status": "ok",
                "roach_host": self.config.host,
                "poll_interval_s": self.poll_interval,
                "fetched_at_utc": fetched_at.isoformat(),
                "fields": fields,
            }
            self._latest_snapshot = snapshot
            self._latest_record = record
            signature = tuple(sorted((key, repr(value)) for key, value in fields.items()))
            if signature == self._last_emitted_signature:
                return []
            self._last_emitted_signature = signature
            return [record]
        except Exception as exc:
            self._latest_snapshot = {
                "status": "error",
                "roach_host": self.config.host,
                "poll_interval_s": self.poll_interval,
                "fetched_at_utc": fetched_at.isoformat(),
                "error": str(exc),
            }
            return []


def parse_external_station_specs(spec: str) -> List[Tuple[str, str]]:
    """Parse ``ID:Name`` comma-separated station specs."""
    entries: List[Tuple[str, str]] = []
    for item in str(spec).split(","):
        text = item.strip()
        if not text:
            continue
        if ":" in text:
            station_id, station_name = text.split(":", 1)
        else:
            station_id, station_name = text, text
        entries.append((station_id.strip(), station_name.strip()))
    return entries


def parse_roach_hosts(spec: str) -> List[str]:
    """Parse one comma-separated list of ROACH hosts."""
    return [item.strip() for item in str(spec).split(",") if item.strip()]


def _sanitize_field_name(name: str) -> str:
    """Normalize one external source field name for Influx field keys."""
    return re.sub(r"[^0-9A-Za-z_]+", "_", str(name)).strip("_").lower()


def _fetch_roach_sensor_dict(host: str, timeout: float) -> Mapping[str, Any]:
    """Fetch one ROACH sensor dictionary using the legacy KATCP protocol."""
    try:
        import corr  # type: ignore
        from katcp import Message  # type: ignore
    except ImportError as exc:
        raise RuntimeError("corr/katcp not available for ROACH sensor polling") from exc

    fpga = corr.katcp_wrapper.FpgaClient(host, timeout=timeout)
    if not fpga.wait_connected(timeout):
        raise RuntimeError(f"could not connect to ROACH host {host}")
    factor = [0] + [0.001] * 3 + [1] * 4 + [0.001] * 18
    try:
        reply, sensors = fpga.blocking_request(Message.request("sensor-list"))
        if reply.arguments[0] != "ok":
            raise RuntimeError(f"sensor-list failed: {reply.arguments[0]}")
        count = int(reply.arguments[1])
        reply, values = fpga.blocking_request(Message.request("sensor-value"))
        if reply.arguments[0] != "ok":
            raise RuntimeError(f"sensor-value failed: {reply.arguments[0]}")
        if count != int(reply.arguments[1]):
            raise RuntimeError("sensor-list and sensor-value lengths differ")
        sensor_dict: Dict[str, Any] = {}
        for index in range(1, count):
            name = sensors[index].arguments[0][4:]
            sensor_dict[name] = int(values[index].arguments[-1]) * factor[index]
            sensor_dict[f"{name}.status"] = values[index].arguments[-2]
        return sensor_dict
    finally:
        try:
            fpga.stop()
        except Exception:
            pass


__all__ = [
    "ControlRoomTempAdapter",
    "ControlRoomTempConfig",
    "ExternalSourceAdapter",
    "ExternalSourceManager",
    "SolarPowerStationAdapter",
    "SolarPowerStationConfig",
    "RoachSensorAdapter",
    "RoachSensorConfig",
    "WeatherStationAdapter",
    "WeatherStationConfig",
    "parse_external_station_specs",
    "parse_roach_hosts",
]
