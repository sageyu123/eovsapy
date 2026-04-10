"""Tests for external telemetry source adapters."""

from __future__ import annotations

import unittest

from eovsapy.external_sources import (
    ControlRoomTempAdapter,
    ControlRoomTempConfig,
    ExternalSourceManager,
    RoachSensorAdapter,
    RoachSensorConfig,
    SolarPowerStationAdapter,
    SolarPowerStationConfig,
    WeatherStationAdapter,
    WeatherStationConfig,
    parse_external_station_specs,
    parse_roach_hosts,
)


def _build_magnum_payload(
    *,
    packet_date_local: str = "2026-03-23 12:34:56",
    age_seconds: int = 18,
    charge_pct: int = 92,
    dc_volts: float = 24.6,
    dc_amps: float = -3.4,
) -> dict:
    """Build one representative Magnum JSON payload."""
    return {
        "packet_date_local": packet_date_local,
        "timeZone": "UTC",
        "ageSeconds": age_seconds,
        "b_state_of_charge": charge_pct,
        "b_dc_volts": dc_volts,
        "b_dc_amps": dc_amps,
        "b_amph_in_out": -12.0,
        "b_amph_trip": 3.5,
        "b_amph_cumulative": 19.1,
        "i_temp_battery_C": "26 &deg;C / 79 &deg;F",
        "i_temp_transformer": "31 &deg;C / 88 &deg;F",
        "i_temp_fet": "29 &deg;C / 84 &deg;F",
        "i_status": "Float Charging",
    }


class ExternalSourceTests(unittest.TestCase):
    """Validate non-ACC external source handling."""

    def test_parse_external_station_specs_accepts_named_and_bare_ids(self) -> None:
        specs = parse_external_station_specs("MW5127:Ant12, MW5241")
        self.assertEqual(specs, [("MW5127", "Ant12"), ("MW5241", "MW5241")])

    def test_parse_roach_hosts_splits_csv(self) -> None:
        hosts = parse_roach_hosts("roach1.solar.pvt, roach2.solar.pvt")
        self.assertEqual(hosts, ["roach1.solar.pvt", "roach2.solar.pvt"])

    def test_solar_adapter_normalizes_payload_and_emits_once(self) -> None:
        payload = _build_magnum_payload()
        adapter = SolarPowerStationAdapter(
            SolarPowerStationConfig(
                station_id="MW5127",
                station_name="Ant12",
                json_url="http://example.invalid/mw/json.php?station_id=MW5127&hours=24",
                page_url="http://example.invalid/MW5127",
                timeout=5.0,
                stale_after_s=300.0,
            ),
            poll_interval=60.0,
            fetch_json=lambda url, timeout: payload,
        )

        emitted = adapter.poll(now_monotonic=0.0)

        self.assertEqual(len(emitted), 1)
        record = emitted[0]
        self.assertEqual(record["measurement"], "eovsa_external_solar_power")
        self.assertEqual(record["time"], "2026-03-23T12:34:56+00:00")
        self.assertEqual(record["tags"]["station_id"], "MW5127")
        self.assertEqual(record["tags"]["station_name"], "Ant12")
        self.assertEqual(record["fields"]["charge_pct"], 92)
        self.assertEqual(record["fields"]["dc_volts"], 24.6)
        self.assertEqual(record["fields"]["dc_amps"], -3.4)
        self.assertEqual(record["fields"]["battery_temp_c"], 26.0)
        self.assertEqual(record["fields"]["transformer_temp_c"], 31.0)
        self.assertEqual(record["fields"]["fet_temp_c"], 29.0)
        self.assertFalse(record["fields"]["stale"])
        self.assertEqual(adapter.latest_snapshot["status"], "ok")
        self.assertEqual(adapter.latest_snapshot["source_timestamp_utc"], "2026-03-23T12:34:56+00:00")

    def test_solar_adapter_deduplicates_identical_payloads(self) -> None:
        payloads = [
            _build_magnum_payload(),
            _build_magnum_payload(),
        ]

        def fetch_json(url: str, timeout: float) -> dict:
            return payloads.pop(0)

        adapter = SolarPowerStationAdapter(
            SolarPowerStationConfig(
                station_id="MW5127",
                station_name="Ant12",
                json_url="http://example.invalid/mw/json.php?station_id=MW5127&hours=24",
                page_url="http://example.invalid/MW5127",
            ),
            poll_interval=60.0,
            fetch_json=fetch_json,
        )

        first = adapter.poll(now_monotonic=0.0)
        second = adapter.poll(now_monotonic=61.0)

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])

    def test_solar_adapter_emits_when_source_timestamp_changes(self) -> None:
        payloads = [
            _build_magnum_payload(packet_date_local="2026-03-23 12:34:56"),
            _build_magnum_payload(packet_date_local="2026-03-23 12:35:56"),
        ]

        def fetch_json(url: str, timeout: float) -> dict:
            return payloads.pop(0)

        adapter = SolarPowerStationAdapter(
            SolarPowerStationConfig(
                station_id="MW5241",
                station_name="Ant13",
                json_url="http://example.invalid/mw/json.php?station_id=MW5241&hours=24",
                page_url="http://example.invalid/MW5241",
            ),
            poll_interval=60.0,
            fetch_json=fetch_json,
        )

        first = adapter.poll(now_monotonic=0.0)
        second = adapter.poll(now_monotonic=61.0)

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertNotEqual(first[0]["time"], second[0]["time"])

    def test_external_source_manager_groups_latest_snapshots(self) -> None:
        adapter = SolarPowerStationAdapter(
            SolarPowerStationConfig(
                station_id="MW5127",
                station_name="Ant12",
                json_url="http://example.invalid/mw/json.php?station_id=MW5127&hours=24",
                page_url="http://example.invalid/MW5127",
            ),
            poll_interval=60.0,
            fetch_json=lambda url, timeout: _build_magnum_payload(),
        )
        manager = ExternalSourceManager(adapters=[adapter])

        emitted = manager.poll()
        snapshot = manager.snapshot()
        records = manager.latest_records()

        self.assertEqual(len(emitted), 1)
        self.assertEqual(len(records), 1)
        self.assertIn("solar_power", snapshot)
        self.assertIn("MW5127", snapshot["solar_power"])
        self.assertEqual(snapshot["solar_power"]["MW5127"]["station_name"], "Ant12")

    def test_weather_adapter_normalizes_xml(self) -> None:
        payload = (
            "<oriondata>"
            "<meas name=\"mtSampTime\">2026/03/23 13:40:00</meas>"
            "<meas name=\"mt2MinRollAvgWindSpeed\">11.5</meas>"
            "<meas name=\"mtWindDirection\">182</meas>"
            "<meas name=\"mtPeakWindSpeed\">18.0</meas>"
            "<meas name=\"mtOutdoorTemp\">71.2</meas>"
            "<meas name=\"mtOutdoorHumidity\">24</meas>"
            "<meas name=\"mtRawBaromPress\">26.1</meas>"
            "<meas name=\"mtRainRate\">0.0</meas>"
            "<meas name=\"mtDailyRain\">0.02</meas>"
            "</oriondata>"
        )
        adapter = WeatherStationAdapter(
            WeatherStationConfig(xml_url="http://weather.invalid/sample.xml"),
            poll_interval=15.0,
            fetch_text=lambda url, timeout: payload,
        )

        emitted = adapter.poll(now_monotonic=0.0)

        self.assertEqual(len(emitted), 1)
        record = emitted[0]
        self.assertEqual(record["measurement"], "eovsa_external_weather")
        self.assertEqual(record["time"], "2026-03-23T13:40:00+00:00")
        self.assertEqual(record["fields"]["wind_mph"], 11.5)
        self.assertEqual(record["fields"]["wind_direction_deg"], 182.0)
        self.assertAlmostEqual(record["fields"]["pressure_mbar"], 26.1 * 33.8637526)

    def test_control_room_adapter_emits_only_on_change(self) -> None:
        payloads = [
            "<datavalues><units>F</units><sensor1temp>73.4</sensor1temp></datavalues>",
            "<datavalues><units>F</units><sensor1temp>73.4</sensor1temp></datavalues>",
            "<datavalues><units>F</units><sensor1temp>74.1</sensor1temp></datavalues>",
        ]

        def fetch_text(url: str, timeout: float) -> str:
            return payloads.pop(0)

        adapter = ControlRoomTempAdapter(
            ControlRoomTempConfig(xml_url="http://control.invalid/state.xml"),
            poll_interval=30.0,
            fetch_text=fetch_text,
        )

        first = adapter.poll(now_monotonic=0.0)
        second = adapter.poll(now_monotonic=31.0)
        third = adapter.poll(now_monotonic=62.0)

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(len(third), 1)
        self.assertIn("temperature_c", first[0]["fields"])
        self.assertEqual(first[0]["fields"]["units"], "F")

    def test_roach_sensor_adapter_sanitizes_fields(self) -> None:
        adapter = RoachSensorAdapter(
            RoachSensorConfig(host="roach1.solar.pvt"),
            poll_interval=60.0,
            fetch_sensors=lambda host, timeout: {
                "temp.ambient0": 27.0,
                "temp.ambient0.status": "nominal",
                "voltage.12v": 12.2,
            },
        )

        emitted = adapter.poll(now_monotonic=0.0)

        self.assertEqual(len(emitted), 1)
        record = emitted[0]
        self.assertEqual(record["measurement"], "eovsa_external_roach_sensors")
        self.assertEqual(record["fields"]["temp_ambient0"], 27.0)
        self.assertEqual(record["fields"]["temp_ambient0_status"], "nominal")
        self.assertEqual(record["fields"]["voltage_12v"], 12.2)


if __name__ == "__main__":
    unittest.main()
