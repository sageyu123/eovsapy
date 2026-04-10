from pathlib import Path
import unittest

from eovsapy.ant16_debug import (
    ANT_INDEX,
    build_snapshot,
    decode_axis_status,
    decode_central_status,
    evaluate_snapshot,
)
from eovsapy.util import Time


def _base_record():
    antennas = []
    for _ in range(16):
        antennas.append(
            {
                "Controller": {},
                "Frontend": {
                    "BrightScram": {"State": 0},
                    "WindScram": {"State": 0},
                    "FEM": {"Temperature": 25.0},
                },
                "Parser": {"Command": "", "CommErr": 0},
            }
        )

    antenna = antennas[ANT_INDEX]
    antenna["Controller"] = {
        "RunMode": 4,
        "PowerSwitch": 1,
        "RunControl": 1,
        "DataMode": 1,
        "RemoteControl": 0,
        "AzimuthVirtualAxis": 1500000,
        "ElevationVirtualAxis": 300000,
        "AzimuthPosition": 1490000,
        "ElevationPosition": 290000,
        "Azimuth1": 1500500,
        "Elevation1": 300200,
        "AzimuthPositionCorrected": 1499000,
        "ElevationPositionCorrected": 299900,
        "AzimuthMasterStatus": (1 << 16) | (1 << 17),
        "ElevationStatus": (1 << 16) | (1 << 17),
        "CentralStatus": (1 << 18) | (1 << 20),
        "AzimuthTrip0": 0,
        "ElevationTrip0": 0,
        "cRIOClockms": 0,
        "SystemClockMJDay": 61041,
        "SystemClockms": 0,
    }
    antenna["Parser"] = {"Command": "TRACK ANT16", "CommErr": 0}

    lv_time = Time("2026-01-01 00:00:00").lv
    return {
        "timestamp": {
            "labview": lv_time,
            "iso_utc": "2026-01-01T00:00:00",
        },
        "data": {
            "Antenna": antennas,
            "FEMA": {
                "Thermal": {"SecondStageTemp": 18.5},
                "FRMServo": {
                    "Homed": 1,
                    "SelectedRx": 2,
                    "RxSelect": {
                        "AmplifierFault": 0,
                        "MinusLimit": 0,
                        "PlusLimit": 0,
                        "MotorCurrent": 0.5,
                        "Position": 2.0,
                        "PositionError": 0.0,
                        "PositionOffset": 0.0,
                    },
                    "ZFocus": {
                        "AmplifierFault": 0,
                        "MinusLimit": 0,
                        "PlusLimit": 0,
                        "MotorCurrent": 0.6,
                        "Position": 3.0,
                        "PositionError": 0.0,
                        "PositionOffset": 0.0,
                    },
                    "PositionAngle": {
                        "AmplifierFault": 0,
                        "MinusLimit": 0,
                        "PlusLimit": 0,
                        "MotorCurrent": 0.7,
                        "Position": 4.0,
                        "PositionError": 0.0,
                        "PositionOffset": 0.0,
                    },
                },
            },
        },
    }


class Ant16DebugTests(unittest.TestCase):
    def test_decode_axis_status_flags(self):
        status = decode_axis_status((1 << 0) | (1 << 4) | (1 << 16) | (1 << 21))
        self.assertTrue(status.tripped)
        self.assertTrue(status.low_soft_limit)
        self.assertTrue(status.drive_enabled)
        self.assertTrue(status.brake_alarm)
        self.assertFalse(status.permit)

    def test_decode_central_status_defaults_and_motion_mode(self):
        status = decode_central_status(0)
        self.assertTrue(status.operate)
        self.assertTrue(status.remote)
        self.assertEqual(status.motion_mode, "STOP")

        local_position = decode_central_status((1 << 1) | (1 << 28))
        self.assertTrue(local_position.local)
        self.assertFalse(local_position.remote)
        self.assertEqual(local_position.motion_mode, "POSITION")

    def test_build_snapshot_for_track_mode(self):
        snapshot = build_snapshot(_base_record(), trip_info_path=Path("/tmp/not-used"))
        self.assertEqual(snapshot.run_mode, "TRACK")
        self.assertEqual(snapshot.run_control, "OPERATE")
        self.assertEqual(snapshot.data_mode, "RA-DEC")
        self.assertEqual(snapshot.selected_rx, "HI")
        self.assertAlmostEqual(snapshot.requested_ha_deg, 150.0)
        self.assertAlmostEqual(snapshot.requested_dec_deg, 30.0)
        self.assertAlmostEqual(snapshot.actual_ha_deg, 150.05)
        self.assertAlmostEqual(snapshot.actual_dec_deg, 30.02)
        self.assertAlmostEqual(snapshot.delta_ha_deg, 0.05)
        self.assertAlmostEqual(snapshot.delta_dec_deg, 0.02)

    def test_build_snapshot_for_position_mode_uses_corrected_positions(self):
        record = _base_record()
        controller = record["data"]["Antenna"][ANT_INDEX]["Controller"]
        controller["RunMode"] = 1
        controller["AzimuthPosition"] = 1000000
        controller["ElevationPosition"] = 200000
        controller["Azimuth1"] = 1001500
        controller["Elevation1"] = 200700
        controller["AzimuthPositionCorrected"] = 1000500
        controller["ElevationPositionCorrected"] = 200200

        snapshot = build_snapshot(record, trip_info_path=Path("/tmp/not-used"))
        self.assertEqual(snapshot.run_mode, "POSITION")
        self.assertAlmostEqual(snapshot.requested_ha_deg, 100.0)
        self.assertAlmostEqual(snapshot.requested_dec_deg, 20.0)
        self.assertAlmostEqual(snapshot.actual_ha_deg, 100.1)
        self.assertAlmostEqual(snapshot.actual_dec_deg, 20.05)
        self.assertAlmostEqual(snapshot.delta_ha_deg, 0.1)
        self.assertAlmostEqual(snapshot.delta_dec_deg, 0.05)

    def test_evaluate_snapshot_reports_multiple_failures(self):
        record = _base_record()
        antenna = record["data"]["Antenna"][ANT_INDEX]
        antenna["Parser"]["CommErr"] = 5
        antenna["Frontend"]["BrightScram"]["State"] = 1
        antenna["Controller"]["AzimuthMasterStatus"] |= 1 << 0
        antenna["Controller"]["CentralStatus"] |= 1 << 1
        record["data"]["FEMA"]["Thermal"]["SecondStageTemp"] = 80.0
        record["data"]["FEMA"]["FRMServo"]["ZFocus"]["AmplifierFault"] = 1

        snapshot = build_snapshot(record, trip_info_path=Path("/tmp/not-used"))
        issues = evaluate_snapshot(snapshot)
        messages = [issue.message for issue in issues if issue.level == "fail"]
        self.assertTrue(any("parser comm error" in message for message in messages))
        self.assertTrue(any("bright scram" in message for message in messages))
        self.assertTrue(any("azimuth drive tripped" in message for message in messages))
        self.assertTrue(any("controller is in LOCAL mode" in message for message in messages))
        self.assertTrue(any("cryo second-stage temperature" in message for message in messages))
        self.assertTrue(any("FRM Z focus amplifier fault" in message for message in messages))


if __name__ == "__main__":
    unittest.main()
