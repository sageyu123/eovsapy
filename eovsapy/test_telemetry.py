import os
import socketserver
import struct
import tempfile
import threading
import unittest

from eovsapy.acc_exporter import entity_records_to_line_protocol
from eovsapy.telemetry import (
    ACCStateframeConfig,
    BinaryFrameLogReader,
    FrameDefinition,
    FullFrameTelemetryNormalizer,
    LiveStateframeReader,
    StateframeOperationalTelemetryNormalizer,
    build_entity_records,
)


def _build_definition():
    return FrameDefinition(
        pointers={
            "Timestamp": ["d", 0],
            "Version": ["d", 8],
            "Binsize": ["i", 16],
            "Schedule": {
                "Task": ["8s", 20],
                "Data": {
                    "ScanState": ["i", 28],
                    "PhaseTracking": ["i", 32],
                    "Azimuth": ["2d", 36, [2]],
                    "Elevation": ["2d", 52, [2]],
                    "TrackFlag": ["2B", 68, [2]],
                    "Weather": {
                        "Wind": ["f", 70],
                        "Temperature": ["f", 74],
                    },
                    "SolarPower": [
                        {
                            "Timestamp": ["d", 78],
                            "Charge": ["i", 86],
                            "Volts": ["f", 90],
                            "Amps": ["f", 94],
                            "BatteryTemp": ["f", 98],
                        }
                    ],
                    "Roach": [
                        {
                            "Status": ["i", 102],
                            "Temp.ambient": ["f", 106],
                            "Temp.fpga": ["f", 110],
                            "Voltage.12v": ["f", 114],
                            "Current.12v": ["f", 118],
                        }
                    ],
                },
            },
            "LODM": {
                "Subarray1": ["H", 122],
                "Subarray2": ["H", 124],
                "LO1A": {
                    "STB": ["H", 126],
                    "ESR": ["H", 128],
                    "SweepStatus": ["H", 130],
                    "ERR": ["H", 132],
                    "FSeqFile": ["8s", 134],
                    "CommErr": ["B", 142],
                },
                "LO1B": {
                    "STB": ["H", 143],
                    "ESR": ["H", 145],
                    "SweepStatus": ["H", 147],
                    "ERR": ["H", 149],
                    "FSeqFile": ["8s", 151],
                    "CommErr": ["B", 159],
                },
                "LO2_Lock": ["h", 160],
                "CommErr": ["B", 162],
                "Version": ["f", 163],
            },
            "FEMA": {
                "Timestamp": ["d", 167],
                "Version": ["f", 175],
                "PowerStrip": {
                    "RFSwitchStatus": ["B", 179],
                    "ComputerStatus": ["B", 180],
                    "Volts": ["f", 181],
                    "Current": ["f", 185],
                },
                "Thermal": {
                    "FirstStageTemp": ["f", 189],
                    "SecondStageTemp": ["f", 193],
                    "FocusBoxTemp": ["f", 197],
                    "RadiationShieldTemp": ["f", 201],
                },
                "Receiver": {
                    "LoFreqEnabled": ["B", 205],
                    "HiFreqEnabled": ["B", 206],
                    "NoiseDiodeEnabled": ["B", 207],
                },
                "FRMServo": {
                    "Homed": ["B", 208],
                    "SelectedRx": ["B", 209],
                    "PositionAngle": ["f", 210],
                },
            },
            "FEMB": {
                "Timestamp": ["d", 214],
                "Version": ["f", 222],
                "PowerStrip": {
                    "RFSwitchStatus": ["B", 226],
                    "ComputerStatus": ["B", 227],
                    "Volts": ["f", 228],
                    "Current": ["f", 232],
                },
                "Thermal": {
                    "FirstStageTemp": ["f", 236],
                    "SecondStageTemp": ["f", 240],
                    "FocusBoxTemp": ["f", 244],
                    "RadiationShieldTemp": ["f", 248],
                },
                "Receiver": {
                    "LoFreqEnabled": ["B", 252],
                    "HiFreqEnabled": ["B", 253],
                    "NoiseDiodeEnabled": ["B", 254],
                },
                "FRMServo": {
                    "Homed": ["B", 255],
                    "SelectedRx": ["B", 256],
                    "PositionAngle": ["f", 257],
                },
            },
            "DCM": [
                {
                    "Mode": ["B", 261],
                    "Slot": ["B", 262],
                    "Offset_Attn": ["f", 263],
                    "VPol": ["f", 267],
                    "HPol": ["f", 271],
                    "CommErr": ["B", 275],
                },
                {
                    "Mode": ["B", 276],
                    "Slot": ["B", 277],
                    "Offset_Attn": ["f", 278],
                    "VPol": ["f", 282],
                    "HPol": ["f", 286],
                    "CommErr": ["B", 290],
                },
            ],
            "Antenna": [
                {
                    "Controller": {
                        "RunMode": ["i", 291],
                        "Azimuth1": ["i", 295],
                        "Elevation1": ["i", 299],
                        "RAOffset": ["i", 303],
                        "DecOffset": ["i", 307],
                        "ElOffset": ["i", 311],
                        "AzOffset": ["i", 315],
                    },
                    "Frontend": {
                        "FEM": {
                            "Temperature": ["f", 319],
                            "CommErr": ["B", 323],
                        },
                        "TEC": {
                            "Temperature": ["f", 324],
                            "InputVoltage": ["f", 328],
                            "MainCurrent": ["f", 332],
                            "Alarm": ["i", 336],
                            "Error": ["i", 340],
                        },
                        "BrightScram": {
                            "Active": ["B", 344],
                            "State": ["B", 345],
                        },
                        "WindScram": {
                            "Active": ["B", 346],
                            "State": ["B", 347],
                        },
                    },
                },
                {
                    "Controller": {
                        "RunMode": ["i", 348],
                        "Azimuth1": ["i", 352],
                        "Elevation1": ["i", 356],
                        "RAOffset": ["i", 360],
                        "DecOffset": ["i", 364],
                        "ElOffset": ["i", 368],
                        "AzOffset": ["i", 372],
                    },
                    "Frontend": {
                        "FEM": {
                            "Temperature": ["f", 376],
                            "CommErr": ["B", 380],
                        },
                        "TEC": {
                            "Temperature": ["f", 381],
                            "InputVoltage": ["f", 385],
                            "MainCurrent": ["f", 389],
                            "Alarm": ["i", 393],
                            "Error": ["i", 397],
                        },
                        "BrightScram": {
                            "Active": ["B", 401],
                            "State": ["B", 402],
                        },
                        "WindScram": {
                            "Active": ["B", 403],
                            "State": ["B", 404],
                        },
                    },
                },
            ],
        },
        version=66.0,
        xml_path="/tmp/stateframe_v66.00.xml",
    )


def _build_payload():
    payload = bytearray(512)
    struct.pack_into("<d", payload, 0, 1000.0)
    struct.pack_into("<d", payload, 8, 66.0)
    struct.pack_into("<i", payload, 16, 512)
    struct.pack_into("<8s", payload, 20, b"TRACK\x00\x00\x00")
    struct.pack_into("<i", payload, 28, 3)
    struct.pack_into("<i", payload, 32, 1)
    struct.pack_into("<2d", payload, 36, 123.4, 234.5)
    struct.pack_into("<2d", payload, 52, 45.6, 56.7)
    struct.pack_into("<2B", payload, 68, 1, 0)
    struct.pack_into("<f", payload, 70, 7.5)
    struct.pack_into("<f", payload, 74, 18.25)
    struct.pack_into("<d", payload, 78, 1000.0)
    struct.pack_into("<i", payload, 86, 88)
    struct.pack_into("<f", payload, 90, 24.5)
    struct.pack_into("<f", payload, 94, 3.2)
    struct.pack_into("<f", payload, 98, 22.0)
    struct.pack_into("<i", payload, 102, 1)
    struct.pack_into("<f", payload, 106, 30.0)
    struct.pack_into("<f", payload, 110, 55.0)
    struct.pack_into("<f", payload, 114, 12.1)
    struct.pack_into("<f", payload, 118, 1.9)
    struct.pack_into("<H", payload, 122, 1)
    struct.pack_into("<H", payload, 124, 2)
    struct.pack_into("<H", payload, 126, 10)
    struct.pack_into("<H", payload, 128, 20)
    struct.pack_into("<H", payload, 130, 30)
    struct.pack_into("<H", payload, 132, 40)
    struct.pack_into("<8s", payload, 134, b"fseqA\x00\x00\x00")
    struct.pack_into("<B", payload, 142, 0)
    struct.pack_into("<H", payload, 143, 11)
    struct.pack_into("<H", payload, 145, 21)
    struct.pack_into("<H", payload, 147, 31)
    struct.pack_into("<H", payload, 149, 41)
    struct.pack_into("<8s", payload, 151, b"fseqB\x00\x00\x00")
    struct.pack_into("<B", payload, 159, 1)
    struct.pack_into("<h", payload, 160, 1)
    struct.pack_into("<B", payload, 162, 0)
    struct.pack_into("<f", payload, 163, 66.0)
    struct.pack_into("<d", payload, 167, 1000.0)
    struct.pack_into("<f", payload, 175, 1.0)
    struct.pack_into("<B", payload, 179, 1)
    struct.pack_into("<B", payload, 180, 1)
    struct.pack_into("<f", payload, 181, 120.0)
    struct.pack_into("<f", payload, 185, 2.5)
    struct.pack_into("<f", payload, 189, 50.0)
    struct.pack_into("<f", payload, 193, 20.0)
    struct.pack_into("<f", payload, 197, 18.0)
    struct.pack_into("<f", payload, 201, 40.0)
    struct.pack_into("<B", payload, 205, 1)
    struct.pack_into("<B", payload, 206, 1)
    struct.pack_into("<B", payload, 207, 0)
    struct.pack_into("<B", payload, 208, 1)
    struct.pack_into("<B", payload, 209, 2)
    struct.pack_into("<f", payload, 210, 90.0)
    struct.pack_into("<d", payload, 214, 1000.0)
    struct.pack_into("<f", payload, 222, 1.0)
    struct.pack_into("<B", payload, 226, 0)
    struct.pack_into("<B", payload, 227, 1)
    struct.pack_into("<f", payload, 228, 121.0)
    struct.pack_into("<f", payload, 232, 2.6)
    struct.pack_into("<f", payload, 236, 51.0)
    struct.pack_into("<f", payload, 240, 21.0)
    struct.pack_into("<f", payload, 244, 19.0)
    struct.pack_into("<f", payload, 248, 41.0)
    struct.pack_into("<B", payload, 252, 1)
    struct.pack_into("<B", payload, 253, 0)
    struct.pack_into("<B", payload, 254, 1)
    struct.pack_into("<B", payload, 255, 0)
    struct.pack_into("<B", payload, 256, 1)
    struct.pack_into("<f", payload, 257, 45.0)
    struct.pack_into("<B", payload, 261, 3)
    struct.pack_into("<B", payload, 262, 4)
    struct.pack_into("<f", payload, 263, 1.5)
    struct.pack_into("<f", payload, 267, 10.0)
    struct.pack_into("<f", payload, 271, 11.0)
    struct.pack_into("<B", payload, 275, 0)
    struct.pack_into("<B", payload, 276, 5)
    struct.pack_into("<B", payload, 277, 6)
    struct.pack_into("<f", payload, 278, 2.5)
    struct.pack_into("<f", payload, 282, 12.0)
    struct.pack_into("<f", payload, 286, 13.0)
    struct.pack_into("<B", payload, 290, 1)
    struct.pack_into("<i", payload, 291, 4)
    struct.pack_into("<i", payload, 295, 1234000)
    struct.pack_into("<i", payload, 299, 456000)
    struct.pack_into("<4i", payload, 303, 0, 0, 0, 0)
    struct.pack_into("<f", payload, 319, 280.0)
    struct.pack_into("<B", payload, 323, 0)
    struct.pack_into("<f", payload, 324, 12.0)
    struct.pack_into("<f", payload, 328, 5.0)
    struct.pack_into("<f", payload, 332, 1.5)
    struct.pack_into("<i", payload, 336, 0)
    struct.pack_into("<i", payload, 340, 0)
    struct.pack_into("<B", payload, 344, 1)
    struct.pack_into("<B", payload, 345, 2)
    struct.pack_into("<B", payload, 346, 0)
    struct.pack_into("<B", payload, 347, 0)
    struct.pack_into("<i", payload, 348, 1)
    struct.pack_into("<i", payload, 352, 2345000)
    struct.pack_into("<i", payload, 356, 567000)
    struct.pack_into("<4i", payload, 360, 1, 0, 0, 0)
    struct.pack_into("<f", payload, 376, 281.0)
    struct.pack_into("<B", payload, 380, 1)
    struct.pack_into("<f", payload, 381, 13.0)
    struct.pack_into("<f", payload, 385, 5.1)
    struct.pack_into("<f", payload, 389, 1.6)
    struct.pack_into("<i", payload, 393, 1)
    struct.pack_into("<i", payload, 397, 2)
    struct.pack_into("<B", payload, 401, 0)
    struct.pack_into("<B", payload, 402, 1)
    struct.pack_into("<B", payload, 403, 1)
    struct.pack_into("<B", payload, 404, 1)
    return bytes(payload)


class TelemetryTests(unittest.TestCase):
    def test_acc_ini_parser_reads_live_config(self):
        text = """
[Stateframe]
bin size = 512
template path = /tmp/stateframe_v66.00.xml

[Network]
TCP.schedule.port = 1234
TCP.stateframe.port = 5678
TCP.schedule.stateframe.port = 9012
"""
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(text)
            path = handle.name
        try:
            config = ACCStateframeConfig.from_acc_ini(path, host="127.0.0.1")
            self.assertEqual(config.host, "127.0.0.1")
            self.assertEqual(config.binsize, 512)
            self.assertEqual(config.sfport, 5678)
            self.assertEqual(config.scdport, 1234)
            self.assertEqual(config.scdsfport, 9012)
            self.assertEqual(config.xml_path, "/tmp/stateframe_v66.00.xml")
        finally:
            os.unlink(path)

    def test_binary_log_reader_reads_record(self):
        definition = _build_definition()
        payload = _build_payload()
        with tempfile.NamedTemporaryFile(prefix="sf_20260320_v66_0_", suffix=".log", delete=False) as handle:
            handle.write(payload)
            path = handle.name
        try:
            reader = BinaryFrameLogReader(path, definition)
            frames = list(reader.iter_frames())
            self.assertEqual(len(frames), 1)
            self.assertEqual(frames[0].record_size, 512)
            self.assertEqual(frames[0].embedded_version, 66.0)
            self.assertEqual(frames[0].timestamp_lv, 1000.0)
        finally:
            os.unlink(path)

    def test_normalizer_extracts_operational_subset(self):
        definition = _build_definition()
        payload = _build_payload()
        with tempfile.NamedTemporaryFile(prefix="sf_20260320_v66_0_", suffix=".log", delete=False) as handle:
            handle.write(payload)
            path = handle.name
        try:
            reader = BinaryFrameLogReader(path, definition)
            frame = next(reader.iter_frames())
            record = StateframeOperationalTelemetryNormalizer().normalize_frame(frame).record
            self.assertEqual(record["schema"]["stateframe_version"], 66.0)
            self.assertEqual(record["schedule"]["task"], "TRACK")
            self.assertEqual(record["weather"]["wind"], 7.5)
            self.assertEqual(record["lo"]["lo1a"]["fseqfile"], "fseqA")
            self.assertEqual(record["power"]["solar_power"][0]["charge"], 88)
            self.assertEqual(record["antennas"][0]["antenna"], 1)
            self.assertEqual(record["antennas"][0]["actual_azimuth_deg"], 123.4)
            self.assertEqual(record["antennas"][0]["track_source_flag"], True)
            self.assertEqual(record["antennas"][1]["track_source_flag"], False)
            self.assertEqual(record["antennas"][1]["dcm"]["comm_err"], 1)
            self.assertEqual(record["schedule"]["run_mode_summary"], {"4": 1, "1": 1})
        finally:
            os.unlink(path)

    def test_live_reader_reads_one_frame_from_socket(self):
        definition = _build_definition()
        payload = _build_payload()

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                _ = self.request.recv(4)
                self.request.sendall(payload[:200])
                self.request.sendall(payload[200:])

        with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            try:
                config = ACCStateframeConfig(
                    host="127.0.0.1",
                    sfport=server.server_address[1],
                    binsize=len(payload),
                    xml_path="/tmp/stateframe_v66.00.xml",
                )
                reader = LiveStateframeReader(config, definition)
                frame = reader.read_frame()
            finally:
                thread.join(timeout=2)

        self.assertEqual(frame.embedded_version, 66.0)
        self.assertEqual(frame.source_path, f"tcp://127.0.0.1:{server.server_address[1]}")
        record = StateframeOperationalTelemetryNormalizer().normalize_frame(frame).record
        self.assertEqual(record["source"]["kind"], "live_acc_stateframe")
        self.assertEqual(record["weather"]["temperature"], 18.25)

    def test_full_normalizer_extracts_full_tree(self):
        definition = _build_definition()
        payload = _build_payload()
        with tempfile.NamedTemporaryFile(prefix="sf_20260320_v66_0_", suffix=".log", delete=False) as handle:
            handle.write(payload)
            path = handle.name
        try:
            reader = BinaryFrameLogReader(path, definition)
            frame = next(reader.iter_frames())
            record = FullFrameTelemetryNormalizer().normalize_frame(frame).record
            self.assertEqual(record["source"]["frame_kind"], "stateframe")
            self.assertEqual(record["data"]["Schedule"]["Task"], "TRACK")
            self.assertEqual(record["data"]["Schedule"]["Data"]["Weather"]["Wind"], 7.5)
            self.assertEqual(record["data"]["Antenna"][0]["Controller"]["RunMode"], 4)
            self.assertEqual(record["data"]["DCM"][1]["CommErr"], 1)
        finally:
            os.unlink(path)

    def test_build_entity_records_emits_tagged_measurements(self):
        definition = _build_definition()
        payload = _build_payload()
        with tempfile.NamedTemporaryFile(prefix="sf_20260320_v66_0_", suffix=".log", delete=False) as handle:
            handle.write(payload)
            path = handle.name
        try:
            reader = BinaryFrameLogReader(path, definition)
            frame = next(reader.iter_frames())
            structured = FullFrameTelemetryNormalizer().normalize_frame(frame).record
            records = build_entity_records(structured)
            measurements = {record["measurement"] for record in records}
            self.assertIn("eovsa_stateframe_schedule", measurements)
            self.assertIn("eovsa_stateframe_schedule_data_weather", measurements)
            self.assertIn("eovsa_stateframe_antenna_controller", measurements)
            antenna_controller = next(
                record for record in records
                if record["measurement"] == "eovsa_stateframe_antenna_controller"
                and record["tags"].get("antenna") == "1"
            )
            self.assertEqual(antenna_controller["fields"]["runmode"], 4)
            self.assertEqual(antenna_controller["fields"]["azimuth1"], 1234000)
        finally:
            os.unlink(path)

    def test_entity_records_render_as_line_protocol(self):
        records = [
            {
                "measurement": "eovsa_stateframe_weather",
                "time": "2026-03-22T07:40:27+00:00",
                "tags": {"source_kind": "live_acc_stateframe", "antenna": "1"},
                "fields": {"wind": 7.5, "ok": True, "label": "north"},
            }
        ]
        line = entity_records_to_line_protocol(records)
        self.assertIn("eovsa_stateframe_weather,antenna=1,source_kind=live_acc_stateframe", line)
        self.assertIn("wind=7.5", line)
        self.assertIn("ok=true", line)
        self.assertIn('label="north"', line)
        self.assertRegex(line.strip(), r" 17\d{18,}$")


if __name__ == "__main__":
    unittest.main()
