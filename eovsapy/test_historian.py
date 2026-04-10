import os
import struct
import tempfile
import unittest
import json
from datetime import datetime

from eovsapy.historian import (
    FileSystemRawFrameStore,
    FullFidelityFrameRecord,
    HybridHistorianSink,
    InfluxDBEnvelopeStore,
    JsonlEnvelopeIndexStore,
    PostgreSQLPayloadStore,
    TimescaleDBEnvelopeStore,
    TimescaleDBEnvelopeScaffold,
    backfill_logs_to_sink,
    build_timescaledb_historian_sink,
    iter_full_fidelity_records_from_logs,
)
from eovsapy.telemetry import BinaryFrame, FrameDefinition


def _build_definition():
    return FrameDefinition(
        pointers={
            "Timestamp": ["d", 0],
            "Version": ["d", 8],
            "Binsize": ["i", 16],
        },
        version=66.0,
        xml_path="/tmp/stateframe_v66.00.xml",
    )


def _build_payload(timestamp=1000.0, version=66.0, binsize=32):
    payload = bytearray(binsize)
    struct.pack_into("<d", payload, 0, timestamp)
    struct.pack_into("<d", payload, 8, version)
    struct.pack_into("<i", payload, 16, binsize)
    return bytes(payload)


class _FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.connection.executed.append((statement.strip(), params))


class _FakeConnection:
    def __init__(self):
        self.executed = []
        self.commit_count = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commit_count += 1


class HistorianTests(unittest.TestCase):
    def test_full_fidelity_record_preserves_raw_payload(self):
        definition = _build_definition()
        payload = _build_payload()
        frame = BinaryFrame(
            payload=payload,
            timestamp_lv=1000.0,
            timestamp_utc=None,
            embedded_version=66.0,
            record_size=len(payload),
            frame_index=3,
            source_path="/tmp/sf_20260320_v66.0.log",
            definition=definition,
        )
        record = FullFidelityFrameRecord.from_binary_frame(frame, "stateframe")
        self.assertEqual(record.frame_kind, "stateframe")
        self.assertEqual(record.payload, payload)
        self.assertEqual(record.record_size, len(payload))
        self.assertEqual(len(record.payload_sha256), 64)
        self.assertTrue(record.payload_b64)

    def test_replay_backfill_yields_records_and_hybrid_artifacts(self):
        payload = _build_payload()
        with tempfile.TemporaryDirectory() as tempdir:
            log_path = os.path.join(tempdir, "sf_20260320_v66.0.log")
            xml_path = os.path.join(tempdir, "stateframe_v66.00.xml")
            with open(log_path, "wb") as handle:
                handle.write(payload)
                handle.write(payload)
            with open(xml_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "<Cluster><Name>Stateframe</Name><NumElts>3</NumElts>"
                    "<DBL><Name>Timestamp</Name><Val>0</Val></DBL>"
                    "<DBL><Name>Version</Name><Val>66.0</Val></DBL>"
                    "<I32><Name>Binsize</Name><Val>32</Val></I32>"
                    "</Cluster>"
                )

            records = list(
                iter_full_fidelity_records_from_logs(
                    [log_path],
                    xml_paths={log_path: xml_path},
                )
            )
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0].frame_kind, "stateframe")
            self.assertEqual(records[0].embedded_version, 66.0)
            self.assertEqual(records[1].frame_index, 1)

            raw_root = os.path.join(tempdir, "raw")
            jsonl_path = os.path.join(tempdir, "index", "frames.jsonl")
            sink = HybridHistorianSink(
                FileSystemRawFrameStore(raw_root),
                JsonlEnvelopeIndexStore(jsonl_path),
            )
            count = backfill_logs_to_sink(sink, [log_path], xml_paths={log_path: xml_path})
            self.assertEqual(count, 2)
            raw_path = os.path.join(
                raw_root,
                "stateframe",
                records[0].payload_sha256[:2],
                f"{records[0].payload_sha256}.bin",
            )
            self.assertTrue(os.path.exists(raw_path))
            with open(jsonl_path, "r", encoding="utf-8") as handle:
                lines = [json.loads(line) for line in handle]
            self.assertEqual(len(lines), 2)
            self.assertEqual(lines[0]["frame_kind"], "stateframe")
            self.assertEqual(lines[0]["raw_ref"]["payload_path"], raw_path)

    def test_influx_and_timescale_envelope_scaffolds(self):
        definition = _build_definition()
        payload = _build_payload()
        frame = BinaryFrame(
            payload=payload,
            timestamp_lv=1000.0,
            timestamp_utc=None,
            embedded_version=66.0,
            record_size=len(payload),
            frame_index=0,
            source_path="/tmp/sf_20260320_v66.0.log",
            definition=definition,
        )
        record = FullFidelityFrameRecord.from_binary_frame(frame, "stateframe")
        raw_ref = {
            "store_kind": "filesystem_raw",
            "payload_path": "/tmp/raw/abc.bin",
            "payload_sha256": record.payload_sha256,
        }

        influx_doc = InfluxDBEnvelopeStore().store_envelope(record, raw_ref)
        self.assertEqual(influx_doc["measurement"], "eovsa_frame_envelope")
        self.assertEqual(influx_doc["fields"]["raw_payload_ref"], "/tmp/raw/abc.bin")

        timescale_row = TimescaleDBEnvelopeScaffold().store_envelope(record, raw_ref)
        self.assertEqual(timescale_row["frame_kind"], "stateframe")
        self.assertEqual(timescale_row["raw_payload_ref"], "/tmp/raw/abc.bin")

    def test_postgresql_payload_store_creates_table_and_upserts_payload(self):
        definition = _build_definition()
        payload = _build_payload()
        frame = BinaryFrame(
            payload=payload,
            timestamp_lv=1000.0,
            timestamp_utc=None,
            embedded_version=66.0,
            record_size=len(payload),
            frame_index=0,
            source_path="/tmp/sh_20260320_v66.0.log",
            definition=definition,
        )
        record = FullFidelityFrameRecord.from_binary_frame(frame, "scanheader")
        connection = _FakeConnection()

        store = PostgreSQLPayloadStore(connection, schema="telemetry_historian", ensure_schema=True)
        raw_ref = store.store_payload(record)

        self.assertEqual(raw_ref["store_kind"], "postgresql_payload")
        self.assertEqual(raw_ref["payload_sha256"], record.payload_sha256)
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS telemetry_historian.eovsa_frame_payload" in sql for sql, _ in connection.executed))
        self.assertTrue(any("INSERT INTO telemetry_historian.eovsa_frame_payload" in sql for sql, _ in connection.executed))

    def test_timescaledb_envelope_store_creates_hypertable_and_scanheader_view(self):
        definition = _build_definition()
        payload = _build_payload()
        frame = BinaryFrame(
            payload=payload,
            timestamp_lv=1000.0,
            timestamp_utc=datetime.fromisoformat("2026-03-23T00:00:00+00:00"),
            embedded_version=66.0,
            record_size=len(payload),
            frame_index=7,
            source_path="tcp://acc.solar.pvt:6341",
            definition=definition,
        )
        record = FullFidelityFrameRecord.from_binary_frame(frame, "scanheader")
        raw_ref = {
            "store_kind": "postgresql_payload",
            "payload_sha256": record.payload_sha256,
            "payload_table": "telemetry_historian.eovsa_frame_payload",
        }
        connection = _FakeConnection()

        store = TimescaleDBEnvelopeStore(connection, schema="telemetry_historian", ensure_schema=True)
        row = store.store_envelope(record, raw_ref)

        self.assertEqual(row["frame_kind"], "scanheader")
        self.assertEqual(row["raw_payload_ref"], "telemetry_historian.eovsa_frame_payload")
        self.assertTrue(any("CREATE EXTENSION IF NOT EXISTS timescaledb" in sql for sql, _ in connection.executed))
        self.assertTrue(any("create_hypertable" in sql.lower() for sql, _ in connection.executed))
        self.assertTrue(any("CREATE OR REPLACE VIEW telemetry_historian.eovsa_hbin_envelope" in sql for sql, _ in connection.executed))
        self.assertTrue(any("INSERT INTO telemetry_historian.eovsa_frame_envelope" in sql for sql, _ in connection.executed))

    def test_build_timescaledb_historian_sink_uses_postgres_payload_and_envelope_stores(self):
        connection = _FakeConnection()

        sink = build_timescaledb_historian_sink(connection=connection, schema="telemetry_historian")

        self.assertIsInstance(sink.raw_store, PostgreSQLPayloadStore)
        self.assertIsInstance(sink.envelope_store, TimescaleDBEnvelopeStore)
        self.assertTrue(any("telemetry_historian.eovsa_frame_payload" in sql for sql, _ in connection.executed))
        self.assertTrue(any("telemetry_historian.eovsa_frame_envelope" in sql for sql, _ in connection.executed))


if __name__ == "__main__":
    unittest.main()
