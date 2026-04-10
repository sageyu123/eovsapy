"""Raw-preserving historian helpers for stateframe/scanheader replacement work.

This module is aimed at eventual Python 3 replacement of the legacy ``fBin``
and ``hBin`` SQL write path. The first implementation step deliberately avoids
flattening the full payload into an analytics schema. Instead it preserves the
binary record, version metadata, and replay provenance behind a sink-oriented
API so side-by-side equivalence testing can be done before any cutover.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import base64
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from .telemetry import (
    ACCStateframeConfig,
    BinaryFrame,
    BinaryFrameLogReader,
    FrameDefinition,
    _infer_xml_name,
    _parse_acc_ini_text,
    _read_text_from_path_or_url,
)


def _infer_frame_kind_from_path(path: str) -> str:
    """Infer ``stateframe`` or ``scanheader`` from a log filename."""
    base = os.path.basename(path)
    if base.startswith("sf_"):
        return "stateframe"
    if base.startswith("sh_"):
        return "scanheader"
    raise ValueError(f"Cannot infer frame kind from path: {path}")


@dataclass(frozen=True)
class FullFidelityFrameRecord:
    """Canonical raw-preserving record for historian replacement work.

    :param frame_kind: ``stateframe`` or ``scanheader``.
    :type frame_kind: str
    :param timestamp_lv: LabVIEW timestamp from the payload.
    :type timestamp_lv: float
    :param timestamp_utc: UTC timestamp derived from ``timestamp_lv``.
    :type timestamp_utc: datetime | None
    :param embedded_version: Version embedded in the binary payload.
    :type embedded_version: float
    :param definition_version: Version read from the XML definition.
    :type definition_version: float
    :param source_path: Source log path or TCP endpoint.
    :type source_path: str
    :param xml_path: XML definition path, if known.
    :type xml_path: str | None
    :param frame_index: Source-order frame index.
    :type frame_index: int
    :param record_size: Raw binary record size in bytes.
    :type record_size: int
    :param payload: Raw binary payload exactly as read.
    :type payload: bytes
    :param payload_sha256: SHA-256 of the raw payload for equivalence testing.
    :type payload_sha256: str
    :param payload_b64: Base64-encoded payload for transport/scaffolding use.
    :type payload_b64: str
    """

    frame_kind: str
    timestamp_lv: float
    timestamp_utc: Optional[datetime]
    embedded_version: float
    definition_version: float
    source_path: str
    xml_path: Optional[str]
    frame_index: int
    record_size: int
    payload: bytes
    payload_sha256: str
    payload_b64: str

    @classmethod
    def from_binary_frame(cls, frame: BinaryFrame, frame_kind: str) -> "FullFidelityFrameRecord":
        """Build a raw-preserving historian record from a generic binary frame."""
        payload_sha256 = hashlib.sha256(frame.payload).hexdigest()
        payload_b64 = base64.b64encode(frame.payload).decode("ascii")
        return cls(
            frame_kind=frame_kind,
            timestamp_lv=frame.timestamp_lv,
            timestamp_utc=frame.timestamp_utc,
            embedded_version=frame.embedded_version,
            definition_version=frame.definition.version,
            source_path=frame.source_path,
            xml_path=frame.definition.xml_path,
            frame_index=frame.frame_index,
            record_size=frame.record_size,
            payload=frame.payload,
            payload_sha256=payload_sha256,
            payload_b64=payload_b64,
        )

    def envelope(self) -> Dict[str, Any]:
        """Return the metadata envelope without mutating the raw payload."""
        return {
            "frame_kind": self.frame_kind,
            "timestamp": {
                "labview": self.timestamp_lv,
                "iso_utc": self.timestamp_utc.isoformat() if self.timestamp_utc else None,
            },
            "schema": {
                "embedded_version": self.embedded_version,
                "definition_version": self.definition_version,
                "xml_path": self.xml_path,
            },
            "source": {
                "source_path": self.source_path,
                "frame_index": self.frame_index,
                "record_size_bytes": self.record_size,
            },
            "equivalence": {
                "payload_sha256": self.payload_sha256,
            },
        }


class HistorianSink(ABC):
    """Abstract sink boundary for full-fidelity frame records."""

    @abstractmethod
    def write_record(self, record: FullFidelityFrameRecord) -> Any:
        """Write one full-fidelity record to the sink."""

    def write_records(self, records: Iterable[FullFidelityFrameRecord]) -> int:
        """Write multiple records and return the count."""
        count = 0
        for record in records:
            self.write_record(record)
            count += 1
        return count


class RawFrameStore(ABC):
    """Abstract raw payload store for full-fidelity frame bodies."""

    @abstractmethod
    def store_payload(self, record: FullFidelityFrameRecord) -> Dict[str, Any]:
        """Persist the raw frame payload and return a reference document."""


class EnvelopeIndexStore(ABC):
    """Abstract query/index store for frame metadata."""

    @abstractmethod
    def store_envelope(self, record: FullFidelityFrameRecord, raw_ref: Dict[str, Any]) -> Dict[str, Any]:
        """Persist queryable envelope metadata and return the stored document."""


class FileSystemRawFrameStore(RawFrameStore):
    """Store raw frame payloads on disk by frame kind and SHA-256."""

    def __init__(self, root_dir: str) -> None:
        self.root_dir = root_dir

    def store_payload(self, record: FullFidelityFrameRecord) -> Dict[str, Any]:
        frame_dir = os.path.join(self.root_dir, record.frame_kind, record.payload_sha256[:2])
        os.makedirs(frame_dir, exist_ok=True)
        payload_path = os.path.join(frame_dir, f"{record.payload_sha256}.bin")
        if not os.path.exists(payload_path):
            with open(payload_path, "wb") as handle:
                handle.write(record.payload)
        return {
            "store_kind": "filesystem_raw",
            "payload_path": payload_path,
            "payload_sha256": record.payload_sha256,
            "payload_size_bytes": record.record_size,
        }


class JsonlEnvelopeIndexStore(EnvelopeIndexStore):
    """Append frame envelopes to a JSONL file for audit and replay bookkeeping."""

    def __init__(self, jsonl_path: str) -> None:
        self.jsonl_path = jsonl_path
        os.makedirs(os.path.dirname(jsonl_path) or ".", exist_ok=True)

    def store_envelope(self, record: FullFidelityFrameRecord, raw_ref: Dict[str, Any]) -> Dict[str, Any]:
        document = record.envelope()
        document["raw_ref"] = raw_ref
        with open(self.jsonl_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(document, sort_keys=True) + "\n")
        return document


class InfluxDBEnvelopeStore(EnvelopeIndexStore):
    """Scaffold for an InfluxDB envelope/index store in a hybrid design.

    This scaffold intentionally does not attempt to map the full raw payload
    into conventional time-series fields. It provides:

    - an envelope/index point that is safe for Influx-style querying
    - references to an external raw payload store

    Notes:
    - InfluxDB alone is not a good fit for full-fidelity replacement if the
      design requires flattening all nested arrays/clusters into measurements.
    - A hybrid model is recommended: store queryable envelope/index metadata in
      InfluxDB and preserve the raw frame payload separately for equivalence,
      future re-parsing, and schema evolution.
    """

    def __init__(self) -> None:
        self._documents: List[Dict[str, Any]] = []

    def build_envelope_point(
        self,
        record: FullFidelityFrameRecord,
        raw_ref: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Return an Influx-style envelope point for one frame."""
        return {
            "measurement": "eovsa_frame_envelope",
            "tags": {
                "frame_kind": record.frame_kind,
                "embedded_version": str(record.embedded_version),
                "definition_version": str(record.definition_version),
            },
            "time": record.timestamp_utc.isoformat() if record.timestamp_utc else None,
            "fields": {
                "timestamp_lv": record.timestamp_lv,
                "frame_index": record.frame_index,
                "record_size_bytes": record.record_size,
                "payload_sha256": record.payload_sha256,
                "source_path": record.source_path,
                "xml_path": record.xml_path or "",
                "raw_store_kind": raw_ref.get("store_kind", ""),
                "raw_payload_ref": raw_ref.get("payload_path", raw_ref.get("payload_sha256", "")),
            },
        }

    def store_envelope(self, record: FullFidelityFrameRecord, raw_ref: Dict[str, Any]) -> Dict[str, Any]:
        point = self.build_envelope_point(record, raw_ref)
        self._documents.append(point)
        return point


class TimescaleDBEnvelopeScaffold(EnvelopeIndexStore):
    """Scaffold for a Timescale/Postgres envelope table in a hybrid design."""

    def __init__(self) -> None:
        self._rows: List[Dict[str, Any]] = []

    def build_row(self, record: FullFidelityFrameRecord, raw_ref: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "frame_kind": record.frame_kind,
            "timestamp_utc": record.timestamp_utc.isoformat() if record.timestamp_utc else None,
            "timestamp_lv": record.timestamp_lv,
            "embedded_version": record.embedded_version,
            "definition_version": record.definition_version,
            "xml_path": record.xml_path,
            "source_path": record.source_path,
            "frame_index": record.frame_index,
            "record_size_bytes": record.record_size,
            "payload_sha256": record.payload_sha256,
            "raw_store_kind": raw_ref.get("store_kind"),
            "raw_payload_ref": raw_ref.get("payload_path", raw_ref.get("payload_sha256")),
        }

    def store_envelope(self, record: FullFidelityFrameRecord, raw_ref: Dict[str, Any]) -> Dict[str, Any]:
        row = self.build_row(record, raw_ref)
        self._rows.append(row)
        return row


class PostgreSQLPayloadStore(RawFrameStore):
    """Store raw frame payloads directly in PostgreSQL.

    The legacy ``hBin`` path wrote binary scanheader records into SQL Server.
    For the PostgreSQL/Timescale replacement, keep the same full-fidelity
    principle by storing one deduplicated raw payload row per SHA-256. Envelope
    rows then reference the payload by hash.

    :param connection: Open PostgreSQL DB-API connection.
    :type connection: Any
    :param schema: Target schema name.
    :type schema: str
    :param ensure_schema: When ``True``, create the payload table if needed.
    :type ensure_schema: bool
    """

    def __init__(self, connection: Any, *, schema: str = "public", ensure_schema: bool = True) -> None:
        self.connection = connection
        self.schema = schema
        if ensure_schema:
            self.ensure_schema()

    @property
    def payload_table(self) -> str:
        """Return the fully qualified payload table name."""
        return f"{self.schema}.eovsa_frame_payload"

    def ensure_schema(self) -> None:
        """Create the raw-payload table if needed."""
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"""
            CREATE TABLE IF NOT EXISTS {self.payload_table} (
                payload_sha256 TEXT PRIMARY KEY,
                frame_kind TEXT NOT NULL,
                record_size_bytes INTEGER NOT NULL,
                payload BYTEA NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
        ]
        with self.connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        self.connection.commit()

    def store_payload(self, record: FullFidelityFrameRecord) -> Dict[str, Any]:
        """Insert one raw payload row if the SHA-256 is new."""
        statement = f"""
            INSERT INTO {self.payload_table} (
                payload_sha256,
                frame_kind,
                record_size_bytes,
                payload
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (payload_sha256) DO NOTHING
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                statement,
                (
                    record.payload_sha256,
                    record.frame_kind,
                    record.record_size,
                    record.payload,
                ),
            )
        self.connection.commit()
        return {
            "store_kind": "postgresql_payload",
            "payload_sha256": record.payload_sha256,
            "payload_table": self.payload_table,
            "payload_size_bytes": record.record_size,
        }


class TimescaleDBEnvelopeStore(EnvelopeIndexStore):
    """Store frame envelopes in PostgreSQL/TimescaleDB.

    This is the queryable companion to :class:`PostgreSQLPayloadStore`. It
    keeps one time-indexed row per frame in a hypertable and also creates a
    scanheader-focused view so ``hBin`` replacement work has an obvious entry
    point for scan-header-only queries.

    :param connection: Open PostgreSQL DB-API connection.
    :type connection: Any
    :param schema: Target schema name.
    :type schema: str
    :param ensure_schema: When ``True``, create tables/views if needed.
    :type ensure_schema: bool
    """

    def __init__(self, connection: Any, *, schema: str = "public", ensure_schema: bool = True) -> None:
        self.connection = connection
        self.schema = schema
        if ensure_schema:
            self.ensure_schema()

    @property
    def envelope_table(self) -> str:
        """Return the fully qualified envelope table name."""
        return f"{self.schema}.eovsa_frame_envelope"

    @property
    def scanheader_view(self) -> str:
        """Return the fully qualified scanheader envelope view name."""
        return f"{self.schema}.eovsa_hbin_envelope"

    def ensure_schema(self) -> None:
        """Create the Timescale envelope schema objects if needed."""
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            "CREATE EXTENSION IF NOT EXISTS timescaledb",
            f"""
            CREATE TABLE IF NOT EXISTS {self.envelope_table} (
                id BIGSERIAL PRIMARY KEY,
                frame_kind TEXT NOT NULL,
                timestamp_utc TIMESTAMPTZ,
                timestamp_lv DOUBLE PRECISION NOT NULL,
                embedded_version DOUBLE PRECISION NOT NULL,
                definition_version DOUBLE PRECISION NOT NULL,
                xml_path TEXT,
                source_path TEXT NOT NULL,
                frame_index BIGINT NOT NULL,
                record_size_bytes INTEGER NOT NULL,
                payload_sha256 TEXT NOT NULL,
                raw_store_kind TEXT,
                raw_payload_ref TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (frame_kind, source_path, frame_index, payload_sha256)
            )
            """,
            (
                "SELECT create_hypertable("
                f"'{self.envelope_table}', 'timestamp_utc', if_not_exists => TRUE, migrate_data => TRUE)"
            ),
            f"""
            CREATE INDEX IF NOT EXISTS eovsa_frame_envelope_kind_time_idx
            ON {self.envelope_table} (frame_kind, timestamp_utc DESC)
            """,
            f"""
            CREATE INDEX IF NOT EXISTS eovsa_frame_envelope_payload_idx
            ON {self.envelope_table} (payload_sha256)
            """,
            f"""
            CREATE OR REPLACE VIEW {self.scanheader_view} AS
            SELECT *
            FROM {self.envelope_table}
            WHERE frame_kind = 'scanheader'
            """,
        ]
        with self.connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        self.connection.commit()

    def build_row(self, record: FullFidelityFrameRecord, raw_ref: Dict[str, Any]) -> Dict[str, Any]:
        """Build one Timescale envelope row payload."""
        return {
            "frame_kind": record.frame_kind,
            "timestamp_utc": record.timestamp_utc,
            "timestamp_lv": record.timestamp_lv,
            "embedded_version": record.embedded_version,
            "definition_version": record.definition_version,
            "xml_path": record.xml_path,
            "source_path": record.source_path,
            "frame_index": record.frame_index,
            "record_size_bytes": record.record_size,
            "payload_sha256": record.payload_sha256,
            "raw_store_kind": raw_ref.get("store_kind"),
            "raw_payload_ref": raw_ref.get("payload_table", raw_ref.get("payload_path", raw_ref.get("payload_sha256"))),
        }

    def store_envelope(self, record: FullFidelityFrameRecord, raw_ref: Dict[str, Any]) -> Dict[str, Any]:
        """Insert one envelope row into the Timescale hypertable."""
        row = self.build_row(record, raw_ref)
        statement = f"""
            INSERT INTO {self.envelope_table} (
                frame_kind,
                timestamp_utc,
                timestamp_lv,
                embedded_version,
                definition_version,
                xml_path,
                source_path,
                frame_index,
                record_size_bytes,
                payload_sha256,
                raw_store_kind,
                raw_payload_ref
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (frame_kind, source_path, frame_index, payload_sha256) DO NOTHING
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                statement,
                (
                    row["frame_kind"],
                    row["timestamp_utc"],
                    row["timestamp_lv"],
                    row["embedded_version"],
                    row["definition_version"],
                    row["xml_path"],
                    row["source_path"],
                    row["frame_index"],
                    row["record_size_bytes"],
                    row["payload_sha256"],
                    row["raw_store_kind"],
                    row["raw_payload_ref"],
                ),
            )
        self.connection.commit()
        return row


def connect_postgresql(connection_dsn: Optional[str] = None, **connect_kwargs: Any) -> Any:
    """Open a PostgreSQL connection using ``psycopg`` or ``psycopg2``.

    :param connection_dsn: PostgreSQL DSN string, if used.
    :type connection_dsn: str | None
    :param connect_kwargs: Additional keyword arguments passed to the driver.
    :type connect_kwargs: dict
    :returns: Open PostgreSQL connection.
    :rtype: Any
    :raises ImportError: If neither ``psycopg`` nor ``psycopg2`` is installed.
    """
    try:
        import psycopg  # type: ignore

        return psycopg.connect(connection_dsn, **connect_kwargs)
    except ImportError:
        pass
    try:
        import psycopg2  # type: ignore

        if connection_dsn is not None:
            return psycopg2.connect(connection_dsn, **connect_kwargs)
        return psycopg2.connect(**connect_kwargs)
    except ImportError as exc:
        raise ImportError("Neither psycopg nor psycopg2 is installed") from exc


def build_timescaledb_historian_sink(
    connection: Optional[Any] = None,
    *,
    connection_dsn: Optional[str] = None,
    schema: str = "public",
    connect_kwargs: Optional[Dict[str, Any]] = None,
) -> HybridHistorianSink:
    """Build a PostgreSQL/Timescale historian sink for raw-preserving frames.

    The resulting sink stores the raw payload in PostgreSQL and indexes one
    hypertable envelope row per frame. ``scanheader`` records therefore become
    the Timescale-backed replacement for legacy ``hBin`` writes, while
    ``stateframe`` records can use the same path for ``fBin`` replacement work.

    :param connection: Existing PostgreSQL connection, if already open.
    :type connection: Any | None
    :param connection_dsn: PostgreSQL DSN used when ``connection`` is omitted.
    :type connection_dsn: str | None
    :param schema: Target schema name.
    :type schema: str
    :param connect_kwargs: Additional PostgreSQL connection kwargs.
    :type connect_kwargs: dict | None
    :returns: Composite Timescale-backed historian sink.
    :rtype: HybridHistorianSink
    """
    if connection is None:
        connection = connect_postgresql(connection_dsn, **(connect_kwargs or {}))
    raw_store = PostgreSQLPayloadStore(connection, schema=schema, ensure_schema=True)
    envelope_store = TimescaleDBEnvelopeStore(connection, schema=schema, ensure_schema=True)
    return HybridHistorianSink(raw_store, envelope_store)


class HybridHistorianSink(HistorianSink):
    """Composite sink that writes raw payloads and queryable envelopes together."""

    def __init__(self, raw_store: RawFrameStore, envelope_store: EnvelopeIndexStore) -> None:
        self.raw_store = raw_store
        self.envelope_store = envelope_store

    def write_record(self, record: FullFidelityFrameRecord) -> Dict[str, Any]:
        raw_ref = self.raw_store.store_payload(record)
        envelope_doc = self.envelope_store.store_envelope(record, raw_ref)
        return {
            "raw_ref": raw_ref,
            "envelope": envelope_doc,
        }


def iter_full_fidelity_records_from_logs(
    log_paths: Sequence[str],
    *,
    xml_paths: Optional[Dict[str, str]] = None,
    follow: bool = False,
    poll_interval: float = 1.0,
) -> Iterator[FullFidelityFrameRecord]:
    """Iterate raw-preserving historian records from replay/backfill logs."""
    for log_path in log_paths:
        frame_kind = _infer_frame_kind_from_path(log_path)
        xml_path = None if xml_paths is None else xml_paths.get(log_path)
        if xml_path is None:
            version = BinaryFrameLogReader.peek_embedded_version(log_path)
            xml_path = _infer_xml_name(log_path, version)
        reader = BinaryFrameLogReader.from_log(
            log_path,
            xml_path=xml_path,
            follow=follow,
            poll_interval=poll_interval,
        )
        for frame in reader.iter_frames():
            yield FullFidelityFrameRecord.from_binary_frame(frame, frame_kind)


def iter_full_fidelity_records_from_live_acc(
    acc_ini_path: str,
    *,
    frame_kind: str = "stateframe",
    xml_path: Optional[str] = None,
    host: str = "acc.solar.pvt",
    timeout: float = 0.5,
    poll_interval: float = 1.0,
) -> Iterator[FullFidelityFrameRecord]:
    """Iterate raw-preserving historian records from live ACC TCP reads."""
    config = _load_live_frame_config(
        acc_ini_path,
        frame_kind=frame_kind,
        xml_path=xml_path,
        host=host,
    )
    definition = FrameDefinition.from_xml(config.xml_path)  # type: ignore[arg-type]
    frame_index = 0
    from .telemetry import LiveStateframeReader

    reader = LiveStateframeReader(config, definition, timeout=timeout)
    for frame in reader.iter_frames(poll_interval=poll_interval):
        # Preserve correct frame kind for scanheader too.
        adjusted = BinaryFrame(
            payload=frame.payload,
            timestamp_lv=frame.timestamp_lv,
            timestamp_utc=frame.timestamp_utc,
            embedded_version=frame.embedded_version,
            record_size=frame.record_size,
            frame_index=frame_index,
            source_path=frame.source_path,
            definition=frame.definition,
        )
        frame_index += 1
        yield FullFidelityFrameRecord.from_binary_frame(adjusted, frame_kind)


def backfill_logs_to_sink(
    sink: HistorianSink,
    log_paths: Sequence[str],
    *,
    xml_paths: Optional[Dict[str, str]] = None,
) -> int:
    """Replay/backfill log files into a historian sink."""
    return sink.write_records(iter_full_fidelity_records_from_logs(log_paths, xml_paths=xml_paths))


def _load_live_frame_config(
    acc_ini_path: str,
    *,
    frame_kind: str,
    xml_path: Optional[str],
    host: str,
) -> ACCStateframeConfig:
    """Load ACC config for either stateframe or scanheader live reads."""
    values = _parse_acc_ini_text(_read_text_from_path_or_url(acc_ini_path))
    if frame_kind not in {"stateframe", "scanheader"}:
        raise ValueError(f"Unsupported frame_kind: {frame_kind}")
    if frame_kind == "stateframe":
        port = values["sfport"]
    else:
        port = values.get("scdsfport")
        if port is None:
            raise ValueError("ACC.ini does not contain TCP.schedule.stateframe.port for scanheader reads")
    resolved_xml_path = xml_path or values.get("xmlpath")
    if not resolved_xml_path:
        raise ValueError("xml_path is required for live historian reads")
    return ACCStateframeConfig(
        host=host,
        sfport=port,
        binsize=values["binsize"],
        xml_path=resolved_xml_path,
        scdport=values.get("scdport"),
        scdsfport=values.get("scdsfport"),
    )


__all__ = [
    "PostgreSQLPayloadStore",
    "EnvelopeIndexStore",
    "FileSystemRawFrameStore",
    "FullFidelityFrameRecord",
    "HistorianSink",
    "HybridHistorianSink",
    "InfluxDBEnvelopeStore",
    "JsonlEnvelopeIndexStore",
    "RawFrameStore",
    "TimescaleDBEnvelopeStore",
    "TimescaleDBEnvelopeScaffold",
    "backfill_logs_to_sink",
    "build_timescaledb_historian_sink",
    "connect_postgresql",
    "iter_full_fidelity_records_from_live_acc",
    "iter_full_fidelity_records_from_logs",
]
