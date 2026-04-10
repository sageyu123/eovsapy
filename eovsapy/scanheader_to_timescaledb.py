"""Ingest live or replayed scanheader records into PostgreSQL/TimescaleDB.

This module is the first concrete ``hBin`` replacement entry point in the
Python 3 stack. It preserves the raw scanheader payload in PostgreSQL and
writes a Timescale hypertable envelope row per frame for query/index use.
"""

from __future__ import annotations

import argparse
from typing import Optional, Sequence

from .historian import (
    backfill_logs_to_sink,
    build_timescaledb_historian_sink,
    iter_full_fidelity_records_from_live_acc,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    :returns: Configured parser for live or replay ingestion.
    :rtype: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        description="Ingest live or replayed EOVSA scanheader records into PostgreSQL/TimescaleDB.",
    )
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN for the TimescaleDB target.")
    parser.add_argument("--schema", default="public", help="Target PostgreSQL schema.")
    parser.add_argument(
        "--acc-ini",
        help="ACC.ini path for live scanheader reads. Required with --live.",
    )
    parser.add_argument(
        "--xml-path",
        help="Scanheader XML path for live reads or optional override for replay.",
    )
    parser.add_argument(
        "--host",
        default="acc.solar.pvt",
        help="ACC host for live reads.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=0.5,
        help="Live ACC socket timeout in seconds.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Live ACC poll interval in seconds.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Read scanheader records live from ACC instead of replay logs.",
    )
    parser.add_argument(
        "log_paths",
        nargs="*",
        help="Replay/backfill scanheader log paths when --live is not used.",
    )
    return parser


def run(argv: Optional[Sequence[str]] = None) -> int:
    """Run the scanheader-to-Timescale ingest command.

    :param argv: Optional CLI-style argument vector.
    :type argv: Sequence[str] | None
    :returns: Process exit status.
    :rtype: int
    :raises ValueError: If neither ``--live`` nor replay log paths are provided.
    """
    args = build_parser().parse_args(argv)
    sink = build_timescaledb_historian_sink(connection_dsn=args.dsn, schema=args.schema)

    if args.live:
        if not args.acc_ini:
            raise ValueError("--acc-ini is required with --live")
        records = iter_full_fidelity_records_from_live_acc(
            args.acc_ini,
            frame_kind="scanheader",
            xml_path=args.xml_path,
            host=args.host,
            timeout=args.timeout,
            poll_interval=args.poll_interval,
        )
        sink.write_records(records)
        return 0

    if not args.log_paths:
        raise ValueError("Provide replay log paths or use --live")

    xml_paths = None
    if args.xml_path:
        xml_paths = {log_path: args.xml_path for log_path in args.log_paths}
    backfill_logs_to_sink(sink, args.log_paths, xml_paths=xml_paths)
    return 0


def main() -> int:
    """Run the CLI and return the process exit code."""
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
