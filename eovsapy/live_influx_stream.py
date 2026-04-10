"""Stream live operational telemetry into InfluxDB."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
import os
import sys
import time
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

from .telemetry import iter_live_operational_telemetry

DEFAULT_ACC_INI = "/common/python/runtime-cache/acc.ini"
DEFAULT_STATEFRAME_XML = "/common/python/runtime-cache/stateframe.xml"


def _flatten_mapping(
    data: Mapping[str, Any],
    *,
    prefix: str = "",
    include_antennas: bool = False,
) -> Dict[str, Any]:
    """Flatten nested telemetry records into scalar Influx field candidates."""
    flat: Dict[str, Any] = {}
    for key, value in data.items():
        if key == "source":
            continue
        if key == "schema":
            continue
        if key == "timestamp":
            continue
        if key == "antennas" and not include_antennas:
            continue
        field_name = f"{prefix}_{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            flat.update(_flatten_mapping(value, prefix=field_name, include_antennas=include_antennas))
        elif isinstance(value, (list, tuple)):
            if all(not isinstance(item, (dict, list, tuple)) for item in value):
                flat[field_name] = json.dumps(value)
            else:
                for index, item in enumerate(value):
                    item_name = f"{field_name}_{index}"
                    if isinstance(item, Mapping):
                        flat.update(_flatten_mapping(item, prefix=item_name, include_antennas=include_antennas))
                    elif isinstance(item, (list, tuple)):
                        flat.update(_flatten_mapping({str(index): item}, prefix=field_name, include_antennas=include_antennas))
                    else:
                        flat[item_name] = item
        else:
            flat[field_name] = value
    return flat


def _normalize_field_value(value: Any) -> Optional[Any]:
    """Convert a telemetry scalar into an InfluxDB field-compatible value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, str):
        return value
    return str(value)


def _record_timestamp(record: Mapping[str, Any]) -> Optional[datetime]:
    """Extract the record timestamp as a timezone-aware datetime when available."""
    iso_utc = record.get("timestamp", {}).get("iso_utc") if isinstance(record.get("timestamp"), Mapping) else None
    if not iso_utc:
        return None
    try:
        return datetime.fromisoformat(str(iso_utc).replace("Z", "+00:00"))
    except ValueError:
        return None


def _iter_points(
    records: Iterable[Dict[str, Any]],
    *,
    measurement: str,
    include_antennas: bool = False,
) -> Iterator[Tuple["Point", Dict[str, Any], Optional[datetime]]]:
    """Yield InfluxDB points together with their source records."""
    try:
        from influxdb_client import Point
    except ImportError as exc:
        raise RuntimeError(
            "influxdb-client is required for non-dry-run streaming. "
            "Install it in the active environment first."
        ) from exc

    for record in records:
        source = record.get("source", {})
        schema = record.get("schema", {})
        point = Point(measurement)
        point.tag("source_kind", str(source.get("kind", "")))
        point.tag("source_path", str(source.get("source_path", "")))
        point.tag("stateframe_version", str(schema.get("stateframe_version", "")))
        point.tag("definition_version", str(schema.get("definition_version", "")))

        for key, value in _flatten_mapping(record, include_antennas=include_antennas).items():
            normalized = _normalize_field_value(value)
            if normalized is None:
                continue
            point.field(key, normalized)

        timestamp = _record_timestamp(record)
        if timestamp is not None:
            point.time(timestamp)

        yield point, record, timestamp


def _build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the live Influx streamer."""
    parser = argparse.ArgumentParser(
        description="Stream live normalized ACC stateframe telemetry into InfluxDB.",
    )
    parser.add_argument("--acc-ini", default=os.environ.get("EOVSA_ACC_INI", DEFAULT_ACC_INI))
    parser.add_argument("--xml-path", default=os.environ.get("EOVSA_STATEFRAME_XML", DEFAULT_STATEFRAME_XML))
    parser.add_argument("--host", default=os.environ.get("EOVSA_ACC_HOST", "acc.solar.pvt"))
    parser.add_argument("--sf-num", type=int, default=int(os.environ.get("EOVSA_SF_NUM", "1")))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("EOVSA_ACC_TIMEOUT", "0.5")))
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=float(os.environ.get("EOVSA_POLL_INTERVAL", "1.0")),
    )
    parser.add_argument("--measurement", default=os.environ.get("EOVSA_INFLUX_MEASUREMENT", "eovsa_stateframe"))
    parser.add_argument("--url", default=os.environ.get("EOVSA_INFLUX_URL", os.environ.get("INFLUXDB_V2_URL")))
    parser.add_argument("--org", default=os.environ.get("EOVSA_INFLUX_ORG", os.environ.get("INFLUXDB_V2_ORG")))
    parser.add_argument(
        "--bucket",
        default=os.environ.get("EOVSA_INFLUX_BUCKET", os.environ.get("INFLUXDB_V2_BUCKET")),
    )
    parser.add_argument(
        "--token-env",
        default="EOVSA_INFLUX_TOKEN",
        help="Environment variable name that holds the InfluxDB token.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print sample records instead of writing to InfluxDB.")
    parser.add_argument(
        "--include-antennas",
        action="store_true",
        default=os.environ.get("EOVSA_INCLUDE_ANTENNAS", "1").strip().lower() not in {"0", "false", "no"},
        help="Also flatten per-antenna subsets.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N records; 0 means run forever.")
    parser.add_argument("--log-every", type=int, default=10, help="Report progress every N records.")
    parser.add_argument(
        "--report-rate",
        action="store_true",
        help="Report cumulative write rate in records/sec during progress logging.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Run the live telemetry to InfluxDB streaming CLI."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    token = os.environ.get(args.token_env)
    if not args.dry_run:
        missing = [name for name, value in (("url", args.url), ("org", args.org), ("bucket", args.bucket)) if not value]
        if not token:
            missing.append(args.token_env)
        if missing:
            parser.error("Missing required InfluxDB settings: " + ", ".join(missing))

    records = iter_live_operational_telemetry(
        args.acc_ini,
        xml_path=args.xml_path,
        host=args.host,
        sf_num=args.sf_num,
        timeout=args.timeout,
        poll_interval=args.poll_interval,
    )

    if args.dry_run:
        count = 0
        for record in records:
            print(json.dumps(record, sort_keys=True))
            count += 1
            if args.limit and count >= args.limit:
                break
        return 0

    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    count = 0
    start_time = time.perf_counter()
    with InfluxDBClient(url=args.url, token=token, org=args.org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        for point, record, point_timestamp in _iter_points(
            records,
            measurement=args.measurement,
            include_antennas=args.include_antennas,
        ):
            write_api.write(bucket=args.bucket, org=args.org, record=point)
            count += 1
            if args.log_every > 0 and count % args.log_every == 0:
                elapsed = max(time.perf_counter() - start_time, 1e-9)
                rate_suffix = ""
                if args.report_rate:
                    rate_suffix = f"; rate={count / elapsed:.2f} rec/s"
                print(
                    f"wrote {count} records; latest point_time={point_timestamp.isoformat() if point_timestamp else None}"
                    f"{rate_suffix}",
                    file=sys.stderr,
                )
            if args.limit and count >= args.limit:
                break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
