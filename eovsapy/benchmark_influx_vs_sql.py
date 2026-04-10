"""Benchmark representative InfluxDB queries against legacy SQL stateframe queries.

This script is intended to be run from the Python 3 ``eovsapy`` environment on
``pipeline`` after the exporter and Telegraf stack is deployed. It compares a
small set of representative telemetry queries that exist in both the old SQL
historian and the current structured InfluxDB bucket.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
import statistics
import time
from typing import Callable, List, Sequence

from astropy.time import Time
from influxdb_client import InfluxDBClient

from . import dbutil


@dataclass(frozen=True)
class BenchmarkCase:
    """One SQL vs Influx comparison case."""

    name: str
    description: str
    sql_query_builder: Callable[[str, int, int], str]
    influx_query: str


@dataclass(frozen=True)
class TimingSummary:
    """Timing summary for one query family."""

    rows_returned: int
    timings_s: Sequence[float]

    @property
    def median_s(self) -> float:
        """Median wall time."""
        return statistics.median(self.timings_s)

    @property
    def min_s(self) -> float:
        """Minimum wall time."""
        return min(self.timings_s)

    @property
    def max_s(self) -> float:
        """Maximum wall time."""
        return max(self.timings_s)


def _build_cases(start_iso: str, stop_iso: str) -> List[BenchmarkCase]:
    """Return benchmark cases for the requested timerange."""
    del start_iso, stop_iso
    return [
        BenchmarkCase(
            name="weather_wind",
            description="Ambient weather wind speed, external-source Influx query vs legacy SQL weather channel",
            sql_query_builder=lambda ver, start_lv, stop_lv: (
                f"select Timestamp,Sche_Data_Weat_AvgWind "
                f"from fV{ver}_vD1 "
                f"where Timestamp between {start_lv} and {stop_lv} "
                f"order by Timestamp"
            ),
            influx_query="""
from(bucket: "stateframe")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "eovsa_external_weather")
  |> filter(fn: (r) => r._field == "wind_mph")
""".strip(),
        ),
        BenchmarkCase(
            name="antenna1_actual_azimuth",
            description="Antenna 1 actual azimuth, raw controller field query in the structured exporter path",
            sql_query_builder=lambda ver, start_lv, stop_lv: (
                f"select Timestamp,Ante_Cont_AzimuthPositionCorrected "
                f"from fV{ver}_vD16 "
                f"where (I16 % 16) = 0 and Timestamp between {start_lv} and {stop_lv} "
                f"order by Timestamp"
            ),
            influx_query="""
from(bucket: "stateframe")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "eovsa_stateframe_antenna_controller")
  |> filter(fn: (r) => r.source_kind == "live_acc_stateframe")
  |> filter(fn: (r) => r.antenna == "1")
  |> filter(fn: (r) => r._field == "azimuth1")
""".strip(),
        ),
    ]


def _time_sql(cursor, query: str, runs: int) -> TimingSummary:
    """Measure repeated execution of one SQL query."""
    timings: List[float] = []
    rows_returned = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        cursor.execute(query)
        rows = cursor.fetchall()
        elapsed = time.perf_counter() - t0
        timings.append(elapsed)
        rows_returned = len(rows)
    return TimingSummary(rows_returned=rows_returned, timings_s=timings)


def _time_influx(query_api, org: str, query: str, runs: int) -> TimingSummary:
    """Measure repeated execution of one Influx Flux query."""
    timings: List[float] = []
    rows_returned = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        rows = list(query_api.query_stream(query=query, org=org))
        elapsed = time.perf_counter() - t0
        timings.append(elapsed)
        rows_returned = len(rows)
    return TimingSummary(rows_returned=rows_returned, timings_s=timings)


def _format_markdown_table(results: List[dict]) -> str:
    """Render one markdown summary table."""
    lines = [
        "| Case | SQL rows | SQL median (s) | Influx rows | Influx median (s) | Speedup (SQL / Influx) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in results:
        speedup = item["sql"]["median_s"] / item["influx"]["median_s"] if item["influx"]["median_s"] else float("inf")
        lines.append(
            "| {name} | {sql_rows} | {sql_med:.4f} | {influx_rows} | {influx_med:.4f} | {speedup:.2f}x |".format(
                name=item["name"],
                sql_rows=item["sql"]["rows_returned"],
                sql_med=item["sql"]["median_s"],
                influx_rows=item["influx"]["rows_returned"],
                influx_med=item["influx"]["median_s"],
                speedup=speedup,
            )
        )
    return "\n".join(lines)


def main() -> int:
    """Run the benchmark and print JSON plus markdown summaries."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=float, default=6.0, help="Lookback window in hours.")
    parser.add_argument("--runs", type=int, default=5, help="Repeated runs per query.")
    parser.add_argument("--sql-host", default="sqlserver.solar.pvt", help="SQL Server host.")
    parser.add_argument("--sql-database", default="eOVSA06", help="SQL Server database name.")
    parser.add_argument("--influx-url", default=os.environ.get("EOVSA_INFLUX_URL", "http://127.0.0.1:8086"))
    parser.add_argument("--influx-org", default=os.environ.get("EOVSA_INFLUX_ORG", "eovsa"))
    parser.add_argument("--influx-token", default=os.environ.get("EOVSA_INFLUX_TOKEN"))
    args = parser.parse_args()

    if not args.influx_token:
        raise SystemExit("EOVSA_INFLUX_TOKEN or --influx-token is required")

    stop = datetime.now(timezone.utc)
    start = stop - timedelta(hours=float(args.hours))
    trange = Time([start, stop])
    start_lv, stop_lv = [int(value) for value in trange.lv]
    start_iso = start.isoformat()
    stop_iso = stop.isoformat()

    try:
        cnxn, cursor = dbutil.get_cursor(host=args.sql_host, database=args.sql_database)
    except Exception as exc:
        raise SystemExit(
            "SQL benchmark setup failed. The current Python environment could not "
            f"open the legacy SQL connection ({exc}). On pipeline this usually "
            "means the pyodbc extension needs to be rebuilt or replaced for the "
            "active Python runtime."
        ) from exc
    version = dbutil.find_table_version(cursor, start_lv)
    query_api = InfluxDBClient(
        url=args.influx_url,
        token=args.influx_token,
        org=args.influx_org,
        timeout=60_000,
    ).query_api()

    results: List[dict] = []
    for case in _build_cases(start_iso, stop_iso):
        sql_query = case.sql_query_builder(version, start_lv, stop_lv)
        influx_query = (
            f'option v = {{timeRangeStart: time(v: "{start_iso}"), timeRangeStop: time(v: "{stop_iso}")}}\n'
            + case.influx_query
        )
        sql_summary = _time_sql(cursor, sql_query, args.runs)
        influx_summary = _time_influx(query_api, args.influx_org, influx_query, args.runs)
        results.append(
            {
                "name": case.name,
                "description": case.description,
                "timerange": {"start_utc": start_iso, "stop_utc": stop_iso, "hours": args.hours},
                "sql_query": sql_query,
                "influx_query": influx_query,
                "sql": {
                    "rows_returned": sql_summary.rows_returned,
                    "median_s": sql_summary.median_s,
                    "min_s": sql_summary.min_s,
                    "max_s": sql_summary.max_s,
                    "runs": list(sql_summary.timings_s),
                },
                "influx": {
                    "rows_returned": influx_summary.rows_returned,
                    "median_s": influx_summary.median_s,
                    "min_s": influx_summary.min_s,
                    "max_s": influx_summary.max_s,
                    "runs": list(influx_summary.timings_s),
                },
            }
        )

    cnxn.close()

    print(json.dumps({"results": results}, indent=2))
    print()
    print(_format_markdown_table(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
