#!/usr/bin/env python
"""Benchmark calwidget_v2 products against legacy SQL calibrations."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

from eovsapy.util import Time

from eovsapy.calwidget_html.calwidget_v2_analysis import (
    CalWidgetV2Error,
    analyze_phacal_input,
    analyze_refcal_input,
    attach_sidecar_delay,
    find_sidecar_by_timestamp,
    load_sidecar,
    metrics_to_jsonable,
    save_metrics_json,
    sql2phacalX,
    sql2refcalX,
    sql_refcal_to_scan,
)
from eovsapy.calwidget_html.calwidget_v2_plots import save_phacal_report, save_refcal_report


def _parse_time(value: str) -> Time:
    """Parse either ISO time or a LabVIEW integer timestamp."""

    text = str(value).strip()
    if text.isdigit():
        return Time(int(text), format="lv")
    return Time(text)


def _load_legacy_refcal(time_arg: str):
    """Load one legacy SQL refcal."""

    result = sql2refcalX(_parse_time(time_arg))
    if result is None:
        raise CalWidgetV2Error("No legacy SQL refcal was found for {0}.".format(time_arg))
    return result


def _load_legacy_phacal(time_arg: str):
    """Load one legacy SQL phacal."""

    result = sql2phacalX(_parse_time(time_arg))
    if result is None:
        raise CalWidgetV2Error("No legacy SQL phacal was found for {0}.".format(time_arg))
    return result


def _refcal_from_ref_sql_time(ref_sql_time: str):
    """Build a v2-compatible refcal object from SQL plus sidecar metadata."""

    legacy = _load_legacy_refcal(ref_sql_time)
    refcal = sql_refcal_to_scan(legacy)
    sidecar_file = find_sidecar_by_timestamp(legacy["timestamp"])
    if sidecar_file is None:
        raise CalWidgetV2Error(
            "No v2 in-band sidecar was found for ref-sql-time {0}. Use --ref-npz or analyze the refcal once in v2.".format(
                ref_sql_time
            )
        )
    attach_sidecar_delay(refcal, load_sidecar(sidecar_file))
    refcal.sidecar_path = sidecar_file
    return refcal


def run_refcal(args) -> int:
    """Run refcal comparison."""

    v2 = analyze_refcal_input(args.npz, fix_drift=args.fix_drift)
    legacy = _load_legacy_refcal(args.sql_time)
    metrics = save_refcal_report(v2, legacy, args.outdir, ant=args.ant)
    summary = metrics_to_jsonable(metrics)
    if args.json_summary:
        save_metrics_json(metrics, args.json_summary)
    print("refcal comparison complete")
    print("npz:       {0}".format(args.npz))
    print("sql-time:  {0}".format(args.sql_time))
    print("outdir:    {0}".format(args.outdir))
    print("phase rms mean: {0:.4f} rad".format(float(np.nanmean(metrics["phase_wrapped_rms"]))))
    print("flag agreement mean: {0:.4f}".format(float(np.nanmean(metrics["flag_agreement"]))))
    return 0


def run_phacal(args) -> int:
    """Run phacal comparison."""

    if args.ref_npz:
        refcal = analyze_refcal_input(args.ref_npz, fix_drift=args.fix_drift)
    elif args.ref_sql_time:
        refcal = _refcal_from_ref_sql_time(args.ref_sql_time)
    else:
        raise CalWidgetV2Error("Phacal mode requires --ref-npz or --ref-sql-time.")
    v2 = analyze_phacal_input(args.npz, refcal, fix_drift=args.fix_drift)
    legacy = _load_legacy_phacal(args.sql_time)
    metrics = save_phacal_report(v2, legacy, args.outdir, ant=args.ant)
    if args.json_summary:
        save_metrics_json(metrics, args.json_summary)
    print("phacal comparison complete")
    print("npz:       {0}".format(args.npz))
    print("sql-time:  {0}".format(args.sql_time))
    print("ref:       {0}".format(args.ref_npz or args.ref_sql_time))
    print("outdir:    {0}".format(args.outdir))
    print("phase rms mean: {0:.4f} rad".format(float(np.nanmean(metrics["phase_wrapped_rms"]))))
    print("flag agreement mean: {0:.4f}".format(float(np.nanmean(metrics["flag_agreement"]))))
    print("mbd diff mean: {0:.4f} ns".format(float(np.nanmean(metrics["mbd_diff"]))))
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)

    ref = sub.add_parser("refcal", help="Compare one v2 refcal against legacy SQL")
    ref.add_argument("--npz", required=True, help="Raw scan NPZ saved by legacy pcal_anal.py")
    ref.add_argument("--sql-time", required=True, help="Legacy refcal SQL time (ISO or LabVIEW integer)")
    ref.add_argument("--outdir", required=True, help="Directory for report plots")
    ref.add_argument("--ant", type=int, default=None, help="0-based antenna index for detailed plots")
    ref.add_argument("--json-summary", default=None, help="Optional JSON summary output path")
    ref.add_argument("--fix-drift", dest="fix_drift", action="store_true", default=True, help="Apply legacy drift correction")
    ref.add_argument("--no-fix-drift", dest="fix_drift", action="store_false", help="Disable legacy drift correction")
    ref.set_defaults(func=run_refcal)

    pha = sub.add_parser("phacal", help="Compare one v2 phacal against legacy SQL")
    pha.add_argument("--npz", required=True, help="Raw phasecal NPZ saved by legacy pcal_anal.py")
    pha.add_argument("--sql-time", required=True, help="Legacy phacal SQL time (ISO or LabVIEW integer)")
    pha.add_argument("--ref-npz", default=None, help="Raw refcal NPZ for v2 analysis")
    pha.add_argument("--ref-sql-time", default=None, help="Legacy refcal SQL time if a v2 sidecar already exists")
    pha.add_argument("--outdir", required=True, help="Directory for report plots")
    pha.add_argument("--ant", type=int, default=None, help="0-based antenna index for detailed plots")
    pha.add_argument("--json-summary", default=None, help="Optional JSON summary output path")
    pha.add_argument("--fix-drift", dest="fix_drift", action="store_true", default=True, help="Apply legacy drift correction")
    pha.add_argument("--no-fix-drift", dest="fix_drift", action="store_false", help="Disable legacy drift correction")
    pha.set_defaults(func=run_phacal)
    return parser


def main(argv=None) -> int:
    """CLI entrypoint."""

    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        Path(args.outdir).mkdir(parents=True, exist_ok=True)
        return args.func(args)
    except CalWidgetV2Error as exc:
        print("error: {0}".format(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
