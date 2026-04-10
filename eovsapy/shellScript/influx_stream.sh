#!/usr/bin/env bash
# Retired on 2026-03-23.
# The supported Influx ingestion path is:
#   eovsapy.acc_exporter -> /telegraf -> Telegraf -> InfluxDB

set -eu
echo "influx_stream.sh is retired; use acc_exporter.service instead." >&2
exit 1
