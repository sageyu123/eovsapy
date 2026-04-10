#!/usr/bin/env bash
# Retired on 2026-03-23.
# Keep this as a harmless no-op so stale cron entries cannot restart the old
# live_influx_stream path.

set -eu
echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') influx_stream_watchdog.sh retired; exporter/Telegraf is the supported path." >> /tmp/influx_stream_watchdog.log
exit 0
