#!/usr/bin/env bash
# Launch the ACC exporter service that exposes full telemetry over HTTP.

set -eu

ENV_FILE="${EOVSA_PYTHON_ENV_FILE:-/home/user/.eovsa_exporter_env}"
CONDA_ROOT="${EOVSA_CONDA_ROOT:-/common/python/miniforge3}"
CONDA_ENV_NAME="${EOVSA_CONDA_ENV_NAME:-eovsapy3p10}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EOVSAPY_SRC_ROOT_DEFAULT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
EOVSAPY_SRC_ROOT="${EOVSA_PYTHON_SRC_ROOT:-${EOVSAPY_SRC_ROOT_DEFAULT}}"
ACC_INI_DEFAULT="/common/python/runtime-cache/acc.ini"
STATEFRAME_XML_DEFAULT="/common/python/runtime-cache/stateframe.xml"
if [ ! -f "${ACC_INI_DEFAULT}" ]; then
  ACC_INI_DEFAULT="ftp://acc.solar.pvt/ni-rt/startup/acc.ini"
fi
if [ ! -f "${STATEFRAME_XML_DEFAULT}" ] && [ -f "/common/python/current/stateframe.xml" ]; then
  STATEFRAME_XML_DEFAULT="/common/python/current/stateframe.xml"
fi
ACC_INI="${EOVSA_ACC_INI:-${ACC_INI_DEFAULT}}"
STATEFRAME_XML="${EOVSA_STATEFRAME_XML:-${STATEFRAME_XML_DEFAULT}}"
POLL_INTERVAL="${EOVSA_POLL_INTERVAL:-1.0}"
WORKDIR="${EOVSA_STREAM_WORKDIR:-/data1/workdir}"
EXPORTER_PORT="${EOVSA_EXPORTER_PORT:-9108}"
EXPORTER_BIND_HOST="${EOVSA_EXPORTER_BIND_HOST:-0.0.0.0}"
MEASUREMENT_PREFIX="${EOVSA_EXPORTER_MEASUREMENT_PREFIX:-eovsa_stateframe}"
STALE_AFTER="${EOVSA_EXPORTER_STALE_AFTER:-10.0}"

if [ -n "${ENV_FILE}" ] && [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
elif [ -z "${CONDA_PREFIX:-}" ] && [ -f "${CONDA_ROOT}/etc/profile.d/conda.sh" ]; then
  # shellcheck disable=SC1091
  source "${CONDA_ROOT}/etc/profile.d/conda.sh"
  conda activate "${CONDA_ENV_NAME}"
fi

# Make the installed shared-source tree importable regardless of whether the
# service env came from systemd, a sourced shell file, or direct execution.
export PYTHONPATH="${EOVSAPY_SRC_ROOT}:/common/python:/common/python/suncasa-src${PYTHONPATH:+:${PYTHONPATH}}"

cd "${WORKDIR}"

exec python -m eovsapy.acc_exporter \
  --acc-ini "${ACC_INI}" \
  --xml-path "${STATEFRAME_XML}" \
  --poll-interval "${POLL_INTERVAL}" \
  --bind-host "${EXPORTER_BIND_HOST}" \
  --port "${EXPORTER_PORT}" \
  --measurement-prefix "${MEASUREMENT_PREFIX}" \
  --stale-after "${STALE_AFTER}" \
  "$@"
