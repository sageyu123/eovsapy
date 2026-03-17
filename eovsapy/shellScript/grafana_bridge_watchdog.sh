#!/usr/bin/env bash
# Ensure grafana_stateframe_bridge.py is running and healthy on ovsa.

set -u

PORT=9105
POLL_INTERVAL=30
SESSION_NAME="grafana_bridge"
PYTHON_SCRIPT="/common/python/current/grafana_stateframe_bridge.py"
LOG_FILE="${HOME}/grafana_bridge_watchdog.log"
LOCK_FILE="/tmp/grafana_bridge_watchdog.lock"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  printf '%s %s\n' "$(timestamp)" "$1" >> "${LOG_FILE}"
}

run_bridge_cmd='
export PATH="${HOME}/.pyenv/bin:${PATH}"
if [ -x "${HOME}/.pyenv/bin/pyenv" ]; then
  eval "$(${HOME}/.pyenv/bin/pyenv init -)"
fi
pyenv global 2.7.18
python '"${PYTHON_SCRIPT}"' --port '"${PORT}"' --poll-interval '"${POLL_INTERVAL}"'
'

(
  flock -n 9 || exit 0

  health_ok=0
  if curl -fsS --max-time 4 "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then
    health_ok=1
  fi

  if [ "${health_ok}" -eq 1 ]; then
    exit 0
  fi

  log "Bridge unhealthy or down on port ${PORT}; restarting."

  if pgrep -f "${PYTHON_SCRIPT} --port ${PORT}" >/dev/null 2>&1; then
    pkill -f "${PYTHON_SCRIPT} --port ${PORT}" >/dev/null 2>&1 || true
    sleep 1
  fi

  if screen -list | grep -q "[.]${SESSION_NAME}[[:space:]]"; then
    screen -S "${SESSION_NAME}" -X quit >/dev/null 2>&1 || true
    sleep 1
  fi

  screen -S "${SESSION_NAME}" -dm bash -lc "${run_bridge_cmd}"
  sleep 2

  if curl -fsS --max-time 4 "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then
    log "Bridge restarted successfully."
    exit 0
  fi

  log "Bridge restart attempted but health check still failing."
  exit 1
) 9>"${LOCK_FILE}"
