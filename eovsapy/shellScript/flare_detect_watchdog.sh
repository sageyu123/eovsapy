#!/bin/bash
#
# Ensure start_flare_detect.sh stays healthy after boot.
# - If not running, start it.
# - If running too long (likely hung), restart it.
#

set -u

START_SCRIPT="/common/python/current/start_flare_detect.sh"
WORKER_PATTERN="find_flare4date.py"
WATCHDOG_LOG="/tmp/flare_detect_watchdog.log"
JOB_LOG="/tmp/start_flare_detect.log"
LOCK_FILE="/tmp/flare_detect_watchdog.lock"
MAX_RUNTIME_SEC=$((6 * 3600))
MATCH_KIND=""

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
    echo "$(timestamp) $*" >> "${WATCHDOG_LOG}"
}

find_pid() {
    local pid
    pid="$(pgrep -f "${START_SCRIPT}" | head -n 1)"
    if [ -n "${pid}" ]; then
        MATCH_KIND="launcher"
        echo "${pid}"
        return 0
    fi

    pid="$(pgrep -f "${WORKER_PATTERN}" | head -n 1)"
    if [ -n "${pid}" ]; then
        MATCH_KIND="worker"
        echo "${pid}"
        return 0
    fi

    MATCH_KIND=""
    return 1
}

restart_process() {
    local old_pid="$1"

    if [ -n "${old_pid}" ]; then
        log "Restart requested for PID=${old_pid}"
        kill "${old_pid}" 2>/dev/null || true
        sleep 5
        if ps -p "${old_pid}" > /dev/null 2>&1; then
            log "PID=${old_pid} still alive after SIGTERM; sending SIGKILL"
            kill -9 "${old_pid}" 2>/dev/null || true
        fi
    fi

    nohup /bin/bash "${START_SCRIPT}" >> "${JOB_LOG}" 2>&1 &
    local new_pid="$!"
    log "Started ${START_SCRIPT} with PID=${new_pid}"
}

main() {
    local pid runtime_sec

    pid="$(find_pid 2>/dev/null || true)"
    if [ -z "${pid}" ]; then
        log "Process missing; starting ${START_SCRIPT}"
        restart_process ""
        return 0
    fi

    # If worker is alive, treat service as healthy. Launcher may be short-lived.
    if [ "${MATCH_KIND}" = "worker" ]; then
        log "Healthy worker PID=${pid} (matched ${WORKER_PATTERN})"
        return 0
    fi

    runtime_sec="$(ps -o etimes= -p "${pid}" 2>/dev/null | tr -d ' ')"
    if [ -z "${runtime_sec}" ]; then
        log "Could not read runtime for PID=${pid}; restarting"
        restart_process "${pid}"
        return 0
    fi

    if [ "${runtime_sec}" -gt "${MAX_RUNTIME_SEC}" ]; then
        log "PID=${pid} runtime=${runtime_sec}s exceeds ${MAX_RUNTIME_SEC}s; restarting"
        restart_process "${pid}"
        return 0
    fi

    log "Healthy launcher PID=${pid} runtime=${runtime_sec}s"
    return 0
}

(
    flock -n 9 || exit 0
    main
) 9>"${LOCK_FILE}"
