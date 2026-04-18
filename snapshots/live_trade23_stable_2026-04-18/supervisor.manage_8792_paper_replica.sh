#!/bin/bash
set -euo pipefail
unset NICE || true

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -f "${ROOT_DIR}/.runtime_env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ROOT_DIR}/.runtime_env"
  set +a
fi
SCRIPT_PATH="${ROOT_DIR}/manage_8792_paper_replica.sh"
PORT="${PORT:-8792}"
LOCK_DIR="/tmp/polymarket_8792_paper_replica_supervisor.lockdir"
SUP_PID_FILE="/tmp/polymarket_8792_paper_replica_supervisor.pid"
CHILD_PID_FILE="/tmp/polymarket_8792_paper_replica_child.pid"
CHILD_START_TS_FILE="/tmp/polymarket_8792_paper_replica_child_start_ts"
HEARTBEAT_FILE="/tmp/polymarket_8792_paper_replica_supervisor.heartbeat"
LOG_FILE="/tmp/polymarket_8792_paper_replica_supervisor.log"
SERVER_LOG_FILE="/tmp/host_8792_paper_replica.log"
RESTART_REASON_FILE="/tmp/polymarket_8792_paper_replica_restart_reasons.jsonl"
PRE_RESTART_SNAPSHOT_FILE="/tmp/polymarket_8792_paper_replica_pre_restart.json"
RESTART_MARK_FILE="/tmp/polymarket_8792_paper_replica_restarting"
DEFAULT_TRADE_LOG_NAMESPACE="${DEFAULT_TRADE_LOG_NAMESPACE:-host_8792_paper_replica}"
DEFAULT_TRADE_LOG_DIR="${DEFAULT_TRADE_LOG_DIR:-${ROOT_DIR}/trade_logs/hosts/${DEFAULT_TRADE_LOG_NAMESPACE}}"
DEFAULT_MULTI_STATE_DB_PATH="${DEFAULT_MULTI_STATE_DB_PATH:-${DEFAULT_TRADE_LOG_DIR}/20_multi_market_state.sqlite}"
DEFAULT_RESUME_LATEST_RUN_ON_RESTART="${DEFAULT_RESUME_LATEST_RUN_ON_RESTART:-1}"
DEFAULT_FORCE_NEW_SERVER_RUN_ON_START="${DEFAULT_FORCE_NEW_SERVER_RUN_ON_START:-0}"
DEFAULT_NODE_CANDIDATES=(
  "${HOME}/.local/node22/bin/node"
  "$(command -v node 2>/dev/null || true)"
  "/opt/homebrew/bin/node"
  "/usr/local/bin/node"
)
NODE_BIN="${NODE_BIN:-}"
if [[ -z "${NODE_BIN}" ]]; then
  for candidate in "${DEFAULT_NODE_CANDIDATES[@]}"; do
    if [[ -n "${candidate}" && -x "${candidate}" ]]; then
      NODE_BIN="${candidate}"
      break
    fi
  done
fi
HEALTH_URL="http://localhost:${PORT}/api/health?lite=1"
STATE_URL="http://localhost:${PORT}/api/state"
BOTS_URL="http://localhost:${PORT}/api/v2/bots"
HEAL_URL="http://localhost:${PORT}/api/watchdog/heal"
READ_ONLY_WORKER_ORIGIN="${PAPER_REPLICA_READ_ONLY_ORIGIN:-http://127.0.0.1:29003}"
READ_ONLY_WORKER_HEALTH_URL="${READ_ONLY_WORKER_ORIGIN%/}/api/health"
EXPECTED_READ_ONLY_WORKER_UPSTREAM="${PAPER_REPLICA_EXPECTED_READ_ONLY_UPSTREAM:-http://127.0.0.1:${PORT}}"
EXPECTED_READ_ONLY_WORKER_SOURCE_PORT="${PAPER_REPLICA_EXPECTED_READ_ONLY_SOURCE_PORT:-${PORT}}"
READ_ONLY_WORKER_RESTART_SCRIPT="${READ_ONLY_WORKER_RESTART_SCRIPT:-${ROOT_DIR}/start_dublin_replica_worker_tunnel_remote.sh}"
REQUIRE_NONESSENTIAL_WORKER_FOR_START="${REQUIRE_NONESSENTIAL_WORKER_FOR_START:-0}"
MAX_READ_ONLY_WORKER_FAILS="${MAX_READ_ONLY_WORKER_FAILS:-6}"
EXPECTED_STRATEGY_ID="${EXPECTED_STRATEGY_ID:-}"
EXPECTED_STRATEGY_PATH="${EXPECTED_STRATEGY_PATH:-}"
CHECK_INTERVAL_SEC="${CHECK_INTERVAL_SEC:-0.5}"
HTTP_TIMEOUT_SEC="${HTTP_TIMEOUT_SEC:-5}"
MAX_BAD_HEALTH="${MAX_BAD_HEALTH:-8}"
MAX_STREAM_AGE_MS="${MAX_STREAM_AGE_MS:-4000}"
MAX_PAIR_LAG_MS="${MAX_PAIR_LAG_MS:-4000}"
MAX_QUOTE_GAP_MS="${MAX_QUOTE_GAP_MS:-800}"
MAX_QUOTE_KPI_STREAK="${MAX_QUOTE_KPI_STREAK:-24}"
QUOTE_BROADCAST_HEALTH_ENABLED="${QUOTE_BROADCAST_HEALTH_ENABLED:-0}"
MAX_RUNTIME_LAG_MS="${MAX_RUNTIME_LAG_MS:-600}"
MAX_HEARTBEAT_STALE_SEC="${MAX_HEARTBEAT_STALE_SEC:-10}"
STARTUP_HEALTH_GRACE_SEC="${STARTUP_HEALTH_GRACE_SEC:-20}"
CHILD_HANDOFF_GRACE_SEC="${CHILD_HANDOFF_GRACE_SEC:-12}"
HEARTBEAT_LOG_EVERY_SEC="${HEARTBEAT_LOG_EVERY_SEC:-15}"
MAX_ENDPOINT_DOWN_FAILS="${MAX_ENDPOINT_DOWN_FAILS:-8}"
HEAL_REQUEST_COOLDOWN_SEC="${HEAL_REQUEST_COOLDOWN_SEC:-1}"
MAX_DEGRADED_HEALTH_FAILS="${MAX_DEGRADED_HEALTH_FAILS:-8}"
RESTART_SAFE_WAIT_SEC="${RESTART_SAFE_WAIT_SEC:-0}"
RESTART_SAFE_END_WINDOW_SEC="${RESTART_SAFE_END_WINDOW_SEC:-20}"
IMMEDIATE_RESTART_ON_UNHEALTHY="${IMMEDIATE_RESTART_ON_UNHEALTHY:-0}"
AGENT_SCRIPT="${ROOT_DIR}/restart_audit_agent.js"
NO_TRADE_MONITOR_ENABLED="${NO_TRADE_MONITOR_ENABLED:-1}"
NO_TRADE_MONITOR_INTERVAL_SEC="${NO_TRADE_MONITOR_INTERVAL_SEC:-10}"
NO_TRADE_STREAK_THRESHOLD="${NO_TRADE_STREAK_THRESHOLD:-3}"
NO_TRADE_PAGE_LOAD_WARN_SEC="${NO_TRADE_PAGE_LOAD_WARN_SEC:-2.5}"
PAGE_CHECK_PATH="${PAGE_CHECK_PATH:-/live}"
STALL_MONITOR_ENABLED="${STALL_MONITOR_ENABLED:-1}"
STALL_MONITOR_INTERVAL_SEC="${STALL_MONITOR_INTERVAL_SEC:-10}"
STALL_RUNTIME_LAG_TRIGGER_MS="${STALL_RUNTIME_LAG_TRIGGER_MS:-2500}"
STALL_PAIR_LAG_MAX_MS="${STALL_PAIR_LAG_MAX_MS:-100}"
STALL_STREAM_AGE_MAX_MS="${STALL_STREAM_AGE_MAX_MS:-100}"
STALL_CONSECUTIVE_THRESHOLD="${STALL_CONSECUTIVE_THRESHOLD:-2}"
STALL_RESTART_THRESHOLD="${STALL_RESTART_THRESHOLD:-6}"
STALL_LOG_WINDOW_LINES="${STALL_LOG_WINDOW_LINES:-120}"
STALL_SLOW_LOG_THRESHOLD="${STALL_SLOW_LOG_THRESHOLD:-3}"
STALL_HARD_RESTART_RUNTIME_LAG_MS="${STALL_HARD_RESTART_RUNTIME_LAG_MS:-500}"
STALL_HARD_RESTART_CPU_PCT="${STALL_HARD_RESTART_CPU_PCT:-85}"
STALL_HARD_RESTART_HEAP_MB="${STALL_HARD_RESTART_HEAP_MB:-700}"
STALL_FAST_RESTART_RUNTIME_LAG_MS="${STALL_FAST_RESTART_RUNTIME_LAG_MS:-400}"
STALL_FAST_RESTART_CPU_PCT="${STALL_FAST_RESTART_CPU_PCT:-55}"
STALL_FAST_RESTART_HEAP_MB="${STALL_FAST_RESTART_HEAP_MB:-500}"
ACTIVE_SLUG_RESTART_RECOVERY_ENABLED="${ACTIVE_SLUG_RESTART_RECOVERY_ENABLED:-1}"
ACTIVE_SLUG_RESTART_MIN_AGE_SEC="${ACTIVE_SLUG_RESTART_MIN_AGE_SEC:-15}"

log_line() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" >> "${LOG_FILE}"
}

set_child_pid_if_changed() {
  local pid="${1:-}"
  local tag="${2:-child_pid_sync}"
  [[ -n "${pid}" ]] || return 1
  local cur=""
  if [[ -f "${CHILD_PID_FILE}" ]]; then
    cur="$(cat "${CHILD_PID_FILE}" 2>/dev/null || true)"
  fi
  if [[ "${cur}" == "${pid}" ]]; then
    return 0
  fi
  echo "${pid}" > "${CHILD_PID_FILE}"
  log_line "${tag} pid=${pid} prev=${cur:-none}"
  return 0
}

find_supervisor_pids() {
  ps ax -o pid=,command= 2>/dev/null | awk -v script="${SCRIPT_PATH}" '
    {
      pid = $1
      $1 = ""
      sub(/^[[:space:]]+/, "", $0)
      cmd = $0
      if (cmd == "" || pid == "") next
      if (cmd !~ /(^|[[:space:]])(__supervise)($|[[:space:]])/) next
      if (index(cmd, script) == 0) next
      print pid
    }
  ' || true
}

kill_duplicate_supervisors() {
  local keep_pid="${1:-}"
  [[ -n "${keep_pid}" ]] || return 0
  local pid=""
  while read -r pid; do
    [[ -n "${pid}" ]] || continue
    [[ "${pid}" == "${keep_pid}" ]] && continue
    if kill -0 "${pid}" 2>/dev/null; then
      log_line "duplicate_supervisor_kill pid=${pid} keep=${keep_pid}"
      kill "${pid}" 2>/dev/null || true
      sleep 1
      kill -9 "${pid}" 2>/dev/null || true
    fi
  done < <(find_supervisor_pids)
}

canonical_supervisor_pid() {
  local pid=""
  if [[ -f "${SUP_PID_FILE}" ]]; then
    pid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
    if pid_is_alive "${pid}"; then
      echo "${pid}"
      return 0
    fi
  fi
  local first=""
  first="$(find_supervisor_pids | sort -n | head -n 1 || true)"
  if pid_is_alive "${first}"; then
    echo "${first}"
    return 0
  fi
  return 1
}

exit_if_not_canonical_supervisor() {
  local self_pid="${1:-$$}"
  local canonical=""
  canonical="$(canonical_supervisor_pid || true)"
  [[ -n "${canonical}" ]] || return 0
  if [[ "${canonical}" != "${self_pid}" ]]; then
    log_line "supervisor_duplicate_exit pid=${self_pid} canonical=${canonical}"
    exit 0
  fi
}

pid_is_alive() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" 2>/dev/null
}

cleanup_stale_supervisor_state() {
  local spid=""
  local cpid=""
  if [[ -f "${SUP_PID_FILE}" ]]; then
    spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
    if ! pid_is_alive "${spid}"; then
      local live_sup=""
      live_sup="$(find_supervisor_pids | head -n 1 || true)"
      if pid_is_alive "${live_sup}"; then
        echo "${live_sup}" > "${SUP_PID_FILE}"
        spid="${live_sup}"
        log_line "supervisor_pid_recovered pid=${live_sup}"
      else
        rm -f "${SUP_PID_FILE}"
        spid=""
      fi
    fi
  fi
  if [[ -f "${CHILD_PID_FILE}" ]]; then
    cpid="$(cat "${CHILD_PID_FILE}" 2>/dev/null || true)"
    if ! pid_is_alive "${cpid}"; then
      local listener_pid=""
      listener_pid="$(detect_existing_listener_pid || true)"
      if pid_is_alive "${listener_pid}"; then
        set_child_pid_if_changed "${listener_pid}" "child_pid_recovered_from_listener"
        cpid="${listener_pid}"
      else
        rm -f "${CHILD_PID_FILE}"
        cpid=""
      fi
    fi
  fi
  if [[ -d "${LOCK_DIR}" ]]; then
    if [[ -z "${spid}" ]] && [[ -f "${SUP_PID_FILE}" ]]; then
      spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
    fi
    if ! pid_is_alive "${spid}"; then
      rmdir "${LOCK_DIR}" 2>/dev/null || rm -rf "${LOCK_DIR}" 2>/dev/null || true
    fi
  fi
  kill_stray_strategy_pids "${cpid:-}" "cleanup_stale_state_prune_strays"
}

detect_existing_listener_pid() {
  local pid=""
  pid="$(lsof -t -iTCP:${PORT} -sTCP:LISTEN 2>/dev/null | head -n 1 || true)"
  if [[ -n "${pid}" ]]; then
    echo "${pid}"
    return 0
  fi
  return 1
}

find_strategy_pids_for_port() {
  local pid=""
  while read -r pid; do
    [[ -n "${pid}" ]] || continue
    local proc=""
    proc="$(ps eww -p "${pid}" -o pid=,args= 2>/dev/null || true)"
    [[ -n "${proc}" ]] || continue
    [[ "${proc}" == *"live_bot_server_strategy_paper_replica.ts"* ]] || continue
    [[ "${proc}" == *"PORT=${PORT}"* ]] || continue
    awk '{print $1}' <<< "${proc}"
  done < <(pgrep -f "live_bot_server_strategy_paper_replica.ts" 2>/dev/null || true)
}

kill_stray_strategy_pids() {
  local keep_pid="${1:-}"
  local reason="${2:-stray_strategy_cleanup}"
  local pid=""
  while read -r pid; do
    [[ -n "${pid}" ]] || continue
    [[ -n "${keep_pid}" && "${pid}" == "${keep_pid}" ]] && continue
    if kill -0 "${pid}" 2>/dev/null; then
      log_line "${reason} pid=${pid} keep=${keep_pid:-none}"
      kill "${pid}" 2>/dev/null || true
      sleep 1
      kill -9 "${pid}" 2>/dev/null || true
    fi
  done < <(find_strategy_pids_for_port)
}

to_int() {
  local v="${1:-}"
  if [[ -z "${v}" ]]; then
    echo ""
    return 1
  fi
  awk 'BEGIN{ v=ARGV[1]+0; printf "%.0f\n", v }' "${v}" 2>/dev/null || return 1
}

slug_start_ms_from_slug() {
  local slug="${1:-}"
  [[ -n "${slug}" ]] || return 1
  local tail="${slug##*-}"
  [[ "${tail}" =~ ^[0-9]{10,13}$ ]] || return 1
  if (( ${#tail} >= 13 )); then
    echo "${tail}"
  else
    echo "${tail}000"
  fi
}

read_only_worker_health_json() {
  curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${READ_ONLY_WORKER_HEALTH_URL}" 2>/dev/null || return 1
}

is_read_only_worker_ok() {
  local json=""
  json="$(read_only_worker_health_json)" || return 1
  jq -e --arg expected "${EXPECTED_READ_ONLY_WORKER_UPSTREAM}" --arg expectedSourcePort "${EXPECTED_READ_ONLY_WORKER_SOURCE_PORT}" '
    (.ok == true)
    and ((.worker // .workerLabel // "") | tostring | length > 0)
    and (
      $expected == ""
      or ((.upstreamOrigin // "") | tostring) == $expected
      or ((.liveOnlySourceHostPort // .sourceHostPort // "") | tostring) == $expectedSourcePort
    )
  ' <<< "${json}" >/dev/null 2>&1
}

restart_read_only_worker_tunnel() {
  if [[ ! -x "${READ_ONLY_WORKER_RESTART_SCRIPT}" ]]; then
    log_line "read_only_worker_restart unavailable script=${READ_ONLY_WORKER_RESTART_SCRIPT}"
    return 1
  fi
  if "${READ_ONLY_WORKER_RESTART_SCRIPT}" >> "${LOG_FILE}" 2>&1; then
    log_line "read_only_worker_restart status=ok script=${READ_ONLY_WORKER_RESTART_SCRIPT}"
    return 0
  fi
  log_line "read_only_worker_restart status=failed script=${READ_ONLY_WORKER_RESTART_SCRIPT}"
  return 1
}

ensure_nonessential_worker_supervisor() {
  local worker_origin="${READ_ONLY_WORKER_ORIGIN}"
  if [[ ! "${worker_origin}" =~ ^https?://(127\.0\.0\.1|localhost)(:[0-9]+)?(/.*)?$ ]]; then
    log_line "ensure_nonessential_worker skip=remote_origin origin=${worker_origin}"
    return 0
  fi
  if is_read_only_worker_ok; then
    log_line "ensure_nonessential_worker status=already_up origin=${worker_origin} upstream=${EXPECTED_READ_ONLY_WORKER_UPSTREAM}"
    return 0
  fi
  restart_read_only_worker_tunnel || true
  sleep 3
  if is_read_only_worker_ok; then
    log_line "ensure_nonessential_worker status=recovered origin=${worker_origin} upstream=${EXPECTED_READ_ONLY_WORKER_UPSTREAM}"
    return 0
  fi
  local worker_health=""
  worker_health="$(read_only_worker_health_json || true)"
  log_line "ensure_nonessential_worker status=unhealthy origin=${worker_origin} upstream=${EXPECTED_READ_ONLY_WORKER_UPSTREAM} health=${worker_health:-null}"
  return 1
}

is_health_ok() {
  local json=""
  json="$(curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${HEALTH_URL}" 2>/dev/null)" || return 1
  local slug=""
  local connected=""
  local stream_age_raw=""
  local pair_lag_raw=""
  slug="$(jq -r '.current.slug // empty' <<< "${json}" 2>/dev/null || true)"
  connected="$(jq -r '.health.quoteFeed.streamConnected // false' <<< "${json}" 2>/dev/null || true)"
  stream_age_raw="$(jq -r '.health.quoteFeed.streamLastMsgAgeMs // .health.quoteFeed.streamLastPriceMsgAgeMs // 99999999' <<< "${json}" 2>/dev/null || true)"
  pair_lag_raw="$(jq -r '.health.quoteFeed.pairLagMs // (if (.health.quoteFeed.upLagMs // null) != null and (.health.quoteFeed.downLagMs // null) != null then ([.health.quoteFeed.upLagMs,.health.quoteFeed.downLagMs]|max) else 99999999 end)' <<< "${json}" 2>/dev/null || true)"
  local quote_gap_raw=""
  local quote_streak_raw=""
  quote_gap_raw="$(jq -r '.health.quoteKpi.broadcastGapMsLatest // 0' <<< "${json}" 2>/dev/null || true)"
  quote_streak_raw="$(jq -r '([.health.quoteKpi.gapViolationStreak // 0, .health.quoteKpi.feedViolationStreak // 0] | max)' <<< "${json}" 2>/dev/null || true)"
  local stream_age=""
  local pair_lag=""
  local quote_gap=""
  local quote_streak=""
  stream_age="$(to_int "${stream_age_raw}" || true)"
  pair_lag="$(to_int "${pair_lag_raw}" || true)"
  quote_gap="$(to_int "${quote_gap_raw}" || true)"
  quote_streak="$(to_int "${quote_streak_raw}" || true)"
  [[ -n "${slug}" ]] || return 1
  [[ -n "${pair_lag}" ]] || return 1
  if [[ -z "${stream_age}" ]]; then
    if [[ "${connected}" == "true" ]] && (( pair_lag <= MAX_PAIR_LAG_MS )); then
      stream_age="${pair_lag}"
    else
      return 1
    fi
  fi
  [[ -n "${quote_gap}" ]] || quote_gap=0
  [[ -n "${quote_streak}" ]] || quote_streak=0
  (( stream_age <= MAX_STREAM_AGE_MS )) || return 1
  (( pair_lag <= MAX_PAIR_LAG_MS )) || return 1
  return 0
}

health_snapshot_json() {
  local json=""
  json="$(curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${HEALTH_URL}" 2>/dev/null)" || return 1
  jq -c '{
    t: now | todate,
    slug: (.current.slug // null),
    runId: (.health.runId // null),
    runStartMs: (.health.runStartMs // null),
    serverStartedAtMs: (.health.serverStartedAtMs // null),
    strategyPath: (.health.strategyPath // null),
    strategyBots: (.health.strategyBots // []),
    streamConnected: (.health.quoteFeed.streamConnected // false),
    streamAgeMs: (.health.quoteFeed.streamLastMsgAgeMs // .health.quoteFeed.streamLastPriceMsgAgeMs // null),
    pairLagMs: (.health.quoteFeed.pairLagMs // null),
    quoteGapMs: (.health.quoteKpi.broadcastGapMsLatest // null),
    quoteStreak: ([.health.quoteKpi.gapViolationStreak // 0, .health.quoteKpi.feedViolationStreak // 0] | max),
    runtimeLagMs: (.health.runtimeLagMs // null),
    cpuPct: (.health.processStats.cpuPct // null),
    heapUsedMb: (.health.processStats.heapUsedMb // null),
    watchdogReason: (.health.latencyWatchdog.watchdogLastReason // null),
    watchdogHealCount: (.health.latencyWatchdog.watchdogHealCount // null)
  }' <<< "${json}" 2>/dev/null || return 1
}

health_issue_codes() {
  local snap="${1:-}"
  [[ -n "${snap}" ]] || { echo "endpoint_down"; return 0; }
  jq -r --argjson maxStream "${MAX_STREAM_AGE_MS}" --argjson maxPair "${MAX_PAIR_LAG_MS}" --argjson maxGap "${MAX_QUOTE_GAP_MS}" --argjson maxStreak "${MAX_QUOTE_KPI_STREAK}" --argjson maxRuntime "${MAX_RUNTIME_LAG_MS}" --argjson quoteGapEnabled "$([[ "${QUOTE_BROADCAST_HEALTH_ENABLED}" == "1" ]] && echo true || echo false)" '
    [
      (if (.slug // "") == "" then "missing_slug" else empty end),
      (if (.streamConnected // false) != true then "stream_disconnected" else empty end),
      (if (.streamAgeMs != null and (.streamAgeMs | tonumber) > $maxStream) then "stream_age_high" else empty end),
      (if ((.pairLagMs // 99999999) > $maxPair) then "pair_lag_high" else empty end),
      (if $quoteGapEnabled and ((.quoteGapMs // 0) > $maxGap) then "quote_gap_high" else empty end),
      (if $quoteGapEnabled and ((.quoteStreak // 0) > $maxStreak) then "quote_kpi_streak_high" else empty end),
      (if ((.runtimeLagMs // 0) > $maxRuntime) then "runtime_lag_high" else empty end),
      (if ((.watchdogReason // null) != null and (.watchdogReason | tostring | length) > 0) then ("watchdog:" + (.watchdogReason | tostring)) else empty end)
    ] | join(",")
  ' <<< "${snap}" 2>/dev/null || echo "parse_error"
}

log_restart_reason() {
  local action="${1:-unknown}"
  local reason="${2:-unknown}"
  local snapshot="${3:-}"
  local line
  line="$(jq -cn --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg action "${action}" --arg reason "${reason}" --arg port "${PORT}" --argjson snap "${snapshot:-null}" '{ts:$ts,port:($port|tonumber),action:$action,reason:$reason,health:$snap}')"
  echo "${line}" >> "${RESTART_REASON_FILE}"
  log_line "restart_reason action=${action} reason=${reason} snapshot=${snapshot:-null}"
}

sanitize_heal_reason_codes() {
  local raw="${1:-}"
  if [[ -z "${raw}" ]]; then
    echo ""
    return 0
  fi
  awk -F',' '
    {
      for (i = 1; i <= NF; i++) {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
        if ($i == "" || $i ~ /^watchdog:/) continue
        if (!seen[$i]++) out[++n] = $i
      }
    }
    END {
      max = (n < 6 ? n : 6)
      for (i = 1; i <= max; i++) {
        printf "%s%s", out[i], (i < max ? "," : "")
      }
    }
  ' <<< "${raw}"
}

restart_worthy_issue_codes() {
  local raw="${1:-}"
  if [[ -z "${raw}" ]]; then
    echo ""
    return 0
  fi
  awk -F',' '
    BEGIN {
      severe["missing_slug"] = 1
      severe["stream_disconnected"] = 1
      severe["pair_lag_high"] = 1
      severe["endpoint_down"] = 1
      severe["parse_error"] = 1
    }
    {
      for (i = 1; i <= NF; i++) {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
        if ($i == "" || $i ~ /^watchdog:/) continue
        if (!severe[$i]) continue
        if (!seen[$i]++) out[++n] = $i
      }
    }
    END {
      for (i = 1; i <= n; i++) {
        printf "%s%s", out[i], (i < n ? "," : "")
      }
    }
  ' <<< "${raw}"
}

capture_pre_restart_snapshot() {
  local health=""
  local bots=""
  health="$(curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${HEALTH_URL}" 2>/dev/null || true)"
  bots="$(curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${BOTS_URL}" 2>/dev/null || true)"
  jq -cn --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson health "${health:-null}" --argjson bots "${bots:-null}" '{capturedAt:$ts,health:$health,bots:$bots}' > "${PRE_RESTART_SNAPSHOT_FILE}" 2>/dev/null || true
}

request_watchdog_heal() {
  local reason="${1:-supervisor_degraded_health}"
  curl -fsS -m "${HTTP_TIMEOUT_SEC}" -X POST -H 'content-type: application/json' \
    --data "{\"reason\":\"${reason}\"}" \
    "${HEAL_URL}" >/dev/null 2>&1
}

read_bots_json() {
  curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${BOTS_URL}" 2>/dev/null || return 1
}

slug_trade_seen_from_bots_json() {
  local slug="${1:-}"
  local bots_json="${2:-}"
  [[ -n "${slug}" && -n "${bots_json}" ]] || return 1
  jq -e --arg slug "${slug}" '
    def items: (.items // []);
    def running_items: [items[] | select((.status // "" | tostring | ascii_downcase) == "running")];
    def pick: (running_items[0] // items[0] // null);
    pick as $bot
    | (
        (($bot.recentTradeEvents // []) | any(.;
          ((.marketSlug // .sessionSlug // .slug // "") | tostring) == $slug
          and ((.event // "") | tostring | test("enter|fill|buy|sell|tp_placed|exit_fill"; "i"))
        ))
        or
        ((((($bot.runtime // {}).marketSlug // $bot.marketSlug // "") | tostring) == $slug) and (((($bot.runtime // {}).entered) // false) == true))
      )
  ' <<< "${bots_json}" >/dev/null 2>&1
}

page_stability_probe_json() {
  local target_url="http://localhost:${PORT}${PAGE_CHECK_PATH}"
  curl -sS -m "${HTTP_TIMEOUT_SEC}" -o /dev/null \
    -w "{\"httpCode\":%{http_code},\"timeTotalSec\":%{time_total}}" \
    "${target_url}" 2>/dev/null || return 1
}

page_probe_is_bad() {
  local probe_json="${1:-}"
  [[ -n "${probe_json}" ]] || return 0
  jq -e --argjson warnSec "${NO_TRADE_PAGE_LOAD_WARN_SEC}" '
    ((.httpCode // 0) != 200)
    or
    (((.timeTotalSec // 999) | tonumber) > $warnSec)
  ' <<< "${probe_json}" >/dev/null 2>&1
}

stall_log_signature_json() {
  local lines="${STALL_LOG_WINDOW_LINES}"
  if [[ ! -f "${SERVER_LOG_FILE}" ]]; then
    echo '{"slowCount":0,"watchdogCount":0,"livePosSyncTimeoutCount":0,"quoteHoldCount":0}'
    return 0
  fi
  tail -n "${lines}" "${SERVER_LOG_FILE}" 2>/dev/null | awk '
    BEGIN {
      slow=0; watchdog=0; livepos=0; quotehold=0;
    }
    /BOT RUNTIME SLOW DETAIL/ { slow++ }
    /WATCHDOG HEAL/ { watchdog++ }
    /LIVE POS SYNC ERROR \| .*timeout/ { livepos++ }
    /QUOTE HOLD/ { quotehold++ }
    END {
      printf "{\"slowCount\":%d,\"watchdogCount\":%d,\"livePosSyncTimeoutCount\":%d,\"quoteHoldCount\":%d}\n", slow, watchdog, livepos, quotehold;
    }
  '
}

stall_signature_detected() {
  local snap="${1:-}"
  local log_sig="${2:-}"
  [[ -n "${snap}" ]] || return 1
  [[ -n "${log_sig}" ]] || log_sig='{}'
  jq -e \
    --argjson runtimeMin "${STALL_RUNTIME_LAG_TRIGGER_MS}" \
    --argjson pairMax "${STALL_PAIR_LAG_MAX_MS}" \
    --argjson streamMax "${STALL_STREAM_AGE_MAX_MS}" \
    --argjson slowMin "${STALL_SLOW_LOG_THRESHOLD}" '
    . as $snap
    | (
        (($snap.pairLagMs // 99999999) <= $pairMax) and
        (($snap.streamAgeMs // 99999999) <= $streamMax)
      ) as $feedFresh
    | (
        (($snap.runtimeLagMs // 0) >= $runtimeMin) or
        (($snap.watchdogReason // "" | tostring) == "main_loop_stall")
      ) as $loopBad
    | (
        ((($log.slowCount // 0) >= $slowMin)) or
        ((($log.livePosSyncTimeoutCount // 0) >= 2)) or
        ((($log.watchdogCount // 0) >= 1))
      ) as $logBad
    | ($feedFresh and $loopBad and $logBad)
  ' --argjson log "${log_sig}" <<< "${snap}" >/dev/null 2>&1
}

stall_signature_hard_restart_recommended() {
  local snap="${1:-}"
  [[ -n "${snap}" ]] || return 1
  jq -e \
    --argjson runtimeMin "${STALL_HARD_RESTART_RUNTIME_LAG_MS}" \
    --argjson cpuMin "${STALL_HARD_RESTART_CPU_PCT}" \
    --argjson heapMin "${STALL_HARD_RESTART_HEAP_MB}" '
    (
      (($snap.runtimeLagMs // 0) >= $runtimeMin)
      and
      (
        (($snap.cpuPct // 0) >= $cpuMin)
        or
        (($snap.heapUsedMb // 0) >= $heapMin)
      )
    )
  ' <<< "${snap}" >/dev/null 2>&1
}

stall_signature_fast_restart_recommended() {
  local snap="${1:-}"
  [[ -n "${snap}" ]] || return 1
  jq -e \
    --argjson runtimeMin "${STALL_FAST_RESTART_RUNTIME_LAG_MS}" \
    --argjson cpuMin "${STALL_FAST_RESTART_CPU_PCT}" \
    --argjson heapMin "${STALL_FAST_RESTART_HEAP_MB}" '
    (
      (($snap.runtimeLagMs // 0) >= $runtimeMin)
      and
      (($snap.watchdogReason // "" | tostring) == "main_loop_stall")
      and
      (
        (($snap.cpuPct // 0) >= $cpuMin)
        or
        (($snap.heapUsedMb // 0) >= $heapMin)
      )
    )
  ' <<< "${snap}" >/dev/null 2>&1
}

active_slug_restart_recovery_needed() {
  local snap="${1:-}"
  [[ "${ACTIVE_SLUG_RESTART_RECOVERY_ENABLED}" == "1" ]] || return 1
  [[ -n "${snap}" ]] || return 1
  local slug=""
  local server_started_ms=""
  slug="$(jq -r '.slug // empty' <<< "${snap}" 2>/dev/null || true)"
  server_started_ms="$(jq -r '.serverStartedAtMs // empty' <<< "${snap}" 2>/dev/null || true)"
  [[ -n "${slug}" && -n "${server_started_ms}" ]] || return 1
  [[ "${server_started_ms}" =~ ^[0-9]+$ ]] || return 1
  local slug_start_ms=""
  slug_start_ms="$(slug_start_ms_from_slug "${slug}" || true)"
  [[ -n "${slug_start_ms}" ]] || return 1
  local min_age_ms=$(( ACTIVE_SLUG_RESTART_MIN_AGE_SEC * 1000 ))
  local server_started_delta_ms=$(( server_started_ms - slug_start_ms ))
  if (( server_started_delta_ms < min_age_ms )); then
    return 1
  fi
  return 0
}

monitor_no_trade_streak() {
  local health_json="${1:-}"
  local bots_json="${2:-}"
  [[ "${NO_TRADE_MONITOR_ENABLED}" == "1" ]] || return 0
  [[ -n "${health_json}" ]] || return 0

  local current_slug=""
  current_slug="$(jq -r '.current.slug // empty' <<< "${health_json}" 2>/dev/null || true)"
  [[ -n "${current_slug}" ]] || return 0

  local trade_seen_now=0
  if slug_trade_seen_from_bots_json "${current_slug}" "${bots_json}"; then
    trade_seen_now=1
  fi

  if [[ -z "${NO_TRADE_TRACKED_SLUG:-}" ]]; then
    NO_TRADE_TRACKED_SLUG="${current_slug}"
    NO_TRADE_TRACKED_SLUG_TRADE_SEEN="${trade_seen_now}"
    return 0
  fi

  if [[ "${current_slug}" == "${NO_TRADE_TRACKED_SLUG}" ]]; then
    if (( trade_seen_now == 1 )); then
      NO_TRADE_TRACKED_SLUG_TRADE_SEEN=1
    fi
    return 0
  fi

  local finalized_slug="${NO_TRADE_TRACKED_SLUG}"
  local finalized_trade_seen="${NO_TRADE_TRACKED_SLUG_TRADE_SEEN:-0}"
  if (( finalized_trade_seen == 1 )); then
    NO_TRADE_STREAK_COUNT=0
  else
    NO_TRADE_STREAK_COUNT=$(( ${NO_TRADE_STREAK_COUNT:-0} + 1 ))
  fi
  log_line "no_trade_slug_finalize slug=${finalized_slug} had_trade=${finalized_trade_seen} streak=${NO_TRADE_STREAK_COUNT} next_slug=${current_slug}"

  if (( finalized_trade_seen == 0 )) && (( NO_TRADE_STREAK_COUNT >= NO_TRADE_STREAK_THRESHOLD )); then
    if [[ "${NO_TRADE_LAST_ALERT_SLUG:-}" != "${finalized_slug}" ]]; then
      local snap=""
      local codes=""
      local restart_codes=""
      local page_probe=""
      local page_bad=0
      snap="$(health_snapshot_json || true)"
      codes="$(health_issue_codes "${snap}" || true)"
      restart_codes="$(restart_worthy_issue_codes "${codes:-}" || true)"
      page_probe="$(page_stability_probe_json || true)"
      if page_probe_is_bad "${page_probe}"; then
        page_bad=1
      fi
      log_line "no_trade_streak_alert streak=${NO_TRADE_STREAK_COUNT} finalized_slug=${finalized_slug} current_slug=${current_slug} page=${page_probe:-null} codes=${codes:-none} snapshot=${snap:-null}"
      capture_pre_restart_snapshot
      request_watchdog_heal "supervisor_no_trade_streak:${NO_TRADE_STREAK_COUNT}" || true
      if [[ -n "${restart_codes}" ]] || (( page_bad == 1 )); then
        log_restart_reason "restart" "no_trade_streak" "${snap:-null}"
        log_line "no_trade_streak_restart streak=${NO_TRADE_STREAK_COUNT} child=$(cat "${CHILD_PID_FILE}" 2>/dev/null || true) page_bad=${page_bad} restart_codes=${restart_codes:-none}"
        wait_for_safe_restart_window "no_trade_streak" || true
        stop_child
        start_child
        run_restart_agent "post_restart" "no_trade_streak"
        NO_TRADE_STREAK_COUNT=0
      fi
      NO_TRADE_LAST_ALERT_SLUG="${finalized_slug}"
    fi
  fi

  NO_TRADE_TRACKED_SLUG="${current_slug}"
  NO_TRADE_TRACKED_SLUG_TRADE_SEEN="${trade_seen_now}"
}

restart_window_snapshot_json() {
  curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${STATE_URL}" 2>/dev/null | jq -c '{
    slug: (.current.slug // null),
    startMs: (.current.startMs // null),
    endMs: (.current.endMs // null),
    nowMs: (.t // null),
    paperEntered: (.paper.st.entered // false),
    liveEntered: (.live.st.entered // false),
    hostEntered: (.st.entered // false),
    liveExposure: (.liveExecution.openExposure // false),
    orderFlowActive: (.liveExecution.orderFlowActive // false),
    botOpenExposureCount: (.botRuntimeState.openExposureCount // 0)
  }' || return 1
}

restart_window_state() {
  local snap="${1:-}"
  [[ -n "${snap}" ]] || { echo "unknown"; return 1; }
  jq -r --argjson endWindow "${RESTART_SAFE_END_WINDOW_SEC}" '
    def truthy(v): (v == true);
    . as $s
    | (($s.paperEntered // false) or ($s.liveEntered // false) or ($s.hostEntered // false) or ($s.liveExposure // false) or ($s.orderFlowActive // false) or (($s.botOpenExposureCount // 0) > 0)) as $busy
    | (($s.nowMs // null) != null and ($s.endMs // null) != null) as $hasClock
    | (if $hasClock then ((($s.endMs | tonumber) - ($s.nowMs | tonumber)) / 1000) else null end) as $remainSec
    | if $busy then
        "busy"
      elif ($hasClock and ($remainSec != null) and ($remainSec <= $endWindow)) then
        "safe"
      elif ($hasClock and ($remainSec != null)) then
        "early"
      else
        "safe"
      end
  ' <<< "${snap}" 2>/dev/null || echo "unknown"
}

wait_for_safe_restart_window() {
  local reason="${1:-manual_restart}"
  if [[ "${IMMEDIATE_RESTART_ON_UNHEALTHY}" == "1" ]] && [[ "${reason}" != "manual_restart" ]]; then
    log_line "restart_window_bypass reason=${reason}"
    return 0
  fi
  local started_s
  started_s="$(date +%s)"
  local last_log_s=0
  while true; do
    local snap=""
    snap="$(restart_window_snapshot_json || true)"
    local state=""
    state="$(restart_window_state "${snap}" || true)"
    if [[ "${state}" == "safe" ]]; then
      log_line "restart_window_safe reason=${reason} snapshot=${snap:-null}"
      return 0
    fi
    local now_s
    now_s="$(date +%s)"
    if (( now_s - started_s >= RESTART_SAFE_WAIT_SEC )); then
      log_line "restart_window_timeout reason=${reason} state=${state:-unknown} snapshot=${snap:-null}"
      return 1
    fi
    if (( now_s - last_log_s >= 10 )); then
      log_line "restart_window_wait reason=${reason} state=${state:-unknown} snapshot=${snap:-null}"
      last_log_s="${now_s}"
    fi
    sleep 2
  done
}

run_restart_agent() {
  local event_name="${1:-post_start}"
  local reason="${2:-}"
  if [[ ! -f "${AGENT_SCRIPT}" ]]; then
    log_line "restart_agent_missing path=${AGENT_SCRIPT}"
    return 0
  fi
  (
    cd "${ROOT_DIR}"
    "${NODE_BIN}" "${AGENT_SCRIPT}" --port "${PORT}" --event "${event_name}" --reason "${reason}" --root "${ROOT_DIR}" --pre "${PRE_RESTART_SNAPSHOT_FILE}"
  ) >> "${LOG_FILE}" 2>&1 || true
}

run_with_transition_mark() {
  echo "$(date +%s)" > "${RESTART_MARK_FILE}" 2>/dev/null || true
  "$@"
  local rc=$?
  rm -f "${RESTART_MARK_FILE}" 2>/dev/null || true
  return "${rc}"
}

restart_supervisor_cmd() {
  wait_for_safe_restart_window "manual_restart" || true
  stop_supervisor
  start_supervisor
}

is_endpoint_up() {
  curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${HEALTH_URL}" >/dev/null 2>&1
}

is_expected_strategy_ok() {
  if [[ -z "${EXPECTED_STRATEGY_ID}" && -z "${EXPECTED_STRATEGY_PATH}" ]]; then
    return 0
  fi
  local json=""
  json="$(curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${HEALTH_URL}" 2>/dev/null)" || return 1
  jq -e --arg sid "${EXPECTED_STRATEGY_ID}" --arg sp "${EXPECTED_STRATEGY_PATH}" '
    def bot_match:
      (($sid == "" or (.strategyId // "") == $sid) and ($sp == "" or (.strategyPath // "") == $sp));
    (
      (.health.strategyBots // []) | map(select(bot_match)) | length
    ) > 0
    or
    (
      (($sid == "" or (.health.strategyId // "") == $sid) and ($sp == "" or (.health.strategyPath // "") == $sp))
    )
  ' <<< "${json}" >/dev/null 2>&1
}

adopt_existing_if_healthy() {
  local ep=""
  ep="$(detect_existing_listener_pid || true)"
  if [[ -n "${ep}" ]] && is_health_ok && is_expected_strategy_ok; then
    set_child_pid_if_changed "${ep}" "child_adopt"
    kill_stray_strategy_pids "${ep}" "child_adopt_prune_strays"
    return 0
  fi
  return 1
}

adopt_existing_if_endpoint_up() {
  local ep=""
  ep="$(detect_existing_listener_pid || true)"
  if [[ -n "${ep}" ]] && is_endpoint_up && is_expected_strategy_ok; then
    set_child_pid_if_changed "${ep}" "child_adopt_endpoint"
    kill_stray_strategy_pids "${ep}" "child_adopt_endpoint_prune_strays"
    return 0
  fi
  return 1
}

adopt_existing_listener_pid() {
  local tag="${1:-child_adopt_listener}"
  local ep=""
  ep="$(detect_existing_listener_pid || true)"
  if [[ -n "${ep}" ]]; then
    set_child_pid_if_changed "${ep}" "${tag}"
    kill_stray_strategy_pids "${ep}" "${tag}_prune_strays"
    return 0
  fi
  return 1
}

start_child() {
  if adopt_existing_if_healthy; then
    return 0
  fi
  if adopt_existing_if_endpoint_up; then
    return 0
  fi
  kill_stray_strategy_pids "" "pre_start_prune_strays"
  local stale_pids
  stale_pids="$(lsof -t -iTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${stale_pids}" ]]; then
    kill ${stale_pids} 2>/dev/null || true
    sleep 1
    stale_pids="$(lsof -t -iTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${stale_pids}" ]]; then
      kill -9 ${stale_pids} 2>/dev/null || true
      sleep 1
    fi
  fi

  local spawned=""
  spawned="$(
  (
    cd "${ROOT_DIR}"
    unset NICE
    set -a
    source ./.env
    set +a
    if [[ -z "${TRADE_LOG_DIR:-}" ]]; then
      _ns="${TRADE_LOG_NAMESPACE:-${STRATEGY_ID:-}}"
      if [[ -n "${_ns}" ]]; then
        export TRADE_LOG_DIR="${ROOT_DIR}/trade_logs/strategies/${_ns}"
      fi
    fi
    if [[ -n "${TRADE_LOG_DIR:-}" ]]; then
      mkdir -p "${TRADE_LOG_DIR}"
    fi
    local -a child_env=(
      "HOST=127.0.0.1"
      "BIND_HOST=127.0.0.1"
      "PAPER_REPLICA_STRATEGY_PATH=${PAPER_REPLICA_STRATEGY_PATH:-strategy_external_inflection_positive_iteration.js}"
      "LIVE_CLAIM_ENABLED=${LIVE_CLAIM_ENABLED:-1}"
      "TRADE_LOG_NAMESPACE=${TRADE_LOG_NAMESPACE:-${DEFAULT_TRADE_LOG_NAMESPACE}}"
      "TRADE_LOG_DIR=${TRADE_LOG_DIR:-${DEFAULT_TRADE_LOG_DIR}}"
      "MULTI_STATE_DB_PATH=${MULTI_STATE_DB_PATH:-${DEFAULT_MULTI_STATE_DB_PATH}}"
      "RESUME_LATEST_RUN_ON_RESTART=${RESUME_LATEST_RUN_ON_RESTART:-${DEFAULT_RESUME_LATEST_RUN_ON_RESTART}}"
      "FORCE_NEW_SERVER_RUN_ON_START=${FORCE_NEW_SERVER_RUN_ON_START:-${DEFAULT_FORCE_NEW_SERVER_RUN_ON_START}}"
      "QUOTE_PUMP_INTERVAL_MS=25"
      "QUOTE_PUMP_HEALTHY_INTERVAL_MS=50"
      "QUOTE_BROADCAST_INTERVAL_MS=25"
      "QUOTE_CLIENT_BROADCAST_ENABLED=1"
      "PRICES_CLIENT_BROADCAST_ENABLED=1"
      "PRICES_CLIENT_BROADCAST_INTERVAL_MS=250"
      "PRICES_STATE_BROADCAST_INTERVAL_MS=500"
      "PRICES_CLIENT_BROADCAST_MINIMAL=1"
      "BOT_RUNTIME_CLIENT_BROADCAST_ENABLED=1"
      "WS_SEND_MAX_BUFFER_BYTES=131072"
      "WS_TERMINATE_BUFFER_BYTES=262144"
      "WS_BACKPRESSURE_STRIKES_MAX=2"
      "QUOTE_STREAM_FORCE_RECONNECT_COOLDOWN_MS=12000"
      "QUOTE_STREAM_CONNECT_GRACE_MS=12000"
      "QUOTE_STREAM_OPEN_WARMUP_MS=7000"
      "QUOTE_STREAM_STALE_RECONNECT_MS=15000"
      "QUOTE_STREAM_HARD_RECONNECT_MS=30000"
      "QUOTE_KPI_RECONNECT_MIN_FEED_LAG_MS=10000"
      "QUOTE_KPI_RECONNECT_MIN_STREAM_AGE_MS=12000"
      "QUOTE_KPI_RECONNECT_MIN_UNHEALTHY_MS=10000"
      "QUOTE_FEED_HARD_FAIL_MS=3000"
      "WATCHDOG_EXIT_STARTUP_GRACE_MS=30000"
      "MAIN_LOOP_STALL_HARD_FAIL_MS=30000"
      "WATCHDOG_EXIT_ON_MAIN_LOOP_STALL=0"
      "MAIN_LOOP_PERIOD_MS=100"
      "BOT_RUNTIME_POLL_MS=100"
      "CURRENT_BUCKET_MARKET_CACHE_TTL_MS=10000"
      "BOT_RUNTIME_BROADCAST_MIN_INTERVAL_MS=250"
      "BOT_RUNTIME_BROADCAST_IDLE_INTERVAL_MS=1000"
      "QUOTE_MAX_STALE_MS=150"
      "QUOTE_STREAM_HEALTHY_MAX_AGE_MS=250"
      "QUOTE_FETCH_TIMEOUT_MS=1500"
      "QUOTE_FALLBACK_TRIGGER_MS=250"
      "QUOTE_FALLBACK_TIMEOUT_MS=1200"
      "QUOTE_FALLBACK_COOLDOWN_MS=1500"
      "QUOTE_BROADCAST_HEALTH_ENABLED=0"
      "BOT_RUN_TELEMETRY_INTERVAL_MS=2000"
      "BOT_RUN_TRUTH_RECON_INTERVAL_MS=3000"
      "PRICES_HEALTH_BROADCAST_INTERVAL_MS=1000"
      "HEALTH_SNAPSHOT_CACHE_MS=250"
      "HOT_HOST_TRACE_LIGHT_MODE=1"
      "HOT_SERVICE_MODE=1"
      "READ_ONLY_ORIGIN=${PAPER_REPLICA_READ_ONLY_ORIGIN:-http://127.0.0.1:29003}"
      "RUN_AUDIT_READ_ONLY_ORIGIN=${PAPER_REPLICA_RUN_AUDIT_READ_ONLY_ORIGIN:-${PAPER_REPLICA_READ_ONLY_ORIGIN:-http://127.0.0.1:29003}}"
      "RUN_AUDIT_WARM_ENABLED=0"
      "SESSION_CARD_MATERIALIZE_ENABLED=0"
      "PARA_SYNC_ENABLED=0"
      "HOST=${APP_BIND_HOST:-127.0.0.1}"
      "BIND_HOST=${APP_BIND_HOST:-127.0.0.1}"
      "PORT=${PORT}"
    )
    if command -v setsid >/dev/null 2>&1; then
      env -u NICE "${child_env[@]}" nohup setsid "${NODE_BIN}" --require tsx/cjs live_bot_server_strategy_paper_replica.ts >>"${SERVER_LOG_FILE}" 2>&1 </dev/null &
    else
      env -u NICE "${child_env[@]}" nohup "${NODE_BIN}" --require tsx/cjs live_bot_server_strategy_paper_replica.ts >>"${SERVER_LOG_FILE}" 2>&1 </dev/null &
    fi
    echo "$!"
  )
)"
  spawned="$(printf '%s\n' "${spawned}" | tail -n 1 | tr -d '[:space:]')"
  if [[ -z "${spawned}" ]]; then
    log_line "child_start_failed reason=missing_spawn_pid"
    return 1
  fi
  echo "${spawned}" > "${CHILD_PID_FILE}"
  date +%s > "${CHILD_START_TS_FILE}" 2>/dev/null || true
  log_line "child_start pid=${spawned}"
  local i=0
  while (( i < 100 )); do
    if [[ -n "${spawned}" ]] && kill -0 "${spawned}" 2>/dev/null && is_endpoint_up; then
      local listener_pid=""
      listener_pid="$(detect_existing_listener_pid || true)"
      if [[ -n "${listener_pid}" ]]; then
        set_child_pid_if_changed "${listener_pid}" "child_ready_listener"
        kill_stray_strategy_pids "${listener_pid}" "child_ready_prune_strays"
      fi
      run_restart_agent "post_start" "child_started"
      return 0
    fi
    if adopt_existing_if_healthy || adopt_existing_if_endpoint_up; then
      run_restart_agent "post_start" "child_adopted"
      return 0
    fi
    sleep 0.2
    i=$((i + 1))
  done
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) child_start_unhealthy pid=${spawned}" >> "${LOG_FILE}"
  return 1
}

stop_child() {
  rm -f "${CHILD_START_TS_FILE}"
  if [[ -f "${CHILD_PID_FILE}" ]]; then
    local cpid
    cpid="$(cat "${CHILD_PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${cpid}" ]]; then
      kill "${cpid}" 2>/dev/null || true
      sleep 1
      kill -9 "${cpid}" 2>/dev/null || true
    fi
    rm -f "${CHILD_PID_FILE}"
  fi
  local lpids
  lpids="$(lsof -t -iTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${lpids}" ]]; then
    kill ${lpids} 2>/dev/null || true
    sleep 1
    lpids="$(lsof -t -iTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${lpids}" ]]; then
      kill -9 ${lpids} 2>/dev/null || true
    fi
  fi
  kill_stray_strategy_pids "" "stop_child_prune_strays"
}

run_supervisor() {
  set +e
  local should_stop_child_on_exit=0
  local lock_wait_last_log_s=0
  while ! mkdir "${LOCK_DIR}" 2>/dev/null; do
    local lock_pid=""
    if [[ -f "${SUP_PID_FILE}" ]]; then
      lock_pid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
    fi
    if [[ -n "${lock_pid}" ]] && kill -0 "${lock_pid}" 2>/dev/null; then
      local now_s
      now_s="$(date +%s)"
      if (( now_s - lock_wait_last_log_s >= 15 )); then
        log_line "supervisor_lock_held pid=${lock_pid} action=wait"
        lock_wait_last_log_s="${now_s}"
      fi
      sleep 2
      continue
    fi
    rmdir "${LOCK_DIR}" 2>/dev/null || rm -rf "${LOCK_DIR}" 2>/dev/null || true
    if mkdir "${LOCK_DIR}" 2>/dev/null; then
      log_line "supervisor_lock_recovered stale_pid=${lock_pid:-none}"
      break
    fi
    sleep 1
  done
  echo $$ > "${SUP_PID_FILE}"
  trap 'should_stop_child_on_exit=1; exit 0' INT TERM
  trap 'rc=$?; if (( should_stop_child_on_exit == 1 )); then stop_child; fi; rm -f "${SUP_PID_FILE}" "${HEARTBEAT_FILE}"; rmdir "${LOCK_DIR}" 2>/dev/null || true; log_line "supervisor_exit pid=$$ rc=${rc} stop_child=${should_stop_child_on_exit}"; exit ${rc}' EXIT

  log_line "supervisor_start pid=$$ port=${PORT}"
  start_child
  local bad_health=0
  local degraded_health=0
  local endpoint_down=0
  local read_only_worker_failures=0
  local last_heal_request_s=0
  local last_heal_request_codes=""
  local supervisor_started_s
  supervisor_started_s="$(date +%s)"
  local last_heartbeat_log_s=0
  local last_no_trade_monitor_s=0
  local last_stall_monitor_s=0
  local NO_TRADE_TRACKED_SLUG=""
  local NO_TRADE_TRACKED_SLUG_TRADE_SEEN=0
  local NO_TRADE_STREAK_COUNT=0
  local NO_TRADE_LAST_ALERT_SLUG=""
  local stall_signature_count=0
  local last_stall_heal_s=0
  local last_active_slug_restart_recovery_slug=""

  while true; do
    sleep "${CHECK_INTERVAL_SEC}"
    local now_s
    now_s="$(date +%s)"
    echo "${now_s}" > "${HEARTBEAT_FILE}" 2>/dev/null || true
    if adopt_existing_if_healthy; then
      bad_health=0
      continue
    fi
    local cpid=""
    if [[ -f "${CHILD_PID_FILE}" ]]; then
      cpid="$(cat "${CHILD_PID_FILE}" 2>/dev/null || true)"
    fi
    if [[ -z "${cpid}" ]] || ! kill -0 "${cpid}" 2>/dev/null; then
      local child_started_s=""
      local handoff_age=999999
      if [[ -f "${CHILD_START_TS_FILE}" ]]; then
        child_started_s="$(cat "${CHILD_START_TS_FILE}" 2>/dev/null || true)"
      fi
      if [[ -n "${child_started_s}" ]] && [[ "${child_started_s}" =~ ^[0-9]+$ ]]; then
        handoff_age=$(( now_s - child_started_s ))
      fi
      if adopt_existing_if_healthy || adopt_existing_if_endpoint_up || adopt_existing_listener_pid "child_missing_recovered_listener"; then
        log_line "child_missing_recovered_via_adopt prev=${cpid:-none}"
        bad_health=0
        degraded_health=0
        endpoint_down=0
        continue
      fi
      if (( handoff_age >= 0 && handoff_age < CHILD_HANDOFF_GRACE_SEC )); then
        log_line "child_missing_within_handoff_grace prev=${cpid:-none} age=${handoff_age}s"
        continue
      fi
      capture_pre_restart_snapshot
      log_restart_reason "restart" "child_missing" "null"
      log_line "child_missing restarting"
      start_child
      run_restart_agent "post_restart" "child_missing"
      bad_health=0
      degraded_health=0
      endpoint_down=0
      continue
    fi

    local in_grace=0
    if (( now_s - supervisor_started_s < STARTUP_HEALTH_GRACE_SEC )); then
      in_grace=1
    fi

    if (( now_s - last_heartbeat_log_s >= HEARTBEAT_LOG_EVERY_SEC )); then
      log_line "supervisor_heartbeat child=${cpid} bad_health=${bad_health} worker_failures=${read_only_worker_failures}"
      last_heartbeat_log_s="${now_s}"
    fi

    if [[ "${NO_TRADE_MONITOR_ENABLED}" == "1" ]] && (( now_s - last_no_trade_monitor_s >= NO_TRADE_MONITOR_INTERVAL_SEC )); then
      last_no_trade_monitor_s="${now_s}"
      local nt_health=""
      local nt_bots=""
      nt_health="$(curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${HEALTH_URL}" 2>/dev/null || true)"
      nt_bots="$(read_bots_json || true)"
      monitor_no_trade_streak "${nt_health}" "${nt_bots}"
    fi

    if [[ "${STALL_MONITOR_ENABLED}" == "1" ]] && (( now_s - last_stall_monitor_s >= STALL_MONITOR_INTERVAL_SEC )); then
      last_stall_monitor_s="${now_s}"
      local stall_snap=""
      local stall_log_sig=""
      local stall_page_probe=""
      stall_snap="$(health_snapshot_json || true)"
      stall_log_sig="$(stall_log_signature_json || true)"
      if stall_signature_detected "${stall_snap}" "${stall_log_sig}"; then
        stall_signature_count=$((stall_signature_count + 1))
        log_line "stall_signature_detected count=${stall_signature_count} snapshot=${stall_snap:-null} logSig=${stall_log_sig:-null}"
        if (( stall_signature_count >= STALL_CONSECUTIVE_THRESHOLD )) && (( now_s - last_stall_heal_s >= HEAL_REQUEST_COOLDOWN_SEC )); then
          request_watchdog_heal "supervisor_stall_signature:${stall_signature_count}" || true
          last_stall_heal_s="${now_s}"
        fi
        local hard_restart_stall=0
        if (( stall_signature_count >= STALL_CONSECUTIVE_THRESHOLD )) && stall_signature_hard_restart_recommended "${stall_snap}"; then
          hard_restart_stall=1
        fi
        local fast_restart_stall=0
        if (( stall_signature_count >= STALL_CONSECUTIVE_THRESHOLD )) && stall_signature_fast_restart_recommended "${stall_snap}"; then
          fast_restart_stall=1
        fi
        if (( fast_restart_stall == 1 )); then
          stall_page_probe="${stall_page_probe:-$(page_stability_probe_json || true)}"
          capture_pre_restart_snapshot
          log_restart_reason "restart" "stall_signature_fast" "${stall_snap:-null}"
          log_line "stall_signature_fast_restart count=${stall_signature_count} page=${stall_page_probe:-null} snapshot=${stall_snap:-null} logSig=${stall_log_sig:-null}"
          stop_child
          start_child
          run_restart_agent "post_restart" "stall_signature_fast"
          stall_signature_count=0
          bad_health=0
          degraded_health=0
          endpoint_down=0
          continue
        fi
        if (( stall_signature_count >= STALL_RESTART_THRESHOLD )); then
          stall_page_probe="$(page_stability_probe_json || true)"
          capture_pre_restart_snapshot
          if page_probe_is_bad "${stall_page_probe}"; then
            log_restart_reason "restart" "stall_signature" "${stall_snap:-null}"
            log_line "stall_signature_restart count=${stall_signature_count} page=${stall_page_probe:-null} snapshot=${stall_snap:-null} logSig=${stall_log_sig:-null}"
            wait_for_safe_restart_window "stall_signature" || true
            stop_child
            start_child
            run_restart_agent "post_restart" "stall_signature"
            stall_signature_count=0
            bad_health=0
            degraded_health=0
            endpoint_down=0
            continue
          fi
        fi
        if (( hard_restart_stall == 1 )); then
          stall_page_probe="${stall_page_probe:-$(page_stability_probe_json || true)}"
          capture_pre_restart_snapshot
          log_restart_reason "restart" "stall_signature_hard" "${stall_snap:-null}"
          log_line "stall_signature_hard_restart count=${stall_signature_count} page=${stall_page_probe:-null} snapshot=${stall_snap:-null} logSig=${stall_log_sig:-null}"
          wait_for_safe_restart_window "stall_signature_hard" || true
          stop_child
          start_child
          run_restart_agent "post_restart" "stall_signature_hard"
          stall_signature_count=0
          bad_health=0
          degraded_health=0
          endpoint_down=0
          continue
        fi
      else
        stall_signature_count=0
      fi
    fi

    local active_slug_restart_snap=""
    active_slug_restart_snap="$(health_snapshot_json || true)"
    if active_slug_restart_recovery_needed "${active_slug_restart_snap}"; then
      local active_slug_restart_slug=""
      active_slug_restart_slug="$(jq -r '.slug // empty' <<< "${active_slug_restart_snap}" 2>/dev/null || true)"
      if [[ -n "${active_slug_restart_slug}" ]] && [[ "${active_slug_restart_slug}" != "${last_active_slug_restart_recovery_slug}" ]]; then
        capture_pre_restart_snapshot
        log_restart_reason "restart" "restart_inside_active_slug" "${active_slug_restart_snap:-null}"
        log_line "active_slug_restart_recovery slug=${active_slug_restart_slug} snapshot=${active_slug_restart_snap:-null}"
        request_watchdog_heal "supervisor_restart_inside_active_slug:${active_slug_restart_slug}" || true
        stop_child
        start_child
        run_restart_agent "post_restart" "restart_inside_active_slug"
        last_active_slug_restart_recovery_slug="${active_slug_restart_slug}"
        bad_health=0
        degraded_health=0
        endpoint_down=0
        stall_signature_count=0
        continue
      fi
    fi

    if is_read_only_worker_ok; then
      read_only_worker_failures=0
    else
      read_only_worker_failures=$((read_only_worker_failures + 1))
      local worker_health=""
      worker_health="$(read_only_worker_health_json || true)"
      log_line "read_only_worker_unhealthy count=${read_only_worker_failures} origin=${READ_ONLY_WORKER_ORIGIN} expected_upstream=${EXPECTED_READ_ONLY_WORKER_UPSTREAM} health=${worker_health:-null}"
      restart_read_only_worker_tunnel || true
      if is_read_only_worker_ok; then
        log_line "read_only_worker_recovered count=${read_only_worker_failures} origin=${READ_ONLY_WORKER_ORIGIN}"
        read_only_worker_failures=0
      elif (( read_only_worker_failures >= MAX_READ_ONLY_WORKER_FAILS )); then
        log_line "read_only_worker_degraded count=${read_only_worker_failures} action=continue_without_worker"
      fi
    fi

    local endpoint_up_now=0
    if is_endpoint_up; then endpoint_up_now=1; fi
    if is_health_ok || { (( in_grace == 1 )) && (( endpoint_up_now == 1 )); }; then
      bad_health=0
      degraded_health=0
      endpoint_down=0
    else
      local snap=""
      snap="$(health_snapshot_json || true)"
      local codes=""
      codes="$(health_issue_codes "${snap}" || true)"
      local restart_codes=""
      restart_codes="$(restart_worthy_issue_codes "${codes:-}")"
      if (( endpoint_up_now == 1 )) && [[ -z "${restart_codes}" ]] && [[ -n "${codes}" ]]; then
        bad_health=0
        degraded_health=0
        endpoint_down=0
        log_line "health_watchdog_only_ignored codes=${codes} snapshot=${snap:-null}"
        continue
      fi
      bad_health=$((bad_health + 1))
      if (( endpoint_up_now == 1 )); then
        degraded_health=$((degraded_health + 1))
        endpoint_down=0
        log_line "health_degraded count=${degraded_health} codes=${codes} snapshot=${snap:-null}"
        local heal_codes=""
        heal_codes="$(sanitize_heal_reason_codes "${codes:-}")"
        local should_heal=0
        if (( now_s - last_heal_request_s >= HEAL_REQUEST_COOLDOWN_SEC )); then
          should_heal=1
        fi
        if [[ -n "${heal_codes}" ]] && [[ "${heal_codes}" != "${last_heal_request_codes}" ]]; then
          should_heal=1
        fi
        if (( should_heal == 1 )); then
          log_line "heal_request codes=${heal_codes:-degraded_health} raw_codes=${codes:-unknown}"
          request_watchdog_heal "supervisor_degraded_health:${heal_codes:-degraded_health}" || true
          last_heal_request_s="${now_s}"
          last_heal_request_codes="${heal_codes}"
        else
          log_line "heal_request_suppressed cooldown=${HEAL_REQUEST_COOLDOWN_SEC}s codes=${heal_codes:-degraded_health}"
        fi
        if (( degraded_health >= MAX_DEGRADED_HEALTH_FAILS )) && [[ -n "${restart_codes}" ]]; then
          capture_pre_restart_snapshot
          log_restart_reason "restart" "degraded_health" "${snap:-null}"
          log_line "health_fail_restart child=${cpid} reason=degraded_health codes=${codes} restart_codes=${restart_codes}"
          stop_child
          start_child
          run_restart_agent "post_restart" "degraded_health"
          bad_health=0
          degraded_health=0
          endpoint_down=0
        elif (( degraded_health >= MAX_DEGRADED_HEALTH_FAILS )); then
          log_line "health_fail_deferred child=${cpid} reason=degraded_health codes=${codes} action=heal_only"
        fi
      else
        endpoint_down=$((endpoint_down + 1))
        degraded_health=0
        log_line "endpoint_down count=${endpoint_down} bad_health=${bad_health}"
        kill_stray_strategy_pids "${cpid}" "endpoint_down_prune_strays"
        if (( endpoint_down >= MAX_ENDPOINT_DOWN_FAILS )); then
          capture_pre_restart_snapshot
          log_restart_reason "restart" "endpoint_unresponsive" "${snap:-null}"
          log_line "health_fail_restart child=${cpid} reason=endpoint_unresponsive"
          wait_for_safe_restart_window "endpoint_unresponsive" || true
          stop_child
          start_child
          run_restart_agent "post_restart" "endpoint_unresponsive"
          bad_health=0
          degraded_health=0
          endpoint_down=0
        fi
      fi
    fi
  done
}

start_supervisor() {
  if ! ensure_nonessential_worker_supervisor; then
    if [[ "${REQUIRE_NONESSENTIAL_WORKER_FOR_START}" == "1" ]]; then
      echo "Failed to ensure replica readonly worker before starting ${PORT}. Check ${LOG_FILE}" >&2
      return 1
    fi
    log_line "ensure_nonessential_worker degraded=startup_timeout action=continue_without_worker"
  fi
  cleanup_stale_supervisor_state
  kill_stray_strategy_pids "" "start_supervisor_prune_strays"
  local any_sup
  any_sup="$(find_supervisor_pids | tr '\n' ' ' | xargs 2>/dev/null || true)"
  if [[ -n "${any_sup}" ]]; then
    # Reuse active supervisor processes started by launchd/manage.
    if [[ -f "${SUP_PID_FILE}" ]]; then
      local file_spid
      file_spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
      if pid_is_alive "${file_spid}"; then
        echo "Supervisor already running: pid=${file_spid}"
        return 0
      fi
    fi
    local first_sup
    first_sup="$(find_supervisor_pids | head -n 1)"
    if pid_is_alive "${first_sup}"; then
      echo "${first_sup}" > "${SUP_PID_FILE}"
      echo "Supervisor already running: pid=${first_sup}"
      return 0
    fi
  fi
  if [[ -d "${LOCK_DIR}" ]]; then
    local lspid=""
    if [[ -f "${SUP_PID_FILE}" ]]; then
      lspid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
    fi
    if [[ -z "${lspid}" ]] || ! kill -0 "${lspid}" 2>/dev/null; then
      rmdir "${LOCK_DIR}" 2>/dev/null || rm -rf "${LOCK_DIR}" 2>/dev/null || true
    fi
  fi
  if [[ -f "${SUP_PID_FILE}" ]]; then
    local spid
    spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
    if pid_is_alive "${spid}"; then
      echo "Supervisor already running: pid=${spid}"
      return 0
    fi
    rm -f "${SUP_PID_FILE}"
  fi
  if [[ -f "${CHILD_PID_FILE}" ]]; then
    local cpid
    cpid="$(cat "${CHILD_PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${cpid}" ]] && ! kill -0 "${cpid}" 2>/dev/null; then
      local listener_pid=""
      listener_pid="$(detect_existing_listener_pid || true)"
      if pid_is_alive "${listener_pid}"; then
        set_child_pid_if_changed "${listener_pid}" "child_pid_recovered_start"
      else
        rm -f "${CHILD_PID_FILE}"
      fi
    fi
  fi
  local -a launch_env=(
    "PORT=${PORT}"
    "TRADE_LOG_NAMESPACE=${TRADE_LOG_NAMESPACE:-}"
    "TRADE_LOG_DIR=${TRADE_LOG_DIR:-}"
    "MULTI_STATE_DB_PATH=${MULTI_STATE_DB_PATH:-}"
    "RESUME_LATEST_RUN_ON_RESTART=${RESUME_LATEST_RUN_ON_RESTART:-}"
    "FORCE_NEW_SERVER_RUN_ON_START=${FORCE_NEW_SERVER_RUN_ON_START:-}"
    "DEFAULT_TRADE_LOG_NAMESPACE=${DEFAULT_TRADE_LOG_NAMESPACE:-}"
    "DEFAULT_TRADE_LOG_DIR=${DEFAULT_TRADE_LOG_DIR:-}"
    "DEFAULT_MULTI_STATE_DB_PATH=${DEFAULT_MULTI_STATE_DB_PATH:-}"
    "DEFAULT_RESUME_LATEST_RUN_ON_RESTART=${DEFAULT_RESUME_LATEST_RUN_ON_RESTART:-}"
    "DEFAULT_FORCE_NEW_SERVER_RUN_ON_START=${DEFAULT_FORCE_NEW_SERVER_RUN_ON_START:-}"
  )
  env -u NICE "${launch_env[@]}" nohup "${SCRIPT_PATH}" __supervise >>"${LOG_FILE}" 2>&1 </dev/null &
  local i=0
  while (( i < 40 )); do
    if [[ -f "${SUP_PID_FILE}" ]]; then
      local wait_spid
      wait_spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
      if pid_is_alive "${wait_spid}"; then
        break
      fi
      rm -f "${SUP_PID_FILE}"
    fi
    sleep 0.2
    i=$((i + 1))
  done
  local spid=""
  if [[ -f "${SUP_PID_FILE}" ]]; then
    spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
  fi
  if [[ -z "${spid}" ]] || ! pid_is_alive "${spid}"; then
    rm -f "${SUP_PID_FILE}"
    echo "Failed to start supervisor on port ${PORT}. Check ${LOG_FILE}" >&2
    return 1
  fi
  echo "Started supervisor on port ${PORT}. pid=${spid}"
}

stop_supervisor() {
  local spid=""
  if [[ -f "${SUP_PID_FILE}" ]]; then
    spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"
  fi
  if [[ -n "${spid}" ]]; then
    kill "${spid}" 2>/dev/null || true
    sleep 1
    kill -9 "${spid}" 2>/dev/null || true
  fi
  local other_sups
  other_sups="$(find_supervisor_pids | tr '\n' ' ' | xargs 2>/dev/null || true)"
  if [[ -n "${other_sups}" ]]; then
    kill ${other_sups} 2>/dev/null || true
    sleep 1
    kill -9 ${other_sups} 2>/dev/null || true
  fi
  local i=0
  while (( i < 50 )); do
    local live_sup=""
    live_sup="$(find_supervisor_pids | head -n 1 || true)"
    if [[ -z "${live_sup}" ]] && [[ ! -d "${LOCK_DIR}" ]]; then
      break
    fi
    sleep 0.2
    i=$((i + 1))
  done
  stop_child
  rm -f "${SUP_PID_FILE}" "${HEARTBEAT_FILE}"
  rmdir "${LOCK_DIR}" 2>/dev/null || true
  echo "Stopped supervisor and child on port ${PORT}."
}

status_supervisor() {
  local spid="" cpid="" listening="no" hb_state="none"
  local listener_pid="" owned_listener="no"
  if [[ -f "${SUP_PID_FILE}" ]]; then spid="$(cat "${SUP_PID_FILE}" 2>/dev/null || true)"; fi
  if [[ -f "${CHILD_PID_FILE}" ]]; then cpid="$(cat "${CHILD_PID_FILE}" 2>/dev/null || true)"; fi
  if [[ -n "${spid}" ]] && ! kill -0 "${spid}" 2>/dev/null; then
    local live_sup=""
    live_sup="$(find_supervisor_pids | head -n 1 || true)"
    if pid_is_alive "${live_sup}"; then
      spid="${live_sup}"
      echo "${spid}" > "${SUP_PID_FILE}"
    fi
  fi
  local sup_state="none" child_state="none"
  if [[ -n "${spid}" ]]; then
    if kill -0 "${spid}" 2>/dev/null; then sup_state="alive"; else sup_state="dead"; fi
  fi
  if [[ -n "${cpid}" ]]; then
    if kill -0 "${cpid}" 2>/dev/null; then child_state="alive"; else child_state="dead"; fi
  fi
  if [[ -f "${HEARTBEAT_FILE}" ]]; then
    local now hb
    now="$(date +%s)"
    hb="$(cat "${HEARTBEAT_FILE}" 2>/dev/null || true)"
    if [[ -n "${hb}" ]]; then
      local age=$(( now - hb ))
      if (( age <= MAX_HEARTBEAT_STALE_SEC )); then hb_state="fresh"; else hb_state="stale(${age}s)"; fi
    else
      hb_state="stale";
    fi
  fi
  listener_pid="$(detect_existing_listener_pid || true)"
  if [[ -n "${listener_pid}" ]]; then listening="yes"; fi
  if [[ -n "${listener_pid}" ]] && [[ -n "${cpid}" ]] && [[ "${listener_pid}" == "${cpid}" ]]; then
    owned_listener="yes"
  fi
  echo "supervisor_pid=${spid:-none}"
  echo "supervisor_state=${sup_state}"
  echo "supervisor_heartbeat=${hb_state}"
  echo "child_pid=${cpid:-none}"
  echo "child_state=${child_state}"
  echo "listener_pid=${listener_pid:-none}"
  echo "listener_owned_by_child_pid=${owned_listener}"
  echo "listening_${PORT}=${listening}"
  curl -fsS -m "${HTTP_TIMEOUT_SEC}" "${HEALTH_URL}" | jq '{ok:(.current!=null),slug:.current.slug,runId:.health.runId,pairLagMs:.health.quoteFeed.pairLagMs,streamAgeMs:.health.quoteFeed.streamLastMsgAgeMs,streamConnected:.health.quoteFeed.streamConnected,quoteGapMs:.health.quoteKpi.broadcastGapMsLatest,quoteStreak:[.health.quoteKpi.gapViolationStreak,.health.quoteKpi.feedViolationStreak]|max}' || true
}

show_logs() {
  echo "--- supervisor log (${LOG_FILE}) ---"
  tail -n 80 "${LOG_FILE}" 2>/dev/null || true
  echo "--- server log (${SERVER_LOG_FILE}) ---"
  tail -n 80 "${SERVER_LOG_FILE}" 2>/dev/null || true
}

CMD="${1:-status}"
case "${CMD}" in
  __supervise)
    run_supervisor
    ;;
  start)
    run_with_transition_mark start_supervisor
    ;;
  stop)
    run_with_transition_mark stop_supervisor
    ;;
  restart)
    run_with_transition_mark restart_supervisor_cmd
    ;;
  status)
    status_supervisor
    ;;
  logs)
    show_logs
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}" >&2
    exit 1
    ;;
esac
