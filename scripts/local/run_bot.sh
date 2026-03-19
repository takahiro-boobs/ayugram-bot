#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BOT_VENV_PYTHON="${BOT_VENV_PYTHON:-${REPO_ROOT}/.venv311/bin/python}"
BOT_LOG_FILE="${BOT_LOG_FILE:-${REPO_ROOT}/bot_supervisor.log}"

cd "${REPO_ROOT}"
LOCK_DIR="${TMPDIR:-/tmp}/slezhka-bot-supervisor.lock"
PID_FILE="$LOCK_DIR/pid"
STOP_ON_CONFLICT_CODE="${BOT_STOP_ON_CONFLICT_CODE:-75}"

acquire_lock() {
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$PID_FILE"
    return 0
  fi

  existing_pid=""
  if [ -f "$PID_FILE" ]; then
    existing_pid="$(cat "$PID_FILE" 2>/dev/null)"
  fi

  if [ -n "$existing_pid" ] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "=== supervisor already running pid=$existing_pid, skip $(date)" >> "$BOT_LOG_FILE"
    return 1
  fi

  echo "=== stale supervisor lock detected, cleaning $(date)" >> "$BOT_LOG_FILE"
  rm -f "$PID_FILE" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || rm -rf "$LOCK_DIR" 2>/dev/null || true

  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$PID_FILE"
    return 0
  fi

  echo "=== failed to acquire supervisor lock, skip $(date)" >> "$BOT_LOG_FILE"
  return 1
}

if ! acquire_lock; then
  exit 0
fi

cleanup() {
  rm -f "$PID_FILE" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

if [ ! -x "$BOT_VENV_PYTHON" ]; then
  echo "Python interpreter not found: $BOT_VENV_PYTHON"
  echo "Set BOT_VENV_PYTHON to your venv python path."
  exit 1
fi

while true; do
  echo "=== restart $(date)" >> "$BOT_LOG_FILE"
  "$BOT_VENV_PYTHON" bot.py >> "$BOT_LOG_FILE" 2>&1
  exit_code=$?
  echo "=== stopped code=$exit_code $(date)" >> "$BOT_LOG_FILE"
  if [ "$exit_code" -eq "$STOP_ON_CONFLICT_CODE" ]; then
    echo "=== conflict exit detected, supervisor stops without restart $(date)" >> "$BOT_LOG_FILE"
    exit 0
  fi
  sleep 2
done
