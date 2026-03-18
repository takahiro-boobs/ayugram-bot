#!/bin/bash
cd "$(dirname "$0")"
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
    echo "=== supervisor already running pid=$existing_pid, skip $(date)" >> bot_supervisor.log
    return 1
  fi

  echo "=== stale supervisor lock detected, cleaning $(date)" >> bot_supervisor.log
  rm -f "$PID_FILE" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || rm -rf "$LOCK_DIR" 2>/dev/null || true

  if mkdir "$LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" > "$PID_FILE"
    return 0
  fi

  echo "=== failed to acquire supervisor lock, skip $(date)" >> bot_supervisor.log
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

while true; do
  echo "=== restart $(date)" >> bot_supervisor.log
  ./.venv311/bin/python bot.py >> bot_supervisor.log 2>&1
  exit_code=$?
  echo "=== stopped code=$exit_code $(date)" >> bot_supervisor.log
  if [ "$exit_code" -eq "$STOP_ON_CONFLICT_CODE" ]; then
    echo "=== conflict exit detected, supervisor stops without restart $(date)" >> bot_supervisor.log
    exit 0
  fi
  sleep 2
done
