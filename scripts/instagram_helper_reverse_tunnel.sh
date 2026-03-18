#!/bin/bash
set -euo pipefail

SSH_KEY="${HOME}/.ssh/codex_tvf_ed25519"
REMOTE_HOST="4abbf189760e.vps.myjino.ru"
REMOTE_PORT="49297"
REMOTE_USER="root"
REMOTE_BIND="127.0.0.1:17374"
LOCAL_TARGET="127.0.0.1:17374"

exec /usr/bin/ssh \
  -NT \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o StrictHostKeyChecking=no \
  -i "${SSH_KEY}" \
  -p "${REMOTE_PORT}" \
  -R "${REMOTE_BIND}:${LOCAL_TARGET}" \
  "${REMOTE_USER}@${REMOTE_HOST}"
