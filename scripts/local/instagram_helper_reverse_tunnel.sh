#!/bin/bash
set -euo pipefail

SSH_KEY="${TUNNEL_SSH_KEY:-${HOME}/.ssh/codex_tvf_ed25519}"
REMOTE_HOST="${TUNNEL_SERVER_HOST:-}"
REMOTE_PORT="${TUNNEL_SERVER_PORT:-22}"
REMOTE_USER="${TUNNEL_SERVER_USER:-}"
REMOTE_BIND="${TUNNEL_REMOTE_BIND:-127.0.0.1:17374}"
LOCAL_TARGET="${TUNNEL_LOCAL_TARGET:-127.0.0.1:17374}"

if [ -z "$REMOTE_HOST" ] || [ -z "$REMOTE_USER" ]; then
  echo "Missing TUNNEL_SERVER_HOST or TUNNEL_SERVER_USER" >&2
  exit 1
fi

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
