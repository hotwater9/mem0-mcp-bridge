#!/usr/bin/env bash
# Codex MCP wrapper for mem0-mcp-bridge
# Install: place in ~/bin/mem-codex.sh, chmod +x
# Then in ~/.codex/config.toml:
#   [mcp_servers.memory]
#   command = "/home/<you>/bin/mem-codex.sh"
#
# Behavior: auto-detects project namespace from current git repo name

set -euo pipefail

MEM0_HOST="${MEM0_HOST:-http://your-server:8765}"
USER_ID="${USER:?export USER=<your-identifier> first}"
TOKEN="${MEM0_TOKEN:?export MEM0_TOKEN=<token> first}"

if PROJECT_ROOT=$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null); then
  PROJECT=$(basename "$PROJECT_ROOT")
else
  PROJECT="_personal"
fi

exec curl -sN \
  -H "Authorization: Bearer ${TOKEN}" \
  "${MEM0_HOST}/mcp/${PROJECT}/sse/${USER_ID}"
