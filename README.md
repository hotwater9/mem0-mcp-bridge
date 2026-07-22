# mem0-mcp-bridge

A self-hosted MCP (Model Context Protocol) server that wraps the [mem0](https://github.com/mem0ai/mem0) SDK, providing persistent AI memory for Claude Code, Codex, and any MCP-compatible client.

> **中文简介**：把 mem0 SDK 包装成 MCP SSE 服务，让 Claude Code / Codex 等 AI 编程工具拥有跨会话的项目级记忆能力。URL 路径硬隔离项目命名空间，同项目团队成员共享一池记忆。

## Why this exists

We tried mem0's official [OpenMemory](https://github.com/mem0ai/mem0/tree/main/openmemory) wrapper first. It had multiple issues:
- Required pre-existing user records via REST API
- Opinionated tag-based namespacing that didn't fit our setup
- Version drift in its mem0 dependency
- MCP SDK handshake bugs causing tool calls to hang

This bridge wraps mem0 SDK directly with a thin MCP layer (~650 lines), giving you:
- **Hard project isolation** via URL path (`/mcp/<project>/sse/<user>`)
- **Team-shared memory** — same project, shared pool; `user_id` is attribution only
- **7 MCP tools** — the 4 standard ones (add/search/list/delete) plus get/update/history for fine-grained editing
- **Built-in web UI** for browsing memories and viewing stats
- **Metrics persistence** in SQLite (survives container restarts)

## Architecture

```
Claude Code / Codex (MCP client)
        │ /mcp/<project>/sse/<user>
        ▼
bridge (this repo)                  ┐
  - URL → project / user parsing    │
  - Bearer token auth               │  only this layer is custom
  - 7 MCP tools over mem0 SDK       ┘
        │
        ▼
mem0 SDK v2.x (official library)
        │
   ┌────┴────────┐
   ▼             ▼
LLM gateway        Qdrant (vector DB)
(any OpenAI-compatible)
```

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/hotwater9/mem0-mcp-bridge.git
cd mem0-mcp-bridge
cp .env.example .env
# Edit .env: fill in your LLM gateway URL, API key, and generate a shared token
openssl rand -hex 32  # paste output into SHARED_TOKEN
chmod 600 .env
```

### 2. Start services

```bash
docker compose up -d --build
```

Verify:
```bash
curl http://localhost:8765/health
# {"status":"ok","chat_model":"claude-sonnet-4-6","embed_model":"text-embedding-3-large","embed_dims":3072}
```

### 3. Connect Claude Code

Set environment variables (once per machine):

```bash
export USER=yourname
export MEM0_TOKEN=<your-SHARED_TOKEN-value>
```

Add `.mcp.json` to your project root:

```json
{
  "mcpServers": {
    "memory": {
      "type": "sse",
      "url": "http://your-server:8765/mcp/<project-name>/sse/${USER}",
      "headers": {
        "Authorization": "Bearer ${MEM0_TOKEN}"
      }
    }
  }
}
```

Replace `your-server` with your host address and `<project-name>` with your repo name.

### 4. Connect Codex

See [mcp-templates/mem-codex.sh](mcp-templates/mem-codex.sh) — a wrapper script that auto-detects the project from the current git repo.

## MCP Tools

| Tool | Purpose |
|------|---------|
| `add_memory` | Store a memory (mem0 uses LLM to extract structured facts) |
| `search_memory` | Semantic search across project memories |
| `list_memories` | List recent memories in the project |
| `delete_memory` | Delete by ID |
| `get_memory` | Get a single memory by ID |
| `update_memory` | Replace memory text (old version goes to history) |
| `memory_history` | View edit history of a memory |

All tools operate within the project namespace from the SSE URL. Cross-project access is rejected at the bridge level.

## Web UI

Open `http://your-server:8765/` in a browser. Enter your token to:
- Browse memories filtered by project / user
- Semantic search
- Delete individual memories
- View usage stats (LLM calls, embedding costs, retrieval quality)

## Configuration

### LLM Gateway

Any OpenAI-compatible endpoint works (litellm, openrouter, direct Anthropic/OpenAI API). Set `LITELLM_BASE_URL` and `LITELLM_API_KEY` in `.env`.

### Embedding Model

Default: `text-embedding-3-large` (3072 dims). To change, update `LITELLM_EMBED_MODEL` and `EMBED_DIMS` in `.env`, then **delete the qdrant collection** (dimension change requires rebuild):

```bash
curl -X DELETE http://localhost:6333/collections/memories
docker compose restart bridge
```

### Chat Model

Change `LITELLM_CHAT_MODEL` in `.env` and restart bridge. Existing memories are unaffected.

## Backup & Restore

```bash
# Backup qdrant data
docker run --rm \
  -v mem0-mcp-bridge_qdrant_data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/qdrant-$(date +%F).tar.gz /data

# Restore
docker compose down
docker volume rm mem0-mcp-bridge_qdrant_data
docker volume create mem0-mcp-bridge_qdrant_data
docker run --rm \
  -v mem0-mcp-bridge_qdrant_data:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/qdrant-YYYY-MM-DD.tar.gz -C /
docker compose up -d
```

## Troubleshooting

See [docs/ops.md](docs/ops.md) for detailed operational guide including common failure modes and fixes.

## Project Isolation Model

- **Project** (`agent_id`) = memory namespace. Hard boundary enforced at bridge level.
- **User** (`user_id`) = attribution tag. Same-project users share one pool — read/write/delete are unrestricted within a project.
- Personal notes should go in Claude Code's built-in auto-memory (`~/.claude/projects/.../memory/`), not here.

## Requirements

- Docker + Docker Compose
- An OpenAI-compatible LLM gateway (for chat + embeddings)
- Network access from clients to the bridge port (8765)

## License

MIT
