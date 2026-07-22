# Operations Guide

Operational reference for the mem0-mcp-bridge self-hosted deployment.

- Isolation model: **project-level shared** — same `agent_id` (project name) shares one memory pool; `user_id` is attribution only
- mem0ai version: >=2.0.7,<3 (v2 series, filters API + auto metadata index)
- Bridge protocol: MCP SSE + HTTP REST
- LLM gateway: any OpenAI-compatible endpoint (litellm recommended for prompt caching)
- Observability: built-in LLM token / cache / embedding / retrieval quality metrics (persisted in SQLite)

---

## Port Reference

| Port | Service | Purpose |
|------|---------|---------|
| **8765** | bridge (MCP wrapper) | All MCP clients connect here |
| **6333** | Qdrant vector DB | REST API + Web Dashboard (`/dashboard`) |
| 6334 | Qdrant gRPC | Internal only (bridge → qdrant) |

### Bridge Endpoints (port 8765)

| Path | Method | Auth | Purpose |
|------|--------|------|---------|
| `/` | GET | None | Web UI (HTML) |
| `/health` | GET | None | Health check, returns model/embedding config |
| `/memories` | GET | Bearer | List memories (supports project/user/limit/offset filters) |
| `/memories/{id}` | DELETE | Bearer | Delete by ID |
| `/search` | POST | Bearer | Semantic search |
| `/stats` | GET | Bearer | Usage statistics (aggregated from qdrant + SQLite) |
| `/mcp/{project}/sse/{user}` | GET | Bearer | MCP SSE entry point (clients connect here) |
| `/messages/` | POST | Bearer | MCP message channel (internal to SSE protocol) |

---

## Service Management

### Start / Stop / Restart

```bash
cd /path/to/mem0-mcp-bridge

# Start
docker compose up -d

# Stop (data preserved)
docker compose stop

# Restart bridge (after config or code change)
docker compose restart bridge

# Restart qdrant (rarely needed)
docker compose restart qdrant

# Full teardown (containers + networks, keeps data volumes)
docker compose down

# Teardown + delete all data (irreversible!)
docker compose down -v
```

### After Code / Config Changes

| What changed | Action |
|--------------|--------|
| `bridge/server.py` or `bridge/static/*` | `docker compose up -d --build bridge` |
| `bridge/Dockerfile` or `requirements.txt` | `docker compose up -d --build bridge` (slower, reinstalls deps) |
| `.env` variables | `docker compose restart bridge` |
| `docker-compose.yml` | `docker compose up -d` |

### Logs

```bash
# Real-time (Ctrl+C to exit)
docker compose logs -f bridge

# Last 100 lines
docker compose logs --tail=100 bridge

# Qdrant logs
docker compose logs --tail=50 qdrant
```

### Shell into containers

```bash
# Bridge container
docker compose exec bridge bash
# e.g.: pip show mem0ai

# Qdrant container
docker compose exec qdrant sh
```

---

## Stats Dashboard

Open `http://your-server:8765/` → top-right Stats button.

### KPI Cards

| Card | Meaning |
|------|---------|
| **Memories** | Total memories stored in qdrant |
| **Projects** | Distinct `agent_id` count (one per namespace) |
| **Users** | Distinct `user_id` count |
| **LLM Calls** | Cumulative LLM calls since startup |

### Retrieval Quality

| Metric | Meaning |
|--------|---------|
| **avg top-1 score** | Average similarity score of best search result |
| **0-result ratio** | Percentage of searches returning empty |

Score interpretation:
- >= 0.7 (good): high relevance
- 0.5–0.7 (moderate): partially relevant
- < 0.5 (poor): memories don't match actual queries

### LLM Usage

Each `add_memory` ≈ 2 LLM calls (extraction + dedup decision). Metrics persist in SQLite (`/state/metrics.db`).

### Embedding Usage

| Metric | Meaning |
|--------|---------|
| Write count | `add_memory` calls (each triggers 1 embedding) |
| Search count | `search_memory` calls |
| Embedding tokens | Cumulative tokens for your embedding model |
| avg top-1 score | Mean top-1 similarity across all searches |
| 0-result ratio | Searches returning empty |

### Revision Rate

Percentage of memories where `updated_at != created_at`. 30%+ means mem0's dedup is working (merging repeated info into existing memories).

### Qdrant Health

| Field | Meaning | Healthy |
|-------|---------|---------|
| `points_count` | Total points = total memories | Matches "Memories" card |
| `indexed_vectors_count` | Indexed vectors | Should ≈ points_count |
| `status` | Collection state | `green` |
| `embed dim` | Current embedding dimension | Must match `EMBED_DIMS` env |

---

## Backup & Restore

### Manual backup (recommended weekly)

```bash
cd /path/to/mem0-mcp-bridge

docker run --rm \
  -v mem0-mcp-bridge_qdrant_data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/qdrant-$(date +%F).tar.gz /data
```

### Automated backup (cron)

```bash
# Add to crontab: weekly Sunday 3am
0 3 * * 0 cd /path/to/mem0-mcp-bridge && docker run --rm -v mem0-mcp-bridge_qdrant_data:/data -v $(pwd):/backup alpine tar czf /backup/qdrant-$(date +\%F).tar.gz /data
```

### Restore

```bash
docker compose down
docker volume rm mem0-mcp-bridge_qdrant_data
docker volume create mem0-mcp-bridge_qdrant_data

docker run --rm \
  -v mem0-mcp-bridge_qdrant_data:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/qdrant-YYYY-MM-DD.tar.gz -C /

docker compose up -d
```

---

## Common Issues & Fixes

### Bridge container keeps restarting

```bash
docker compose logs --tail=80 bridge
```

| Log pattern | Cause | Fix |
|-------------|-------|-----|
| `KeyError: 'LITELLM_API_KEY'` | `.env` not populated | Check `.env`, run `docker compose config` |
| `BaseLlmConfig got unexpected keyword` | Unsupported config field | See `server.py` top docstring |
| `Connection refused` to qdrant | Qdrant not running | `docker compose restart qdrant` |
| `port already allocated` | Port conflict | `ss -tlnp \| grep -E '8765\|6333'` |

### Vector dimension error

Qdrant collection dimension doesn't match embedder output.

```bash
curl -X DELETE http://localhost:6333/collections/memories
docker compose restart bridge
# Bridge recreates collection with correct EMBED_DIMS on startup
```

### `temperature and top_p cannot both be specified`

Fixed in mem0 v2.0.7. If you see this, check `pip show mem0ai` — you may be on an older version.

### MCP -32602 / `Invalid request parameters`

Should not happen (bridge uses `stateless=True`). If it does, check MCP SDK version compatibility.

### `/memories` returns 500 without filters

mem0 v2 `get_all` requires at least one filter. Bridge falls back to qdrant scroll for unfiltered listing. If broken, check `server.py` scroll fallback code.

### Collection predates v3 hybrid search (warning)

Informational only — v2 doesn't use hybrid search. To resolve (destroys existing memories):

```bash
docker compose stop bridge
curl -X DELETE http://localhost:6333/collections/memories
curl -X DELETE http://localhost:6333/collections/mem0migrations
docker compose up -d bridge
```

---

## Upgrading

### Upgrade bridge code

1. Edit files in `bridge/`
2. Syntax check: `python -c "import ast; ast.parse(open('bridge/server.py').read())"`
3. Rebuild: `docker compose up -d --build bridge`
4. Verify: `curl http://localhost:8765/health`

### Upgrade mem0 SDK

Current pin: `mem0ai>=2.0.7,<3`.

1. Check mem0 changelog for breaking changes
2. Update version in `bridge/requirements.txt`
3. **Backup qdrant volume first**
4. Rebuild: `docker compose up -d --build bridge`
5. End-to-end test: add / search / list / delete / stats / project isolation
6. Rollback if broken: revert requirements.txt, rebuild

#### v1 → v2 migration reference

| Call | v1 | v2 |
|------|----|----|
| `Memory.search()` | `search(q, user_id=u, agent_id=p, limit=N)` | `search(q, filters={"user_id":u,"agent_id":p}, top_k=N)` |
| `Memory.get_all()` | `get_all(user_id=u, agent_id=p, limit=N)` | `get_all(filters={"user_id":u,"agent_id":p}, top_k=N)` |
| `Memory.add()` | unchanged | unchanged |

### Switch chat model

Change `LITELLM_CHAT_MODEL` in `.env`, restart bridge. Existing memories unaffected.

### Switch embedding model

**Warning: changes vector dimensions, destroys all existing memories.**

1. Update `LITELLM_EMBED_MODEL` and `EMBED_DIMS` in `.env`
2. Delete collection: `curl -X DELETE http://localhost:6333/collections/memories`
3. Restart bridge

---

## Capacity & Scaling

| Aspect | Designed for | Scale limit |
|--------|-------------|-------------|
| Qdrant single node | ~10M memories | One 4C8G machine |
| Team size | ≤20 people | Single shared token model |
| Cross-team | Not supported | Would need per-user auth gateway |

### When to scale

- Memories > 1M: consider qdrant cluster mode
- Team > 50: consider per-user JWT auth instead of shared token
- Cross-region: consider CDN or regional deployment

### Not yet supported

- Per-user token revocation (shared token — rotate means everyone rotates)
- Audit log (which user wrote what in which project) — easy to add (~50 lines)
- Cross-project search (intentionally isolated by design)
