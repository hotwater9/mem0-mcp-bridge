"""
Custom MCP bridge over mem0 SDK.

Exposes 7 MCP tools (add/search/list/delete/get/update/history) over SSE.
The 4 official mem0 OpenMemory tools (add/search/list/delete) plus 3 detail
tools (get/update/history) for fine-grained editing without delete-and-readd.

URL pattern: /mcp/<project>/sse/<user>
- project = repo name, used as agent_id (memory namespace; project is the
            only isolation boundary)
- user   = developer slug, used as user_id (attribution only — same project
            users share one pool: read/write/delete all unrestricted)

Auth: Authorization: Bearer <SHARED_TOKEN>

==============================================================================
WHY THIS EXISTS
==============================================================================
We tried mem0's official OpenMemory wrapper first; it had multiple bugs and
custom requirements that didn't fit our setup (REST API requiring pre-existing
user records, opinionated tag-based namespacing, version drift in its mem0
dependency). This bridge wraps mem0 SDK directly with a thin custom MCP layer
so we control versions and API shape end-to-end and get URL-based hard
project isolation for free.

==============================================================================
GOTCHAS / MAINTENANCE NOTES (each one bit us during initial deployment)
==============================================================================

Pinned to mem0ai>=2.0.7,<3 (in requirements.txt). v2.0 changed search/get_all
to require `filters={...}` + `top_k` instead of `user_id=` + `limit=`. add()
still takes user_id/agent_id directly.

1. *_base_url passed via env, not config dict   (search "ANTHROPIC_BASE_URL" below)
   mem0 v2.0.7's BaseLlmConfig / BaseEmbedderConfig do NOT accept
   `anthropic_base_url` / `openai_base_url` keyword args. Putting them
   in config raises TypeError. The Anthropic and OpenAI SDKs themselves
   read ANTHROPIC_BASE_URL / OPENAI_BASE_URL from env, so we set those
   before importing mem0 and the SDKs pick them up transparently.
   (mem0 main has the field, may land in v2.1+ — re-check on upgrade.)

2. embedder needs explicit embedding_dims=3072   (search "embedding_dims")
   mem0 BaseEmbedderConfig default is 1536. mem0's openai embedder
   sends `dimensions=embedding_dims` to the API, which TRUNCATES
   text-embedding-3-large's natural 3072-dim output down to 1536.
   That doesn't match our qdrant collection's 3072 dim → 400 error
   on every search. Always keep this in sync with EMBED_DIMS env.

3. server.run(stateless=True)   (search "stateless=True")
   MCP SDK 1.28+ ServerSession blocks all non-`initialize` requests
   with -32602 until it receives the `notifications/initialized`
   notification. Some clients (Claude Code as of 2026-06) skip that
   notification, so tool calls would hang forever. stateless=True
   sets _initialization_state=Initialized at session construction.
   Each SSE connection here is a fresh short-lived Server, so the
   stateless semantics are appropriate.

4. handle_sse must `return Response()`   (search "Response()  # SSE")
   The SSE transport context manager writes its own response, but
   Starlette's Route system still tries to call the endpoint's return
   value as ASGI. Returning None → TypeError 'NoneType' not callable.
   Empty Response() satisfies the framework; the transport already
   closed the wire so the no-op write is a no-op.

5. Unfiltered list_memories scrolls qdrant directly  (search "_qdrant.scroll")
   mem0.get_all() refuses without at least one of user_id/agent_id/run_id
   in `filters`. For the "list all" UI mode (no filters set) we bypass
   mem0 and scroll qdrant directly, returning raw payload+id.

6. Metrics persist in /state/metrics.db (SQLite, one row per call). The
   /state path needs to be a docker volume — see docker-compose.yml's
   bridge_state mount. Without the mount the DB lives in the container
   rootfs and dies on container recreate. /stats aggregates this table
   via SUM(CASE WHEN kind=...). See _db_init / _db_record / _agg_stats.

PREVIOUSLY-NEEDED HACKS NO LONGER REQUIRED ON v2.0.7:
  * SDK monkey-patch dropping top_p when temperature is set — v2.0.7's
    mem0/llms/anthropic.py `_get_common_params` already does this.
"""
import contextvars
import json
import logging
import os
import sqlite3
import threading
import time
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# (mem0 v2.0.7+ has a built-in fix that strips top_p when temperature is also
# set — see mem0/llms/anthropic.py `_get_common_params`. We no longer need the
# SDK-level monkey-patch we used on v0.1.x.)

# --- LLM usage instrumentation ---
# Wraps anthropic Messages.create (chat tokens + prompt-cache hits) and
# openai Embeddings.create (embed tokens). Plus per-operation counters in
# add/search handlers. Each call is persisted as one row in /state/metrics.db
# so counters survive container restarts and can be aggregated by time /
# project / user later. The /stats endpoint returns SUM(CASE WHEN kind=...)
# aggregations over this table.
import anthropic.resources.messages as _ant_msgs
import openai.resources.embeddings as _oai_emb

_DB_PATH = os.environ.get("METRICS_DB_PATH", "/state/metrics.db")

# Per-MCP-connection (project, user) — set in _call_tool. The SDK-level
# instrumentation reads this so chat/embed rows triggered by mem0 SDK during
# a tool invocation are attributed to the right project/user. REST handlers
# that drive memory operations (e.g. /search) also set this for the duration
# of the request.
_call_ctx: contextvars.ContextVar[tuple[str | None, str | None]] = (
    contextvars.ContextVar("call_ctx", default=(None, None))
)

_db_lock = threading.Lock()
_db: sqlite3.Connection  # initialized by _db_init()


def _db_init() -> None:
    """Open /state/metrics.db (create dir + file if missing), enable WAL,
    ensure schema. Schema is idempotent so safe to call on every boot.
    Adds new columns via ALTER TABLE for installs upgraded from older builds.
    """
    global _db
    Path(_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    _db = sqlite3.connect(_DB_PATH, check_same_thread=False, isolation_level=None)
    _db.execute("PRAGMA journal_mode=WAL")
    _db.execute("PRAGMA synchronous=NORMAL")
    _db.executescript(
        """
        CREATE TABLE IF NOT EXISTS calls (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          ts                    REAL NOT NULL,
          kind                  TEXT NOT NULL,
          project               TEXT,
          user                  TEXT,
          input_tokens          INTEGER DEFAULT 0,
          output_tokens         INTEGER DEFAULT 0,
          cache_creation_tokens INTEGER DEFAULT 0,
          cache_read_tokens     INTEGER DEFAULT 0,
          embed_tokens          INTEGER DEFAULT 0,
          result_count          INTEGER,
          top1_score            REAL,
          result_ids            TEXT          -- JSON array of up to 5 memory IDs
        );
        CREATE INDEX IF NOT EXISTS idx_calls_ts        ON calls(ts);
        CREATE INDEX IF NOT EXISTS idx_calls_kind      ON calls(kind);
        CREATE INDEX IF NOT EXISTS idx_calls_proj_user ON calls(project, user);

        -- Project whitelist. Bridge refuses MCP connects whose URL project
        -- is not in this table (or whose row has deleted_at IS NOT NULL).
        -- Soft-delete: DELETE /projects/{name} sets deleted_at; the row stays
        -- so the UI can show it greyed-out and restore it. Memories in qdrant
        -- are untouched by soft-delete — DELETE /projects/{name}/memories
        -- clears them on demand and is independent of soft-delete state.
        CREATE TABLE IF NOT EXISTS projects (
          name        TEXT PRIMARY KEY,
          description TEXT DEFAULT '',
          created_at  REAL NOT NULL,
          created_by  TEXT,
          deleted_at  REAL  -- NULL = active; non-NULL = soft-deleted
        );
        """
    )
    # Forward-compatible ALTER for installs created before result_ids existed.
    cols = {row[1] for row in _db.execute("PRAGMA table_info(calls)").fetchall()}
    if "result_ids" not in cols:
        _db.execute("ALTER TABLE calls ADD COLUMN result_ids TEXT")
    # Forward-compatible ALTER for the projects table — adds soft-delete
    # column to installs that already created the table without it.
    pcols = {row[1] for row in _db.execute("PRAGMA table_info(projects)").fetchall()}
    if "deleted_at" not in pcols:
        _db.execute("ALTER TABLE projects ADD COLUMN deleted_at REAL")


def _db_record(kind: str, **fields: Any) -> None:
    """Insert one call row. (project, user) come from contextvar.
    Never raises: metrics failure must not break a real request."""
    proj, usr = _call_ctx.get()
    cols = ["ts", "kind", "project", "user", *fields.keys()]
    vals = [time.time(), kind, proj, usr, *fields.values()]
    placeholders = ",".join(["?"] * len(cols))
    try:
        with _db_lock:
            _db.execute(
                f"INSERT INTO calls ({','.join(cols)}) VALUES ({placeholders})",
                vals,
            )
    except Exception:
        logging.getLogger("mem0-bridge").exception(
            "metrics insert failed (kind=%s)", kind
        )


_orig_msg_create = _ant_msgs.Messages.create


def _instrumented_msg_create(self, *args, **kwargs):
    resp = _orig_msg_create(self, *args, **kwargs)
    usage = getattr(resp, "usage", None)
    if usage is not None:
        _db_record(
            "chat",
            input_tokens=int(getattr(usage, "input_tokens", 0) or 0),
            output_tokens=int(getattr(usage, "output_tokens", 0) or 0),
            cache_creation_tokens=int(
                getattr(usage, "cache_creation_input_tokens", 0) or 0
            ),
            cache_read_tokens=int(getattr(usage, "cache_read_input_tokens", 0) or 0),
        )
    return resp


_ant_msgs.Messages.create = _instrumented_msg_create

_orig_emb_create = _oai_emb.Embeddings.create


def _instrumented_emb_create(self, *args, **kwargs):
    resp = _orig_emb_create(self, *args, **kwargs)
    usage = getattr(resp, "usage", None)
    if usage is not None:
        _db_record(
            "embed",
            embed_tokens=int(getattr(usage, "total_tokens", 0) or 0),
        )
    return resp


_oai_emb.Embeddings.create = _instrumented_emb_create


def _record_add() -> None:
    _db_record("add")


def _record_search(items: Any) -> None:
    """Record one 'search' row. Stores up to top-5 memory IDs in result_ids
    (JSON array) so we can later count per-memory popularity via json_each.
    """
    listy = items if isinstance(items, list) else None
    result_count = len(listy) if listy else 0
    top1_score: float | None = None
    result_ids_json: str | None = None
    if listy:
        first = listy[0]
        score = first.get("score") if isinstance(first, dict) else None
        if score is not None:
            try:
                top1_score = float(score)
            except (TypeError, ValueError):
                pass
        ids: list[str] = []
        for item in listy[:5]:
            if isinstance(item, dict):
                mid = item.get("id") or item.get("memory_id")
                if mid:
                    ids.append(str(mid))
        if ids:
            result_ids_json = json.dumps(ids)
    _db_record(
        "search",
        result_count=result_count,
        top1_score=top1_score,
        result_ids=result_ids_json,
    )


def _record_get(memory_id: str) -> None:
    """Record one 'get' row (direct ID access via get_memory MCP tool)."""
    _db_record(
        "get",
        result_count=1,
        result_ids=json.dumps([str(memory_id)]),
    )


def _hit_counts_for(ids: list[str]) -> dict[str, int]:
    """Batch lookup: given memory IDs, return {id: hit_count} aggregated
    over all 'search' and 'get' rows. Hits = times the ID appeared in
    result_ids JSON. Missing IDs are absent from the dict (caller fills 0).

    Aggregates in Python so we don't depend on SQLite's JSON1 extension
    (some bundled SQLite builds < 3.38 lack json_each)."""
    if not ids:
        return {}
    target = set(ids)
    counts: dict[str, int] = {}
    try:
        with _db_lock:
            rows = _db.execute(
                "SELECT result_ids FROM calls "
                "WHERE kind IN ('search','get') AND result_ids IS NOT NULL"
            ).fetchall()
        for (rids,) in rows:
            try:
                parsed = json.loads(rids)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(parsed, list):
                continue
            for mid in parsed:
                sid = str(mid)
                if sid in target:
                    counts[sid] = counts.get(sid, 0) + 1
        return counts
    except Exception:
        logging.getLogger("mem0-bridge").exception("_hit_counts_for failed")
        return {}


def _agg_stats(project: str | None = None) -> dict[str, Any]:
    """Aggregate the calls table for /stats. Output keys mirror the field
    shape the existing /stats JSON exposes under llm/embedding/search_quality
    so the dashboard keeps working unchanged."""
    where = ""
    params: dict[str, Any] = {"now": time.time()}
    if project:
        where = "WHERE project = :project"
        params["project"] = project
    sql = f"""
        SELECT
          COALESCE(MIN(ts), :now)                                                      AS since_ts,
          SUM(CASE WHEN kind='chat'   THEN 1 ELSE 0 END)                               AS chat_calls,
          SUM(CASE WHEN kind='chat'   THEN input_tokens ELSE 0 END)                    AS input_tokens,
          SUM(CASE WHEN kind='chat'   THEN output_tokens ELSE 0 END)                   AS output_tokens,
          SUM(CASE WHEN kind='chat'   THEN cache_creation_tokens ELSE 0 END)           AS cache_creation_tokens,
          SUM(CASE WHEN kind='chat'   THEN cache_read_tokens ELSE 0 END)               AS cache_read_tokens,
          SUM(CASE WHEN kind='embed'  THEN 1 ELSE 0 END)                               AS embed_calls,
          SUM(CASE WHEN kind='embed'  THEN embed_tokens ELSE 0 END)                    AS embed_tokens,
          SUM(CASE WHEN kind='add'    THEN 1 ELSE 0 END)                               AS add_calls,
          SUM(CASE WHEN kind='search' THEN 1 ELSE 0 END)                               AS search_calls,
          SUM(CASE WHEN kind='search' AND (result_count IS NULL OR result_count=0)
              THEN 1 ELSE 0 END)                                                       AS search_zero_results,
          SUM(CASE WHEN kind='search' AND top1_score IS NOT NULL
              THEN top1_score ELSE 0.0 END)                                            AS top1_sum,
          SUM(CASE WHEN kind='search' AND top1_score IS NOT NULL
              THEN 1 ELSE 0 END)                                                       AS top1_count
        FROM calls
        {where}
    """
    with _db_lock:
        row = _db.execute(sql, params).fetchone()
    keys = [
        "since_ts",
        "chat_calls",
        "input_tokens",
        "output_tokens",
        "cache_creation_tokens",
        "cache_read_tokens",
        "embed_calls",
        "embed_tokens",
        "add_calls",
        "search_calls",
        "search_zero_results",
        "top1_sum",
        "top1_count",
    ]
    return {k: (v if v is not None else 0) for k, v in zip(keys, row)}


_db_init()


# --- Project whitelist helpers --------------------------------------------
# All read/write through these so the rest of the code never touches the
# `projects` table directly. Names are validated by `_project_name_ok`
# (alnum + dash/underscore/dot, 2-64 chars) — same shape we accept in URLs.

import re

_PROJECT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{1,63}$")


def _project_name_ok(name: str) -> bool:
    return bool(name) and bool(_PROJECT_NAME_RE.match(name))


def _project_exists(name: str) -> bool:
    """True iff the project is registered AND active (not soft-deleted).
    Used by handle_sse to gate MCP connections."""
    if not name:
        return False
    with _db_lock:
        row = _db.execute(
            "SELECT 1 FROM projects WHERE name = ? AND deleted_at IS NULL",
            (name,),
        ).fetchone()
    return row is not None


def _list_projects() -> list[dict[str, Any]]:
    """Return all projects (active + soft-deleted) so the UI can render
    deleted rows greyed-out with a Restore button. Ordered: active first
    (by created_at desc), then deleted (by deleted_at desc)."""
    with _db_lock:
        rows = _db.execute(
            "SELECT name, description, created_at, created_by, deleted_at "
            "FROM projects "
            "ORDER BY (deleted_at IS NOT NULL), "
            "         COALESCE(deleted_at, created_at) DESC"
        ).fetchall()
    return [
        {
            "name": r[0],
            "description": r[1] or "",
            "created_at": r[2],
            "created_by": r[3],
            "deleted_at": r[4],
        }
        for r in rows
    ]


def _create_project(
    name: str, description: str = "", created_by: str | None = None
) -> str:
    """Insert or auto-restore. Returns:
      'created'  — fresh INSERT
      'restored' — name existed in soft-deleted state, deleted_at cleared
                   (and description updated if a non-empty one was given)
      'exists'   — name already active; nothing changed
    """
    desc = description or ""
    now = time.time()
    with _db_lock:
        row = _db.execute(
            "SELECT deleted_at FROM projects WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            _db.execute(
                "INSERT INTO projects (name, description, created_at, created_by) "
                "VALUES (?, ?, ?, ?)",
                (name, desc, now, created_by),
            )
            return "created"
        if row[0] is None:
            return "exists"
        # soft-deleted → restore. Only overwrite description when caller
        # actually supplied one, so the original isn't blanked accidentally.
        if desc:
            _db.execute(
                "UPDATE projects SET deleted_at = NULL, description = ? "
                "WHERE name = ?",
                (desc, name),
            )
        else:
            _db.execute(
                "UPDATE projects SET deleted_at = NULL WHERE name = ?",
                (name,),
            )
        return "restored"


def _restore_project(name: str) -> bool:
    """Clear deleted_at on a soft-deleted project. Returns False if the
    project is missing OR already active."""
    with _db_lock:
        cur = _db.execute(
            "UPDATE projects SET deleted_at = NULL "
            "WHERE name = ? AND deleted_at IS NOT NULL",
            (name,),
        )
    return cur.rowcount > 0


def _delete_project(name: str) -> bool:
    """Soft delete: stamp deleted_at = now() so the row persists for UI
    rendering + restore. qdrant memories are untouched — use
    _clear_project_memories for the irreversible cleanup. Returns False
    if the project is missing OR already soft-deleted."""
    with _db_lock:
        cur = _db.execute(
            "UPDATE projects SET deleted_at = ? "
            "WHERE name = ? AND deleted_at IS NULL",
            (time.time(), name),
        )
    return cur.rowcount > 0


def _hard_delete_project_row(name: str) -> bool:
    """Physical row drop — only succeeds when the project is already
    soft-deleted. Returns False if missing or still active. qdrant cleanup
    is run separately by the calling REST handler so each step's blast
    radius is explicit and observable in logs."""
    with _db_lock:
        cur = _db.execute(
            "DELETE FROM projects WHERE name = ? AND deleted_at IS NOT NULL",
            (name,),
        )
    return cur.rowcount > 0


def _bootstrap_project(name: str) -> bool:
    """Bootstrap-only: insert iff name absent (active or deleted) so an
    auto-import never silently un-deletes a project the operator removed.
    Returns True if inserted."""
    with _db_lock:
        row = _db.execute(
            "SELECT 1 FROM projects WHERE name = ?", (name,)
        ).fetchone()
        if row is not None:
            return False
        _db.execute(
            "INSERT INTO projects (name, description, created_at, created_by) "
            "VALUES (?, ?, ?, ?)",
            (name, "(auto-imported on boot)", time.time(), "bootstrap"),
        )
        return True


def _retrieval_counts() -> dict[str, int]:
    """Per-project retrieval count = rows in `calls` where kind ∈ ('search','get').
    Each search counts as 1 (regardless of how many top-k hits it returned),
    each get counts as 1. Returns {project_name: count}; projects absent
    from the dict have zero retrievals on record."""
    with _db_lock:
        rows = _db.execute(
            "SELECT project, COUNT(*) FROM calls "
            "WHERE kind IN ('search','get') AND project IS NOT NULL "
            "GROUP BY project"
        ).fetchall()
    return {r[0]: int(r[1]) for r in rows}


from mcp.server import Server  # noqa: E402
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool
from mem0 import Memory
from qdrant_client import QdrantClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("mem0-bridge")

LITELLM_BASE_URL = os.environ["LITELLM_BASE_URL"].rstrip("/")
LITELLM_API_KEY = os.environ["LITELLM_API_KEY"]
LITELLM_CHAT_MODEL = os.environ.get("LITELLM_CHAT_MODEL", "claude-sonnet-4-6")
LITELLM_EMBED_MODEL = os.environ.get("LITELLM_EMBED_MODEL", "text-embedding-3-large")
EMBED_DIMS = int(os.environ.get("EMBED_DIMS", "3072"))
QDRANT_HOST = os.environ.get("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", "memories")
SHARED_TOKEN = os.environ["SHARED_TOKEN"]

# mem0 v0.1.115 的 BaseLlmConfig/BaseEmbedderConfig 不认识 *_base_url 字段，
# 用环境变量传给底层 SDK（anthropic / openai SDK 自己会读这些 env）。
os.environ["ANTHROPIC_API_KEY"] = LITELLM_API_KEY
os.environ["ANTHROPIC_BASE_URL"] = LITELLM_BASE_URL
os.environ["OPENAI_API_KEY"] = LITELLM_API_KEY
os.environ["OPENAI_BASE_URL"] = f"{LITELLM_BASE_URL}/v1"

memory = Memory.from_config(
    {
        "llm": {
            "provider": "anthropic",
            "config": {
                "model": LITELLM_CHAT_MODEL,
            },
        },
        "embedder": {
            "provider": "openai",
            "config": {
                "model": LITELLM_EMBED_MODEL,
                "embedding_dims": EMBED_DIMS,  # 否则 mem0 默认 1536，跟 text-embedding-3-large 实际 3072 不匹配
            },
        },
        "vector_store": {
            "provider": "qdrant",
            "config": {
                "host": QDRANT_HOST,
                "port": QDRANT_PORT,
                "collection_name": QDRANT_COLLECTION,
                "embedding_model_dims": EMBED_DIMS,
            },
        },
    }
)

log.info(
    "mem0 ready: chat=%s embed=%s dims=%d qdrant=%s:%d/%s",
    LITELLM_CHAT_MODEL,
    LITELLM_EMBED_MODEL,
    EMBED_DIMS,
    QDRANT_HOST,
    QDRANT_PORT,
    QDRANT_COLLECTION,
)


def make_mcp_server(project: str, user: str) -> Server:
    """Build a per-connection MCP server bound to (project, user)."""
    server = Server("mem0-bridge")

    @server.list_tools()
    async def _list_tools() -> list[Tool]:
        return [
            Tool(
                name="add_memory",
                description=(
                    f"Add a new memory in project '{project}'. This method is "
                    "called everytime the user informs anything about themselves, "
                    "their preferences, project conventions, technical decisions, "
                    "or anything useful in future conversation. This can also be "
                    "called when the user asks you to remember something."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "text": {
                            "type": "string",
                            "description": "Memory content (sentence or short note).",
                        }
                    },
                    "required": ["text"],
                },
            ),
            Tool(
                name="search_memory",
                description=(
                    f"Search through stored memories in project '{project}'. "
                    "This method is called EVERYTIME the user asks anything."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "limit": {"type": "integer", "default": 5},
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="list_memories",
                description=f"List all memories in project '{project}'.",
                inputSchema={
                    "type": "object",
                    "properties": {"limit": {"type": "integer", "default": 20}},
                },
            ),
            Tool(
                name="delete_memory",
                description=f"Delete a memory by its id from project '{project}'.",
                inputSchema={
                    "type": "object",
                    "properties": {"memory_id": {"type": "string"}},
                    "required": ["memory_id"],
                },
            ),
            Tool(
                name="get_memory",
                description=f"Fetch a single memory by its id from project '{project}'.",
                inputSchema={
                    "type": "object",
                    "properties": {"memory_id": {"type": "string"}},
                    "required": ["memory_id"],
                },
            ),
            Tool(
                name="update_memory",
                description=f"Replace the text of an existing memory in project '{project}'.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "memory_id": {"type": "string"},
                        "text": {"type": "string"},
                    },
                    "required": ["memory_id", "text"],
                },
            ),
            Tool(
                name="memory_history",
                description=f"Show change history (versions) of a memory in project '{project}'.",
                inputSchema={
                    "type": "object",
                    "properties": {"memory_id": {"type": "string"}},
                    "required": ["memory_id"],
                },
            ),
        ]

    def _belongs_to_namespace(record: Any) -> bool:
        """Return True iff record belongs to this connection's project.
        Project is the isolation boundary; user_id is attribution only."""
        if not isinstance(record, dict):
            return False
        return record.get("agent_id") == project

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        # Project is the only isolation boundary. Read/list filter on agent_id;
        # add records user_id for attribution; mutations check project via
        # _belongs_to_namespace before touching mem0.
        # Tag any chat/embed/op rows recorded during this tool with this
        # connection's (project, user) — see _db_record.
        _call_ctx.set((project, user))
        scope = {"agent_id": project}
        try:
            if name == "add_memory":
                r = memory.add(
                    arguments["text"],
                    user_id=user,
                    agent_id=project,
                    metadata={"project": project, "source": "mcp"},
                )
                _record_add()
            elif name == "search_memory":
                r = memory.search(
                    query=arguments["query"],
                    filters=scope,
                    top_k=arguments.get("limit", 5),
                )
                _record_search(
                    r.get("results", r) if isinstance(r, dict) else r
                )
            elif name == "list_memories":
                r = memory.get_all(
                    filters=scope,
                    top_k=arguments.get("limit", 20),
                )
            elif name == "delete_memory":
                existing = memory.get(arguments["memory_id"])
                if not _belongs_to_namespace(existing):
                    return [TextContent(type="text", text="Memory not found in this project")]
                r = memory.delete(arguments["memory_id"])
            elif name == "get_memory":
                r = memory.get(arguments["memory_id"])
                if not _belongs_to_namespace(r):
                    return [TextContent(type="text", text="Memory not found in this project")]
                _record_get(arguments["memory_id"])
            elif name == "update_memory":
                existing = memory.get(arguments["memory_id"])
                if not _belongs_to_namespace(existing):
                    return [TextContent(type="text", text="Memory not found in this project")]
                r = memory.update(arguments["memory_id"], data=arguments["text"])
            elif name == "memory_history":
                existing = memory.get(arguments["memory_id"])
                if not _belongs_to_namespace(existing):
                    return [TextContent(type="text", text="Memory not found in this project")]
                r = memory.history(arguments["memory_id"])
            else:
                return [TextContent(type="text", text=f"Unknown tool: {name}")]
            return [TextContent(type="text", text=str(r))]
        except Exception as e:
            log.exception("tool %s failed (project=%s user=%s)", name, project, user)
            return [TextContent(type="text", text=f"Error: {e}")]

    return server


sse_transport = SseServerTransport("/messages/")


def _auth_ok(headers: dict) -> bool:
    auth = headers.get("authorization", "") or headers.get("Authorization", "")
    return auth.startswith("Bearer ") and auth[7:] == SHARED_TOKEN


# --- Static UI page (served at GET /) ------------------------------------
_INDEX_PATH = Path(__file__).parent / "static" / "index.html"
_INDEX_HTML = _INDEX_PATH.read_text(encoding="utf-8") if _INDEX_PATH.exists() else (
    "<!doctype html><meta charset='utf-8'>"
    "<title>mem0 bridge</title>"
    "<p>UI not bundled. Use REST endpoints directly.</p>"
)


async def serve_index(request: Request) -> Response:
    # The page itself is public; it asks the user for a token via prompt() and
    # stores it in localStorage. All API calls require Bearer auth.
    return Response(_INDEX_HTML, media_type="text/html; charset=utf-8")


async def health(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "chat_model": LITELLM_CHAT_MODEL,
            "embed_model": LITELLM_EMBED_MODEL,
            "embed_dims": EMBED_DIMS,
        }
    )


async def search_rest(request: Request) -> JSONResponse:
    """POST /search → semantic search.
    body: {"project": str, "user": str, "query": str, "limit": int=10}
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    query = body.get("query")
    if not query:
        return JSONResponse({"error": "missing query"}, status_code=400)

    project = body.get("project")
    user = body.get("user")
    try:
        limit = min(int(body.get("limit", 10)), 100)
    except (TypeError, ValueError):
        return JSONResponse({"error": "limit must be integer"}, status_code=400)

    filters: dict[str, Any] = {}
    if user:
        filters["user_id"] = user
    if project:
        filters["agent_id"] = project

    try:
        # Attribute embed call to the project/user this REST request named.
        _call_ctx.set((project, user))
        result = memory.search(query=query, filters=filters or None, top_k=limit)
        items = result.get("results", result) if isinstance(result, dict) else result
        _record_search(items)
        # Enrich with retrieval hit count (the count rises by 1 BEFORE this
        # call's row was inserted, so this query reflects historical hits).
        if isinstance(items, list) and items:
            ids = [str(it.get("id")) for it in items if isinstance(it, dict) and it.get("id") is not None]
            counts = _hit_counts_for(ids)
            for it in items:
                if isinstance(it, dict):
                    it["hit_count"] = counts.get(str(it.get("id")), 0)
        return JSONResponse(
            {
                "query": query,
                "project": project,
                "user": user,
                "memories": items,
            }
        )
    except Exception as e:
        log.exception("REST search failed")
        return JSONResponse({"error": str(e)}, status_code=500)


async def add_rest(request: Request) -> JSONResponse:
    """POST /add → add a memory.
    body: {"project": str, "user": str, "text": str}
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    text = body.get("text")
    if not text:
        return JSONResponse({"error": "missing text"}, status_code=400)

    project = body.get("project")
    user = body.get("user") or "system"
    if not project:
        return JSONResponse({"error": "missing project"}, status_code=400)

    try:
        _call_ctx.set((project, user))
        r = memory.add(
            text,
            user_id=user,
            agent_id=project,
            metadata={"project": project, "source": "rest"},
        )
        _record_add()
        return JSONResponse({"status": "ok", "result": str(r)})
    except Exception as e:
        log.exception("REST add failed")
        return JSONResponse({"error": str(e)}, status_code=500)


async def update_memory_rest(request: Request) -> JSONResponse:
    """PUT /memories/{memory_id} → update memory text in-place.
    body: {"text": str, "project": str}
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    memory_id = request.path_params["memory_id"]
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    text = body.get("text")
    if not text:
        return JSONResponse({"error": "missing text"}, status_code=400)

    project = body.get("project")
    user = body.get("user") or "system"

    try:
        _call_ctx.set((project, user))
        existing = memory.get(memory_id)
        if not isinstance(existing, dict) or existing.get("agent_id") != project:
            return JSONResponse(
                {"error": "memory not found in this project"}, status_code=404
            )
        r = memory.update(memory_id, data=text)
        return JSONResponse({"status": "updated", "id": memory_id, "result": str(r)})
    except Exception as e:
        log.exception("REST update failed (id=%s)", memory_id)
        return JSONResponse({"error": str(e)}, status_code=500)


async def delete_memory_rest(request: Request) -> JSONResponse:
    """DELETE /memories/{memory_id}"""
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    memory_id = request.path_params["memory_id"]
    try:
        memory.delete(memory_id)
        return JSONResponse({"status": "deleted", "id": memory_id})
    except Exception as e:
        log.exception("REST delete failed (id=%s)", memory_id)
        return JSONResponse({"error": str(e)}, status_code=500)


# Direct qdrant client for stats aggregation (mem0 doesn't expose count APIs)
_qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, check_compatibility=False)


def _projects_bootstrap() -> None:
    """On boot, scan qdrant for distinct agent_ids and seed the projects
    table. Pre-existing setups keep working; subsequent unknown projects
    must go through the management UI."""
    seen: set[str] = set()
    try:
        offset = None
        scanned = 0
        while scanned < 20000:  # hard cap; on first boot rooms may be huge
            points, offset = _qdrant.scroll(
                collection_name=QDRANT_COLLECTION,
                limit=500,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            if not points:
                break
            for p in points:
                aid = (p.payload or {}).get("agent_id")
                if aid:
                    seen.add(str(aid))
            scanned += len(points)
            if offset is None:
                break
    except Exception:
        # qdrant not ready or collection missing — fine, nothing to seed.
        log.exception("projects bootstrap: qdrant scroll failed")
    inserted = 0
    for name in seen:
        if _project_name_ok(name) and _bootstrap_project(name):
            inserted += 1
    log.info(
        "projects bootstrap: scanned %d distinct agent_ids, inserted %d new",
        len(seen),
        inserted,
    )


def _clear_project_memories(name: str) -> int:
    """Hard-delete every qdrant point whose payload.agent_id == name.
    Returns the count of points removed (best-effort: qdrant's delete-by-filter
    response may not include a count on every version, in which case we
    return -1)."""
    from qdrant_client.http import models as qmodels

    flt = qmodels.Filter(
        must=[
            qmodels.FieldCondition(
                key="agent_id", match=qmodels.MatchValue(value=name)
            )
        ]
    )
    # Count first so we have a deterministic return value; some qdrant
    # versions return only a status from delete().
    try:
        cnt = _qdrant.count(
            collection_name=QDRANT_COLLECTION, count_filter=flt, exact=True
        )
        n = int(getattr(cnt, "count", 0))
    except Exception:
        n = -1
    _qdrant.delete(
        collection_name=QDRANT_COLLECTION,
        points_selector=qmodels.FilterSelector(filter=flt),
        wait=True,
    )
    return n


_projects_bootstrap()


async def stats_rest(request: Request) -> JSONResponse:
    """GET /stats → 聚合 qdrant 里的记忆使用情况。

    扫描 collection 全部点（限制 10k 以内）按 agent_id / user_id 聚合。
    点数 > 10k 时 by_project/by_user 是抽样结果（total 数仍然准确）。

    Query params:
      - project: 只统计指定项目的记忆（按 agent_id 过滤）
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        from qdrant_client.http import models as qmodels

        filter_project = request.query_params.get("project")
        scroll_filter = None
        if filter_project:
            scroll_filter = qmodels.Filter(
                must=[
                    qmodels.FieldCondition(
                        key="agent_id",
                        match=qmodels.MatchValue(value=filter_project),
                    )
                ]
            )

        info = _qdrant.get_collection(QDRANT_COLLECTION)

        by_project: dict[str, int] = {}
        by_user: dict[str, int] = {}
        by_pair: dict[tuple, int] = {}
        by_category: dict[str, int] = {}

        # Activity timeline: last 30 days (inclusive of today, UTC dates)
        today_utc = datetime.now(timezone.utc).date()
        thirty_days_ago = today_utc - timedelta(days=29)
        activity: dict[str, int] = {
            (thirty_days_ago + timedelta(days=i)).isoformat(): 0 for i in range(30)
        }

        text_lengths: list[int] = []
        all_with_ts: list[dict[str, Any]] = []
        updated_count = 0
        with_ts_count = 0

        def _to_iso_date(v: Any) -> str | None:
            if not v:
                return None
            try:
                return (
                    datetime.fromisoformat(str(v).replace("Z", "+00:00"))
                    .astimezone(timezone.utc)
                    .date()
                    .isoformat()
                )
            except Exception:
                return None

        max_scan = 10000
        scanned = 0
        offset = None
        while scanned < max_scan:
            points, offset = _qdrant.scroll(
                collection_name=QDRANT_COLLECTION,
                scroll_filter=scroll_filter,
                limit=min(500, max_scan - scanned),
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            if not points:
                break
            for p in points:
                payload = p.payload or {}
                proj = payload.get("agent_id") or payload.get("project") or "<none>"
                usr = payload.get("user_id") or "<none>"
                by_project[proj] = by_project.get(proj, 0) + 1
                by_user[usr] = by_user.get(usr, 0) + 1
                by_pair[(proj, usr)] = by_pair.get((proj, usr), 0) + 1

                text = payload.get("data") or payload.get("memory") or ""
                if text:
                    text_lengths.append(len(text))

                created = payload.get("created_at")
                updated = payload.get("updated_at")
                created_iso = _to_iso_date(created)
                if created_iso and created_iso in activity:
                    activity[created_iso] += 1
                if created:
                    with_ts_count += 1
                    if updated and str(updated) != str(created):
                        updated_count += 1

                # Categories may live at top-level or in metadata
                cats = payload.get("categories")
                if cats is None:
                    cats = (payload.get("metadata") or {}).get("categories")
                if cats:
                    if isinstance(cats, str):
                        cats = [cats]
                    for c in cats:
                        by_category[str(c)] = by_category.get(str(c), 0) + 1

                all_with_ts.append(
                    {
                        "id": str(p.id),
                        "memory": text,
                        "user_id": payload.get("user_id"),
                        "agent_id": payload.get("agent_id"),
                        "created_at": created,
                        "updated_at": updated,
                    }
                )
            scanned += len(points)
            if offset is None:
                break

        # Length distribution
        length_stats: dict[str, Any] = {}
        if text_lengths:
            text_lengths_sorted = sorted(text_lengths)
            n = len(text_lengths_sorted)
            length_stats = {
                "count": n,
                "avg": round(statistics.mean(text_lengths_sorted), 1),
                "p50": text_lengths_sorted[n // 2],
                "p90": text_lengths_sorted[min(n - 1, int(n * 0.9))],
                "max": text_lengths_sorted[-1],
            }

        # Recent 10 by created_at (lexicographic on ISO is fine)
        all_with_ts.sort(key=lambda x: x.get("created_at") or "", reverse=True)
        recent = all_with_ts[:10]

        # LLM/embed/search snapshot — aggregated from /state/metrics.db so the
        # numbers persist across container restarts. since_ts is the timestamp
        # of the very first recorded call (or "now" if the table is empty), so
        # uptime_sec below is "metric coverage age", not container uptime.
        s = _agg_stats(project=filter_project)
        # Anthropic Sonnet 4-6 prompt caching: read 0.1× input price, write 1.25×
        # Saved vs no-cache = cache_read × (1 - 0.1) × $3/1M = × 2.7/1M
        # Overhead from creation = cache_creation × (1.25 - 1) × $3/1M = × 0.75/1M
        savings_usd = s["cache_read_tokens"] * 2.7 / 1_000_000
        overhead_usd = s["cache_creation_tokens"] * 0.75 / 1_000_000
        net_savings_usd = savings_usd - overhead_usd
        cached_total = s["cache_read_tokens"] + s["cache_creation_tokens"]
        full_input_total = s["input_tokens"] + cached_total
        cache_hit_ratio = (
            s["cache_read_tokens"] / full_input_total if full_input_total else 0
        )

        return JSONResponse(
            {
                "total_memories": scanned if filter_project else getattr(info, "points_count", None),
                "filter_project": filter_project,
                "embedding_dim": EMBED_DIMS,
                "qdrant": {
                    "points_count": getattr(info, "points_count", None),
                    "vectors_count": getattr(info, "vectors_count", None),
                    "indexed_vectors_count": getattr(
                        info, "indexed_vectors_count", None
                    ),
                    "segments_count": getattr(info, "segments_count", None),
                    "status": str(getattr(info, "status", "")),
                },
                "scanned": scanned,
                "scan_truncated": scanned >= max_scan,
                "by_project": dict(sorted(by_project.items(), key=lambda x: -x[1])),
                "by_user": dict(sorted(by_user.items(), key=lambda x: -x[1])),
                "by_pair": [
                    {"project": p, "user": u, "count": c}
                    for (p, u), c in sorted(by_pair.items(), key=lambda x: -x[1])[:50]
                ],
                "by_category": dict(
                    sorted(by_category.items(), key=lambda x: -x[1])
                ),
                "activity_30d": activity,
                "length_stats": length_stats,
                "recent": recent,
                "update_ratio": {
                    "updated": updated_count,
                    "total": with_ts_count,
                    "ratio": (updated_count / with_ts_count) if with_ts_count else 0,
                },
                "llm": {
                    "since_ts": s["since_ts"],
                    "uptime_sec": int(time.time() - s["since_ts"]),
                    "calls": s["chat_calls"],
                    "input_tokens": s["input_tokens"],
                    "output_tokens": s["output_tokens"],
                    "cache_creation_tokens": s["cache_creation_tokens"],
                    "cache_read_tokens": s["cache_read_tokens"],
                    "cache_hit_ratio": round(cache_hit_ratio, 3),
                    "estimated_savings_usd": round(net_savings_usd, 4),
                },
                "embedding": {
                    "calls": s["embed_calls"],
                    "tokens": s["embed_tokens"],
                    "estimated_cost_usd": round(
                        s["embed_tokens"] * 0.13 / 1_000_000, 5
                    ),
                    "write_calls": s["add_calls"],
                    "read_calls": s["search_calls"],
                },
                "search_quality": {
                    "calls": s["search_calls"],
                    "zero_result_count": s["search_zero_results"],
                    "zero_result_ratio": (
                        s["search_zero_results"] / s["search_calls"]
                        if s["search_calls"]
                        else 0
                    ),
                    "scored_count": s["top1_count"],
                    "avg_top1_score": (
                        s["top1_sum"] / s["top1_count"]
                        if s["top1_count"]
                        else None
                    ),
                },
            }
        )
    except Exception as e:
        log.exception("REST stats failed")
        return JSONResponse({"error": str(e)}, status_code=500)


async def list_memories_rest(request: Request) -> JSONResponse:
    """Browse memories for ops/UI use.

    GET /memories?project=<p>&user=<u>&limit=<n>&offset=<n>
      - project, user: filter (both optional; omit = all)
      - limit:  default 20, max 500
      - offset: default 0 (pagination)

    Auth: same Bearer token as MCP.
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    project = request.query_params.get("project")
    user = request.query_params.get("user")
    try:
        limit = min(int(request.query_params.get("limit", "20")), 500)
        offset = max(int(request.query_params.get("offset", "0")), 0)
    except ValueError:
        return JSONResponse({"error": "limit/offset must be integers"}, status_code=400)

    try:
        if user or project:
            # Filtered: use mem0 SDK (v2 requires filters dict with at least one
            # of user_id/agent_id/run_id).
            filters: dict[str, Any] = {}
            if user:
                filters["user_id"] = user
            if project:
                filters["agent_id"] = project
            result = memory.get_all(filters=filters, top_k=limit + offset)
            items = result.get("results", result) if isinstance(result, dict) else result
            items = items[offset : offset + limit] if isinstance(items, list) else items
        else:
            # Unfiltered "list all": mem0.get_all() refuses without filters,
            # so scroll qdrant directly. Returns raw payload + the qdrant id.
            points, _ = _qdrant.scroll(
                collection_name=QDRANT_COLLECTION,
                limit=limit + offset,
                with_payload=True,
                with_vectors=False,
            )
            sliced = points[offset : offset + limit]
            items = [
                {
                    "id": str(p.id),
                    "memory": (p.payload or {}).get("data")
                    or (p.payload or {}).get("memory")
                    or "",
                    "user_id": (p.payload or {}).get("user_id"),
                    "agent_id": (p.payload or {}).get("agent_id"),
                    "metadata": {
                        k: v
                        for k, v in (p.payload or {}).items()
                        if k not in ("data", "memory", "user_id", "agent_id")
                    },
                }
                for p in sliced
            ]

        # Enrich each memory row with its retrieval hit count.
        if isinstance(items, list) and items:
            ids = [str(it.get("id")) for it in items if it.get("id") is not None]
            counts = _hit_counts_for(ids)
            for it in items:
                it["hit_count"] = counts.get(str(it.get("id")), 0)

        return JSONResponse(
            {
                "project": project,
                "user": user,
                "limit": limit,
                "offset": offset,
                "count": len(items) if isinstance(items, list) else None,
                "memories": items,
            }
        )
    except Exception as e:
        log.exception("REST list_memories failed")
        return JSONResponse({"error": str(e)}, status_code=500)


async def popular_rest(request: Request) -> JSONResponse:
    """GET /popular?limit=20 → top-N most-retrieved memories.

    Hits include both 'search' (top-5 results stored per call) and 'get'
    (direct ID access via get_memory MCP tool). Orphan IDs (memory was
    deleted but old call rows still reference them) are returned with
    `orphan: true` and `memory: null`.
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        limit = min(max(int(request.query_params.get("limit", "20")), 1), 100)
    except ValueError:
        return JSONResponse({"error": "limit must be integer"}, status_code=400)

    try:
        # Aggregate in Python to stay portable across SQLite builds
        # without the JSON1 extension (<3.38 in some Python distributions).
        with _db_lock:
            rows = _db.execute(
                "SELECT ts, kind, result_ids FROM calls "
                "WHERE kind IN ('search','get') AND result_ids IS NOT NULL"
            ).fetchall()
        agg: dict[str, dict[str, Any]] = {}
        for ts, kind, rids in rows:
            try:
                parsed = json.loads(rids)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(parsed, list):
                continue
            for mid in parsed:
                sid = str(mid)
                bucket = agg.setdefault(
                    sid,
                    {"hit_count": 0, "search_hits": 0, "get_hits": 0, "last_hit_ts": 0.0},
                )
                bucket["hit_count"] += 1
                if kind == "search":
                    bucket["search_hits"] += 1
                elif kind == "get":
                    bucket["get_hits"] += 1
                if ts and ts > bucket["last_hit_ts"]:
                    bucket["last_hit_ts"] = ts
        sorted_ids = sorted(
            agg.keys(),
            key=lambda k: (-agg[k]["hit_count"], -(agg[k]["last_hit_ts"] or 0)),
        )[:limit]

        items: list[dict[str, Any]] = []
        for mid in sorted_ids:
            b = agg[mid]
            hit_count = b["hit_count"]
            last_hit_ts = b["last_hit_ts"] or None
            search_hits = b["search_hits"]
            get_hits = b["get_hits"]
            content: str | None = None
            project: str | None = None
            user: str | None = None
            try:
                m = memory.get(mid)
                if isinstance(m, dict):
                    content = m.get("memory") or m.get("data")
                    project = m.get("agent_id")
                    user = m.get("user_id")
            except Exception:
                # Orphan or deleted; show "<deleted>" in the UI.
                pass
            items.append(
                {
                    "memory_id": str(mid),
                    "hit_count": int(hit_count),
                    "search_hits": int(search_hits or 0),
                    "get_hits": int(get_hits or 0),
                    "last_hit_ts": last_hit_ts,
                    "memory": content,
                    "project": project,
                    "user": user,
                    "orphan": content is None,
                }
            )

        return JSONResponse({"limit": limit, "count": len(items), "popular": items})
    except Exception as e:
        log.exception("REST popular failed")
        return JSONResponse({"error": str(e)}, status_code=500)


async def list_projects_rest(request: Request) -> JSONResponse:
    """GET /projects → all projects (active + soft-deleted) enriched with
    memory_count (from qdrant) and retrieval_count (from metrics.db).

    Memory counts come from qdrant (exact filter on agent_id). Cheap because
    qdrant indexes payload fields, but we still cap N to keep the dashboard
    responsive: only count for the first 200 projects; beyond that,
    memory_count is reported as null. Retrieval count is computed in one
    aggregate SQL pass and applied to every row regardless of cap."""
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    items = _list_projects()
    retrieval = _retrieval_counts()
    from qdrant_client.http import models as qmodels

    for i, p in enumerate(items):
        p["retrieval_count"] = int(retrieval.get(p["name"], 0))
        if i >= 200:
            p["memory_count"] = None
            continue
        try:
            cnt = _qdrant.count(
                collection_name=QDRANT_COLLECTION,
                count_filter=qmodels.Filter(
                    must=[
                        qmodels.FieldCondition(
                            key="agent_id",
                            match=qmodels.MatchValue(value=p["name"]),
                        )
                    ]
                ),
                exact=True,
            )
            p["memory_count"] = int(getattr(cnt, "count", 0))
        except Exception:
            p["memory_count"] = None
    return JSONResponse({"count": len(items), "projects": items})


async def create_project_rest(request: Request) -> JSONResponse:
    """POST /projects  body: {"name": str, "description": str=""}

    Auto-restore semantics: posting a name that exists in soft-deleted state
    revives it (deleted_at cleared, description overwritten if non-empty).
    Posting a name that's currently active returns 409. New names → 201.
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)
    name = (body.get("name") or "").strip()
    description = (body.get("description") or "").strip()
    if not _project_name_ok(name):
        return JSONResponse(
            {
                "error": "invalid project name (use alnum + dash/underscore/dot, 2-64 chars, must start with alnum)"
            },
            status_code=400,
        )
    action = _create_project(name, description, created_by="ui")
    if action == "exists":
        return JSONResponse({"error": "project already exists"}, status_code=409)
    log.info("project %s: name=%s", action, name)
    return JSONResponse(
        {"status": action, "name": name},
        status_code=201 if action == "created" else 200,
    )


async def delete_project_rest(request: Request) -> JSONResponse:
    """DELETE /projects/{name} → soft delete (sets deleted_at).

    The row stays so the UI can still render it (greyed-out with a Restore
    button); MCP connections to it are 404'd until restored. qdrant
    memories with this agent_id are NOT touched — call
    DELETE /projects/{name}/memories explicitly to clear them."""
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    name = request.path_params["name"]
    if not _delete_project(name):
        return JSONResponse(
            {"error": "project not found or already deleted"}, status_code=404
        )
    log.info("project soft-deleted: name=%s", name)
    return JSONResponse({"status": "deleted", "name": name})


async def restore_project_rest(request: Request) -> JSONResponse:
    """POST /projects/{name}/restore → clear deleted_at on a soft-deleted
    project. 404 if missing or already active."""
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    name = request.path_params["name"]
    if not _restore_project(name):
        return JSONResponse(
            {"error": "project not found or not deleted"}, status_code=404
        )
    log.info("project restored: name=%s", name)
    return JSONResponse({"status": "restored", "name": name})


async def hard_delete_project_rest(request: Request) -> JSONResponse:
    """DELETE /projects/{name}/hard → physical purge. Required state:
    project must already be soft-deleted (defense-in-depth: hard-delete
    requires an explicit prior soft-delete step).

    Order of operations:
      1) clear qdrant memories (irreversible)
      2) drop the projects row

    If qdrant clear succeeds but the row drop fails, the project becomes
    a phantom — soft-deleted with zero memories — but that's still safer
    than the reverse order (drop row first → if qdrant fails, row is
    gone but memories linger, with no whitelist entry to operate on).
    """
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    name = request.path_params["name"]
    if not _project_name_ok(name):
        return JSONResponse({"error": "invalid project name"}, status_code=400)

    # Pre-check: project must exist AND be in soft-deleted state.
    with _db_lock:
        row = _db.execute(
            "SELECT deleted_at FROM projects WHERE name = ?", (name,)
        ).fetchone()
    if row is None:
        return JSONResponse({"error": "project not found"}, status_code=404)
    if row[0] is None:
        return JSONResponse(
            {"error": "project is active — soft-delete it first"},
            status_code=409,
        )

    try:
        memories_removed = _clear_project_memories(name)
    except Exception as e:
        log.exception("hard delete: qdrant clear failed (project=%s)", name)
        return JSONResponse(
            {"error": f"qdrant clear failed: {e}"}, status_code=500
        )

    if not _hard_delete_project_row(name):
        log.error(
            "hard delete: row drop failed after qdrant clear (project=%s, "
            "memories_removed=%s) — project is now a phantom",
            name,
            memories_removed,
        )
        return JSONResponse(
            {
                "error": "qdrant cleared but row drop failed — please retry",
                "memories_removed": memories_removed,
            },
            status_code=500,
        )

    log.warning(
        "project hard-deleted: name=%s memories_removed=%s",
        name,
        memories_removed,
    )
    return JSONResponse(
        {
            "status": "purged",
            "name": name,
            "memories_removed": memories_removed,
        }
    )


async def clear_project_memories_rest(request: Request) -> JSONResponse:
    """DELETE /projects/{name}/memories → hard-delete all qdrant points
    whose agent_id equals {name}. Independent of project whitelist:
    callable for orphaned namespaces (project soft-deleted earlier but
    memories still in qdrant)."""
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    name = request.path_params["name"]
    if not _project_name_ok(name):
        return JSONResponse({"error": "invalid project name"}, status_code=400)
    try:
        n = _clear_project_memories(name)
        log.warning("project memories cleared: name=%s removed=%s", name, n)
        return JSONResponse({"status": "cleared", "name": name, "removed": n})
    except Exception as e:
        log.exception("clear memories failed (project=%s)", name)
        return JSONResponse({"error": str(e)}, status_code=500)


async def handle_sse(request: Request):
    if not _auth_ok(dict(request.headers)):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    project = request.path_params["project"]
    user = request.path_params["user"]
    if not _project_exists(project):
        log.warning("SSE rejected: unknown project=%s user=%s", project, user)
        return JSONResponse(
            {
                "error": f"unknown project '{project}' — ask an admin to "
                f"register it via the management UI at GET /"
            },
            status_code=404,
        )
    log.info("SSE connect: project=%s user=%s", project, user)
    srv = make_mcp_server(project, user)
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await srv.run(
            streams[0],
            streams[1],
            srv.create_initialization_options(),
            stateless=True,  # 跳过 init 握手 gate；某些客户端(Claude Code)不发 notifications/initialized
        )
    # SSE 响应已被 transport 写完，但 Starlette Route 仍要求 endpoint 返回 Response。
    return Response()


async def handle_messages(scope, receive, send):
    headers_dict = {
        k.decode().lower(): v.decode() for k, v in scope.get("headers", [])
    }
    if not _auth_ok(headers_dict):
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send(
            {"type": "http.response.body", "body": b'{"error":"unauthorized"}'}
        )
        return
    await sse_transport.handle_post_message(scope, receive, send)


app = Starlette(
    debug=False,
    routes=[
        Route("/", endpoint=serve_index),
        Route("/health", endpoint=health),
        Route("/memories", endpoint=list_memories_rest, methods=["GET"]),
        Route("/memories/{memory_id}", endpoint=update_memory_rest, methods=["PUT"]),
        Route("/memories/{memory_id}", endpoint=delete_memory_rest, methods=["DELETE"]),
        Route("/search", endpoint=search_rest, methods=["POST"]),
        Route("/add", endpoint=add_rest, methods=["POST"]),
        Route("/stats", endpoint=stats_rest, methods=["GET"]),
        Route("/popular", endpoint=popular_rest, methods=["GET"]),
        Route("/projects", endpoint=list_projects_rest, methods=["GET"]),
        Route("/projects", endpoint=create_project_rest, methods=["POST"]),
        Route("/projects/{name}", endpoint=delete_project_rest, methods=["DELETE"]),
        Route(
            "/projects/{name}/restore",
            endpoint=restore_project_rest,
            methods=["POST"],
        ),
        Route(
            "/projects/{name}/hard",
            endpoint=hard_delete_project_rest,
            methods=["DELETE"],
        ),
        Route(
            "/projects/{name}/memories",
            endpoint=clear_project_memories_rest,
            methods=["DELETE"],
        ),
        Route("/mcp/{project}/sse/{user}", endpoint=handle_sse),
        Mount("/messages/", app=handle_messages),
    ],
)
