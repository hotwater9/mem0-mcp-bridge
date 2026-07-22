# Client Onboarding Guide

Connect the mem0 memory service to Claude Code / Codex in 5 minutes.

> All team members in the same project share one memory pool. Writes carry `user_id` as attribution (visible who wrote it), but read/write/delete are unrestricted within a project — this is the core design.
> Personal notes should go in Claude Code's built-in auto-memory (`~/.claude/projects/.../memory/`), not here.

---

## Prerequisites

Get from your admin:
1. **`MEM0_TOKEN`** — the shared Bearer token
2. **Server address** — where the bridge is running (e.g., `your-server:8765`)

---

## 1. Environment Variables (one-time per machine)

### Linux / macOS

Add to `~/.bashrc` or `~/.zshrc`:

```bash
export USER=yourname              # your identifier, keep consistent across team
export MEM0_TOKEN=<token-from-admin>
```

`source ~/.bashrc` to apply.

### Windows (PowerShell, permanent)

```powershell
[Environment]::SetEnvironmentVariable("USER", "yourname", "User")
[Environment]::SetEnvironmentVariable("MEM0_TOKEN", "<token-from-admin>", "User")
```

Restart PowerShell / VSCode to apply.

### Verify

```bash
echo $USER         # should print your identifier
echo $MEM0_TOKEN   # should print the token
```

---

## 2. Claude Code Setup (once per project)

Create `.mcp.json` in your project root:

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

**Important**: Replace `<project-name>` with your git repo name (e.g., `my-app`). **Team must use the same name** to share memories. Safe to commit to git.

---

## 3. Codex Setup (one-time, global)

Place wrapper script at `~/bin/mem-codex.sh` (auto-detects project from current git repo):

```bash
#!/usr/bin/env bash
set -euo pipefail

MEM0_HOST="${MEM0_HOST:-http://your-server:8765}"
USER_ID="${USER:?export USER=<your-id> first}"
TOKEN="${MEM0_TOKEN:?export MEM0_TOKEN=<token> first}"

if PROJECT_ROOT=$(git -C "$PWD" rev-parse --show-toplevel 2>/dev/null); then
  PROJECT=$(basename "$PROJECT_ROOT")
else
  PROJECT="_personal"
fi

exec curl -sN \
  -H "Authorization: Bearer ${TOKEN}" \
  "${MEM0_HOST}/mcp/${PROJECT}/sse/${USER_ID}"
```

```bash
chmod +x ~/bin/mem-codex.sh
```

Add to `~/.codex/config.toml`:

```toml
[mcp_servers.memory]
command = "/home/<you>/bin/mem-codex.sh"
```

---

## 4. CLAUDE.md (let Claude know the service exists)

The MCP tool descriptions already specify when to call and what to store. You only need a minimal pointer in your project's `CLAUDE.md`:

```markdown
## Project Memory (mem0)

Access the project memory pool via MCP `memory:*` tools.
**When to call, what to store, what not to store — follow the tool descriptions.**
```

> Why so short?
> - Each `memory:*` tool's description already says "when to call" (e.g., search is "called EVERYTIME the user asks anything")
> - CLAUDE.md is loaded every conversation and costs tokens — repeating tool descriptions is waste
> - Personal preferences go in Claude Code's built-in auto-memory, not here

---

## 5. Verify Everything Works

### Test memory write

In your project directory, start Claude Code and say:

```
Remember: this project uses 4-space indentation, imports sorted by isort default
```

Claude should call `memory:add_memory` and confirm success.

### Test memory search

Then ask:

```
How many spaces does this project use for indentation?
```

Claude should call `memory:search_memory`, find the memory, and answer "4 spaces".

### Test project isolation

`cd` to a different project (also configured with `.mcp.json`), ask the same question.

Expected: **not found** (namespace isolation working). If found, check the project name in both `.mcp.json` URLs.

### Web UI

Open `http://your-server:8765/` in a browser, enter your token. You should see the memory you just stored.

---

## 6. FAQ

### Can I use this for a new project immediately?

**Yes** — namespaces are created automatically. The first memory write creates the namespace.

### What if team members use different project names?

They become separate namespaces and can't see each other's memories. **Always use the git repo directory name** as the convention.

### What if I switch computers?

Just redo:
1. Set environment variables (`USER` + `MEM0_TOKEN`)
2. Clone project (`.mcp.json` comes with git)

No sync needed — all data lives on the server.

### Where should personal preferences go?

**Not in project memory** — it pollutes the shared pool.

Use Claude Code's built-in auto-memory for personal notes. When you say "I prefer 4 spaces", Claude Code writes to `~/.claude/projects/<project>/memory/` — local to your machine only.

### What's the difference from Claude Code's built-in memory?

| | Claude Code auto-memory | Project mem0 |
|---|---|---|
| Scope | You only, this machine | All team members, cross-machine |
| Storage | `~/.claude/projects/.../memory/*.md` | Server-side qdrant vector DB |
| Retrieval | Filename / content match | Semantic similarity |
| Best for | Personal notes, temporary context | Project conventions, shared decisions, gotchas |

**Both coexist** without conflict.

### Can teammates see what I write?

**Yes (by design)**. Same project = shared pool. Each memory carries `user_id` for attribution but doesn't restrict access.

If you don't want teammates to see something, don't put it in project memory — use Claude Code's auto-memory instead.

### Can I delete a memory?

Yes. Ask Claude to `memory:list_memories` to find the ID, then say "delete memory <id>". Or use the web UI to browse and delete.

---

## 7. Observability

After using the service for a while, open `http://your-server:8765/` → Stats:

- **Retrieval quality** (avg top-1 score): are your memories actually useful?
- **Write vs search count**: are you storing or actually using?
- **Revision rate**: is mem0's dedup working?
- **Recent 10**: what's being stored lately?

If avg top-1 score stays < 0.5 after a few weeks, the stored content may not match how you're querying — review your memory strategy.
