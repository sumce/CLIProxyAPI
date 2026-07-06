# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Read [AGENTS.md](./AGENTS.md) first — it contains the build commands, architecture overview, and code conventions. This file supplements it with fork-specific and DevEco-related details.

## Fork Context

- **Fork of**: https://github.com/router-for-me/CLIProxyAPI (upstream)
- **Own fork**: https://github.com/sumce/CLIProxyAPI
- **Added**: Huawei DevEco MaaS API support (GLM-5.1, GLM-5 reasoning)

## DevEco Architecture

The DevEco executor (`internal/runtime/executor/deveco_executor.go`) follows the standard executor pipeline:

```
Client (OpenAI format)
  → TranslateRequest(from, "openai")
  → thinking.ApplyThinking()
  → inject thinking.type: "enabled" (Huawei-specific)
  → ApplyPayloadConfigWithRequest()
  → DevEco MaaS API (/chat/completions)
  → TranslateNonStream (response back to client format)
```

### DevEco Key Behaviors

- **Non-streaming uses streaming internally**: `Execute()` sends `stream: true` and aggregates SSE deltas via `aggregatedDelta`. This avoids Huawei's slow `/no-stream/chat/completions` endpoint (2s–90s latency).
- **thinking.type injection**: Huawei requires BOTH `reasoning_effort` AND `thinking.type: "enabled"` to return reasoning_content. The injection happens in `prepareStreamRequest()`.
- **Model ID case**: DevEco API returns uppercase model IDs (`GLM-5.1`). The dynamic model fetch preserves API case. Fallback defaults use uppercase.
- **Chat-Id stability**: Cached per `Session-Id` via `devecoSessionChatID sync.Map` (matches deveco-code IDE behavior).
- **x-deveco-* header forwarding**: If the client sends `x-deveco-session`, `x-deveco-request`, `x-deveco-client`, or `x-deveco-project`, they are forwarded to the DevEco upstream.
- **Auto-refresh**: Token refresh uses `jwt_token` from auth metadata with a `lastRefreshFailedAt` 30s cooldown (matching deveco-code's pattern).

### DevEco Config

```yaml
deveco:
  - enabled: true
    # No extra config needed — auth is in auths/deveco-*.json
```

Auth file format (`auths/deveco-*.json`):
```json
{
  "access_token": "...",
  "auth_kind": "oauth",
  "metadata": {
    "jwt_token": "...",
    "refresh_token": "...",
    "expires_at": 1700000000
  }
}
```

## Management UI

- URL: `http://localhost:8317/management.html`
- Auth: Set `MANAGEMENT_PASSWORD=xxx` env var or `remote_management.secret_key` in config
- DevEco endpoint: `GET /v0/management/deveco`
- Auth-files endpoint: `GET /v0/management/auth-files`

## Build Workflow

- **Push to main**: `.github/workflows/build.yml` builds Windows/Linux/macOS binaries as artifacts
- **Tag push** (e.g. `v1.0.0`): `.github/workflows/release.yaml` builds all platforms + publishes GitHub Release
- `config.yaml` is local-only; use `config.example.yaml` as template

## Key Files for DevEco

| File | Purpose |
|------|---------|
| `internal/runtime/executor/deveco_executor.go` | Main executor (Execute/ExecuteStream/Refresh/injectHeaders) |
| `internal/auth/deveco/deveco.go` | DevEco token refresh + cooldown |
| `internal/registry/model_definitions.go` | `GetDevecoModels()` — default model definitions |
| `internal/thinking/apply.go` | Thinking pipeline — handles reasoning_effort |
| `.github/workflows/build.yml` | CI build on push |
