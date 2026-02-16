# API Overview

OrionBelt exposes two server interfaces:

1. **REST API** — FastAPI-powered HTTP endpoints for session-based model management, validation, and query compilation
2. **MCP Server** — Model Context Protocol server with 9 tools and 3 prompts for AI assistant integration

## REST API

### Base URL

```
http://127.0.0.1:8000
```

### Content Type

All endpoints accept and return JSON:

```
Content-Type: application/json
```

### Routers

| Prefix | Tag | Description |
|--------|-----|-------------|
| `/sessions` | sessions | Session-scoped model management, validation, and query compilation |
| `/dialects` | dialects | Available SQL dialect info |
| `/health` | health | Health check |

### Session-Based Workflow

The primary API workflow uses sessions to manage model state:

1. **Create a session** — `POST /sessions` returns a `session_id`
2. **Load models** — `POST /sessions/{id}/models` with OBML YAML
3. **Query** — `POST /sessions/{id}/query/sql` against loaded models
4. **Close** — `DELETE /sessions/{id}` when done (or let TTL expire)

Sessions automatically expire after 30 minutes of inactivity (configurable via `SESSION_TTL_SECONDS`).

## MCP Server

The MCP server exposes OrionBelt as tools for AI assistants (Claude Desktop, Cursor, etc.):

| Category | Tools |
|----------|-------|
| Session | `create_session`, `close_session`, `list_sessions` |
| Model | `load_model`, `validate_model`, `describe_model`, `list_models` |
| Query | `compile_query` |
| Info | `list_dialects` |

See [MCP Server](mcp.md) for the full tool and prompt reference.

## Error Responses

All errors follow a consistent format:

```json
{
  "detail": "Session 'abc123' not found"
}
```

### Status Codes

| Code | Meaning | When |
|------|---------|------|
| 200 | OK | Successful GET or compilation |
| 201 | Created | Session or model created (POST) |
| 204 | No Content | Session or model deleted |
| 400 | Bad Request | Invalid YAML, unknown dialect, bad operator |
| 404 | Not Found | Session expired/missing, model not found |
| 422 | Unprocessable Entity | Model validation failure, resolution error |

## Middleware

### Request Timing

The `RequestTimingMiddleware` adds `X-Request-Duration-Ms` headers for performance monitoring.

## Interactive Documentation

When the server is running, interactive API docs are available at:

- **Swagger UI**: `http://127.0.0.1:8000/docs`
- **ReDoc**: `http://127.0.0.1:8000/redoc`

Both are auto-generated from the FastAPI route definitions and Pydantic schemas.
