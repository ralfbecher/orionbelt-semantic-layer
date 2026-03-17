# API Overview

OrionBelt exposes a REST API — FastAPI-powered HTTP endpoints for session-based model management, validation, query compilation, and query execution.

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

All API routes are prefixed with `/v1/` except `/health` and `/robots.txt`.

| Prefix | Tag | Description |
|--------|-----|-------------|
| `/v1/sessions` | sessions | Session-scoped model management, validation, query compilation, and execution |
| `/v1/sessions/.../models/.../` | model-discovery | Schema, dimensions, measures, metrics, explain, find, join-graph |
| `/v1/schema`, `/v1/dimensions`, ... | model-discovery | Top-level shortcuts (auto-resolve single session/model) |
| `/v1/convert` | convert | OSI ↔ OBML format conversion with validation |
| `/v1/dialects` | dialects | Available SQL dialect info |
| `/v1/settings` | settings | Public configuration (single-model mode, TTL) |
| `/health` | health | Health check (no prefix) |

### Session-Based Workflow

The primary API workflow uses sessions to manage model state:

1. **Create a session** — `POST /v1/sessions` returns a `session_id`
2. **Load models** — `POST /v1/sessions/{id}/models` with OBML YAML
3. **Compile** — `POST /v1/sessions/{id}/query/sql` to compile OBML to SQL
4. **Execute** — `POST /v1/sessions/{id}/query/execute` to compile and execute against the database (requires `FLIGHT_ENABLED=true`)
5. **Close** — `DELETE /v1/sessions/{id}` when done (or let TTL expire)

Sessions automatically expire after 30 minutes of inactivity (configurable via `SESSION_TTL_SECONDS`).

### Single-Model Mode

When the `MODEL_FILE` environment variable is set, the server runs in **single-model mode**:

1. **Create a session** — `POST /v1/sessions` returns a `session_id` with the model already loaded (`model_count: 1`)
2. **List the model** — `GET /v1/sessions/{id}/models` to get the pre-loaded `model_id`
3. **Compile** — `POST /v1/sessions/{id}/query/sql` to compile OBML to SQL
4. **Execute** — `POST /v1/sessions/{id}/query/execute` to compile and execute (requires `FLIGHT_ENABLED=true`)
4. **Close** — `DELETE /v1/sessions/{id}` when done (or let TTL expire)

Model upload (`POST /v1/sessions/{id}/models`) and removal (`DELETE /v1/sessions/{id}/models/{mid}`) return **403 Forbidden** in this mode. All other endpoints work normally.

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
| 403 | Forbidden | Model upload/removal in single-model mode |
| 404 | Not Found | Session expired/missing, model not found |
| 422 | Unprocessable Entity | Model validation failure, resolution error |
| 502 | Bad Gateway | Database execution failed (query/execute) |
| 503 | Service Unavailable | Query execution not available (FLIGHT_ENABLED not set) |

## Middleware

### Request Timing

The `RequestTimingMiddleware` adds `X-Request-Duration-Ms` headers for performance monitoring.

## Interactive Documentation

When the server is running, interactive API docs are available at:

- **Swagger UI**: `http://127.0.0.1:8000/docs`
- **ReDoc**: `http://127.0.0.1:8000/redoc`

Both are auto-generated from the FastAPI route definitions and Pydantic schemas.
