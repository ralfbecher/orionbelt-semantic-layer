# API Overview

OrionBelt exposes a REST API — FastAPI-powered HTTP endpoints for session-based model management, validation, and query compilation.

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
| `/convert` | convert | OSI ↔ OBML format conversion with validation |
| `/dialects` | dialects | Available SQL dialect info |
| `/settings` | settings | Public configuration (single-model mode, TTL) |
| `/health` | health | Health check |

### Session-Based Workflow

The primary API workflow uses sessions to manage model state:

1. **Create a session** — `POST /sessions` returns a `session_id`
2. **Load models** — `POST /sessions/{id}/models` with OBML YAML
3. **Query** — `POST /sessions/{id}/query/sql` against loaded models
4. **Close** — `DELETE /sessions/{id}` when done (or let TTL expire)

Sessions automatically expire after 30 minutes of inactivity (configurable via `SESSION_TTL_SECONDS`).

### Single-Model Mode

When the `MODEL_FILE` environment variable is set, the server runs in **single-model mode**:

1. **Create a session** — `POST /sessions` returns a `session_id` with the model already loaded (`model_count: 1`)
2. **List the model** — `GET /sessions/{id}/models` to get the pre-loaded `model_id`
3. **Query** — `POST /sessions/{id}/query/sql` against the pre-loaded model
4. **Close** — `DELETE /sessions/{id}` when done (or let TTL expire)

Model upload (`POST /sessions/{id}/models`) and removal (`DELETE /sessions/{id}/models/{mid}`) return **403 Forbidden** in this mode. All other endpoints work normally.

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

## Middleware

### Request Timing

The `RequestTimingMiddleware` adds `X-Request-Duration-Ms` headers for performance monitoring.

## Interactive Documentation

When the server is running, interactive API docs are available at:

- **Swagger UI**: `http://127.0.0.1:8000/docs`
- **ReDoc**: `http://127.0.0.1:8000/redoc`

Both are auto-generated from the FastAPI route definitions and Pydantic schemas.
