# API Endpoints

Complete reference for all OrionBelt REST API endpoints.

## Health Check

### `GET /health`

Returns the service status and version.

**Response:**

```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

---

## Sessions

### `POST /sessions`

Create a new session. Each session has its own model store.

**Request (optional):**

```json
{
  "metadata": {
    "user": "alice",
    "purpose": "revenue analysis"
  }
}
```

**Response (201):**

```json
{
  "session_id": "a1b2c3d4e5f6",
  "created_at": "2025-01-15T10:30:00Z",
  "last_accessed_at": "2025-01-15T10:30:00Z",
  "model_count": 0,
  "metadata": {
    "user": "alice",
    "purpose": "revenue analysis"
  }
}
```

### `GET /sessions`

List all active sessions.

**Response (200):**

```json
{
  "sessions": [
    {
      "session_id": "a1b2c3d4e5f6",
      "created_at": "2025-01-15T10:30:00Z",
      "last_accessed_at": "2025-01-15T10:35:00Z",
      "model_count": 2,
      "metadata": {}
    }
  ]
}
```

### `GET /sessions/{session_id}`

Get info for a specific session. Also refreshes the session's last-accessed time.

**Response (200):** Same as single session in list response.

**Error (404):** Session not found or expired.

### `DELETE /sessions/{session_id}`

Close a session and release its resources.

**Response (204):** No content.

**Error (404):** Session not found.

---

## Session Models

### `POST /sessions/{session_id}/models`

Load an OBML semantic model into a session. The model is parsed, validated, and stored.

!!! note "Single-model mode"
    Returns **403 Forbidden** when `MODEL_FILE` is configured. The model is pre-loaded automatically.

**Request:**

```json
{
  "model_yaml": "version: 1.0\ndataObjects:\n  Orders:\n    code: ORDERS\n    ..."
}
```

**Response (201):**

```json
{
  "model_id": "abcd1234",
  "data_objects": 2,
  "dimensions": 3,
  "measures": 2,
  "metrics": 1,
  "warnings": []
}
```

**Error (403):** Single-model mode: model upload is disabled.

**Error (422):** Model has validation errors.

**Error (404):** Session not found.

### `GET /sessions/{session_id}/models`

List all models loaded in a session.

**Response (200):**

```json
[
  {
    "model_id": "abcd1234",
    "data_objects": 2,
    "dimensions": 3,
    "measures": 2,
    "metrics": 1
  }
]
```

### `GET /sessions/{session_id}/models/{model_id}`

Describe a model's contents — data objects (with fields and joins), dimensions, measures, and metrics.

**Response (200):**

```json
{
  "model_id": "abcd1234",
  "data_objects": [
    {
      "label": "Orders",
      "code": "WAREHOUSE.PUBLIC.ORDERS",
      "columns": ["Order ID", "Price", "Quantity"],
      "join_targets": ["Customers"]
    }
  ],
  "dimensions": [
    {
      "name": "Country",
      "result_type": "string",
      "data_object": "Customers",
      "column": "Country",
      "time_grain": null
    }
  ],
  "measures": [...],
  "metrics": [...]
}
```

**Error (404):** Model or session not found.

### `DELETE /sessions/{session_id}/models/{model_id}`

Remove a model from a session.

!!! note "Single-model mode"
    Returns **403 Forbidden** when `MODEL_FILE` is configured.

**Response (204):** No content.

**Error (403):** Single-model mode: model removal is disabled.

**Error (404):** Model or session not found.

---

## Session Validation

### `POST /sessions/{session_id}/validate`

Validate OBML YAML within a session context. Does not store the model.

**Request:**

```json
{
  "model_yaml": "version: 1.0\ndataObjects:\n  ..."
}
```

**Response (200):**

```json
{
  "valid": true,
  "errors": [],
  "warnings": []
}
```

**Validation failure:**

```json
{
  "valid": false,
  "errors": [
    {
      "code": "UNKNOWN_DATA_OBJECT",
      "message": "Data object 'Unknown' not found",
      "path": "dimensions.Bad.dataObject"
    }
  ],
  "warnings": []
}
```

---

## Session Query Compilation

### `POST /sessions/{session_id}/query/sql`

Compile a semantic query against a model loaded in the session.

**Request:**

```json
{
  "model_id": "abcd1234",
  "query": {
    "select": {
      "dimensions": ["Customer Country"],
      "measures": ["Revenue"]
    },
    "where": [
      {
        "field": "Customer Segment",
        "op": "in",
        "value": ["SMB", "MidMarket"]
      }
    ],
    "order_by": [
      { "field": "Revenue", "direction": "desc" }
    ],
    "limit": 1000
  },
  "dialect": "postgres"
}
```

**Response (200):**

```json
{
  "sql": "SELECT ...",
  "dialect": "postgres",
  "resolved": {
    "fact_tables": ["Orders"],
    "dimensions": ["Customer Country"],
    "measures": ["Revenue"]
  },
  "warnings": []
}
```

**Error responses:**

| Status | Cause |
|--------|-------|
| 400 | Unsupported dialect |
| 404 | Model or session not found |
| 422 | Resolution error |

---

## OSI ↔ OBML Conversion

Stateless endpoints for converting between [OSI (Open Semantic Interchange)](https://github.com/open-semantic-interchange/OSI) and OBML formats. No session required.

### `POST /convert/osi-to-obml`

Convert an OSI YAML model to OBML format.

**Request:**

```json
{
  "input_yaml": "version: \"0.1.1\"\nsemantic_model:\n  - name: my_model\n    ..."
}
```

**Response (200):**

```json
{
  "output_yaml": "version: 1.0\ndataObjects:\n  ...",
  "warnings": [
    "Relationship 'sales_to_date': no type specified, defaulting to many-to-one."
  ],
  "validation": {
    "schema_valid": true,
    "semantic_valid": true,
    "schema_errors": [],
    "semantic_errors": [],
    "semantic_warnings": []
  }
}
```

**Error (400):** Invalid YAML input.

**Error (422):** Conversion failed (e.g. unsupported OSI structure).

### `POST /convert/obml-to-osi`

Convert an OBML YAML model to OSI format.

**Request:**

```json
{
  "input_yaml": "version: 1.0\ndataObjects:\n  ...",
  "model_name": "my_model",
  "model_description": "Sales analytics model",
  "ai_instructions": ""
}
```

The `model_name`, `model_description`, and `ai_instructions` fields are optional (defaults: `"semantic_model"`, `""`, `""`).

**Response (200):** Same structure as `POST /convert/osi-to-obml`.

**Error (400):** Invalid YAML input.

**Error (422):** Conversion failed.

---

## Settings

### `GET /settings`

Return public configuration for API clients (UI, MCP, etc.).

**Response (200):**

```json
{
  "single_model_mode": false,
  "model_yaml": null,
  "session_ttl_seconds": 1800
}
```

When `MODEL_FILE` is configured:

```json
{
  "single_model_mode": true,
  "model_yaml": "version: 1.0\ndataObjects:\n  ...",
  "session_ttl_seconds": 1800
}
```

| Field | Type | Description |
|-------|------|-------------|
| `single_model_mode` | bool | Whether model upload/removal is disabled |
| `model_yaml` | string \| null | Pre-loaded OBML YAML (only when single-model mode is active) |
| `session_ttl_seconds` | int | Session inactivity timeout |

---

## Dialects

### `GET /dialects`

List all available SQL dialects and their capability flags.

**Response (200):**

```json
{
  "dialects": [
    {
      "name": "bigquery",
      "capabilities": {
        "supports_cte": true,
        "supports_qualify": true,
        "supports_arrays": true,
        "supports_window_filters": true,
        "supports_ilike": false,
        "supports_time_travel": false,
        "supports_semi_structured": true
      }
    },
    { "name": "clickhouse", "capabilities": { "..." : true } },
    { "name": "databricks", "capabilities": { "..." : true } },
    { "name": "dremio", "capabilities": { "..." : true } },
    {
      "name": "duckdb",
      "capabilities": {
        "supports_cte": true,
        "supports_qualify": true,
        "supports_arrays": true,
        "supports_window_filters": true,
        "supports_ilike": true,
        "supports_time_travel": false,
        "supports_semi_structured": false
      }
    },
    { "name": "postgres", "capabilities": { "..." : true } },
    { "name": "snowflake", "capabilities": { "..." : true } }
  ]
}
```
