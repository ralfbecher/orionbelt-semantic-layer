# MCP Server

OrionBelt includes a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes the semantic layer as tools for AI assistants like Claude Desktop, Cursor, and other MCP-compatible clients.

## Starting the Server

=== "stdio (default)"

    ```bash
    uv run orionbelt-mcp
    ```

    Used by Claude Desktop and Cursor. Single-user, default session created automatically.

=== "HTTP"

    ```bash
    MCP_TRANSPORT="http" uv run orionbelt-mcp
    ```

    Streamable HTTP on port 9000. Multi-client — each client creates its own session.

=== "SSE (legacy)"

    ```bash
    MCP_TRANSPORT="sse" uv run orionbelt-mcp
    ```

    Server-Sent Events on port 9000. Legacy transport, prefer HTTP for new integrations.

## Configuration

The MCP server reads from the same `.env` file as the REST API:

| Variable                   | Default     | Description                             |
| -------------------------- | ----------- | --------------------------------------- |
| `MCP_TRANSPORT`            | `stdio`     | Transport mode (`stdio`, `http`, `sse`) |
| `MCP_SERVER_HOST`          | `localhost` | Bind host (http/sse only)               |
| `MCP_SERVER_PORT`          | `9000`      | Bind port (http/sse only)               |
| `SESSION_TTL_SECONDS`      | `1800`      | Session inactivity timeout              |
| `SESSION_CLEANUP_INTERVAL` | `60`        | Cleanup sweep interval                  |

## Session Model

In **stdio mode** (single-user), a default session is created automatically. All tools work without passing a `session_id`.

In **HTTP/SSE mode** (multi-client), clients must create sessions explicitly using `create_session` and pass the `session_id` to subsequent tool calls.

## Tools (9)

### Session Management

#### `create_session`

Create a new session with its own model store.

| Parameter       | Type   | Required | Description                               |
| --------------- | ------ | -------- | ----------------------------------------- |
| `metadata_json` | string | No       | JSON object with metadata key-value pairs |

**Returns:** Session ID and creation timestamp.

#### `close_session`

Close a session and release its resources.

| Parameter    | Type   | Required | Description      |
| ------------ | ------ | -------- | ---------------- |
| `session_id` | string | Yes      | Session to close |

#### `list_sessions`

List all active sessions with their model counts and last-accessed times.

### Model Management

#### `load_model`

Parse, validate, and store an OBML semantic model. Returns a `model_id` for use with other tools.

| Parameter    | Type   | Required | Description                             |
| ------------ | ------ | -------- | --------------------------------------- |
| `model_yaml` | string | Yes      | Complete OBML YAML content              |
| `session_id` | string | No       | Target session (optional in stdio mode) |

#### `validate_model`

Validate an OBML model without storing it. Returns structured errors and warnings.

| Parameter    | Type   | Required | Description                              |
| ------------ | ------ | -------- | ---------------------------------------- |
| `model_yaml` | string | Yes      | Complete OBML YAML content               |
| `session_id` | string | No       | Session context (optional in stdio mode) |

#### `describe_model`

Show the contents of a loaded model: data objects, columns, joins, dimensions, measures, and metrics.

| Parameter    | Type   | Required | Description                                        |
| ------------ | ------ | -------- | -------------------------------------------------- |
| `model_id`   | string | Yes      | ID returned by `load_model`                        |
| `session_id` | string | No       | Session holding the model (optional in stdio mode) |

#### `list_models`

List all models loaded in a session.

| Parameter    | Type   | Required | Description                              |
| ------------ | ------ | -------- | ---------------------------------------- |
| `session_id` | string | No       | Session to list (optional in stdio mode) |

### Query Compilation

#### `compile_query`

Compile a semantic query to SQL. Supports two modes:

**Simple mode** — pass dimension and measure names directly:

```
compile_query(model_id="abc12345", dimensions=["Country"], measures=["Revenue"])
```

**Full mode** — pass a complete query as JSON:

```
compile_query(model_id="abc12345", query_json='{"select": {"dimensions": ["Country"], "measures": ["Revenue"]}, "limit": 10}')
```

| Parameter    | Type         | Required | Description                                        |
| ------------ | ------------ | -------- | -------------------------------------------------- |
| `model_id`       | string             | Yes      | ID returned by `load_model`                                                     |
| `dialect`        | string             | No       | Target dialect (default: `postgres`)                                            |
| `dimensions`     | list[string]       | No       | Dimension names (simple mode)                                                   |
| `measures`       | list[string]       | No       | Measure names (simple mode)                                                     |
| `query_json`     | string             | No       | Full query as JSON (full mode)                                                  |
| `session_id`     | string             | No       | Session holding the model (optional in stdio mode)                              |
| `use_path_names` | list[object]       | No       | Secondary join overrides (simple mode): `[{source, target, pathName}]` |

### Information

#### `list_dialects`

List available SQL dialects and their capabilities. Stateless — no session required.

## Prompts (3)

### `write_obml_model`

OBML syntax reference — how to write a semantic model in YAML. Includes the complete format specification, key rules, and a recommended workflow.

### `write_query`

How to use the `compile_query` tool — covers simple mode, full mode with filters/ordering/limits, filter operators, and tips.

### `debug_validation`

All OBML validation error codes with causes and fixes. Organized by category: parse errors, reference errors, semantic errors, and resolution errors.

## Claude Desktop Integration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "orionbelt-semantic-layer": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/path/to/orionbelt-semantic-layer",
        "orionbelt-mcp"
      ]
    }
  }
}
```

### Typical Workflow

1. Ask Claude to use the `write_obml_model` prompt for OBML syntax reference
2. Compose a model YAML interactively
3. Use `validate_model` to check for errors
4. Use `load_model` to load the validated model
5. Use `describe_model` to explore its contents
6. Use `compile_query` to generate SQL for different dialects
