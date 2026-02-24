# Installation

## Prerequisites

- **Python 3.12+**
- [**uv**](https://docs.astral.sh/uv/) — fast Python package manager (recommended)

## Clone the Repository

```bash
git clone https://github.com/ralfbecher/orionbelt-semantic-layer.git
cd orionbelt-semantic-layer
```

## Install Dependencies

```bash
uv sync
```

This installs all runtime dependencies:

| Package                  | Purpose                               |
| ------------------------ | ------------------------------------- |
| `fastapi`                | REST API framework                    |
| `uvicorn`                | ASGI server                           |
| `pydantic`               | Model validation (v2)                 |
| `pydantic-settings`      | Configuration from environment / .env |
| `ruamel.yaml`            | YAML parsing with source positions    |
| `networkx`               | Join graph algorithms                 |
| `fastmcp`                | MCP server framework                  |
| `sqlalchemy` + `alembic` | Database persistence                  |
| `structlog`              | Structured logging                    |
| `opentelemetry-api`      | Observability                         |

### Development Dependencies

```bash
uv sync --group dev
```

Adds `pytest`, `ruff`, `mypy`, `httpx`, `pre-commit`, and type stubs.

### Documentation Dependencies

```bash
uv sync --extra docs
```

Adds `mkdocs-material`, `mkdocs-autorefs`, and `mkdocstrings[python]`.

## Verify the Installation

```bash
# Run the test suite
uv run pytest

# Type check
uv run mypy src/

# Lint
uv run ruff check src/
```

## Configuration

OrionBelt reads configuration from environment variables and a `.env` file. Copy the example:

```bash
cp .env.example .env
```

Key settings:

| Variable                   | Default     | Description                            |
| -------------------------- | ----------- | -------------------------------------- |
| `LOG_LEVEL`                | `INFO`      | Logging level                          |
| `API_SERVER_HOST`          | `localhost` | REST API bind host                     |
| `API_SERVER_PORT`          | `8000`      | REST API bind port                     |
| `MCP_TRANSPORT`            | `stdio`     | MCP transport (`stdio`, `http`, `sse`) |
| `MCP_SERVER_HOST`          | `localhost` | MCP server host (http/sse only)        |
| `MCP_SERVER_PORT`          | `9000`      | MCP server port (http/sse only)        |
| `SESSION_TTL_SECONDS`      | `1800`      | Session inactivity timeout (30 min)    |
| `SESSION_CLEANUP_INTERVAL` | `60`        | Cleanup sweep interval (seconds)       |

## Start the Servers

### REST API

```bash
uv run orionbelt-api
# or with reload:
uv run uvicorn orionbelt.api.app:create_app --factory --reload
```

The API is available at:

- `http://127.0.0.1:8000` — API root
- `http://127.0.0.1:8000/docs` — Swagger UI
- `http://127.0.0.1:8000/redoc` — ReDoc
- `http://127.0.0.1:8000/health` — Health check

### MCP Server

```bash
# stdio (default, for Claude Desktop / Cursor)
uv run orionbelt-mcp

# HTTP transport (for multi-client use)
MCP_TRANSPORT=http uv run orionbelt-mcp
```

## Project Structure

```
orionbelt-semantic-layer/
├── src/orionbelt/
│   ├── api/            # FastAPI app, routers, schemas, deps, middleware
│   │   └── routers/    # sessions, validate, query, dialects
│   ├── ast/            # SQL AST nodes, builder, visitor
│   ├── compiler/       # Resolution, planning (star/CFL), codegen pipeline
│   ├── dialect/        # 5 SQL dialect implementations
│   ├── mcp/            # MCP server (10 tools, 3 prompts)
│   ├── models/         # Pydantic models (semantic, query, errors)
│   ├── parser/         # YAML loader, reference resolver, validator
│   ├── service/        # ModelStore, SessionManager
│   └── settings.py     # Shared configuration
├── tests/
│   ├── unit/           # Unit tests for each module
│   ├── integration/    # End-to-end compilation and API tests
│   └── fixtures/       # Sample models and queries
├── examples/           # Model examples and JSON Schema
├── schema/             # OBML JSON Schema
├── docs/               # MkDocs documentation source
├── mkdocs.yml          # MkDocs configuration
└── pyproject.toml      # Project metadata and dependencies
```
