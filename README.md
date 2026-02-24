<!-- mcp-name: io.github.ralfbecher/orionbelt-semantic-layer -->
<p align="center">
  <img src="docs/assets/ORIONBELT Logo.png" alt="OrionBelt Logo" width="400">
</p>

<h1 align="center">OrionBelt Semantic Layer</h1>

<p align="center"><strong>Compile YAML semantic models into analytical SQL across multiple database dialects</strong></p>

[![Version 0.5.0](https://img.shields.io/badge/version-0.5.0-purple.svg)](https://github.com/ralfbecher/orionbelt-semantic-layer/releases)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/ralfbecher/orionbelt-semantic-layer/blob/main/LICENSE)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.128+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Pydantic v2](https://img.shields.io/badge/Pydantic-v2-E92063.svg?logo=pydantic&logoColor=white)](https://docs.pydantic.dev)
[![Gradio](https://img.shields.io/badge/Gradio-5.0+-F97316.svg?logo=gradio&logoColor=white)](https://www.gradio.app)
[![FastMCP](https://img.shields.io/badge/FastMCP-2.14+-8A2BE2)](https://gofastmcp.com)
[![sqlglot](https://img.shields.io/badge/sqlglot-26.0+-4B8BBE.svg)](https://github.com/tobymao/sqlglot)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED.svg?logo=docker&logoColor=white)](https://docs.docker.com)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://docs.astral.sh/ruff/)
[![mypy](https://img.shields.io/badge/type--checked-mypy-blue.svg)](https://mypy-lang.org)

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8.svg?logo=snowflake&logoColor=white)](https://www.snowflake.com)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01.svg?logo=clickhouse&logoColor=black)](https://clickhouse.com)
[![Dremio](https://img.shields.io/badge/Dremio-31B48D.svg)](https://www.dremio.com)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621.svg?logo=databricks&logoColor=white)](https://www.databricks.com)

OrionBelt Semantic Layer is an **API-first** engine that transforms declarative YAML model definitions into optimized SQL for Postgres, Snowflake, ClickHouse, Dremio, and Databricks. It provides a unified abstraction over your data warehouse, so analysts and applications can query using business concepts (dimensions, measures, metrics) instead of raw SQL. Every capability — model loading, validation, query compilation, and diagram generation — is exposed through a REST API and an MCP server, making OrionBelt easy to integrate into any application, workflow, or AI assistant.

## Features

- **5 SQL Dialects** — Postgres, Snowflake, ClickHouse, Dremio, Databricks SQL with dialect-specific optimizations
- **AST-Based SQL Generation** — Custom SQL AST ensures correct, injection-safe SQL (no string concatenation)
- **OrionBelt ML (OBML)** — YAML-based semantic models with data objects, dimensions, measures, metrics, and joins
- **Star Schema & CFL Planning** — Automatic join path resolution with Composite Fact Layer support for multi-fact queries
- **Vendor-Specific SQL Validation** — Post-generation syntax validation via sqlglot for each target dialect (non-blocking)
- **Validation with Source Positions** — Precise error reporting with line/column numbers from YAML source, including join graph analysis (cycle and multipath detection, secondary join constraints)
- **Session Management** — TTL-scoped sessions with per-client model stores for both REST API and MCP
- **ER Diagram Generation** — Mermaid ER diagrams via API and Gradio UI with theme support, zoom, and secondary join visualization
- **REST API** — FastAPI-powered session endpoints for model loading, validation, compilation, diagram generation, and management
- **MCP Server** — 10 tools + 3 prompts for AI-assisted model development via Claude Desktop and other MCP clients
- **Gradio UI** — Interactive web interface for model editing, query testing, and SQL compilation with live validation feedback
- **[OSI](https://github.com/open-semantic-interchange/OSI) Interoperability** — Bidirectional import/export between OBML and the Open Semantic Interchange format, with validation for both directions
- **Plugin Architecture** — Extensible dialect system with capability flags and registry

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

```bash
git clone https://github.com/ralfbecher/orionbelt-semantic-layer.git
cd orionbelt-semantic-layer
uv sync
```

### Run Tests

```bash
uv run pytest
```

### Start the REST API Server

```bash
uv run orionbelt-api
# or with reload:
uv run uvicorn orionbelt.api.app:create_app --factory --reload
```

The API is available at `http://127.0.0.1:8000`. Interactive docs at `/docs` (Swagger UI) and `/redoc`.

### Start the MCP Server

```bash
# stdio (default, for Claude Desktop / Cursor)
uv run orionbelt-mcp

# HTTP transport (for multi-client use)
MCP_TRANSPORT=http uv run orionbelt-mcp
```

## Example

### Define a Semantic Model

```yaml
# yaml-language-server: $schema=schema/obml-schema.json
version: 1.0

dataObjects:
  Customers:
    code: CUSTOMERS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Customer ID:
        code: CUSTOMER_ID
        abstractType: string
      Country:
        code: COUNTRY
        abstractType: string

  Orders:
    code: ORDERS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Order ID:
        code: ORDER_ID
        abstractType: string
      Order Customer ID:
        code: CUSTOMER_ID
        abstractType: string
      Price:
        code: PRICE
        abstractType: float
      Quantity:
        code: QUANTITY
        abstractType: int
    joins:
      - joinType: many-to-one
        joinTo: Customers
        columnsFrom:
          - Order Customer ID
        columnsTo:
          - Customer ID

dimensions:
  Country:
    dataObject: Customers
    column: Country
    resultType: string

measures:
  Revenue:
    resultType: float
    aggregation: sum
    expression: "{[Orders].[Price]} * {[Orders].[Quantity]}"
```

The `yaml-language-server` comment enables schema validation in editors that support it (VS Code with YAML extension, IntelliJ, etc.). The JSON Schema is at [`schema/obml-schema.json`](schema/obml-schema.json).

### Define a Query

Queries select dimensions and measures by their business names:

```yaml
select:
  dimensions:
    - Country
  measures:
    - Revenue
limit: 100
```

### Compile to SQL (Python)

```python
from orionbelt.compiler.pipeline import CompilationPipeline
from orionbelt.models.query import QueryObject, QuerySelect
from orionbelt.parser.loader import TrackedLoader
from orionbelt.parser.resolver import ReferenceResolver

# Load and parse the model
loader = TrackedLoader()
raw, source_map = loader.load("model.yaml")
model, result = ReferenceResolver().resolve(raw, source_map)

# Define a query
query = QueryObject(
    select=QuerySelect(
        dimensions=["Country"],
        measures=["Revenue"],
    ),
    limit=100,
)

# Compile to SQL
pipeline = CompilationPipeline()
result = pipeline.compile(query, model, "postgres")
print(result.sql)
```

**Generated SQL (Postgres):**

```sql
SELECT
  "Customers"."COUNTRY" AS "Country",
  SUM("Orders"."PRICE" * "Orders"."QUANTITY") AS "Revenue"
FROM WAREHOUSE.PUBLIC.ORDERS AS "Orders"
LEFT JOIN WAREHOUSE.PUBLIC.CUSTOMERS AS "Customers"
  ON "Orders"."CUSTOMER_ID" = "Customers"."CUSTOMER_ID"
GROUP BY "Customers"."COUNTRY"
LIMIT 100
```

Change the dialect to `"snowflake"`, `"clickhouse"`, `"dremio"`, or `"databricks"` to get dialect-specific SQL.

### Use the REST API with Sessions

```bash
# Start the server
uv run orionbelt-api

# Create a session
curl -s -X POST http://127.0.0.1:8000/sessions | jq
# → {"session_id": "a1b2c3d4e5f6", "model_count": 0, ...}

# Load a model into the session
curl -s -X POST http://127.0.0.1:8000/sessions/a1b2c3d4e5f6/models \
  -H "Content-Type: application/json" \
  -d '{"model_yaml": "version: 1.0\ndataObjects:\n  ..."}' | jq
# → {"model_id": "abcd1234", "data_objects": 2, ...}

# Compile a query
curl -s -X POST http://127.0.0.1:8000/sessions/a1b2c3d4e5f6/query/sql \
  -H "Content-Type: application/json" \
  -d '{
    "model_id": "abcd1234",
    "query": {"select": {"dimensions": ["Country"], "measures": ["Revenue"]}},
    "dialect": "postgres"
  }' | jq .sql
```

### Use with Claude Desktop (MCP)

Add to your Claude Desktop config (`claude_desktop_config.json`):

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

Then ask Claude to load a model, validate it, and compile queries interactively.

## Architecture

```
YAML Model          Query Object
    |                    |
    v                    v
 ┌───────────┐    ┌──────────────┐
 │  Parser   │    │  Resolution  │  ← Phase 1: resolve refs, select fact table,
 │  (ruamel) │    │              │    find join paths, classify filters
 └────┬──────┘    └──────┬───────┘
      │                  │
      v                  v
 SemanticModel    ResolvedQuery
      │                  │
      │    ┌─────────────┘
      │    │
      v    v
 ┌───────────────┐
 │   Planner     │  ← Phase 2: Star Schema or CFL (multi-fact)
 │  (star / cfl) │    builds SQL AST with joins, grouping, CTEs
 └───────┬───────┘
         │
         v
    SQL AST (Select, Join, Expr...)
         │
         v
 ┌───────────────┐
 │   Codegen     │  ← Phase 3: dialect renders AST to SQL string
 │  (dialect)    │    handles quoting, time grains, functions
 └───────┬───────┘
         │
         v
    SQL String (dialect-specific)
```

## MCP Server

The MCP server exposes OrionBelt as tools for AI assistants (Claude Desktop, Cursor, etc.):

**Reference** (1): `get_obml_reference`
**Session tools** (3): `create_session`, `close_session`, `list_sessions`
**Model tools** (5): `load_model`, `validate_model`, `describe_model`, `compile_query`, `list_models`
**Stateless** (1): `list_dialects`
**Prompts** (3): `write_obml_model`, `write_query`, `debug_validation`

In stdio mode (default), a shared default session is used automatically. In HTTP/SSE mode, clients must create sessions explicitly.

## Gradio UI

OrionBelt includes an interactive web UI built with [Gradio](https://www.gradio.app/) for exploring and testing the compilation pipeline visually.

### Local Development

For local development, the Gradio UI is automatically mounted at `/ui` on the REST API server when the `ui` extra is installed:

```bash
uv sync --extra ui
uv run orionbelt-api
# → API at http://localhost:8000
# → UI  at http://localhost:8000/ui
```

### Standalone Mode

The UI can also run as a separate process, connecting to the API via `API_BASE_URL`:

```bash
uv sync --extra ui

# Start the REST API (required backend)
uv run orionbelt-api &

# Launch the Gradio UI (standalone on port 7860)
API_BASE_URL=http://localhost:8000 uv run orionbelt-ui
```

### Production (Cloud Run)

In production, the API and UI are deployed as **separate Cloud Run services** behind a shared load balancer. The API image (`Dockerfile`) excludes Gradio for faster cold starts (~2-3s vs ~12s), while the UI image (`Dockerfile.ui`) connects to the API via `API_BASE_URL`:

```
Load Balancer (single IP)
  ├── /ui/*     → orionbelt-ui   (Gradio)
  └── /*        → orionbelt-api  (FastAPI)
```

<p align="center">
  <img src="docs/assets/ui-sqlcompiler-dark.png" alt="SQL Compiler in Gradio UI (dark mode)" width="900">
</p>

The UI provides:

- **Side-by-side editors** — OBML model (YAML) and query (YAML) with syntax highlighting
- **Dialect selector** — Switch between Postgres, Snowflake, ClickHouse, Dremio, and Databricks
- **One-click compilation** — Compile button generates formatted SQL output
- **SQL validation feedback** — Warnings and validation errors from sqlglot are displayed as comments above the generated SQL
- **ER Diagram tab** — Visualize the semantic model as a Mermaid ER diagram with left-to-right layout, FK annotations, dotted lines for secondary joins, and an adjustable zoom slider
- **OSI Import / Export** — Import OSI format models (converted to OBML) and export OBML models to OSI format, with validation feedback
- **Dark / light mode** — Toggle via the header button; all inputs and UI state are persisted across mode switches

The bundled example model (`examples/sem-layer.obml.yml`) is loaded automatically on startup.

<p align="center">
  <img src="docs/assets/ui-er-diagram-dark.png" alt="ER Diagram in Gradio UI (dark mode)" width="900">
</p>

The ER diagram is also available via the REST API:

```bash
# Generate Mermaid ER diagram for a loaded model
curl -s "http://127.0.0.1:8000/sessions/{session_id}/models/{model_id}/diagram/er?theme=default" | jq .mermaid
```

## Docker

### Build and Run

Two separate images — API-only (fast) and UI (with Gradio):

```bash
# API image (no Gradio, fast cold starts)
docker build -t orionbelt-api .
docker run -p 8080:8080 orionbelt-api

# UI image (Gradio, connects to API)
docker build -f Dockerfile.ui -t orionbelt-ui .
docker run -p 7860:7860 \
  -e API_BASE_URL=http://host.docker.internal:8080 \
  orionbelt-ui
```

The API is available at `http://localhost:8080`. The UI is at `http://localhost:7860`. Sessions are ephemeral (in-memory, lost on container restart).

### Deploy to Google Cloud Run

The deploy script builds and pushes both images, then deploys them as separate Cloud Run services:

```bash
./scripts/deploy-gcloud.sh
```

The API and UI services share a single IP via a Google Cloud Application Load Balancer with path-based routing. Cloud Armor provides WAF protection.

A public demo is available at:

> **[http://35.187.174.102/ui](http://35.187.174.102/ui/?__theme=dark)**

API endpoint: `http://35.187.174.102` — Interactive docs: [Swagger UI](http://35.187.174.102/docs) | [ReDoc](http://35.187.174.102/redoc)

### Run Integration Tests

```bash
# Build image and run 15 endpoint tests
./tests/docker/test_docker.sh

# Skip build (use existing image)
./tests/docker/test_docker.sh --no-build

# Run 30 tests against a live Cloud Run deployment
./tests/cloudrun/test_cloudrun.sh https://orionbelt-semantic-layer-mw2bqg2mva-ew.a.run.app
```

## Configuration

Configuration is via environment variables or a `.env` file. See `.env.example` for all options:

| Variable                   | Default     | Description                            |
| -------------------------- | ----------- | -------------------------------------- |
| `LOG_LEVEL`                | `INFO`      | Logging level                          |
| `API_SERVER_HOST`          | `localhost` | REST API bind host                     |
| `API_SERVER_PORT`          | `8000`      | REST API bind port                     |
| `PORT`                     | —           | Override port (Cloud Run sets this)    |
| `DISABLE_SESSION_LIST`     | `false`     | Disable `GET /sessions` endpoint       |
| `MCP_TRANSPORT`            | `stdio`     | MCP transport (`stdio`, `http`, `sse`) |
| `MCP_SERVER_HOST`          | `localhost` | MCP server host (http/sse only)        |
| `MCP_SERVER_PORT`          | `9000`      | MCP server port (http/sse only)        |
| `SESSION_TTL_SECONDS`      | `1800`      | Session inactivity timeout (30 min)    |
| `SESSION_CLEANUP_INTERVAL` | `60`        | Cleanup sweep interval (seconds)       |
| `API_BASE_URL`             | —           | API URL for standalone UI              |
| `ROOT_PATH`                | —           | ASGI root path for UI behind LB       |

## Development

```bash
# Install all dependencies (including dev tools)
uv sync

# Run the test suite
uv run pytest

# Lint
uv run ruff check src/

# Type check
uv run mypy src/

# Format code
uv run ruff format src/ tests/

# Build documentation
uv sync --extra docs
uv run mkdocs serve
```

## Documentation

Full documentation is available at the [docs site](https://ralfbecher.github.io/orionbelt-semantic-layer/) or can be built locally:

```bash
uv sync --extra docs
uv run mkdocs serve   # http://127.0.0.1:8080
```

## OSI Interoperability

OrionBelt includes a bidirectional converter between OBML and the [Open Semantic Interchange (OSI)](https://github.com/open-semantic-interchange/OSI) format. The converter handles the structural differences between the two formats — including metric decomposition, relationship restructuring, and lossless `ai_context` preservation via `customExtensions` — with built-in validation for both directions.

See the [OSI ↔ OBML Mapping Analysis](osi-obml/osi_obml_mapping_analysis.md) for a detailed comparison and conversion reference.

## Companion Project

### [OrionBelt Analytics](https://github.com/ralfbecher/orionbelt-analytics)

OrionBelt Analytics is an ontology-based MCP server that analyzes relational database schemas and generates RDF/OWL ontologies with embedded SQL mappings. It connects to PostgreSQL, Snowflake, and Dremio, providing AI assistants with deep structural and semantic understanding of your data.

Together, the two MCP servers form a powerful combination for AI-guided analytical workflows:

- **OrionBelt Analytics** gives the AI contextual knowledge of your database schema, relationships, and business semantics
- **OrionBelt Semantic Layer** ensures correct, optimized SQL generation from business concepts (dimensions, measures, metrics)

By combining both, an AI assistant can navigate your data landscape through ontologies and compile safe, dialect-aware analytical SQL — enabling a seamless end-to-end analytical journey.

## License

Copyright 2025 [RALFORION d.o.o.](https://ralforion.com)

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

---

<p align="center">
  <a href="https://ralforion.com">
    <img src="docs/assets/RALFORION doo Logo.png" alt="RALFORION d.o.o." width="200">
  </a>
</p>
