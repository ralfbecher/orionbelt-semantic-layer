# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**OrionBelt Semantic Layer** is a SaaS semantic layer product. It contains the architectural specification, a YAML-based model format (OBML), a JSON Schema for validation, and a full Python implementation.

The product compiles YAML semantic models into analytical SQL across multiple database dialects (Postgres, Snowflake, ClickHouse, Dremio, Databricks SQL).

## Repository Structure

- `semantic-layer-spec.md` — Full v1 draft specification defining the entire system: semantic models, query language, SQL planning/generation, REST API, dialects, and persistence
- `examples/sem-layer.obml.yml` — Example OBML semantic model (data objects, dimensions, measures, metrics with joins)
- `schema/obml-schema.json` — JSON Schema (Draft-07) for validating OBML documents
- `examples/examples.txt` — References to related projects
- `src/orionbelt/` — Python implementation
  - `api/` — FastAPI REST layer (app, routers, schemas, middleware, deps)
  - `ast/` — SQL AST nodes, builder, visitor
  - `compiler/` — Resolution, star schema planner, CFL planner, codegen pipeline
  - `dialect/` — 5 SQL dialect implementations with registry
  - `mcp/` — MCP server (FastMCP, 9 tools + 3 prompts)
  - `models/` — Pydantic models (semantic, query, errors)
  - `parser/` — YAML loader, reference resolver, validator
  - `service/` — ModelStore (in-memory registry), SessionManager (TTL-scoped sessions)
  - `settings.py` — Shared settings (pydantic-settings, reads .env)

## Architecture (from spec)

The system follows a multi-phase compilation pipeline:

1. **YAML Semantic Models** — Artifacts organized as `model.yaml` + `facts/`, `dimensions/`, `measures/`, `macros/`, `policies/` directories. Versioned with draft/publish workflow.
2. **Query Language** — YAML-based analytical queries selecting dimensions (with time grain) and measures, with WHERE/HAVING/ORDER BY/LIMIT.
3. **Resolution Phase** — Resolves expressions, selects fact tables, determines join paths, classifies filters, normalizes time.
4. **SQL Planning** — Star Schema (single fact + dimension joins) or CFL (Composite Fact Layer: conformed dimensions, fact stitching via UNION ALL with NULL padding, aggregation outside). Snowflake uses UNION ALL BY NAME. Fanout protection enforced.
5. **SQL AST** — Custom AST nodes (Select, From, Join, Where, GroupBy, Having, OrderBy, CTE, UnionAll, Expression, Function, Cast, Case). All SQL generated from AST only.
6. **Dialect Rendering** — Plugin architecture with capability flags (`supports_cte`, `supports_qualify`, `supports_arrays`, `supports_window_filters`). Each dialect implements `compile(ast) -> sql`, `normalize_identifier`, `render_time_grain`, `render_cast`.

## OBML Format

The YAML model format uses these top-level sections:
- **dataObjects** — Database tables/views with columns, data types, and join definitions (cardinality: many-to-one, one-to-one). Column names must be globally unique across all data objects. Joins support `secondary: true` with a `pathName` for multiple join paths between the same pair.
- **dimensions** — Named dimensions referencing data object columns via `dataObject` + `column` pair, with optional timeGrain
- **measures** — Aggregations with expressions using `{[Column]}` syntax (column names are globally unique), plus optional filters
- **metrics** — Composite metrics combining measures via `{[Measure Name]}` references in the expression

Columns are referenced by `dataObject` + `column` pair.

### Semantic Validation

The `SemanticValidator` (`parser/validator.py`) checks models for:
- Duplicate identifiers and non-unique column names
- Secondary join constraints (must have `pathName`; unique per source/target pair)
- Cyclic joins (DFS cycle detection — secondary joins excluded)
- Multipath joins (diamond patterns — secondary joins excluded; direct + indirect from same start node allowed as "canonical join" exception)
- Unknown join targets, join columns, data object/column references
- Unresolvable measure and dimension references

## Session Management

Both the REST API and MCP server use session-scoped state:

- **SessionManager** (`service/session_manager.py`) — Manages TTL-scoped sessions, each holding its own `ModelStore`. Thread-safe via `threading.Lock`. Background daemon thread purges expired sessions.
- **REST API sessions** — 10 endpoints under `/sessions` prefix: CRUD (create, list, get, delete), model management (load, list, describe, remove), validate, and query compilation.
- **MCP sessions** — 9 tools (3 session tools + 6 model/query tools with optional `session_id`). In stdio mode, a default session is used automatically.
- **Settings** — `SESSION_TTL_SECONDS` (default 1800), `SESSION_CLEANUP_INTERVAL` (default 60)
- **DI pattern** — `api/deps.py` provides `get_session_manager()` for FastAPI `Depends`, initialized via `lifespan` context manager.

## REST API Endpoints

### Session-scoped (under `/sessions`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/sessions` | Create session |
| GET | `/sessions` | List sessions |
| GET | `/sessions/{id}` | Get session info |
| DELETE | `/sessions/{id}` | Close session |
| POST | `/sessions/{id}/models` | Load model |
| GET | `/sessions/{id}/models` | List models |
| GET | `/sessions/{id}/models/{mid}` | Describe model |
| DELETE | `/sessions/{id}/models/{mid}` | Remove model |
| POST | `/sessions/{id}/validate` | Validate YAML |
| POST | `/sessions/{id}/query/sql` | Compile query |

### Other endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/dialects` | List dialects |
| GET | `/health` | Health check |

## MCP Server

Entry point: `orionbelt-mcp` (9 tools, 3 prompts)

**Session tools**: `create_session`, `close_session`, `list_sessions`
**Model tools** (with optional `session_id`): `load_model`, `validate_model`, `describe_model`, `compile_query`, `list_models`
**Stateless**: `list_dialects`

## Implementation Constraints (from spec)

- Python backend with FastAPI
- Pydantic for validation
- YAML parser with line fidelity (for error reporting with source positions)
- Custom SQL AST (not string concatenation)
- Multi-tenant with RBAC, OAuth2/API keys

## Related Projects

- See `examples/examples.txt` for references to related open-source projects
