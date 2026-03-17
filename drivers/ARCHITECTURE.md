# OrionBelt Driver Ecosystem — Architecture Overview

## Deployment Model

On-premise / Docker on LAN. Single Docker container running orionbelt-api
exposes two protocols simultaneously:

  Port 8000  — FastAPI REST API (existing, unchanged)
  Port 8815  — Arrow Flight SQL (new, background thread in same process)

No additional containers. No extra processes. One deployment unit.

## Repository Structure

```
ob-drivers/
├── ARCHITECTURE.md              ← this file
│
├── ob-flight-extension/         ← Flight SQL server, embedded into orionbelt-api
│   └── src/ob_flight/
│       ├── server.py            ← pyarrow FlightServerBase subclass
│       ├── handlers.py          ← GetFlightInfo, DoGet, GetSchema
│       ├── catalog.py           ← ListFlights → OB model data objects
│       ├── auth.py              ← optional token auth middleware
│       └── startup.py           ← background thread launcher for orionbelt-api
│
├── ob-snowflake/                ← DB-API 2.0 driver for Snowflake
├── ob-postgres/                 ← DB-API 2.0 driver for PostgreSQL
├── ob-clickhouse/               ← DB-API 2.0 driver for ClickHouse
├── ob-dremio/                   ← DB-API 2.0 driver for Dremio
└── ob-databricks/               ← DB-API 2.0 driver for Databricks
```

## Protocol Coverage

| Client              | Protocol         | Entry point              |
|---------------------|-----------------|--------------------------|
| DBeaver             | Flight SQL       | port 8815                |
| Tableau             | Flight SQL JDBC  | port 8815 via .jar       |
| Power BI            | ODBC bridge      | port 8815 via Flight ODBC|
| pandas / Jupyter    | DB-API 2.0       | ob-snowflake / ob-postgres|
| dbt                 | SQLAlchemy       | ob+snowflake:// dialect  |
| Superset / Metabase | SQLAlchemy       | ob+postgres:// dialect   |
| REST / MCP / AI     | HTTP REST        | port 8000 (unchanged)    |

## Flight SQL — Internal Call Path (no HTTP hop)

```
DBeaver sends OBML YAML or plain SQL
    │
    ▼
ob_flight.handlers.OBFlightHandler.get_flight_info()
    │
    ├─ is_obml(query)?
    │   YES → from orionbelt.compiler.pipeline import CompilationPipeline
    │          pipeline.compile(query, model, dialect)  ← direct Python call
    │   NO  → pass SQL through unchanged
    │
    ▼
native DB connector (snowflake-connector-python / psycopg2 / etc.)
    │
    ▼
Arrow record batches streamed back via DoGet
```

The Flight handler imports CompilationPipeline directly — same process,
zero network overhead, no session management needed.

## Model Resolution

DBeaver connection "Database" field → ob_model_id or model name.

Resolution order:
1. Exact match on model_id (UUID)
2. Case-insensitive match on model name
3. Fall back to DEFAULT_MODEL env var
4. Raise FlightUnavailableError with helpful message

Model is loaded once per server startup (or on first use) and cached
in process memory. Reloaded on SIGHUP or via REST endpoint
POST /admin/flight/reload-models.

## Docker Compose Integration

```yaml
# docker-compose.yml addition to existing orionbelt-api service
services:
  orionbelt:
    image: orionbelt-api:latest
    ports:
      - "8000:8000"    # REST API
      - "8815:8815"    # Arrow Flight SQL  ← ADD THIS
    environment:
      FLIGHT_ENABLED: "true"
      FLIGHT_PORT: "8815"
      FLIGHT_AUTH_MODE: "none"       # or "token"
      FLIGHT_DEFAULT_MODEL: ""       # optional fallback model id
      FLIGHT_PRELOAD_MODELS: ""      # comma-sep .obml.yaml paths to load at startup
```

## Vendor DB-API Packages

Each vendor package is independent, minimal, and follows the same pattern:

  connect(**kwargs) → Connection → Cursor
    │
    ├─ is_obml(query) → OB CompilationPipeline.compile() → SQL
    └─ plain SQL → native connector directly

The OB compilation call is a **direct Python import**, not a REST call,
when used in the same process as orionbelt-api. When used standalone
(e.g. in a Jupyter notebook), it calls the REST API via httpx.

Auto-detection: if `orionbelt.compiler.pipeline` is importable → direct call.
Otherwise → REST API via ob_api_url parameter.

## Shared ob-core Package (extract if needed)

If the YAML detection + compilation bridge logic is duplicated across all
5 vendor packages, extract it to a shared `ob-core` package:

  ob-core/
    src/ob_core/
      detection.py     ← is_obml(), parse_obml()
      compiler.py      ← direct call or REST fallback
      exceptions.py    ← PEP 249 exception hierarchy
      type_codes.py    ← PEP 249 type objects

All vendor packages depend on ob-core. This is the recommended approach
once all 5 drivers are stable.
