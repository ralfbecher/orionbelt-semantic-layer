# OrionBelt Driver Ecosystem — Architecture Overview

## Deployment Model

On-premise / Docker on LAN. Single Docker container running orionbelt-api
exposes two protocols simultaneously:

  Port 8080  — FastAPI REST API (existing, unchanged)
  Port 8815  — Arrow Flight SQL (background thread in same process)

No additional containers. No extra processes. One deployment unit.

## Package Structure

```
drivers/
├── ARCHITECTURE.md              ← this file
│
├── ob-driver-core/              ← shared foundation (PEP 249 exceptions, type codes,
│   └── src/ob_driver_core/         OBML detection, REST compilation bridge)
│       ├── detection.py         ← is_obml(), parse_obml()
│       ├── compiler.py          ← compile_obml() via POST /v1/query/sql
│       ├── exceptions.py        ← PEP 249 exception hierarchy
│       └── type_codes.py        ← PEP 249 type objects (STRING, NUMBER, etc.)
│
├── ob-bigquery/                 ← DB-API 2.0 driver for BigQuery
├── ob-duckdb/                   ← DB-API 2.0 driver for DuckDB
├── ob-postgres/                 ← DB-API 2.0 driver for PostgreSQL
├── ob-snowflake/                ← DB-API 2.0 driver for Snowflake
├── ob-clickhouse/               ← DB-API 2.0 driver for ClickHouse
├── ob-dremio/                   ← DB-API 2.0 driver for Dremio
├── ob-databricks/               ← DB-API 2.0 driver for Databricks
│
└── ob-flight-extension/         ← Arrow Flight SQL server, embedded into orionbelt-api
    └── src/ob_flight/
        ├── server.py            ← pyarrow FlightServerBase subclass
        ├── catalog.py           ← ListFlights → OB model data objects
        ├── converters.py        ← DB rows → Arrow RecordBatch
        ├── db_router.py         ← vendor routing (dialect → native connector)
        ├── auth.py              ← optional token auth middleware
        └── startup.py           ← background thread launcher for orionbelt-api
```

## Protocol Coverage

| Client              | Protocol         | Entry point              |
|---------------------|-----------------|--------------------------|
| DBeaver             | Flight SQL       | port 8815                |
| Tableau             | Flight SQL JDBC  | port 8815 via .jar       |
| Power BI            | ODBC bridge      | port 8815 via Flight ODBC|
| Python apps         | DB-API 2.0       | ob-duckdb / ob-postgres / etc. |
| REST / MCP / AI     | HTTP REST        | port 8080 (unchanged)    |

## DB-API 2.0 Drivers — REST-Only Compilation

All vendor drivers work against the OrionBelt REST API in **single-model mode**
(`MODEL_FILE` set). They do NOT import the compilation pipeline directly.

```
Python App
    │
    │  cur.execute(obml_query)
    ▼
ob-driver-core/compiler.py
    │
    │  POST /v1/query/sql?dialect=<vendor>
    ▼
OrionBelt REST API (port 8080)
    │
    │  compiled SQL
    ▼
Native connector (psycopg2, snowflake-connector, etc.)
    │
    ▼
Database
```

OBML queries are detected by `is_obml()` (starts with `select:` + has
`dimensions`/`measures`). Plain SQL bypasses the API entirely.

## Flight SQL — Direct Python Call (no HTTP hop)

The Flight server runs inside the API process. It uses `CompilationPipeline`
directly — no REST call for compilation, no session management.

```
DBeaver sends OBML YAML or plain SQL
    │
    ▼
ob_flight.server.OBFlightServer.get_flight_info()
    │
    ├─ is_obml(query)?
    │   YES → CompilationPipeline.compile()  ← direct Python call
    │   NO  → pass SQL through unchanged
    │
    ▼
db_router.connect(dialect) → native connector
    │
    ▼
Arrow RecordBatch streamed back via DoGet
```

## Query Execution via REST

The `POST /v1/query/execute` endpoint compiles OBML and executes the SQL
against the configured database, returning JSON results. Gated by
`FLIGHT_ENABLED=true` (uses the same `db_router` as the Flight server).

```
UI / MCP / any client
    │  POST /v1/query/execute
    ▼
REST API → CompilationPipeline → db_router.connect(DB_VENDOR)
    │
    ▼
JSON response: { columns, rows, row_count, sql, ... }
```

## Docker

Single image, single container, two ports:

```bash
docker build -f Dockerfile.flight -t orionbelt-flight .
docker run -p 8080:8080 -p 8815:8815 --env-file .env orionbelt-flight
```

The container makes **outbound** connections to the database — no extra
port mapping needed. Works with cloud databases (Snowflake, Databricks, etc.)
out of the box.
