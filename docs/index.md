<p align="center">
  <img src="assets/ORIONBELT Logo.png" alt="OrionBelt Logo" width="400">
</p>

# OrionBelt Semantic Layer

**Compile YAML semantic models into analytical SQL across multiple database dialects.**

OrionBelt is a semantic layer engine that transforms declarative YAML model definitions into optimized SQL for Postgres, Snowflake, ClickHouse, Dremio, and Databricks. Query using business concepts — dimensions, measures, and metrics — instead of raw SQL.

## Why OrionBelt?

- **One model, many dialects** — Define your semantic model once in YAML, compile to SQL for any supported warehouse
- **Safe by construction** — AST-based SQL generation prevents injection and ensures syntactic correctness
- **Precise error reporting** — Validation errors include line and column numbers from your YAML source
- **Automatic join resolution** — Declare relationships between data objects; OrionBelt finds optimal join paths using graph algorithms
- **Multi-fact support** — Composite Fact Layer (CFL) planning handles queries spanning multiple fact tables with UNION ALL and CTE-based aggregation
- **Session management** — TTL-scoped sessions isolate model state per client, enabling iterative development workflows

## Key Features

| Feature | Description |
|---------|-------------|
| 5 SQL Dialects | Postgres, Snowflake, ClickHouse, Dremio, Databricks SQL |
| OrionBelt ML (OBML) | YAML-based data objects, dimensions, measures, metrics, joins |
| Star Schema & CFL | Automatic fact selection and join path resolution |
| Session Management | TTL-scoped per-client sessions for the REST API |
| REST API | FastAPI endpoints for session-based model management, validation, and compilation |
| Gradio UI | Interactive web interface for model editing, query testing, SQL compilation, ER diagrams, and OSI import/export |
| Custom Extensions | Vendor-specific metadata at all model levels (model, data object, column, dimension, measure, metric) |
| Plugin Architecture | Extensible dialect system with capability flags |
| Source Tracking | Error messages with YAML line/column positions |

## Quick Example

Define a semantic model in YAML:

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
      Customer ID:
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
          - Customer ID
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
    expression: '{[Orders].[Price]} * {[Orders].[Quantity]}'
```

Compile a query to SQL:

```python
result = pipeline.compile(query, model, "postgres")
```

```sql
SELECT
  "Customers"."COUNTRY" AS "Country",
  SUM("Orders"."PRICE" * "Orders"."QUANTITY") AS "Revenue"
FROM WAREHOUSE.PUBLIC.ORDERS AS "Orders"
LEFT JOIN WAREHOUSE.PUBLIC.CUSTOMERS AS "Customers"
  ON "Orders"."CUSTOMER_ID" = "Customers"."CUSTOMER_ID"
GROUP BY "Customers"."COUNTRY"
```

## Getting Started

Ready to dive in? Start with [Installation](getting-started/installation.md) and then follow the [Quick Start](getting-started/quickstart.md) tutorial.

---

<p align="center">
  <a href="https://ralforion.com"><img src="assets/RALFORION doo Logo.png" alt="RALFORION d.o.o." width="200"></a>
  <br>
  Copyright 2025 RALFORION d.o.o. &mdash; Licensed under Apache 2.0
</p>
