# Semantic Layer SaaS — Full Specification (v1 Draft)

## 1. Overview

This document defines a SaaS semantic layer that:

- Stores YAML semantic models
- Exposes REST APIs for artifact management
- Provides a YAML query language
- Compiles analytical SQL across multiple dialects
- Supports Star Schema and Composite Fact Layer (CFL)
- Is multi-tenant and versioned

Supported SQL dialects (initial):

- Postgres
- Snowflake
- ClickHouse
- Dremio
- Databricks SQL

---

## 2. Core Concepts

- Tenant / Workspace — isolation boundary
- Project — container for models
- Semantic Model — YAML artifacts describing facts/dims/measures
- Query Object — YAML analytical request
- Logical Plan — resolved semantic graph
- Physical Plan — SQL AST
- Dialect — SQL renderer backend

---

## 3. Semantic Model Specification

### 3.1 Artifact Structure

```
model.yaml
facts/*.yaml
dimensions/*.yaml
measures/*.yaml
macros/*.yaml
policies/*.yaml
```

Artifacts are versioned and immutable once published.

---

### 3.2 Facts

Required fields:

- name
- description
- table
- schema
- grain
- dimensions
- measures

Example:

```yaml
name: orders
table: warehouse.orders_f
grain: order_id
dimensions:
  - customer
  - date
measures:
  - revenue
```

---

### 3.3 Dimensions

Required fields:

- name
- table
- primary_key
- attributes

Example:

```yaml
name: customer
table: warehouse.customer_d
primary_key: customer_id
attributes:
  - country
  - segment
```

---

### 3.4 Measures

Required fields:

- name
- expression
- aggregation

Example:

```yaml
name: revenue
expression: price * quantity
aggregation: sum
```

---

### 3.5 Relationships

Fields:

- from
- to
- on
- type
- cardinality

Example:

```yaml
from: orders.customer_id
to: customer.customer_id
type: left
cardinality: many_to_one
```

---

### 3.6 Time Dimensions

Supported grains:

- day
- week
- month
- quarter
- year

---

### 3.7 CFL Rules

- Facts share conformed dimensions
- Grain reconciliation required
- Bridge tables declare cardinality
- Fanout validated

---

### 3.8 Validation Rules

- Unique identifiers
- No cyclic joins without override
- Measures resolve to base expressions
- Join paths deterministic
- References must resolve

---

### 3.9 Versioning

- Draft models mutable
- Published models immutable
- Queries reference explicit versions

---

## 4. Query Language Specification

### 4.1 Query Structure

```yaml
select:
  dimensions:
    - customer.country
    - order.order_date:month
  measures:
    - revenue
    - orders
where:
  - field: customer.segment
    op: in
    value: ["SMB", "MidMarket"]
order_by:
  - field: revenue
    direction: desc
limit: 1000
```

---

### 4.2 Operators

Comparison:

```
= != > >= < <=
```

Set:

```
in not_in
```

Null:

```
is_null is_not_null
```

String:

```
contains starts_with ends_with
```

Time:

```
between relative
```

---

### 4.3 Semantics

- Dimensions → GROUP BY
- Measures → aggregation
- Dim filters → WHERE
- Measure filters → HAVING
- Sorting supports dims + measures

---

### 4.4 Validation

- Unknown fields → 400
- Invalid operator → 400
- Invalid grain → 400
- Ambiguous joins → 422

---

## 5. SQL Planning and Generation

### 5.1 Resolution Phase

Produces:

- Resolved expressions
- Fact selection
- Join path resolution
- Filter classification
- Time normalization

---

### 5.2 Star Schema Planning

- Single fact base
- Dimension joins
- GROUP BY dims
- HAVING for measures

---

### 5.3 CFL Planning

Supported:

- Conformed dimensions
- Fact stitching:
  - union alignment
  - aggregate + join
  - bridge tables

Fanout protection enforced.

---

### 5.4 SQL AST

Nodes:

- Select
- From
- Join
- Where
- GroupBy
- Having
- OrderBy
- CTE
- Expression
- Function
- Cast
- Case

SQL generated from AST only.

---

## 6. Dialect Specification

### 6.1 Interface

```
compile(ast) -> sql
normalize_identifier(name)
render_time_grain(expr, grain)
render_cast(expr, type)
```

Capability flags:

- supports_cte
- supports_qualify
- supports_arrays
- supports_window_filters

---

### 6.2 Vendor Notes

Postgres:

- strict GROUP BY
- date_trunc
- ILIKE

Snowflake:

- QUALIFY
- case-sensitive identifiers
- semi-structured types

ClickHouse:

- aggregation differences
- join limits
- custom date functions

Dremio:

- reduced function surface
- quoting differences

Databricks SQL:

- Spark SQL semantics
- backtick identifiers
- extended time functions

---

### 6.3 Extensibility

- Plugin dialect architecture
- Capability negotiation
- Explicit unsupported feature errors

---

## 7. REST API Specification

### 7.1 Resources

```
/tenants
/projects
/models
/models/{id}/versions
/artifacts
/validate
/query/sql
/dialects
```

---

### 7.2 Model Endpoints

- POST /models
- GET /models/{id}
- PUT /models/{id}
- POST /models/{id}/versions

---

### 7.3 Validation

POST /validate

---

### 7.4 Query Compilation

POST /query/sql

Request:

```json
{
  "model_version": "v12",
  "dialect": "snowflake",
  "query": {}
}
```

Response:

```json
{
  "sql": "SELECT ...",
  "dialect": "snowflake",
  "resolved": {
    "fact_tables": ["orders_f"],
    "dimensions": ["customer.country"],
    "measures": ["revenue"]
  },
  "warnings": []
}
```

---

### 7.5 Errors

- 400 validation
- 401 unauthorized
- 403 forbidden
- 404 not found
- 409 conflict
- 422 semantic ambiguity

Error format:

```json
{
  "error": "VALIDATION_ERROR",
  "message": "...",
  "path": "dimensions[2]"
}
```

---

### 7.6 Security

- OAuth2 or API keys
- RBAC roles
- Tenant isolation
- Audit logging

---

## 8. Persistence

- Raw YAML + normalized storage
- Immutable versions
- Draft/publish workflow
- Schema migration support

---

## 9. Observability

- Structured logs
- Metrics
- Tracing
- Rate limiting

---

## 10. Implementation Constraints

- Python backend
- FastAPI recommended
- Pydantic validation
- YAML parser with line fidelity
- Custom SQL AST

---

## 11. Acceptance Criteria

- CRUD + versioning
- Full validation
- ≥2 dialects supported
- Star schema working
- CFL subset working
- Plugin dialect architecture

---

End of specification.
