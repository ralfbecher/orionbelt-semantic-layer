# Query Language

OrionBelt uses a structured query object to express analytical queries against a semantic model. The query language selects dimensions and measures, applies filters, sorts results, and limits output — all using business names rather than raw SQL.

## Query Object Structure

```yaml
select:
  dimensions:
    - Customer Country
    - "Order Date:month"      # with time grain
  measures:
    - Revenue
    - Order Count
where:
  - field: Customer Segment
    op: in
    value: [SMB, MidMarket]
having:
  - field: Revenue
    op: gt
    value: 10000
order_by:
  - field: Revenue
    direction: desc
limit: 1000
```

### In Python

```python
from orionbelt.models.query import (
    QueryObject,
    QuerySelect,
    QueryFilter,
    QueryOrderBy,
    FilterOperator,
    SortDirection,
)

query = QueryObject(
    select=QuerySelect(
        dimensions=["Customer Country", "Order Date:month"],
        measures=["Revenue", "Order Count"],
    ),
    where=[
        QueryFilter(field="Customer Segment", op=FilterOperator.IN, value=["SMB", "MidMarket"]),
    ],
    having=[
        QueryFilter(field="Revenue", op=FilterOperator.GT, value=10000),
    ],
    order_by=[QueryOrderBy(field="Revenue", direction=SortDirection.DESC)],
    limit=1000,
)
```

## Select

The `select` section specifies which dimensions and measures to include.

### Dimensions

Dimensions are referenced by name as defined in the semantic model. They become `GROUP BY` columns in the generated SQL.

```yaml
select:
  dimensions:
    - Customer Country
    - Product Category
```

### Time Grain Override

Apply a time grain at query time using `"dimension:grain"` syntax:

```yaml
select:
  dimensions:
    - "Order Date:month"     # truncate to month
    - "Order Date:quarter"   # truncate to quarter
    - "Order Date:year"      # truncate to year
```

Supported grains: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`.

This overrides any `timeGrain` set on the dimension definition.

### Measures

Measures are referenced by name. They can be simple aggregations, expression-based measures, or metrics.

```yaml
select:
  measures:
    - Revenue
    - Order Count
    - Revenue per Order    # metric
```

## Secondary Join Paths

When a model defines secondary joins (e.g., `Flights` → `Airports` via departure and arrival), use `usePathNames` to select which join path to use:

```yaml
select:
  dimensions:
    - Airport Name
  measures:
    - Total Ticket Price
usePathNames:
  - source: Flights
    target: Airports
    pathName: arrival
```

Each entry specifies a `(source, target, pathName)` triple. The `pathName` must match a secondary join defined in the model. When active, the secondary join replaces the primary join for that pair.

### In Python

```python
from orionbelt.models.query import QueryObject, QuerySelect, UsePathName

query = QueryObject(
    select=QuerySelect(
        dimensions=["Airport Name"],
        measures=["Total Ticket Price"],
    ),
    use_path_names=[
        UsePathName(source="Flights", target="Airports", path_name="arrival"),
    ],
)
```

### In JSON (full mode)

```json
{
  "select": {
    "dimensions": ["Airport Name"],
    "measures": ["Total Ticket Price"]
  },
  "usePathNames": [
    {"source": "Flights", "target": "Airports", "pathName": "arrival"}
  ]
}
```

If a `usePathNames` entry references a non-existent data object or pathName, the query will return a resolution error.

## Filters

Filters restrict the result set. **Dimension filters** go in `where` (become SQL `WHERE`), and **measure filters** go in `having` (become SQL `HAVING`).

```yaml
where:
  - field: Customer Country
    op: equals
    value: Germany

having:
  - field: Revenue
    op: gte
    value: 5000
```

### Filter Structure

| Property | Type | Description |
|----------|------|-------------|
| `field` | string | Dimension or measure name |
| `op` | string | Filter operator (see table below) |
| `value` | any | Comparison value (string, number, list, etc.) |

### Filter Operators

OrionBelt supports two operator naming conventions — OBML style and SQL style. Both are equivalent.

#### Comparison Operators

| OBML | SQL Style | SQL Output | Value Type |
|-----------|-----------|------------|------------|
| `equals` | `=`, `eq` | `= value` | scalar |
| `notequals` | `!=`, `neq` | `<> value` | scalar |
| `gt` | `>`, `greater` | `> value` | scalar |
| `gte` | `>=`, `greater_eq` | `>= value` | scalar |
| `lt` | `<`, `less` | `< value` | scalar |
| `lte` | `<=`, `less_eq` | `<= value` | scalar |

#### Set Operators

| OBML | SQL Style | SQL Output | Value Type |
|-----------|-----------|------------|------------|
| `inlist` | `in` | `IN (v1, v2, ...)` | list |
| `notinlist` | `not_in` | `NOT IN (v1, v2, ...)` | list |

#### Null Operators

| OBML | SQL Style | SQL Output | Value Type |
|-----------|-----------|------------|------------|
| `set` | `is_not_null` | `IS NOT NULL` | none |
| `notset` | `is_null` | `IS NULL` | none |

#### String Operators

| Operator | SQL Output | Value Type |
|----------|------------|------------|
| `contains` | `LIKE '%value%'` (dialect-specific) | string |
| `notcontains` | `NOT LIKE '%value%'` | string |
| `starts_with` | `LIKE 'value%'` | string |
| `ends_with` | `LIKE '%value'` | string |
| `like` | `LIKE 'pattern'` | string |
| `notlike` | `NOT LIKE 'pattern'` | string |

#### Range Operators

| Operator | SQL Output | Value Type |
|----------|------------|------------|
| `between` | `BETWEEN low AND high` | list of 2 |
| `notbetween` | `NOT BETWEEN low AND high` | list of 2 |
| `relative` | Relative time range | object |

**Relative filter object**

The `relative` operator expects an object with the following keys:

- `unit`: one of `day`, `week`, `month`, `year`
- `count`: positive integer number of units
- `direction` (optional): `past` (default) or `future`
- `include_current` (optional): boolean, default `true`

Example (last 7 days, inclusive of today):

```yaml
where:
  - field: Order Date
    op: relative
    value:
      unit: day
      count: 7
      direction: past
      include_current: true
```

!!! info "String contains is dialect-aware"
    The `contains` operator generates different SQL per dialect:

    - **Postgres/ClickHouse**: `ILIKE '%' || value || '%'`
    - **Snowflake**: `CONTAINS(column, value)`
    - **Dremio/Databricks**: `LOWER(column) LIKE '%' || LOWER(value) || '%'`

## Ordering

Sort results by dimension or measure names:

```yaml
order_by:
  - field: Revenue
    direction: desc
  - field: Customer Country
    direction: asc       # default
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `field` | string | — | Dimension or measure name |
| `direction` | enum | `asc` | `asc` or `desc` |

## Limit

Restrict the number of returned rows:

```yaml
limit: 1000
```

## Validation

Invalid queries return error responses:

| Error | Status | Cause |
|-------|--------|-------|
| Unknown dimension/measure | 400 | Referenced name not in model |
| Invalid operator | 400 | Unrecognized filter operator |
| Invalid time grain | 400 | Unrecognized grain string |
| Ambiguous joins | 422 | Multiple join paths possible |

## Semantics Summary

| Query Element | SQL Equivalent |
|---------------|----------------|
| `select.dimensions` | `SELECT` + `GROUP BY` columns |
| `select.measures` | `SELECT` aggregate expressions |
| `where` | `WHERE` clause |
| `having` | `HAVING` clause |
| `order_by` | `ORDER BY` clause |
| `limit` | `LIMIT` clause |
