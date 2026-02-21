# OrionBelt ML (OBML) Model Format

OrionBelt ML (OBML) is the YAML-based format for defining semantic models in OrionBelt. A model describes your data warehouse tables (data objects), business dimensions, aggregate measures, and composite metrics.

## Top-Level Structure

```yaml
# yaml-language-server: $schema=schema/obml-schema.json
version: 1.0

dataObjects:  # Database tables/views with columns and joins
  ...

dimensions:   # Named dimensions referencing data object columns
  ...

measures:     # Aggregations with expressions
  ...

metrics:      # Composite metrics combining measures
  ...
```

All four sections are dictionaries keyed by name.

## Data Objects

A **data object** maps to a database table or custom SQL statement. Each data object declares its columns and optional join relationships.

```yaml
dataObjects:
  Orders:
    code: ORDERS              # Table name or custom SQL
    database: WAREHOUSE         # Database/catalog
    schema: PUBLIC              # Schema
    columns:
      Order ID:
        code: ORDER_ID        # Physical column name
        abstractType: string
      Order Date:
        code: ORDER_DATE
        abstractType: date
      Customer ID:
        code: CUSTOMER_ID
        abstractType: string
      Price:
        code: PRICE
        abstractType: float
    joins:
      - joinType: many-to-one
        joinTo: Customers
        columnsFrom:
          - Customer ID
        columnsTo:
          - Customer ID
```

### Data Object Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `code` | string | Yes | Table name or SQL statement |
| `database` | string | Yes | Database/catalog name |
| `schema` | string | Yes | Schema name |
| `columns` | map | Yes | Dictionary of column definitions |
| `joins` | list | No | Join relationships to other data objects |
| `comment` | string | No | Documentation |

### Columns

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `code` | string | Yes | Physical column name in the database |
| `abstractType` | enum | Yes | `string`, `int`, `float`, `date`, `time`, `time_tz`, `timestamp`, `timestamp_tz`, `boolean`, `json` |
| `sqlType` | string | No | Informational: SQL data type (e.g. `VARCHAR`, `INTEGER`, `NUMERIC(10,2)`) |
| `sqlPrecision` | int | No | Informational: numeric precision |
| `sqlScale` | int | No | Informational: numeric scale |
| `comment` | string | No | Documentation |

### Joins

Joins define relationships between data objects. The data object that declares the join is the "from" side.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `joinType` | enum | Yes | `many-to-one`, `one-to-one`, `many-to-many` |
| `joinTo` | string | Yes | Target data object name |
| `columnsFrom` | list | Yes | Column names in this data object (join keys) |
| `columnsTo` | list | Yes | Column names in the target data object (join keys) |
| `secondary` | bool | No | Mark as a secondary (alternative) join path (default: `false`) |
| `pathName` | string | No | Unique name for this join path (required when `secondary: true`) |

!!! note "Fact tables declare joins"
    By convention, fact tables (e.g., `Orders`) declare joins to dimension tables (e.g., `Customers`, `Products`). The compiler uses this to identify fact tables — data objects with joins are preferred as base objects during query resolution.

### Secondary Joins

When a data object has multiple relationships to the same target (e.g., a `Flights` table joining to `Airports` via both departure and arrival), mark the additional joins as `secondary` with a unique `pathName`:

```yaml
dataObjects:
  Flights:
    code: FLIGHTS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Departure Airport:
        code: DEP_AIRPORT
        abstractType: string
      Arrival Airport:
        code: ARR_AIRPORT
        abstractType: string
    joins:
      - joinType: many-to-one
        joinTo: Airports
        columnsFrom:
          - Departure Airport
        columnsTo:
          - Airport ID
      - joinType: many-to-one
        joinTo: Airports
        secondary: true
        pathName: arrival
        columnsFrom:
          - Arrival Airport
        columnsTo:
          - Airport ID
```

Rules:

- Every secondary join **must** have a `pathName`
- `pathName` must be unique per `(source, target)` pair (not globally)
- Secondary joins are excluded from cycle detection and multipath validation
- Queries use `usePathNames` to select a secondary join instead of the default primary — see [Query Language](query-language.md#secondary-join-paths)

## Column References

Columns are referenced using the `dataObject` + `column` pair throughout the model:

```yaml
dimensions:
  Product Name:
    dataObject: Products
    column: Product Name
    resultType: string
```

Column names must be unique within each data object. Dimensions, measures, and metrics must have unique names across the whole model.

## Dimensions

A **dimension** defines a business attribute used for grouping (GROUP BY) in queries.

```yaml
dimensions:
  Customer Country:
    dataObject: Customers
    column: Country
    resultType: string

  Order Date:
    dataObject: Orders
    column: Order Date
    resultType: date
    timeGrain: month
```

### Dimension Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `dataObject` | string | Yes | Source data object name |
| `column` | string | Yes | Column name in the data object |
| `resultType` | enum | Yes | Data type of the result (informative only, not used for SQL generation) |
| `label` | string | No | Display label |
| `timeGrain` | enum | No | Time grain: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second` |
| `format` | string | No | Display format |

### Time Dimensions

Set `timeGrain` to apply time grain truncation:

```yaml
dimensions:
  Order Month:
    dataObject: Orders
    column: Order Date
    resultType: date
    timeGrain: month
```

This generates `date_trunc('month', col)` in Postgres/Snowflake or `toStartOfMonth(col)` in ClickHouse.

You can also apply time grain at query time using the `"dimension:grain"` syntax — see [Query Language](query-language.md).

## Measures

A **measure** defines an aggregate computation over data object columns.

### Simple Measure (single column)

```yaml
measures:
  Order Count:
    columns:
      - dataObject: Orders
        column: Order ID
    resultType: int
    aggregation: count
```

### Expression Measure (computed from columns)

Reference columns directly in the expression using `{[DataObject].[Column]}`:

```yaml
measures:
  Revenue:
    resultType: float
    aggregation: sum
    expression: '{[Orders].[Price]} * {[Orders].[Quantity]}'
```

```yaml
measures:
  Profit:
    resultType: float
    aggregation: sum
    expression: '{[Sales].[Salesamount]} - {[Sales].[Salescosts]}'
    total: true
```

### Measure Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `columns` | list | No | List of column references (`dataObject`+`column`) for simple single-column measures |
| `resultType` | enum | Yes | Data type of the result (informative only, not used for SQL generation) |
| `aggregation` | enum | Yes | `sum`, `count`, `count_distinct`, `avg`, `min`, `max`, `listagg` |
| `expression` | string | No | Expression with `{[DataObject].[Column]}` placeholders |
| `distinct` | bool | No | Apply DISTINCT to aggregation |
| `total` | bool | No | Use the total (unfiltered) value when referenced in a metric |
| `delimiter` | string | No | Separator for `listagg` aggregation (default: `","`) |
| `withinGroup` | object | No | Ordering clause for `listagg` — specifies `column` and `order` (`ASC`/`DESC`) |
| `filter` | object | No | Conditional filter applied to this measure |
| `allowFanOut` | bool | No | Allow fan-out joins (default: false) |

### Aggregation Types

| Type | SQL | Example |
|------|-----|---------|
| `sum` | `SUM(expr)` | Total revenue |
| `count` | `COUNT(expr)` | Number of orders |
| `count_distinct` | `COUNT(DISTINCT expr)` | Unique customers |
| `avg` | `AVG(expr)` | Average price |
| `min` | `MIN(expr)` | Earliest date |
| `max` | `MAX(expr)` | Latest date |
| `any_value` | `ANY_VALUE(expr)` | Any single value from the group (`any()` in ClickHouse) |
| `median` | `MEDIAN(expr)` | Median value (`PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY ...)` in Postgres) |
| `mode` | `MODE(expr)` | Most frequent value (`MODE() WITHIN GROUP (ORDER BY ...)` in Postgres, `topK(1)(col)[1]` in ClickHouse; not supported in Dremio) |
| `listagg` | `LISTAGG(expr, sep)` | Concatenated values (dialect-specific: `STRING_AGG` in Postgres, `ARRAY_JOIN(COLLECT_LIST(...))` in Databricks, `arrayStringConcat(groupArray(...))` in ClickHouse) |

### Expression Placeholders

| Placeholder | Resolves to |
|-------------|-------------|
| `{[DataObject].[Column]}` | Column reference by data object and column name |

### Measure Filters

Apply a filter to a measure so it only aggregates matching rows:

```yaml
measures:
  Sales Profit Ratio:
    resultType: float
    aggregation: sum
    expression: '({[Sales].[Salesamount]} / {[Sales].[Salescosts]}) * 100'
    filter:
      column:
        dataObject: Sales
        column: Salescosts
      operator: gt
      values:
        - dataType: float
          valueFloat: 100.00
```

### LISTAGG Measures

Use `listagg` to concatenate column values into a delimited string. OrionBelt renders the correct SQL for each database dialect automatically.

```yaml
measures:
  Product Names:
    columns:
      - dataObject: Products
        column: Product Name
    resultType: string
    aggregation: listagg
    delimiter: ', '
    withinGroup:
      column:
        dataObject: Products
        column: Product Name
      order: ASC
```

The `delimiter` defaults to `","` if omitted. The `withinGroup` clause is optional and specifies ordering of the concatenated values.

## Metrics

A **metric** is a composite measure that combines multiple measures into a derived KPI. The expression references measures by name using `{[Measure Name]}` template syntax.

```yaml
metrics:
  Revenue per Order:
    expression: '{[Revenue]} / {[Order Count]}'

  Net Revenue:
    expression: '{[Sales Amount]} - {[Return Amount]}'
```

All artefacts (data objects, dimensions, measures, metrics) have unique names. The `{[Name]}` placeholders in a metric expression must match existing measure names exactly.

### Metric Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `expression` | string | Yes | Expression with `{[Measure Name]}` placeholders |
| `label` | string | No | Display label |
| `format` | string | No | Display format |

### Metric Expression Placeholders

| Placeholder | Resolves to |
|-------------|-------------|
| `{[Measure Name]}` | Named reference to any defined measure |

## Validation Rules

OrionBelt validates models against these rules:

1. **Unique identifiers** — Column names unique within each data object; dimension, measure, and metric names unique across the model
2. **No cyclic joins** — Join graph must be acyclic (secondary joins are excluded)
3. **No multipath joins** — No ambiguous diamond patterns (secondary joins are excluded). A **canonical join exception** applies: when a data object has a direct join to a target AND also an indirect path through intermediaries, the direct join is treated as canonical and no error is raised. Only true diamonds (two indirect paths to the same target) are flagged.
4. **Secondary join constraints** — Every secondary join must have a `pathName`; `pathName` must be unique per `(source, target)` pair
5. **Measures resolve** — All column references in measures must point to existing data object columns
6. **Join targets exist** — All `joinTo` targets must be defined data objects
7. **References resolve** — All dimension references (dataObject/column) must resolve

Validation errors include source positions (line/column) when available.

## Full Example

See the [Sales Model Walkthrough](../examples/sales-model.md) for a complete annotated example.
