# Compilation Pipeline

OrionBelt compiles semantic queries into SQL through a three-phase pipeline: **Resolution**, **Planning**, and **Code Generation**. Each phase transforms the query into a progressively more concrete representation.

```
QueryObject + SemanticModel
        |
        v
+-----------------+
|  Phase 1:       |
|  Resolution     |  -> ResolvedQuery
+--------+--------+
         |
         v
+-----------------+
|  Phase 2:       |
|  Planning       |  -> QueryPlan (SQL AST)
|  (Star or CFL)  |
+--------+--------+
         |
         v
+-----------------+
|  Phase 3:       |
|  Code Generation|  -> SQL string
|  (Dialect)      |
+-----------------+
```

## Phase 1: Resolution

**Module:** `orionbelt.compiler.resolution`

The resolver transforms a high-level `QueryObject` (business names) into a `ResolvedQuery` (concrete column references and expressions).

### What Resolution Does

1. **Resolve dimensions** — Look up each dimension name in the model, find the source data object and column, apply time grain if requested
2. **Resolve measures** — Expand expression placeholders (`{[DataObject].[Column]}`) into column references, wrap in aggregation functions
3. **Resolve metrics** — Expand measure references (`{[Measure Name]}`), compose expressions
4. **Select base object** — Choose the primary fact table (prefers data objects with joins defined)
5. **Find join paths** — Use the join graph to find the minimal set of joins connecting all required objects
6. **Classify filters** — Dimension filters -> WHERE, measure filters -> HAVING
7. **Resolve ORDER BY** — Map field names to dimension or measure expressions

### ResolvedQuery

The output of resolution contains everything the planner needs:

| Field | Type | Description |
|-------|------|-------------|
| `dimensions` | `list[ResolvedDimension]` | Resolved column refs with data object/field/source |
| `measures` | `list[ResolvedMeasure]` | AST expressions with aggregation |
| `base_object` | `str` | Selected fact table name |
| `required_objects` | `set[str]` | All data objects needed by the query |
| `join_steps` | `list[JoinStep]` | Ordered join sequence |
| `where_filters` | `list[ResolvedFilter]` | Dimension filter expressions |
| `having_filters` | `list[ResolvedFilter]` | Measure filter expressions |
| `order_by_exprs` | `list[tuple[Expr, bool]]` | (expression, is_descending) pairs |
| `limit` | `int | None` | Row limit |
| `requires_cfl` | `bool` | Whether multi-fact CFL planning is needed |
| `use_path_names` | `list[UsePathName]` | Secondary join overrides from the query |

### Join Graph

**Module:** `orionbelt.compiler.graph`

The `JoinGraph` uses [networkx](https://networkx.org/) to model data object relationships:

- **Undirected graph** for finding shortest paths between data objects
- **Directed graph** for cycle detection
- `find_join_path(from_objects, to_objects)` returns the minimal `JoinStep` sequence
- `build_join_condition(step)` generates equality conditions from field mappings
- Accepts optional `use_path_names` to activate secondary joins — when a secondary override is active for a `(source, target)` pair, the primary join is replaced by the matching secondary join

```python
# Example: Orders -> Customers join
JoinStep(
    from_object="Orders",
    to_object="Customers",
    from_columns=["Customer ID"],
    to_columns=["Customer ID"],
    join_type=JoinType.LEFT,
    cardinality=Cardinality.MANY_TO_ONE,
)
```

## Phase 2: Planning

The planner converts a `ResolvedQuery` into a `QueryPlan` containing an SQL AST (`Select` node).

### Star Schema Planner

**Module:** `orionbelt.compiler.star`

Used for single-fact queries (most common case). Builds a straightforward SELECT with joins:

```
SELECT  dimension_columns, aggregate_expressions
FROM    base_fact_table
JOIN    dimension_table ON condition
WHERE   dimension_filters
GROUP BY dimension_columns
HAVING  measure_filters
ORDER BY ...
LIMIT   ...
```

The planner uses the `QueryBuilder` fluent API to construct the AST:

```python
builder = QueryBuilder()
builder.select(...)           # dimensions + measures
builder.from_(fact_table)     # base fact
builder.join(dim_table, on=condition)  # each join step
builder.where(filter_expr)    # WHERE conditions
builder.group_by(dim_cols)    # GROUP BY
builder.having(having_expr)   # HAVING conditions
builder.order_by(expr, desc=True)
builder.limit(1000)
plan = QueryPlan(ast=builder.build())
```

### CFL Planner (Composite Fact Layer)

**Module:** `orionbelt.compiler.cfl`

Used for multi-fact queries — when measures come from different fact tables. The CFL planner uses a **UNION ALL** strategy:

1. **Groups measures by source data object** — Identifies which measures belong to which fact table
2. **Validates fanout** — Ensures dimensions are compatible across facts
3. **Builds UNION ALL legs** — Each fact leg SELECTs conformed dimensions + its own measures (with NULL for the other facts' measures)
4. **Combines into a CTE** — The legs are combined with `UNION ALL` into a single `composite_01` CTE
5. **Outer aggregation** — The outer query aggregates over the union, grouping by conformed dimensions

```sql
WITH composite_01 AS (
  SELECT country, price * quantity AS revenue, NULL AS return_count
  FROM orders JOIN customers ON ...
  UNION ALL
  SELECT country, NULL AS revenue, 1 AS return_count
  FROM returns JOIN customers ON ...
)
SELECT
  country,
  SUM(revenue) AS revenue,
  COUNT(return_count) AS return_count
FROM composite_01
GROUP BY country
```

On Snowflake, `UNION ALL BY NAME` is used instead, so each leg only selects its own measures (no NULL padding needed).

If there is only one fact table, the CFL planner delegates to the Star Schema planner.

## Phase 3: Code Generation

**Module:** `orionbelt.compiler.codegen`

The code generator walks the SQL AST and produces a dialect-specific SQL string. It delegates entirely to the dialect's `compile()` method.

```python
class CodeGenerator:
    def __init__(self, dialect: Dialect) -> None:
        self._dialect = dialect

    def generate(self, ast: Select) -> str:
        return self._dialect.compile(ast)
```

The dialect's `compile()` method recursively visits each AST node:

- `Select` -> `SELECT ... FROM ... JOIN ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT ...`
- `ColumnRef` -> `"table"."column"` (or `` `table`.`column` `` for Databricks)
- `FunctionCall` -> `SUM("col")`, `COUNT(DISTINCT "col")`, etc.
- `BinaryOp` -> `(left op right)`
- `Literal` -> `'string'`, `42`, `NULL`, `TRUE`
- `CTE` -> `WITH name AS (SELECT ...)`

## SQL AST

**Module:** `orionbelt.ast.nodes`

All SQL is generated from an immutable AST — never by string concatenation. The AST nodes are frozen dataclasses:

### Expression Nodes

| Node | Description | Example |
|------|-------------|---------|
| `Literal` | Constant value | `'hello'`, `42`, `NULL` |
| `ColumnRef` | Column reference | `"table"."col"` |
| `Star` | Wildcard | `*`, `"table".*` |
| `AliasedExpr` | Aliased expression | `expr AS "alias"` |
| `FunctionCall` | Function call | `SUM("col")` |
| `BinaryOp` | Binary operator | `(a + b)`, `(x AND y)` |
| `UnaryOp` | Unary operator | `NOT x` |
| `IsNull` | NULL check | `x IS NULL`, `x IS NOT NULL` |
| `InList` | IN list | `x IN (1, 2, 3)` |
| `Between` | Range check | `x BETWEEN 1 AND 10` |
| `CaseExpr` | CASE expression | `CASE WHEN ... THEN ... END` |
| `Cast` | Type cast | `CAST(x AS INTEGER)` |
| `SubqueryExpr` | Subquery | `(SELECT ...)` |
| `RawSQL` | Escape hatch | Raw SQL string |

### Statement Nodes

| Node | Description |
|------|-------------|
| `Select` | Full SELECT statement with columns, from, joins, where, group_by, having, order_by, limit, ctes |
| `From` | FROM clause (table or subquery with alias) |
| `Join` | JOIN clause (type, source, alias, on condition) |
| `OrderByItem` | ORDER BY item (expression, direction, nulls handling) |
| `CTE` | Common Table Expression (name + SELECT or UNION ALL query) |
| `UnionAll` | UNION ALL of multiple SELECT statements |

### QueryBuilder

**Module:** `orionbelt.ast.builder`

Fluent API for constructing AST nodes:

```python
from orionbelt.ast.builder import QueryBuilder, col, func, lit, alias, eq, and_

query = (
    QueryBuilder()
    .select(alias(col("COUNTRY", "Customers"), "Country"))
    .select(alias(func("SUM", col("PRICE", "Orders")), "Revenue"))
    .from_("WAREHOUSE.PUBLIC.ORDERS", alias="Orders")
    .join("WAREHOUSE.PUBLIC.CUSTOMERS", on=eq(col("CUSTOMER_ID", "Orders"), col("CUSTOMER_ID", "Customers")), alias="Customers")
    .where(col("SEGMENT", "Customers"))
    .group_by(col("COUNTRY", "Customers"))
    .order_by(col("Revenue"), desc=True)
    .limit(100)
    .build()
)
```

## Pipeline Orchestration

**Module:** `orionbelt.compiler.pipeline`

The `CompilationPipeline` ties all phases together:

```python
class CompilationPipeline:
    def compile(self, query: QueryObject, model: SemanticModel, dialect_name: str) -> CompilationResult:
        # Phase 1: Resolution
        resolved = QueryResolver().resolve(query, model)

        # Phase 2: Planning
        if resolved.requires_cfl:
            plan = CFLPlanner.plan(resolved, model)
        else:
            plan = StarSchemaPlanner.plan(resolved, model)

        # Phase 3: Code Generation
        dialect = DialectRegistry.get(dialect_name)
        sql = CodeGenerator(dialect).generate(plan.ast)

        return CompilationResult(sql=sql, dialect=dialect_name, resolved=..., warnings=...)
```

The `CompilationResult` includes:

| Field | Type | Description |
|-------|------|-------------|
| `sql` | `str` | Generated SQL string |
| `dialect` | `str` | Dialect name used |
| `resolved` | `ResolvedInfo` | Fact tables, dimensions, measures used |
| `warnings` | `list[str]` | Non-fatal warnings |
