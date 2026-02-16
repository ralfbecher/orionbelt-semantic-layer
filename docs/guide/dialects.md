# SQL Dialects

OrionBelt compiles semantic queries into SQL for five database dialects. Each dialect has its own identifier quoting, function names, and SQL syntax. The plugin architecture allows adding new dialects without modifying the core compiler.

## Supported Dialects

| Dialect | Identifier | Description |
|---------|-----------|-------------|
| PostgreSQL | `postgres` | Standard PostgreSQL with strict GROUP BY |
| Snowflake | `snowflake` | Cloud data warehouse with QUALIFY, semi-structured types |
| ClickHouse | `clickhouse` | Column-oriented OLAP with custom date/aggregation functions |
| Dremio | `dremio` | Data lakehouse with reduced function surface |
| Databricks SQL | `databricks` | Spark SQL semantics with backtick identifiers |

## Capabilities Matrix

Each dialect declares capability flags that the compiler uses to choose SQL generation strategies.

| Capability | Postgres | Snowflake | ClickHouse | Dremio | Databricks |
|-----------|----------|-----------|------------|--------|------------|
| `supports_cte` | Yes | Yes | Yes | Yes | Yes |
| `supports_qualify` | No | Yes | No | No | No |
| `supports_arrays` | Yes | Yes | Yes | No | Yes |
| `supports_window_filters` | No | Yes | No | No | No |
| `supports_ilike` | Yes | Yes | Yes | No | No |
| `supports_time_travel` | No | Yes | No | No | No |
| `supports_semi_structured` | No | Yes | No | No | No |

## Identifier Quoting

| Dialect | Style | Example |
|---------|-------|---------|
| Postgres | Double quotes | `"column_name"` |
| Snowflake | Double quotes | `"column_name"` |
| ClickHouse | Double quotes | `"column_name"` |
| Dremio | Double quotes | `"column_name"` |
| Databricks | Backticks | `` `column_name` `` |

## Time Grain Functions

The `timeGrain` is rendered differently per dialect:

=== "Postgres"

    ```sql
    date_trunc('month', "order_date")
    date_trunc('year', "order_date")
    date_trunc('quarter', "order_date")
    ```

=== "Snowflake"

    ```sql
    DATE_TRUNC('month', "order_date")
    DATE_TRUNC('year', "order_date")
    DATE_TRUNC('quarter', "order_date")
    ```

=== "ClickHouse"

    ```sql
    toStartOfMonth("order_date")
    toStartOfYear("order_date")
    toStartOfQuarter("order_date")
    toMonday("order_date")        -- week
    toDate("order_date")          -- day
    toStartOfHour("order_date")
    toStartOfMinute("order_date")
    toStartOfSecond("order_date")
    ```

=== "Dremio"

    ```sql
    DATE_TRUNC('month', "order_date")
    DATE_TRUNC('year', "order_date")
    ```

=== "Databricks"

    ```sql
    date_trunc('month', `order_date`)
    date_trunc('year', `order_date`)
    ```

## String Contains

The `contains` filter operator is rendered per dialect:

=== "Postgres"

    ```sql
    "column" ILIKE '%' || 'search' || '%'
    ```

=== "Snowflake"

    ```sql
    CONTAINS("column", 'search')
    ```

=== "ClickHouse"

    ```sql
    "column" ILIKE '%' || 'search' || '%'
    ```

=== "Dremio"

    ```sql
    LOWER("column") LIKE '%' || LOWER('search') || '%'
    ```

=== "Databricks"

    ```sql
    lower(`column`) LIKE '%' || lower('search') || '%'
    ```

## CAST Handling

=== "Postgres / Snowflake / Dremio / Databricks"

    ```sql
    CAST(expr AS INTEGER)
    CAST(expr AS VARCHAR)
    CAST(expr AS DATE)
    ```

=== "ClickHouse"

    ClickHouse uses native conversion functions:

    ```sql
    toInt64(expr)      -- int / integer
    toFloat64(expr)    -- float / double
    toString(expr)     -- string / varchar
    toDate(expr)       -- date
    -- Other types fall back to CAST
    CAST(expr AS DateTime)
    ```

## Dialect Plugin Architecture

Each dialect implements the abstract `Dialect` base class:

```python
class Dialect(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def capabilities(self) -> DialectCapabilities: ...

    @abstractmethod
    def quote_identifier(self, name: str) -> str: ...

    @abstractmethod
    def render_time_grain(self, column: Expr, grain: TimeGrain) -> Expr: ...

    @abstractmethod
    def render_cast(self, expr: Expr, target_type: str) -> Expr: ...

    def render_string_contains(self, column: Expr, pattern: Expr) -> Expr: ...

    def compile(self, ast: Select) -> str: ...
```

Dialects register themselves via the `@DialectRegistry.register` decorator:

```python
@DialectRegistry.register
class PostgresDialect(Dialect):
    @property
    def name(self) -> str:
        return "postgres"
    ...
```

The registry provides lookup by name:

```python
from orionbelt.dialect.registry import DialectRegistry

dialect = DialectRegistry.get("snowflake")
sql = dialect.compile(ast)
```

### Adding a New Dialect

1. Create `src/orionbelt/dialect/my_dialect.py`
2. Subclass `Dialect` and implement all abstract methods
3. Decorate with `@DialectRegistry.register`
4. The dialect is automatically available via `DialectRegistry.get("my_dialect")`

## Querying Dialect Info via API

```bash
curl http://127.0.0.1:8000/dialects
```

```json
{
  "dialects": [
    {
      "name": "postgres",
      "capabilities": {
        "supports_cte": true,
        "supports_qualify": false,
        "supports_arrays": true,
        "supports_window_filters": false,
        "supports_ilike": true,
        "supports_time_travel": false,
        "supports_semi_structured": false
      }
    },
    ...
  ]
}
```
