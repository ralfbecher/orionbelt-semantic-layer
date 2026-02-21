# Quick Start

This walkthrough takes you from a YAML semantic model to compiled SQL in under 5 minutes.

## Step 1: Define a Semantic Model

Create a file called `model.yaml`:

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
      Segment:
        code: SEGMENT
        abstractType: string

  Orders:
    code: ORDERS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Order ID:
        code: ORDER_ID
        abstractType: string
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
  Customer Country:
    dataObject: Customers
    column: Country
    resultType: string
  Customer Segment:
    dataObject: Customers
    column: Segment
    resultType: string

measures:
  Revenue:
    resultType: float
    aggregation: sum
    expression: '{[Orders].[Price]} * {[Orders].[Quantity]}'

  Order Count:
    columns:
      - dataObject: Orders
        column: Order ID
    resultType: int
    aggregation: count
```

## Step 2: Load and Validate

```python
from orionbelt.parser.loader import TrackedLoader
from orionbelt.parser.resolver import ReferenceResolver
from orionbelt.parser.validator import SemanticValidator

# Load YAML with source position tracking
loader = TrackedLoader()
raw, source_map = loader.load("model.yaml")

# Resolve references into typed Pydantic models
resolver = ReferenceResolver()
model, result = resolver.resolve(raw, source_map)

if not result.valid:
    for error in result.errors:
        print(f"  {error.code}: {error.message}")
else:
    print("Model is valid!")

# Run semantic validation (cycle check, reference resolution, etc.)
validator = SemanticValidator()
errors = validator.validate(model)
for error in errors:
    print(f"  {error.code}: {error.message}")
```

## Step 3: Compile a Query

```python
from orionbelt.compiler.pipeline import CompilationPipeline
from orionbelt.models.query import (
    QueryObject,
    QuerySelect,
    QueryFilter,
    FilterOperator,
    QueryOrderBy,
    SortDirection,
)

# Define a query: Revenue by country for SMB/MidMarket customers
query = QueryObject(
    select=QuerySelect(
        dimensions=["Customer Country"],
        measures=["Revenue", "Order Count"],
    ),
    where=[
        QueryFilter(
            field="Customer Segment",
            op=FilterOperator.IN,
            value=["SMB", "MidMarket"],
        ),
    ],
    order_by=[QueryOrderBy(field="Revenue", direction=SortDirection.DESC)],
    limit=1000,
)

# Compile to Postgres SQL
pipeline = CompilationPipeline()
result = pipeline.compile(query, model, "postgres")
print(result.sql)
```

**Output:**

```sql
SELECT
  "Customers"."COUNTRY" AS "Customer Country",
  SUM("Orders"."PRICE" * "Orders"."QUANTITY") AS "Revenue",
  COUNT("Orders"."ORDER_ID") AS "Order Count"
FROM WAREHOUSE.PUBLIC.ORDERS AS "Orders"
LEFT JOIN WAREHOUSE.PUBLIC.CUSTOMERS AS "Customers"
  ON "Orders"."CUSTOMER_ID" = "Customers"."CUSTOMER_ID"
WHERE ("Customers"."SEGMENT" IN ('SMB', 'MidMarket'))
GROUP BY "Customers"."COUNTRY"
ORDER BY "Revenue" DESC
LIMIT 1000
```

## Step 4: Try a Different Dialect

Simply change the dialect parameter:

```python
# Snowflake
result = pipeline.compile(query, model, "snowflake")

# ClickHouse
result = pipeline.compile(query, model, "clickhouse")

# Databricks
result = pipeline.compile(query, model, "databricks")
```

Each dialect applies its own identifier quoting, function names, and SQL syntax. See [SQL Dialects](../guide/dialects.md) for details.

## Step 5: Use the REST API with Sessions

Start the server:

```bash
uv run orionbelt-api
```

Create a session and load a model:

```bash
# Create a session
SESSION_ID=$(curl -s -X POST http://127.0.0.1:8000/sessions | jq -r .session_id)

# Load a model into the session
MODEL_ID=$(curl -s -X POST "http://127.0.0.1:8000/sessions/$SESSION_ID/models" \
  -H "Content-Type: application/json" \
  -d "{\"model_yaml\": \"$(cat model.yaml)\"}" | jq -r .model_id)

# Compile a query
curl -s -X POST "http://127.0.0.1:8000/sessions/$SESSION_ID/query/sql" \
  -H "Content-Type: application/json" \
  -d "{
    \"model_id\": \"$MODEL_ID\",
    \"query\": {
      \"select\": {
        \"dimensions\": [\"Customer Country\"],
        \"measures\": [\"Revenue\"]
      }
    },
    \"dialect\": \"postgres\"
  }" | jq .sql

# Clean up
curl -s -X DELETE "http://127.0.0.1:8000/sessions/$SESSION_ID"
```

## Step 6: Use with Claude Desktop (MCP)

Add OrionBelt to your Claude Desktop config:

```json
{
  "mcpServers": {
    "orionbelt-semantic-layer": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/orionbelt-semantic-layer", "orionbelt-mcp"]
    }
  }
}
```

Then in Claude Desktop, you can ask:

> Load this OBML model and compile a query for Revenue by Country using Snowflake dialect.

Claude will use the `load_model`, `describe_model`, and `compile_query` tools to complete the workflow.

## Next Steps

- [OBML Model Format](../guide/model-format.md) — Complete OrionBelt ML specification
- [Query Language](../guide/query-language.md) — Filters, operators, time grains
- [SQL Dialects](../guide/dialects.md) — Dialect capabilities and differences
- [API Endpoints](../api/endpoints.md) — Full REST API documentation
- [MCP Server](../api/mcp.md) — MCP tools and prompts reference
