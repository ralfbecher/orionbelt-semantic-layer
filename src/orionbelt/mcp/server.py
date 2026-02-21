"""FastMCP server exposing OrionBelt's compilation pipeline as MCP tools.

Run via::

    orionbelt-mcp                       # reads .env (default: stdio)
    MCP_TRANSPORT=http orionbelt-mcp    # streamable HTTP on port 9000
    MCP_TRANSPORT=sse  orionbelt-mcp    # legacy SSE on port 9000

Sessions scope each client's ``ModelStore``.  In stdio mode a default session
is used automatically; in HTTP/SSE mode callers must create sessions explicitly.
Settings are loaded from environment variables and ``.env`` file — see
``.env.example`` for available options.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from orionbelt import __version__
from orionbelt.dialect import DialectRegistry
from orionbelt.models.query import QueryObject, QuerySelect, UsePathName
from orionbelt.service.model_store import ModelStore
from orionbelt.service.session_manager import SessionManager, SessionNotFoundError
from orionbelt.settings import Settings

# ---------------------------------------------------------------------------
# Server + shared state
# ---------------------------------------------------------------------------

logger = logging.getLogger("orionbelt.mcp")

mcp = FastMCP("OrionBelt Semantic Layer")
_session_manager: SessionManager | None = None


def _resolve_store(session_id: str | None = None) -> ModelStore:
    """Resolve a session_id to its ModelStore.

    - If *session_id* is provided, look it up in the session manager.
    - If ``None`` and running in stdio mode, use the default session.
    - If ``None`` and running in HTTP/SSE mode, raise ``ToolError``.
    """
    if _session_manager is None:
        raise ToolError("Session manager not initialised")
    if session_id is not None:
        try:
            return _session_manager.get_store(session_id)
        except SessionNotFoundError as exc:
            raise ToolError(str(exc)) from exc
    # No session_id — fall back to default (stdio)
    return _session_manager.get_or_create_default()


# ---------------------------------------------------------------------------
# Resources — auto-injected context for LLMs
# ---------------------------------------------------------------------------

OBML_REFERENCE = """\
# OBML (OrionBelt ML) Reference

OBML is a YAML-based semantic model format. A model has four top-level sections:

## 1. dataObjects — physical tables/views

```yaml
dataObjects:
  Orders:                         # data object name
    code: ORDERS                  # physical table/view name
    database: EDW                 # database
    schema: SALES_MART            # schema
    columns:
      Order ID:                   # column name — must be unique within this data object
        code: ID                  # physical column name
        abstractType: string      # see abstractType values below
      Amount:
        code: AMOUNT
        abstractType: float
    joins:                        # optional — defined on fact tables
      - joinType: many-to-one     # many-to-one | one-to-one
        joinTo: Customers         # target data object name
        columnsFrom:
          - Customer ID           # local column name
        columnsTo:
          - Customer ID           # target column name
```

## 2. dimensions — named analytical dimensions

```yaml
dimensions:
  Customer Country:
    dataObject: Customers         # which data object owns this dimension
    column: Country               # column within that data object
    resultType: string            # data type of the result (informative only)
    timeGrain: month              # optional: year | quarter | month | week | day | hour
```

## 3. measures — aggregations

```yaml
measures:
  Total Revenue:                  # measure name
    columns:                      # column references (for simple aggregations)
      - dataObject: Orders
        column: Amount
    resultType: float
    aggregation: sum              # see aggregation values below
    total: false                  # optional: use total (unfiltered) value in metrics

  Profit:                         # expression-based measure
    resultType: float
    aggregation: sum
    expression: '{[Orders].[Amount]} - {[Orders].[Cost]}'  # {[DataObject].[Column]} syntax

  Filtered Measure:               # measure with a filter
    columns:
      - dataObject: Orders
        column: Amount
    resultType: float
    aggregation: sum
    filter:
      column:
        dataObject: Orders
        column: Status
      operator: equals            # equals | gt | gte | lt | lte | in | not_in | ...
      values:
        - dataType: string
          valueString: completed
```

## 4. metrics — composite calculations from measures

```yaml
metrics:
  Profit Margin:
    expression: '{[Profit]} / {[Total Revenue]}'  # {[Measure Name]} syntax
```

## abstractType Values

string, int, float, date, time, time_tz, timestamp,
timestamp_tz, boolean, json

## Aggregation Values

sum, count, count_distinct, avg, min, max,
any_value, median, mode, listagg

## Key Rules

1. **Column names are unique within each data object**.
   Dimensions, measures, and metrics must be unique across the model.
2. Measure expressions use `{[DataObject].[Column]}` to reference columns.
3. Metric expressions use `{[Measure Name]}` to reference measures by name.
4. Joins are defined on fact tables pointing to dimension tables \
(many-to-one or one-to-one).
5. A dimension references exactly one `dataObject` + `column` pair.

## Complete Minimal Example

```yaml
version: 1.0

dataObjects:
  Orders:
    code: ORDERS
    database: EDW
    schema: SALES
    columns:
      Order ID:
        code: ID
        abstractType: string
      Customer ID:
        code: CUST_ID
        abstractType: string
      Amount:
        code: AMOUNT
        abstractType: float
    joins:
      - joinType: many-to-one
        joinTo: Customers
        columnsFrom:
          - Customer ID
        columnsTo:
          - Cust ID

  Customers:
    code: CUSTOMERS
    database: EDW
    schema: SALES
    columns:
      Cust ID:
        code: ID
        abstractType: string
      Country:
        code: COUNTRY
        abstractType: string

dimensions:
  Customer Country:
    dataObject: Customers
    column: Country
    resultType: string

measures:
  Total Revenue:
    columns:
      - dataObject: Orders
        column: Amount
    resultType: float
    aggregation: sum

metrics:
  Revenue Per Order:
    expression: '{[Total Revenue]} / {[Order Count]}'
```

## Supported SQL Dialects

postgres, snowflake, clickhouse, databricks, dremio

## Workflow

1. `load_model(model_yaml)` — parse, validate, store → returns `model_id`
2. `describe_model(model_id)` — inspect data objects, dimensions, measures, metrics
3. `compile_query(model_id, dimensions=[...], measures=[...])` — generate SQL
"""


@mcp.resource("obml://reference")
def obml_reference() -> str:
    """Full OBML format reference — data objects, dimensions, measures, metrics, joins."""
    return OBML_REFERENCE


@mcp.tool
def get_obml_reference() -> str:
    """Get the OBML format reference.

    IMPORTANT: Call this tool BEFORE composing any OBML YAML to understand
    the correct syntax.  Returns the full specification with examples for
    dataObjects, dimensions, measures, metrics, joins, and expressions.
    """
    return OBML_REFERENCE


# ---------------------------------------------------------------------------
# Session tools
# ---------------------------------------------------------------------------


@mcp.tool
def create_session(metadata_json: str | None = None) -> str:
    """Create a new session and return its session_id.

    Each session has its own model store.  Use the returned session_id with
    other tools to load, query, and manage models within that session.

    Args:
        metadata_json: Optional JSON object with metadata key-value pairs.
    """
    if _session_manager is None:
        raise ToolError("Session manager not initialised")
    metadata: dict[str, str] = {}
    if metadata_json:
        try:
            metadata = json.loads(metadata_json)
        except json.JSONDecodeError as exc:
            raise ToolError(f"Invalid metadata JSON: {exc}") from exc
    info = _session_manager.create_session(metadata=metadata)
    return (
        f"Session created.  session_id: {info.session_id}\n"
        f"  created_at: {info.created_at.isoformat()}"
    )


@mcp.tool
def close_session(session_id: str) -> str:
    """Close a session and release its resources.

    Args:
        session_id: The session to close.
    """
    if _session_manager is None:
        raise ToolError("Session manager not initialised")
    try:
        _session_manager.close_session(session_id)
    except SessionNotFoundError as exc:
        raise ToolError(str(exc)) from exc
    return f"Session '{session_id}' closed."


@mcp.tool
def list_sessions() -> str:
    """List all active sessions."""
    if _session_manager is None:
        raise ToolError("Session manager not initialised")
    sessions = _session_manager.list_sessions()
    if not sessions:
        return "No active sessions."
    lines = ["Active sessions:", ""]
    for s in sessions:
        lines.append(
            f"  {s.session_id}  "
            f"(models: {s.model_count}, "
            f"last accessed: {s.last_accessed_at.isoformat()})"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Model tools (session-aware)
# ---------------------------------------------------------------------------


@mcp.tool
def load_model(model_yaml: str, session_id: str | None = None) -> str:
    """Load an OBML semantic model into a session.

    IMPORTANT: Before composing OBML YAML, call ``get_obml_reference()``
    first to learn the correct format.

    Parse, validate, and store the model.  Returns a model_id that you must
    pass to other tools (describe_model, compile_query, etc.).

    The OBML YAML must start with ``version: 1.0`` and uses YAML **mappings**
    (not lists) for all sections.  Quick structure::

        version: 1.0
        dataObjects:
          <Name>:                    # mapping key = data object name
            code: <TABLE>
            database: <DB>
            schema: <SCHEMA>
            columns:
              <Column Name>:         # unique within this data object
                code: <COLUMN>
                abstractType: string # see OBML reference for all types
            joins:                   # optional, on fact tables
              - joinType: many-to-one
                joinTo: <Target>
                columnsFrom: [<local column>]
                columnsTo: [<target column>]
        dimensions:
          <Dim Name>:
            dataObject: <Name>       # must match a dataObjects key
            column: <Column Name>    # must match a column in that object
            resultType: string
        measures:
          <Measure Name>:
            columns:
              - dataObject: <Name>
                column: <Column Name>
            resultType: float
            aggregation: sum         # see OBML reference for all types
        metrics:
          <Metric Name>:
            expression: '{[Measure A]} / {[Measure B]}'

    Args:
        model_yaml: Complete OBML YAML content (version 1.0).
        session_id: Session to load into (optional in stdio mode).
    """
    logger.info("load_model called (yaml length=%d)", len(model_yaml))
    logger.debug("load_model yaml:\n%s", model_yaml)
    store = _resolve_store(session_id)
    try:
        result = store.load_model(model_yaml)
    except ValueError as exc:
        logger.warning("load_model validation failed: %s", exc)
        hint = (
            "\n\nHint: call get_obml_reference() to see the correct OBML "
            "YAML format.  Common mistakes:\n"
            "- dataObjects must be a YAML mapping (not a list)\n"
            "- Each data object needs: code, database, schema, "
            "columns (mapping of column name → {code, abstractType})\n"
            "- Column names must be unique within each data object\n"
            "- dimensions need: dataObject, column, resultType\n"
            "- measures need: aggregation, resultType, and either "
            "columns or expression"
        )
        raise ToolError(str(exc) + hint) from exc

    parts = [
        f"Model loaded successfully.  model_id: {result.model_id}",
        f"  data objects: {result.data_objects}",
        f"  dimensions:   {result.dimensions}",
        f"  measures:     {result.measures}",
        f"  metrics:      {result.metrics}",
    ]
    if result.warnings:
        parts.append(f"  warnings: {'; '.join(result.warnings)}")
    return "\n".join(parts)


@mcp.tool
def validate_model(model_yaml: str, session_id: str | None = None) -> str:
    """Validate an OBML model without storing it.

    Returns validation errors and warnings.  Useful for checking a model
    before loading it.

    Args:
        model_yaml: Complete OBML YAML content.
        session_id: Session context (optional in stdio mode).
    """
    logger.info("validate_model called (yaml length=%d)", len(model_yaml))
    logger.debug("validate_model yaml:\n%s", model_yaml)
    store = _resolve_store(session_id)
    summary = store.validate(model_yaml)
    if summary.valid:
        msg = "Model is valid."
        if summary.warnings:
            msg += "\nWarnings:"
            for w in summary.warnings:
                msg += f"\n  [{w.code}] {w.message}"
        return msg

    lines = ["Model has validation errors:"]
    for e in summary.errors:
        line = f"  [{e.code}] {e.message}"
        if e.path:
            line += f"  (at {e.path})"
        if e.suggestions:
            line += f"  Did you mean: {', '.join(e.suggestions)}?"
        lines.append(line)
    if summary.warnings:
        lines.append("Warnings:")
        for w in summary.warnings:
            lines.append(f"  [{w.code}] {w.message}")
    return "\n".join(lines)


@mcp.tool
def describe_model(model_id: str, session_id: str | None = None) -> str:
    """Describe the contents of a loaded model.

    Shows data objects (with columns and joins), dimensions, measures, and
    metrics.  Use this after ``load_model`` to explore the model.

    Args:
        model_id: The id returned by ``load_model``.
        session_id: Session that holds the model (optional in stdio mode).
    """
    store = _resolve_store(session_id)
    try:
        desc = store.describe(model_id)
    except KeyError as exc:
        raise ToolError(str(exc)) from exc

    lines: list[str] = [f"Model {model_id}:", ""]

    # Data objects
    lines.append("DATA OBJECTS:")
    for obj in desc.data_objects:
        lines.append(f"  {obj.label}  (code: {obj.code})")
        lines.append(f"    columns: {', '.join(obj.columns)}")
        if obj.join_targets:
            lines.append(f"    joins to: {', '.join(obj.join_targets)}")
    lines.append("")

    # Dimensions
    lines.append("DIMENSIONS:")
    for dim in desc.dimensions:
        grain = f"  grain={dim.time_grain}" if dim.time_grain else ""
        lines.append(f"  {dim.name}  ({dim.result_type}, {dim.data_object}.{dim.column}{grain})")
    lines.append("")

    # Measures
    lines.append("MEASURES:")
    for m in desc.measures:
        expr = f"  expr: {m.expression}" if m.expression else ""
        lines.append(f"  {m.name}  ({m.result_type}, {m.aggregation}{expr})")
    lines.append("")

    # Metrics
    if desc.metrics:
        lines.append("METRICS:")
        for met in desc.metrics:
            lines.append(f"  {met.name}  expr: {met.expression}")
        lines.append("")

    return "\n".join(lines)


@mcp.tool
def compile_query(
    model_id: str,
    dialect: str = "postgres",
    dimensions: list[str] | None = None,
    measures: list[str] | None = None,
    query_json: str | None = None,
    session_id: str | None = None,
    use_path_names: list[dict[str, str]] | None = None,
) -> str:
    """Compile a semantic query to SQL.

    Two modes:

    **Simple mode** — pass ``dimensions`` and ``measures`` lists directly::

        compile_query(model_id="abc12345", dimensions=["Country"], measures=["Revenue"])

    **Full mode** — pass a complete query as JSON via ``query_json``::

        compile_query(
            model_id="abc12345",
            query_json='{"select":{"dimensions":["Country"],"measures":["Revenue"]},"where":[{"field":"Country","op":"equals","value":"US"}],"order_by":[{"field":"Revenue","direction":"desc"}],"limit":10}'
        )

    The full query JSON supports: ``select`` (dimensions + measures), ``where``,
    ``having``, ``order_by``, ``limit``, ``usePathNames``.

    Use ``describe_model`` first to discover available dimension and measure
    names.  Filter operators: equals, notequals, gt, gte, lt, lte, inlist,
    notinlist, in, not_in, contains, notcontains, like, notlike, starts_with,
    ends_with, between, notbetween, set, notset, is_null, is_not_null,
    relative.

    For secondary joins, pass ``use_path_names`` (simple mode) or include
    ``usePathNames`` in query_json (full mode). Each item has ``source``,
    ``target``, and ``pathName`` keys.

    Args:
        model_id: The id returned by ``load_model``.
        dialect: Target SQL dialect (postgres, snowflake, clickhouse, databricks, dremio).
        dimensions: List of dimension names (simple mode).
        measures: List of measure names (simple mode).
        query_json: Full query object as JSON string (full mode).
        session_id: Session that holds the model (optional in stdio mode).
        use_path_names: List of {source, target, pathName} dicts for
            selecting secondary joins (simple mode).
    """
    logger.info("compile_query called (model_id=%s, dialect=%s)", model_id, dialect)
    store = _resolve_store(session_id)

    # Build QueryObject
    if query_json is not None:
        try:
            raw = json.loads(query_json)
            query = QueryObject.model_validate(raw)
        except (json.JSONDecodeError, Exception) as exc:
            raise ToolError(f"Invalid query JSON: {exc}") from exc
    elif dimensions is not None or measures is not None:
        upn_list: list[UsePathName] = []
        if use_path_names:
            for item in use_path_names:
                upn_list.append(
                    UsePathName(
                        source=item["source"],
                        target=item["target"],
                        path_name=item["pathName"],
                    )
                )
        query = QueryObject(
            select=QuerySelect(
                dimensions=dimensions or [],
                measures=measures or [],
            ),
            use_path_names=upn_list,
        )
    else:
        raise ToolError(
            "Provide either dimensions/measures (simple mode) or query_json (full mode)."
        )

    try:
        result = store.compile_query(model_id, query, dialect)
    except KeyError as exc:
        raise ToolError(str(exc)) from exc
    except Exception as exc:
        raise ToolError(f"Compilation error: {exc}") from exc

    parts = [
        f"-- Dialect: {result.dialect}",
        f"-- Fact tables: {', '.join(result.resolved.fact_tables)}",
        f"-- Dimensions: {', '.join(result.resolved.dimensions)}",
        f"-- Measures: {', '.join(result.resolved.measures)}",
        "",
        result.sql,
    ]
    if result.warnings:
        parts.append("")
        parts.append(f"-- Warnings: {'; '.join(result.warnings)}")
    return "\n".join(parts)


@mcp.tool
def list_models(session_id: str | None = None) -> str:
    """List all models currently loaded in a session.

    Args:
        session_id: Session to list models from (optional in stdio mode).
    """
    store = _resolve_store(session_id)
    models = store.list_models()
    if not models:
        return "No models loaded.  Use load_model to load one."
    lines = ["Loaded models:", ""]
    for m in models:
        lines.append(
            f"  {m.model_id}  "
            f"({m.data_objects} objects, {m.dimensions} dims, "
            f"{m.measures} measures, {m.metrics} metrics)"
        )
    return "\n".join(lines)


@mcp.tool
def list_dialects() -> str:
    """List available SQL dialects and their capabilities."""
    names = DialectRegistry.available()
    lines = ["Available dialects:", ""]
    for name in names:
        dialect = DialectRegistry.get(name)
        caps = asdict(dialect.capabilities)
        enabled = [k for k, v in caps.items() if v]
        cap_str = ", ".join(enabled) if enabled else "(none)"
        lines.append(f"  {name}: {cap_str}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


@mcp.prompt
def write_obml_model() -> str:
    """OBML syntax reference — how to write a semantic model in YAML."""
    return """\
# OBML (OrionBelt ML) Syntax Reference

An OBML model is a YAML file with four top-level sections:

```yaml
version: 1.0

dataObjects:
  <ObjectName>:
    code: <TABLE_NAME>             # physical table/view name
    database: <DB>
    schema: <SCHEMA>
    columns:
      <Column Name>:              # unique within this data object
        code: <COLUMN>            # physical column name
        abstractType: string      # see abstractType values below
    joins:                        # optional — define on fact tables
      - joinType: many-to-one     # many-to-one | one-to-one
        joinTo: <TargetObject>
        columnsFrom:
          - <local column name>
        columnsTo:
          - <target column name>

dimensions:
  <Dimension Name>:
    dataObject: <ObjectName>       # which data object owns this dimension
    column: <Column Name>          # column within that data object
    resultType: string             # data type
    timeGrain: month               # optional: year | quarter | month | week | day | hour

measures:
  <Measure Name>:
    columns:                       # column references (for simple aggregations)
      - dataObject: <ObjectName>
        column: <Column Name>
    resultType: float
    aggregation: sum               # see aggregation values below
    expression: '{[Orders].[Amount]} - {[Orders].[Cost]}'  # {[DataObject].[Column]}
    filter:                        # optional measure-level filter
      column:
        dataObject: <ObjectName>
        column: <Column Name>
      operator: gt
      values:
        - dataType: float
          valueFloat: 100.0

metrics:
  <Metric Name>:
    expression: '{[Measure A]} / {[Measure B]}'   # {[Measure Name]} syntax
```

## abstractType Values

string, int, float, date, time, time_tz, timestamp,
timestamp_tz, boolean, json

## Aggregation Values

sum, count, count_distinct, avg, min, max,
any_value, median, mode, listagg

## Key Rules

1. **Column names are unique within each data object**.
   Dimensions, measures, and metrics must be unique across the model.
2. Measure expressions use `{[DataObject].[Column]}` to reference columns.
3. Metric expressions use `{[Measure Name]}` to reference measures.
4. Joins are defined on fact tables pointing to dimension tables.
5. A dimension references exactly one `dataObject` + `column` pair.

## Workflow

1. `load_model(model_yaml)` → get a `model_id`
2. `describe_model(model_id)` → see what's in the model
3. `compile_query(model_id, ...)` → generate SQL
"""


@mcp.prompt
def write_query() -> str:
    """How to use the compile_query tool — simple and full modes."""
    return """\
# Compiling Queries with OrionBelt

## Simple Mode

Pass dimension and measure names directly:

```
compile_query(
  model_id="abc12345",
  dialect="postgres",
  dimensions=["Customer Country"],
  measures=["Total Revenue"]
)
```

## Full Mode (filters, ordering, limits)

Pass a complete query as JSON:

```
compile_query(
  model_id="abc12345",
  dialect="snowflake",
  query_json='{
    "select": {
      "dimensions": ["Customer Country"],
      "measures": ["Total Revenue"]
    },
    "where": [
      {"field": "Customer Country", "op": "equals", "value": "US"}
    ],
    "order_by": [
      {"field": "Total Revenue", "direction": "desc"}
    ],
    "limit": 10
  }'
)
```

## Filter Operators

- Equality: `equals`, `notequals`, `=`, `!=`
- Comparison: `gt`, `gte`, `lt`, `lte`, `>`, `>=`, `<`, `<=`
- Set: `in`, `not_in`, `inlist`, `notinlist`
- Null: `is_null`, `is_not_null`, `set`, `notset`
- String: `contains`, `notcontains`, `like`, `notlike`, `starts_with`, `ends_with`
- Range: `between`, `notbetween`, `relative`

## Supported Dialects

`postgres`, `snowflake`, `clickhouse`, `databricks`, `dremio`

## Tips

- Use `describe_model` first to see available dimension/measure names.
- Use `list_dialects` to check dialect capabilities.
- Dimension names with time grain: append `:month`, `:year`, etc.
"""


@mcp.prompt
def debug_validation() -> str:
    """All OBML validation error codes with causes and fixes."""
    return """\
# OBML Validation Error Codes

## Parse Errors

- `YAML_PARSE_ERROR`: Invalid YAML syntax.
  Fix: Check indentation, quoting, colons.
- `DATA_OBJECT_PARSE_ERROR`: Cannot parse a data object.
  Fix: Check required fields (code, database, schema, columns).
- `DIMENSION_PARSE_ERROR`: Cannot parse a dimension definition.
  Fix: Check required fields (dataObject, column, resultType).
- `MEASURE_PARSE_ERROR`: Cannot parse a measure definition.
  Fix: Check required fields (aggregation, resultType) and either columns or expression.
- `METRIC_PARSE_ERROR`: Cannot parse a metric definition.
  Fix: Check required field (expression).

## Reference Errors

- `UNKNOWN_DATA_OBJECT`: References non-existent data object.
  Fix: Check spelling; suggestions are included.
- `UNKNOWN_COLUMN`: Column name not found in data object.
  Fix: Check column name spelling within the referenced data object.
- `UNKNOWN_DATA_OBJECT_IN_EXPRESSION`: Measure expression `{[DataObject].[Column]}` \
references unknown data object.
  Fix: Check data object name in the expression.
- `UNKNOWN_COLUMN_IN_EXPRESSION`: Measure expression `{[DataObject].[Column]}` \
references unknown column.
  Fix: Check column name in the expression.
- `UNKNOWN_MEASURE_REF`: Metric expression `{[Measure Name]}` references unknown measure.
  Fix: Check measure name in the expression.
- `UNKNOWN_MEASURE`: Query references missing measure.
  Fix: Check measure name in query select.
- `UNKNOWN_DIMENSION`: Query references missing dimension.
  Fix: Check dimension name in query select.
- `UNKNOWN_JOIN_TARGET`: `joinTo` references unknown data object.
  Fix: Check `joinTo` value matches a data object name.
- `UNKNOWN_JOIN_COLUMN`: Join column not found in data object.
  Fix: Check `columnsFrom`/`columnsTo` column names exist.
- `UNKNOWN_PATH_NAME`: `usePathNames` references non-existent path.
  Fix: Check source, target, and pathName match a secondary join.

## Semantic Errors

- `DUPLICATE_IDENTIFIER`: Duplicate name across data objects, dimensions, measures, or metrics.
  Fix: All names must be unique across the model.
- `CYCLIC_JOIN`: Join graph contains a cycle.
  Fix: Remove circular join references.
- `MULTIPATH_JOIN`: Multiple join paths between two data objects.
  Fix: Make join graph unambiguous, or use secondary joins with pathName.
- `JOIN_COLUMN_COUNT_MISMATCH`: `columnsFrom` and `columnsTo` have different lengths.
  Fix: Ensure both lists have the same number of entries.

## Secondary Join Errors

- `SECONDARY_JOIN_MISSING_PATH_NAME`: Secondary join has no `pathName`.
  Fix: Add a `pathName` to the secondary join.
- `DUPLICATE_JOIN_PATH_NAME`: Duplicate `pathName` for the same (source, target) pair.
  Fix: Use a unique `pathName` per (source, target) pair.

## Resolution Errors (at query time)

- `AMBIGUOUS_JOIN`: Multiple join paths found during query resolution.
  Fix: Make join graph unambiguous or use `usePathNames`.
- `INVALID_METRIC_EXPRESSION`: Metric expression could not be parsed.
  Fix: Use `{[Measure Name]}` syntax in metric expressions.
- `INVALID_FILTER_OPERATOR`: Unrecognised filter operator in query.
  Fix: Use a supported operator (equals, gt, gte, lt, lte, inlist, etc.).
- `INVALID_RELATIVE_FILTER`: Malformed relative time filter.
  Fix: Check unit (day/week/month/year), count, direction, include_current.

## Debugging Steps

1. Run `validate_model(model_yaml)` to check for errors.
2. Read the error code and message carefully.
3. Fix the YAML and re-validate.
4. Once valid, use `load_model(model_yaml)` to load it.
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the MCP server using settings from environment / .env file."""
    settings = Settings()

    logging.basicConfig(level=settings.log_level.upper())
    logger.info(
        "OrionBelt MCP Server v%s starting (transport=%s)",
        __version__,
        settings.mcp_transport,
    )

    global _session_manager  # noqa: PLW0603
    _session_manager = SessionManager(
        ttl_seconds=settings.session_ttl_seconds,
        cleanup_interval=settings.session_cleanup_interval,
    )
    _session_manager.start()

    try:
        if settings.mcp_transport == "stdio":
            mcp.run(transport="stdio")
        else:
            mcp.run(
                transport=settings.mcp_transport,
                host=settings.mcp_server_host,
                port=settings.mcp_server_port,
                log_level=settings.log_level.lower(),
            )
    finally:
        _session_manager.stop()


if __name__ == "__main__":
    main()
