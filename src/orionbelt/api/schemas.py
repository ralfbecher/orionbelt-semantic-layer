"""API request/response Pydantic schemas."""

from __future__ import annotations

import json
from datetime import datetime

from pydantic import BaseModel, Field, model_validator

from orionbelt.models.query import QueryObject


class ResolvedInfoResponse(BaseModel):
    """Information about what was resolved during compilation."""

    fact_tables: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    measures: list[str] = Field(default_factory=list)


class ExplainJoinResponse(BaseModel):
    """Explanation of a single join step."""

    from_object: str
    to_object: str
    join_columns: list[str] = Field(default_factory=list)
    reason: str


class ExplainCflLegResponse(BaseModel):
    """Explanation of a single CFL leg."""

    measure_source: str
    common_root: str
    reason: str
    measures: list[str] = Field(default_factory=list)
    joins: list[str] = Field(default_factory=list)


class ExplainPlanResponse(BaseModel):
    """Full query plan explanation with reasoning."""

    planner: str
    planner_reason: str
    base_object: str
    base_object_reason: str
    joins: list[ExplainJoinResponse] = Field(default_factory=list)
    where_filter_count: int = 0
    having_filter_count: int = 0
    has_totals: bool = False
    cfl_legs: list[ExplainCflLegResponse] = Field(default_factory=list)


class QueryCompileResponse(BaseModel):
    """Response body for POST /query/sql."""

    sql: str
    dialect: str
    resolved: ResolvedInfoResponse
    warnings: list[str] = Field(default_factory=list)
    sql_valid: bool = True
    explain: ExplainPlanResponse | None = None


class ColumnMetadata(BaseModel):
    """Metadata for a single result column."""

    name: str
    type: str = Field(description="Type hint: string, number, datetime, binary")
    format: str | None = Field(
        default=None,
        description="Display format pattern from model (e.g. '#,##0.00', '0.00%')",
    )


class QueryExecuteResponse(BaseModel):
    """Response body for POST /query/execute."""

    sql: str
    dialect: str
    columns: list[ColumnMetadata] = Field(default_factory=list)
    rows: list[list[object]] = Field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0
    timezone: str | None = Field(
        default=None,
        description="IANA timezone used to label naive timestamps in results",
    )
    resolved: ResolvedInfoResponse = Field(default_factory=ResolvedInfoResponse)
    warnings: list[str] = Field(default_factory=list)
    sql_valid: bool = True
    explain: ExplainPlanResponse | None = None


class SessionQueryExecuteRequest(BaseModel):
    """Request body for POST /sessions/{session_id}/query/execute."""

    model_id: str
    query: QueryObject
    dialect: str | None = Field(
        default=None,
        description=(
            "SQL dialect. Resolution: explicit value → model.settings.defaultDialect → "
            "DB_VENDOR env → 'postgres'."
        ),
    )


class ValidateRequest(BaseModel):
    """Request body for POST /validate."""

    model_yaml: str | None = Field(
        default=None,
        description="OBML model as YAML string (provide model_yaml OR model_json)",
        max_length=5_000_000,
    )
    model_json: dict[str, object] | str | None = Field(
        default=None,
        description="OBML model as JSON object or JSON string (auto-parsed)",
    )
    extends: list[str] | None = Field(
        default=None,
        description="Optional inline YAML strings of analytical fragments to merge",
    )
    inherits: str | None = Field(
        default=None,
        description="Optional model ID of an already-loaded parent model in the session",
    )

    @model_validator(mode="after")
    def _parse_model_json_string(self) -> ValidateRequest:
        if isinstance(self.model_json, str):
            self.model_json = json.loads(self.model_json)
        return self


class ValidateResponse(BaseModel):
    """Response body for POST /validate."""

    valid: bool
    errors: list[ErrorDetail] = Field(default_factory=list)
    warnings: list[ErrorDetail] = Field(default_factory=list)


class ErrorDetail(BaseModel):
    """A single validation error detail."""

    code: str
    message: str
    path: str | None = None


class DialectInfo(BaseModel):
    """Information about a supported dialect."""

    name: str
    capabilities: dict[str, bool] = Field(default_factory=dict)
    unsupported_aggregations: list[str] = Field(default_factory=list)


class DialectListResponse(BaseModel):
    """Response for GET /dialects."""

    dialects: list[DialectInfo] = Field(default_factory=list)


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = "ok"
    version: str = ""


class FlightSettingsInfo(BaseModel):
    """Arrow Flight SQL server status (included when FLIGHT_ENABLED=true)."""

    enabled: bool = True
    port: int = 8815
    auth_mode: str = "none"
    db_vendor: str = "duckdb"


class ModelSettingsInfo(BaseModel):
    """Model-level ``settings:`` block from the loaded OBML model."""

    model_config = {"populate_by_name": True}

    default_numeric_data_type: str | None = Field(
        default=None,
        alias="defaultNumericDataType",
        description="Default decimal(p, s) type used when a column omits dataType",
    )
    default_timezone: str | None = Field(
        default=None,
        alias="defaultTimezone",
        description="IANA timezone applied to naive timestamps in results",
    )
    override_database_timezone: bool = Field(
        default=False,
        alias="overrideDatabaseTimezone",
        description="When true, model timezone wins over the DB session timezone",
    )
    default_dialect: str | None = Field(
        default=None,
        alias="defaultDialect",
        description="SQL dialect used when callers omit `dialect` on query requests",
    )


class TimezoneResolutionInfo(BaseModel):
    """Timezone resolution chain for naive timestamp coercion at execute time.

    Effective timezone (in priority order):
    - ``override_database_timezone`` true: ``model`` → ``host`` → UTC
    - else: ``database`` (if detected) → ``model`` → ``host`` → UTC

    ``database`` is populated lazily on first query execution per dialect; it
    is ``null`` until then. Reading this endpoint never probes the database.
    """

    model: str | None = Field(default=None, description="settings.defaultTimezone")
    host: str | None = Field(default=None, description="OS / process timezone")
    database: str | None = Field(
        default=None,
        description="Detected database session timezone (null if not probed yet)",
    )
    effective: str = Field(
        description="The timezone that resolve_timezone() returns at this moment"
    )
    override_database_timezone: bool = Field(
        default=False,
        description="Whether the model overrides the DB session timezone",
    )
    now: str = Field(description="Current wall-clock time in the effective TZ (ISO 8601)")
    utc: str = Field(description="Current UTC time (ISO 8601, Z suffix) for reference")
    database_detected: bool = Field(
        default=False,
        description="Whether DB session TZ detection has run for this dialect",
    )
    database_raw: str | None = Field(
        default=None,
        description=(
            "Raw cached DB session TZ value (for diagnostics). "
            "When `database_detected` is true and this is null, detection ran "
            "but did not store a value (query failed or returned SYSTEM)."
        ),
    )


class DialectResolutionInfo(BaseModel):
    """Dialect resolution chain for query compilation.

    Order on each request: explicit ``dialect`` body field →
    ``settings.defaultDialect`` → ``DB_VENDOR`` env → ``"postgres"``.
    ``effective`` here is what gets used when a caller omits ``dialect``.
    """

    model: str | None = Field(default=None, description="settings.defaultDialect")
    env: str | None = Field(default=None, description="DB_VENDOR env (server config)")
    effective: str = Field(description="Dialect used when request omits `dialect`")


class OneshotBatchLimits(BaseModel):
    """Server-side limits for the one-shot batch endpoint."""

    max_queries: int
    max_parallelism: int
    default_timeout_ms: int
    batch_timeout_ms: int


class SettingsResponse(BaseModel):
    """Response for GET /settings — public configuration for clients."""

    version: str = Field(default="", description="OrionBelt Semantic Layer release version")
    api_version: str = Field(default="v1", description="REST API version prefix")
    single_model_mode: bool = False
    model_yaml: str | None = Field(
        default=None,
        description="Pre-loaded OBML YAML content (only when single_model_mode is true)",
    )
    session_ttl_seconds: int = 1800
    session_max_age_seconds: int = Field(
        default=86400,
        description="Absolute max session lifetime in seconds",
    )
    max_sessions: int = Field(
        default=500,
        description="Global concurrent session cap",
    )
    max_models_per_session: int = Field(
        default=10,
        description="Maximum models per session",
    )
    query_execute: bool = Field(
        default=False,
        description="Whether POST /query/execute is available",
    )
    flight: FlightSettingsInfo | None = Field(
        default=None,
        description="Arrow Flight SQL server info (present only when Flight is enabled)",
    )
    model_settings: ModelSettingsInfo | None = Field(
        default=None,
        description="Loaded model's `settings:` block (single-model mode only)",
    )
    timezone: TimezoneResolutionInfo | None = Field(
        default=None,
        description="Timezone resolution chain (single-model mode only)",
    )
    dialect: DialectResolutionInfo | None = Field(
        default=None,
        description="SQL dialect resolution chain",
    )
    oneshot_batch: OneshotBatchLimits | None = Field(
        default=None,
        description="Limits for POST /v1/oneshot/batch",
    )


# ---------------------------------------------------------------------------
# Session schemas
# ---------------------------------------------------------------------------


class SessionCreateRequest(BaseModel):
    """Request body for POST /sessions."""

    metadata: dict[str, str] = Field(default_factory=dict)


class SessionResponse(BaseModel):
    """Single session info."""

    session_id: str
    created_at: datetime
    last_accessed_at: datetime
    model_count: int
    metadata: dict[str, str] = Field(default_factory=dict)
    expires_at: datetime = Field(description="Idle TTL deadline (refreshed on each access)")
    max_expires_at: datetime = Field(description="Absolute lifetime deadline (fixed at creation)")


class SessionListResponse(BaseModel):
    """Response for GET /sessions."""

    sessions: list[SessionResponse]


class ModelLoadRequest(BaseModel):
    """Request body for POST /sessions/{session_id}/models."""

    model_yaml: str | None = Field(
        default=None,
        description="OBML model as YAML string (provide model_yaml OR model_json)",
        max_length=5_000_000,
    )
    model_json: dict[str, object] | str | None = Field(
        default=None,
        description="OBML model as JSON object or JSON string (auto-parsed)",
    )
    extends: list[str] | None = Field(
        default=None,
        description="Optional inline YAML strings of analytical fragments to merge",
    )
    inherits: str | None = Field(
        default=None,
        description="Optional model ID of an already-loaded parent model in the session",
    )
    dedup: bool = Field(
        default=True,
        description=(
            "When True (default), identical OBML content already loaded in this session "
            "reuses the existing model_id (response.model_load == 'reused'). "
            "When False, always loads fresh."
        ),
    )

    @model_validator(mode="after")
    def _parse_model_json_string(self) -> ModelLoadRequest:
        if isinstance(self.model_json, str):
            self.model_json = json.loads(self.model_json)
        return self


class ModelLoadResponse(BaseModel):
    """Response for POST /sessions/{session_id}/models."""

    model_id: str
    data_objects: int
    dimensions: int
    measures: int
    metrics: int
    warnings: list[str] = Field(default_factory=list)
    model_load: str = Field(
        default="fresh",
        description=(
            "Whether the load parsed a fresh model or reused an existing one. "
            "Values: 'fresh' | 'reused'."
        ),
    )


class ModelSummaryResponse(BaseModel):
    """Short model summary for listing."""

    model_id: str
    data_objects: int
    dimensions: int
    measures: int
    metrics: int


class SessionQueryRequest(BaseModel):
    """Request body for POST /sessions/{session_id}/query/sql."""

    model_id: str
    query: QueryObject
    dialect: str | None = Field(
        default=None,
        description=(
            "SQL dialect. Resolution: explicit value → model.settings.defaultDialect → "
            "DB_VENDOR env → 'postgres'."
        ),
    )


class DiagramResponse(BaseModel):
    """Response for GET /sessions/{session_id}/models/{model_id}/diagram/er."""

    mermaid: str = Field(description="Mermaid ER diagram script")


# ---------------------------------------------------------------------------
# OSI ↔ OBML conversion schemas
# ---------------------------------------------------------------------------


class ConvertRequest(BaseModel):
    """Request body for POST /convert/osi-to-obml."""

    input_yaml: str = Field(description="Source YAML content to convert", max_length=5_000_000)


class OBMLtoOSIRequest(ConvertRequest):
    """Request body for POST /convert/obml-to-osi."""

    model_name: str = Field(default="semantic_model", description="Name for the OSI model")
    model_description: str = Field(default="", description="Description for the OSI model")
    ai_instructions: str = Field(default="", description="AI instructions for the OSI model")


class ValidationDetail(BaseModel):
    """Validation result from conversion."""

    schema_valid: bool = True
    semantic_valid: bool = True
    schema_errors: list[str] = Field(default_factory=list)
    semantic_errors: list[str] = Field(default_factory=list)
    semantic_warnings: list[str] = Field(default_factory=list)


class ConvertResponse(BaseModel):
    """Response body for conversion endpoints."""

    output_yaml: str = Field(description="Converted YAML content")
    warnings: list[str] = Field(default_factory=list, description="Conversion warnings")
    validation: ValidationDetail = Field(
        default_factory=ValidationDetail, description="Validation results"
    )


# ---------------------------------------------------------------------------
# Model discovery schemas
# ---------------------------------------------------------------------------


class ColumnDetail(BaseModel):
    """Detail of a data object column."""

    name: str
    code: str
    abstract_type: str
    num_class: str | None = None
    description: str | None = None
    comment: str | None = None
    owner: str | None = None
    synonyms: list[str] = Field(default_factory=list)


class DataObjectDetail(BaseModel):
    """Detail of a data object."""

    name: str
    code: str
    database: str
    schema_name: str = Field(alias="schema")
    columns: list[ColumnDetail] = Field(default_factory=list)
    join_targets: list[str] = Field(default_factory=list)
    description: str | None = None
    comment: str | None = None
    owner: str | None = None
    synonyms: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class DimensionDetail(BaseModel):
    """Detail of a dimension."""

    name: str
    data_object: str
    column: str
    result_type: str
    time_grain: str | None = None
    via: str | None = None
    description: str | None = None
    format: str | None = None
    owner: str | None = None
    synonyms: list[str] = Field(default_factory=list)


class MeasureDetail(BaseModel):
    """Detail of a measure."""

    model_config = {"populate_by_name": True}

    name: str
    result_type: str
    aggregation: str
    expression: str | None = None
    columns: list[dict[str, str]] = Field(default_factory=list)
    distinct: bool = False
    total: bool = False
    description: str | None = None
    format: str | None = None
    data_type: str | None = Field(default=None, alias="dataType")
    owner: str | None = None
    synonyms: list[str] = Field(default_factory=list)


class MetricDetail(BaseModel):
    """Detail of a metric."""

    name: str
    type: str = "derived"
    expression: str | None = None
    measure: str | None = None
    time_dimension: str | None = Field(None, alias="timeDimension")
    component_measures: list[str] = Field(default_factory=list)
    description: str | None = None
    format: str | None = None
    data_type: str | None = Field(default=None, alias="dataType")
    owner: str | None = None
    synonyms: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class ModelFilterDetail(BaseModel):
    """Detail of a static model filter."""

    data_object: str
    column: str
    operator: str
    value: str | int | float | bool | None = None
    values: list[str | int | float | bool] = Field(default_factory=list)


class SchemaResponse(BaseModel):
    """Response for GET /schema — full model structure."""

    model_id: str
    version: float = 1.0
    description: str | None = None
    owner: str | None = None
    data_objects: list[DataObjectDetail] = Field(default_factory=list)
    dimensions: list[DimensionDetail] = Field(default_factory=list)
    measures: list[MeasureDetail] = Field(default_factory=list)
    metrics: list[MetricDetail] = Field(default_factory=list)
    filters: list[ModelFilterDetail] = Field(default_factory=list)
    extends: list[str] = Field(default_factory=list)
    inherits: str | None = None


class ExplainLineageItem(BaseModel):
    """A single item in the lineage chain."""

    type: str
    name: str
    detail: str | None = None


class ExplainResponse(BaseModel):
    """Response for GET /explain/{name} — lineage & composition."""

    name: str
    type: str
    lineage: list[ExplainLineageItem] = Field(default_factory=list)


class SearchRequest(BaseModel):
    """Request body for POST /find."""

    query: str = Field(description="Search term")
    types: list[str] = Field(
        default_factory=lambda: ["dimension", "measure", "metric", "data_object"],
        description="Object types to search (dimension, measure, metric, data_object)",
    )


class SearchResultItem(BaseModel):
    """A single search result."""

    type: str
    name: str
    match_field: str
    score: float = 1.0


class SearchResponse(BaseModel):
    """Response for POST /find."""

    results: list[SearchResultItem] = Field(default_factory=list)


class JoinEdge(BaseModel):
    """A single edge in the join graph."""

    from_object: str
    to_object: str
    cardinality: str
    columns_from: list[str] = Field(default_factory=list)
    columns_to: list[str] = Field(default_factory=list)
    secondary: bool = False
    path_name: str | None = None


class JoinGraphResponse(BaseModel):
    """Response for GET /join-graph — adjacency list."""

    nodes: list[str] = Field(default_factory=list)
    edges: list[JoinEdge] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# OBSL graph / SPARQL schemas
# ---------------------------------------------------------------------------


class SPARQLRequest(BaseModel):
    """Request body for POST /sparql."""

    query: str = Field(description="SPARQL query (SELECT or ASK only)", max_length=100_000)


class SPARQLResponse(BaseModel):
    """Response body for POST /sparql."""

    type: str = Field(description="Query type: select or ask")
    variables: list[str] = Field(default_factory=list, description="Binding variable names")
    results: list[dict[str, str | None]] = Field(
        default_factory=list, description="Rows of variable bindings"
    )
    boolean: bool | None = Field(default=None, description="ASK query result")


# ---------------------------------------------------------------------------
# One-shot batch schemas (PLAN_oneshot_batch.md)
# ---------------------------------------------------------------------------


class OneshotBatchQueryItem(BaseModel):
    """A single query in a one-shot batch."""

    id: str = Field(description="Caller-provided ID, must be unique within the batch")
    query: QueryObject
    execute: bool | None = Field(
        default=None,
        description="Per-query override for compile-only vs. execute (default inherits batch).",
    )
    dialect: str | None = Field(
        default=None,
        description="Per-query dialect override (default inherits batch).",
    )


class OneshotBatchRequest(BaseModel):
    """Request body for POST /v1/oneshot/batch."""

    session_id: str | None = Field(
        default=None,
        description="Existing session to use. If omitted, a new session is created.",
    )
    model_yaml: str | None = Field(
        default=None,
        description=(
            "OBML YAML. Mutually exclusive with `model_id`. One of them must be provided."
        ),
        max_length=5_000_000,
    )
    model_id: str | None = Field(
        default=None,
        description=(
            "ID of an already-loaded model in the given session. "
            "Mutually exclusive with `model_yaml`."
        ),
    )
    queries: list[OneshotBatchQueryItem] = Field(
        description="List of queries to run. Min 1, server caps maximum.",
        min_length=1,
    )
    dialect: str | None = Field(
        default=None,
        description="Default dialect for all queries in the batch.",
    )
    execute: bool = Field(
        default=False,
        description="Default execute flag for all queries.",
    )
    max_parallelism: int | None = Field(
        default=None,
        description="Max concurrent query executions. Server caps this.",
        ge=1,
    )
    fail_fast: bool = Field(
        default=False,
        description="If true, cancel remaining queries on first failure.",
    )
    persist_model: bool = Field(
        default=False,
        description=(
            "If true, a model loaded via `model_yaml` is kept in the session after the call. "
            "Ignored when `model_id` is supplied."
        ),
    )
    dedup: bool = Field(
        default=True,
        description=(
            "When true, identical OBML content already loaded in the resolved session reuses "
            "the existing model_id. When false, always loads fresh. Ignored when `model_id` "
            "is supplied."
        ),
    )

    @model_validator(mode="after")
    def _validate_request(self) -> OneshotBatchRequest:
        # Exactly one of model_yaml / model_id must be provided.
        has_yaml = bool(self.model_yaml)
        has_id = bool(self.model_id)
        if has_yaml and has_id:
            raise ValueError("Provide either model_yaml or model_id, not both")
        if not has_yaml and not has_id:
            raise ValueError("Provide either model_yaml or model_id")
        # Reject duplicate query IDs early so callers get a clear error.
        seen: set[str] = set()
        for q in self.queries:
            if q.id in seen:
                raise ValueError(f"Duplicate query id: '{q.id}'")
            seen.add(q.id)
        return self


class OneshotBatchQueryError(BaseModel):
    """Error envelope for a single failed query in a batch."""

    code: str
    message: str
    path: str | None = None
    hint: str | None = None


class OneshotBatchQueryResult(BaseModel):
    """Result of a single query in a one-shot batch."""

    id: str
    status: str = Field(description="One of: 'ok', 'error', 'cancelled'")
    sql: str | None = None
    dialect: str | None = None
    sql_valid: bool | None = None
    explain: ExplainPlanResponse | None = None
    columns: list[ColumnMetadata] | None = None
    rows: list[list[object]] | None = None
    row_count: int | None = None
    execution_time_ms: float | None = None
    executed: bool | None = Field(
        default=None,
        description="Whether this query executed (vs compile-only). Only set when status='ok'.",
    )
    warnings: list[str] = Field(default_factory=list)
    error: OneshotBatchQueryError | None = None


class OneshotBatchResponse(BaseModel):
    """Response body for POST /v1/oneshot/batch."""

    session_id: str
    model_id: str
    model_persisted: bool
    model_load: str = Field(
        default="fresh",
        description=(
            "How the model was acquired: 'fresh' (parsed and loaded), 'reused' (dedup hit), "
            "or 'referenced' (existing model_id supplied by caller)."
        ),
    )
    results: list[OneshotBatchQueryResult] = Field(default_factory=list)
    batch_warnings: list[str] = Field(default_factory=list)
