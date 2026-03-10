"""API request/response Pydantic schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from orionbelt.models.query import QueryObject


class ResolvedInfoResponse(BaseModel):
    """Information about what was resolved during compilation."""

    fact_tables: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    measures: list[str] = Field(default_factory=list)


class QueryCompileResponse(BaseModel):
    """Response body for POST /query/sql."""

    sql: str
    dialect: str
    resolved: ResolvedInfoResponse
    warnings: list[str] = Field(default_factory=list)
    sql_valid: bool = True


class ValidateRequest(BaseModel):
    """Request body for POST /validate."""

    model_yaml: str = Field(
        description="YAML semantic model content to validate", max_length=5_000_000
    )


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


class ErrorResponse(BaseModel):
    """Standard error response per spec §7.5."""

    error: str
    message: str
    path: str | None = None


class DialectInfo(BaseModel):
    """Information about a supported dialect."""

    name: str
    capabilities: dict[str, bool] = Field(default_factory=dict)


class DialectListResponse(BaseModel):
    """Response for GET /dialects."""

    dialects: list[DialectInfo] = Field(default_factory=list)


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = "ok"
    version: str = ""


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


class SessionListResponse(BaseModel):
    """Response for GET /sessions."""

    sessions: list[SessionResponse]


class ModelLoadRequest(BaseModel):
    """Request body for POST /sessions/{session_id}/models."""

    model_yaml: str = Field(description="OBML YAML content", max_length=5_000_000)


class ModelLoadResponse(BaseModel):
    """Response for POST /sessions/{session_id}/models."""

    model_id: str
    data_objects: int
    dimensions: int
    measures: int
    metrics: int
    warnings: list[str] = Field(default_factory=list)


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
    dialect: str = Field(default="postgres")


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
