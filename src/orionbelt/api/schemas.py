"""API request/response Pydantic schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from orionbelt.models.query import QueryObject


class ResolvedInfoResponse(BaseModel):
    """Information about what was resolved during compilation."""

    fact_tables: list[str] = []
    dimensions: list[str] = []
    measures: list[str] = []


class QueryCompileResponse(BaseModel):
    """Response body for POST /query/sql."""

    sql: str
    dialect: str
    resolved: ResolvedInfoResponse
    warnings: list[str] = []
    sql_valid: bool = True


class ValidateRequest(BaseModel):
    """Request body for POST /validate."""

    model_yaml: str = Field(description="YAML semantic model content to validate")


class ValidateResponse(BaseModel):
    """Response body for POST /validate."""

    valid: bool
    errors: list[ErrorDetail] = []
    warnings: list[ErrorDetail] = []


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
    capabilities: dict[str, bool] = {}


class DialectListResponse(BaseModel):
    """Response for GET /dialects."""

    dialects: list[DialectInfo] = []


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

    model_yaml: str = Field(description="OBML YAML content")


class ModelLoadResponse(BaseModel):
    """Response for POST /sessions/{session_id}/models."""

    model_id: str
    data_objects: int
    dimensions: int
    measures: int
    metrics: int
    warnings: list[str] = []


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
