"""Session-scoped endpoints for model management, validation, and query."""

from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException

from orionbelt.api.deps import get_session_manager, is_session_list_disabled
from orionbelt.api.schemas import (
    DiagramResponse,
    ErrorDetail,
    ModelLoadRequest,
    ModelLoadResponse,
    ModelSummaryResponse,
    QueryCompileResponse,
    ResolvedInfoResponse,
    SessionCreateRequest,
    SessionListResponse,
    SessionQueryRequest,
    SessionResponse,
    ValidateRequest,
    ValidateResponse,
)
from orionbelt.compiler.fanout import FanoutError
from orionbelt.compiler.resolution import ResolutionError
from orionbelt.dialect.registry import UnsupportedDialectError
from orionbelt.service.diagram import generate_mermaid_er
from orionbelt.service.model_store import ModelStore, ModelValidationError
from orionbelt.service.session_manager import SessionInfo, SessionManager, SessionNotFoundError

router = APIRouter()


# -- helpers -----------------------------------------------------------------


def _get_store(session_id: str, mgr: SessionManager) -> ModelStore:
    """Resolve session_id to ModelStore, raise 404 if missing/expired."""
    try:
        return mgr.get_store(session_id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found") from None


def _session_response(info: SessionInfo) -> SessionResponse:
    """Convert a SessionInfo dataclass to a Pydantic response."""
    d = asdict(info)
    return SessionResponse(**d)


# -- session CRUD ------------------------------------------------------------


@router.post("", response_model=SessionResponse, status_code=201)
async def create_session(
    body: SessionCreateRequest | None = None,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> SessionResponse:
    """Create a new session."""
    metadata = body.metadata if body else {}
    info = mgr.create_session(metadata=metadata)
    return _session_response(info)


@router.get("", response_model=SessionListResponse)
async def list_sessions(
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> SessionListResponse:
    """List all active sessions."""
    if is_session_list_disabled():
        raise HTTPException(status_code=403, detail="Session listing is disabled")
    sessions = mgr.list_sessions()
    return SessionListResponse(sessions=[_session_response(s) for s in sessions])


@router.get("/{session_id}", response_model=SessionResponse)
async def get_session(
    session_id: str,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> SessionResponse:
    """Get info for a specific session."""
    try:
        info = mgr.get_session(session_id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found") from None
    return _session_response(info)


@router.delete("/{session_id}", status_code=204)
async def close_session(
    session_id: str,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> None:
    """Close a session and release its resources."""
    try:
        mgr.close_session(session_id)
    except SessionNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found") from None


# -- model management -------------------------------------------------------


@router.post(
    "/{session_id}/models",
    response_model=ModelLoadResponse,
    status_code=201,
)
async def load_model(
    session_id: str,
    body: ModelLoadRequest,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> ModelLoadResponse:
    """Load an OBML model into a session."""
    store = _get_store(session_id, mgr)
    try:
        result = store.load_model(body.model_yaml)
    except ModelValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Invalid OBML model: parsing or validation failed",
                "errors": [
                    {"code": e.code, "message": e.message, "path": e.path}
                    for e in exc.errors
                ],
                "warnings": [
                    {"code": w.code, "message": w.message, "path": w.path}
                    for w in exc.warnings
                ],
            },
        ) from None
    return ModelLoadResponse(
        model_id=result.model_id,
        data_objects=result.data_objects,
        dimensions=result.dimensions,
        measures=result.measures,
        metrics=result.metrics,
        warnings=result.warnings,
    )


@router.get("/{session_id}/models", response_model=list[ModelSummaryResponse])
async def list_models(
    session_id: str,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> list[ModelSummaryResponse]:
    """List all models loaded in a session."""
    store = _get_store(session_id, mgr)
    return [
        ModelSummaryResponse(
            model_id=m.model_id,
            data_objects=m.data_objects,
            dimensions=m.dimensions,
            measures=m.measures,
            metrics=m.metrics,
        )
        for m in store.list_models()
    ]


@router.get("/{session_id}/models/{model_id}")
async def describe_model(
    session_id: str,
    model_id: str,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> dict:  # type: ignore[type-arg]
    """Describe a model loaded in a session."""
    store = _get_store(session_id, mgr)
    try:
        desc = store.describe(model_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Model '{model_id}' not found") from None
    return asdict(desc)


@router.get(
    "/{session_id}/models/{model_id}/diagram/er",
    response_model=DiagramResponse,
)
async def model_diagram_er(
    session_id: str,
    model_id: str,
    show_columns: bool = True,
    theme: str = "default",
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> DiagramResponse:
    """Generate a Mermaid ER diagram for a loaded model."""
    store = _get_store(session_id, mgr)
    try:
        model = store.get_model(model_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Model '{model_id}' not found") from None
    mermaid = generate_mermaid_er(model, show_columns=show_columns, theme=theme)
    return DiagramResponse(mermaid=mermaid)


@router.delete("/{session_id}/models/{model_id}", status_code=204)
async def remove_model(
    session_id: str,
    model_id: str,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> None:
    """Remove a model from a session."""
    store = _get_store(session_id, mgr)
    try:
        store.remove_model(model_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Model '{model_id}' not found") from None


# -- validation & query -----------------------------------------------------


@router.post("/{session_id}/validate", response_model=ValidateResponse)
async def validate_model(
    session_id: str,
    body: ValidateRequest,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> ValidateResponse:
    """Validate OBML YAML within a session context."""
    store = _get_store(session_id, mgr)
    summary = store.validate(body.model_yaml)
    return ValidateResponse(
        valid=summary.valid,
        errors=[ErrorDetail(code=e.code, message=e.message, path=e.path) for e in summary.errors],
        warnings=[
            ErrorDetail(code=w.code, message=w.message, path=w.path) for w in summary.warnings
        ],
    )


@router.post("/{session_id}/query/sql", response_model=QueryCompileResponse)
async def compile_query(
    session_id: str,
    body: SessionQueryRequest,
    mgr: SessionManager = Depends(get_session_manager),  # noqa: B008
) -> QueryCompileResponse:
    """Compile a query against a model loaded in a session."""
    store = _get_store(session_id, mgr)
    try:
        result = store.compile_query(body.model_id, body.query, body.dialect)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Model '{body.model_id}' not found"
        ) from None
    except UnsupportedDialectError:
        raise HTTPException(
            status_code=400, detail=f"Unsupported dialect: '{body.dialect}'"
        ) from None
    except ResolutionError:
        raise HTTPException(
            status_code=422,
            detail="Query resolution failed: check dimensions, measures, and filters",
        ) from None
    except FanoutError:
        raise HTTPException(
            status_code=422, detail="Query would cause row fanout due to reversed many-to-one joins"
        ) from None
    return QueryCompileResponse(
        sql=result.sql,
        dialect=result.dialect,
        resolved=ResolvedInfoResponse(
            fact_tables=result.resolved.fact_tables,
            dimensions=result.resolved.dimensions,
            measures=result.resolved.measures,
        ),
        warnings=result.warnings,
        sql_valid=result.sql_valid,
    )
