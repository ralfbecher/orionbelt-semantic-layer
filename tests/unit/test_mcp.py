"""Unit tests for MCP server tools — direct function calls, no transport.

FastMCP's ``@mcp.tool`` wraps functions in ``FunctionTool`` objects.  We call
the underlying function via ``.fn`` to test the business logic directly.
"""

from __future__ import annotations

import json

import pytest
from fastmcp.exceptions import ToolError

# Import the module-level state so we can swap it between tests
import orionbelt.mcp.server as mcp_mod
from orionbelt.mcp.server import (
    OBML_REFERENCE,
    close_session,
    compile_query,
    create_session,
    describe_model,
    list_dialects,
    list_models,
    list_sessions,
    load_model,
    obml_reference,
    validate_model,
)
from orionbelt.service.session_manager import SessionManager
from tests.conftest import SAMPLE_MODEL_YAML

# Unwrap FunctionTool → raw functions
_load_model = load_model.fn
_validate_model = validate_model.fn
_describe_model = describe_model.fn
_compile_query = compile_query.fn
_list_models = list_models.fn
_list_dialects = list_dialects.fn
_create_session = create_session.fn
_close_session = close_session.fn
_list_sessions = list_sessions.fn


@pytest.fixture(autouse=True)
def _fresh_session_manager() -> None:
    """Give each test a fresh SessionManager with a default session."""
    mcp_mod._session_manager = SessionManager(ttl_seconds=3600, cleanup_interval=9999)


# ---------------------------------------------------------------------------
# Session tools
# ---------------------------------------------------------------------------


class TestCreateSession:
    def test_create(self) -> None:
        result = _create_session()
        assert "session_id:" in result
        assert "created_at:" in result

    def test_create_with_metadata(self) -> None:
        result = _create_session(metadata_json='{"env": "test"}')
        assert "session_id:" in result

    def test_create_bad_metadata(self) -> None:
        with pytest.raises(ToolError, match="Invalid metadata JSON"):
            _create_session(metadata_json="{bad")


class TestCloseSession:
    def test_close(self) -> None:
        result = _create_session()
        sid = _extract_session_id(result)
        close_result = _close_session(sid)
        assert "closed" in close_result

    def test_close_missing(self) -> None:
        with pytest.raises(ToolError, match="not found"):
            _close_session("nonexist123")


class TestListSessions:
    def test_empty(self) -> None:
        result = _list_sessions()
        assert "No active sessions" in result

    def test_after_create(self) -> None:
        _create_session()
        result = _list_sessions()
        assert "Active sessions" in result


# ---------------------------------------------------------------------------
# load_model (with default session)
# ---------------------------------------------------------------------------


class TestLoadModel:
    def test_load_valid(self) -> None:
        result = _load_model(SAMPLE_MODEL_YAML)
        assert "model_id:" in result
        assert "data objects: 2" in result
        assert "dimensions:   1" in result
        assert "measures:     3" in result

    def test_load_invalid_raises_tool_error(self) -> None:
        bad_yaml = """\
version: 1.0
dataObjects:
  T:
    code: T
    database: DB
    schema: S
    columns:
      F1:
        code: COL
        abstractType: string
dimensions:
  D1:
    dataObject: MISSING
    column: F1
    resultType: string
"""
        with pytest.raises(ToolError, match="validation failed"):
            _load_model(bad_yaml)

    def test_load_with_session_id(self) -> None:
        sid = _extract_session_id(_create_session())
        result = _load_model(SAMPLE_MODEL_YAML, session_id=sid)
        assert "model_id:" in result

    def test_load_bad_session_id(self) -> None:
        with pytest.raises(ToolError, match="not found"):
            _load_model(SAMPLE_MODEL_YAML, session_id="nonexist123")


# ---------------------------------------------------------------------------
# validate_model
# ---------------------------------------------------------------------------


class TestValidateModel:
    def test_valid(self) -> None:
        result = _validate_model(SAMPLE_MODEL_YAML)
        assert "valid" in result.lower()

    def test_invalid(self) -> None:
        result = _validate_model("key: [unclosed")
        assert "YAML_PARSE_ERROR" in result


# ---------------------------------------------------------------------------
# describe_model
# ---------------------------------------------------------------------------


class TestDescribeModel:
    def test_describe(self) -> None:
        load_result = _load_model(SAMPLE_MODEL_YAML)
        model_id = _extract_model_id(load_result)
        result = _describe_model(model_id)
        assert "Customers" in result
        assert "Orders" in result
        assert "Customer Country" in result
        assert "Total Revenue" in result

    def test_describe_missing(self) -> None:
        with pytest.raises(ToolError, match="No model loaded"):
            _describe_model("nonexist")

    def test_describe_with_session_id(self) -> None:
        sid = _extract_session_id(_create_session())
        load_result = _load_model(SAMPLE_MODEL_YAML, session_id=sid)
        model_id = _extract_model_id(load_result)
        result = _describe_model(model_id, session_id=sid)
        assert "Customers" in result


# ---------------------------------------------------------------------------
# compile_query
# ---------------------------------------------------------------------------


class TestCompileQuery:
    def test_simple_mode(self) -> None:
        model_id = _load_and_get_id()
        result = _compile_query(
            model_id=model_id,
            dialect="postgres",
            dimensions=["Customer Country"],
            measures=["Total Revenue"],
        )
        assert "SELECT" in result
        assert "Dialect: postgres" in result

    def test_full_mode(self) -> None:
        model_id = _load_and_get_id()
        q = {
            "select": {
                "dimensions": ["Customer Country"],
                "measures": ["Total Revenue"],
            },
            "limit": 5,
        }
        result = _compile_query(
            model_id=model_id,
            query_json=json.dumps(q),
        )
        assert "SELECT" in result
        assert "LIMIT 5" in result

    def test_no_params_raises(self) -> None:
        model_id = _load_and_get_id()
        with pytest.raises(ToolError, match="Provide either"):
            _compile_query(model_id=model_id)

    def test_bad_json_raises(self) -> None:
        model_id = _load_and_get_id()
        with pytest.raises(ToolError, match="Invalid query JSON"):
            _compile_query(model_id=model_id, query_json="{bad")

    def test_missing_model_raises(self) -> None:
        with pytest.raises(ToolError, match="No model loaded"):
            _compile_query(model_id="nonexist", dimensions=["X"], measures=["Y"])

    def test_with_session_id(self) -> None:
        sid = _extract_session_id(_create_session())
        load_result = _load_model(SAMPLE_MODEL_YAML, session_id=sid)
        model_id = _extract_model_id(load_result)
        result = _compile_query(
            model_id=model_id,
            session_id=sid,
            dimensions=["Customer Country"],
            measures=["Total Revenue"],
        )
        assert "SELECT" in result


# ---------------------------------------------------------------------------
# list_models
# ---------------------------------------------------------------------------


class TestListModels:
    def test_empty(self) -> None:
        result = _list_models()
        assert "No models loaded" in result

    def test_after_load(self) -> None:
        _load_model(SAMPLE_MODEL_YAML)
        result = _list_models()
        assert "2 objects" in result

    def test_with_session_id(self) -> None:
        sid = _extract_session_id(_create_session())
        _load_model(SAMPLE_MODEL_YAML, session_id=sid)
        result = _list_models(session_id=sid)
        assert "2 objects" in result


# ---------------------------------------------------------------------------
# list_dialects
# ---------------------------------------------------------------------------


class TestCompileQueryWithUsePathNames:
    def test_simple_mode_with_use_path_names(self) -> None:
        secondary_yaml = """\
version: 1.0

dataObjects:
  Flights:
    code: FLIGHTS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Flight ID:
        code: FLIGHT_ID
        abstractType: string
      Departure Airport:
        code: DEP_AIRPORT
        abstractType: string
      Arrival Airport:
        code: ARR_AIRPORT
        abstractType: string
      Ticket Price:
        code: TICKET_PRICE
        abstractType: float
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

  Airports:
    code: AIRPORTS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Airport ID:
        code: AIRPORT_ID
        abstractType: string
      Airport Name:
        code: AIRPORT_NAME
        abstractType: string

dimensions:
  Airport Name:
    dataObject: Airports
    column: Airport Name
    resultType: string

measures:
  Total Ticket Price:
    columns:
      - dataObject: Flights
        column: Ticket Price
    resultType: float
    aggregation: sum
"""
        load_result = _load_model(secondary_yaml)
        model_id = _extract_model_id(load_result)
        result = _compile_query(
            model_id=model_id,
            dialect="postgres",
            dimensions=["Airport Name"],
            measures=["Total Ticket Price"],
            use_path_names=[{"source": "Flights", "target": "Airports", "pathName": "arrival"}],
        )
        assert "SELECT" in result
        assert "ARR_AIRPORT" in result

    def test_full_mode_with_use_path_names(self) -> None:
        secondary_yaml = """\
version: 1.0

dataObjects:
  Flights:
    code: FLIGHTS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Flight ID:
        code: FLIGHT_ID
        abstractType: string
      Departure Airport:
        code: DEP_AIRPORT
        abstractType: string
      Arrival Airport:
        code: ARR_AIRPORT
        abstractType: string
      Ticket Price:
        code: TICKET_PRICE
        abstractType: float
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

  Airports:
    code: AIRPORTS
    database: WAREHOUSE
    schema: PUBLIC
    columns:
      Airport ID:
        code: AIRPORT_ID
        abstractType: string
      Airport Name:
        code: AIRPORT_NAME
        abstractType: string

dimensions:
  Airport Name:
    dataObject: Airports
    column: Airport Name
    resultType: string

measures:
  Total Ticket Price:
    columns:
      - dataObject: Flights
        column: Ticket Price
    resultType: float
    aggregation: sum
"""
        load_result = _load_model(secondary_yaml)
        model_id = _extract_model_id(load_result)
        q = {
            "select": {
                "dimensions": ["Airport Name"],
                "measures": ["Total Ticket Price"],
            },
            "usePathNames": [{"source": "Flights", "target": "Airports", "pathName": "arrival"}],
        }
        result = _compile_query(
            model_id=model_id,
            query_json=json.dumps(q),
        )
        assert "SELECT" in result
        assert "ARR_AIRPORT" in result


class TestListDialects:
    def test_lists_all(self) -> None:
        result = _list_dialects()
        assert "postgres" in result
        assert "snowflake" in result
        assert "clickhouse" in result
        assert "databricks" in result
        assert "dremio" in result


# ---------------------------------------------------------------------------
# Resource: OBML reference
# ---------------------------------------------------------------------------


class TestObmlResource:
    def test_reference_contains_key_sections(self) -> None:
        text = OBML_REFERENCE
        assert "dataObjects" in text
        assert "dimensions" in text
        assert "measures" in text
        assert "metrics" in text
        assert "globally unique" in text

    def test_resource_function_returns_reference(self) -> None:
        result = obml_reference.fn()
        assert result == OBML_REFERENCE


# ---------------------------------------------------------------------------
# Session isolation
# ---------------------------------------------------------------------------


class TestSessionIsolation:
    def test_models_not_shared_between_sessions(self) -> None:
        sid_a = _extract_session_id(_create_session())
        sid_b = _extract_session_id(_create_session())
        _load_model(SAMPLE_MODEL_YAML, session_id=sid_a)
        # Session A has a model, session B does not
        result_a = _list_models(session_id=sid_a)
        result_b = _list_models(session_id=sid_b)
        assert "2 objects" in result_a
        assert "No models loaded" in result_b

    def test_default_session_separate_from_explicit(self) -> None:
        # Load into default
        _load_model(SAMPLE_MODEL_YAML)
        # Create explicit session — should have no models
        sid = _extract_session_id(_create_session())
        result = _list_models(session_id=sid)
        assert "No models loaded" in result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_model_id(load_output: str) -> str:
    """Extract model_id from load_model output text."""
    for line in load_output.splitlines():
        if "model_id:" in line:
            return line.split("model_id:")[1].strip()
    raise ValueError(f"Could not find model_id in: {load_output}")


def _extract_session_id(create_output: str) -> str:
    """Extract session_id from create_session output text."""
    for line in create_output.splitlines():
        if "session_id:" in line:
            return line.split("session_id:")[1].strip()
    raise ValueError(f"Could not find session_id in: {create_output}")


def _load_and_get_id() -> str:
    """Load the sample model and return its id."""
    return _extract_model_id(_load_model(SAMPLE_MODEL_YAML))
