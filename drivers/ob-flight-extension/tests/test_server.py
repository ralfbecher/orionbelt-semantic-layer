"""Tests for the OBFlightServer."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from ob_flight.server import OBFlightServer


@pytest.fixture
def mock_session_manager():
    """Mock session manager with a default session and model."""
    model = MagicMock()
    model.data_objects = {}

    model_info = MagicMock()
    model_info.model_id = "test-model"

    model_store = MagicMock()
    model_store.list_models.return_value = [model_info]
    model_store.get_model.return_value = model

    session = MagicMock()
    session.model_store = model_store

    mgr = MagicMock()
    mgr.get_session.return_value = session
    return mgr


class TestGetModel:
    def test_no_session_manager(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = None
        server._default_dialect = "duckdb"
        with pytest.raises(flight.FlightUnavailableError, match="session manager"):
            server._get_model()

    def test_no_models_loaded(self):
        mgr = MagicMock()
        session = MagicMock()
        session.model_store.list_models.return_value = []
        mgr.get_session.return_value = session

        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mgr
        server._default_dialect = "duckdb"
        with pytest.raises(flight.FlightUnavailableError, match="No models"):
            server._get_model()

    def test_no_default_session(self):
        mgr = MagicMock()
        mgr.get_session.side_effect = KeyError("session not found")

        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mgr
        server._default_dialect = "duckdb"
        with pytest.raises(flight.FlightUnavailableError, match="default session"):
            server._get_model()

    def test_success(self, mock_session_manager):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "postgres"

        model, dialect = server._get_model()
        assert model is not None
        assert dialect == "postgres"

    def test_returns_first_model(self, mock_session_manager):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "duckdb"

        model, dialect = server._get_model()
        mock_session_manager.get_session.assert_called_once_with("__default__")
        assert dialect == "duckdb"


class TestCompileObml:
    def test_compile_calls_pipeline(self, mock_session_manager):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "duckdb"

        mock_pipeline_cls = MagicMock()
        mock_result = MagicMock()
        mock_result.sql = "SELECT region FROM orders"
        mock_pipeline_cls.return_value.compile.return_value = mock_result

        mock_qo_cls = MagicMock()
        mock_qo_cls.model_validate.return_value = MagicMock()

        model, _ = server._get_model()

        with patch(
            "orionbelt.compiler.pipeline.CompilationPipeline", mock_pipeline_cls
        ):
            with patch("orionbelt.models.query.QueryObject", mock_qo_cls):
                sql = server._compile_obml(
                    {"select": {"dimensions": ["Region"]}}, model, "duckdb"
                )
                assert sql == "SELECT region FROM orders"
                mock_qo_cls.model_validate.assert_called_once()
                mock_pipeline_cls.return_value.compile.assert_called_once()


class TestGetFlightInfo:
    def test_plain_sql(self, mock_session_manager):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "duckdb"
        server._pending = {}

        descriptor = flight.FlightDescriptor.for_command(b"SELECT 1")
        context = MagicMock()

        info = server.get_flight_info(context, descriptor)
        assert len(info.endpoints) == 1
        # Ticket should be stored
        assert len(server._pending) == 1
        ticket_id = list(server._pending.keys())[0]
        sql, dialect, model = server._pending[ticket_id]
        assert sql == "SELECT 1"
        assert dialect == "duckdb"

    def test_obml_query(self, mock_session_manager):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "duckdb"
        server._pending = {}

        obml = b"select:\n  dimensions:\n    - Region\n  measures:\n    - Revenue\n"
        descriptor = flight.FlightDescriptor.for_command(obml)
        context = MagicMock()

        compiled_sql = "SELECT region, SUM(amount) FROM orders GROUP BY region"
        with patch.object(server, "_compile_obml", return_value=compiled_sql):
            info = server.get_flight_info(context, descriptor)
            assert len(server._pending) == 1
            ticket_id = list(server._pending.keys())[0]
            sql, _, _ = server._pending[ticket_id]
            assert sql == compiled_sql

    def test_returns_endpoint_with_ticket(self, mock_session_manager):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "duckdb"
        server._pending = {}

        descriptor = flight.FlightDescriptor.for_command(b"SELECT 42")
        context = MagicMock()

        info = server.get_flight_info(context, descriptor)
        assert len(info.endpoints) == 1
        endpoint = info.endpoints[0]
        ticket_id = endpoint.ticket.ticket.decode("utf-8")
        assert ticket_id in server._pending

    def test_no_session_manager_raises(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = None
        server._default_dialect = "duckdb"
        server._pending = {}

        descriptor = flight.FlightDescriptor.for_command(b"SELECT 1")
        context = MagicMock()

        with pytest.raises(flight.FlightUnavailableError):
            server.get_flight_info(context, descriptor)


class TestDoGet:
    def test_unknown_ticket(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._pending = {}

        ticket = flight.Ticket(b"nonexistent")
        with pytest.raises(flight.FlightServerError, match="Unknown ticket"):
            server.do_get(MagicMock(), ticket)

    def test_execute_and_stream(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._batch_size = 1024

        # Set up pending query
        model = MagicMock()
        ticket_id = "test-ticket"
        server._pending = {ticket_id: ("SELECT 1 AS n", "duckdb", model)}

        # Mock the DB connection
        from ob_driver_core.type_codes import NUMBER

        mock_cursor = MagicMock()
        mock_cursor.description = (("n", NUMBER, None, None, None, None, None),)
        mock_cursor.fetchmany.side_effect = [[(42.0,)], []]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("ob_flight.server.db_connect", return_value=mock_conn):
            ticket = flight.Ticket(ticket_id.encode("utf-8"))
            stream = server.do_get(MagicMock(), ticket)
            assert stream is not None

        # Ticket should be consumed
        assert ticket_id not in server._pending
        mock_conn.close.assert_called_once()

    def test_ddl_query_returns_ok(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._batch_size = 1024

        ticket_id = "ddl-ticket"
        server._pending = {ticket_id: ("CREATE TABLE t (x INT)", "duckdb", MagicMock())}

        mock_cursor = MagicMock()
        mock_cursor.description = None  # DDL has no description

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("ob_flight.server.db_connect", return_value=mock_conn):
            ticket = flight.Ticket(ticket_id.encode("utf-8"))
            stream = server.do_get(MagicMock(), ticket)
            assert stream is not None

        mock_conn.close.assert_called_once()

    def test_empty_result_set(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._batch_size = 1024

        ticket_id = "empty-ticket"
        server._pending = {
            ticket_id: ("SELECT * FROM t WHERE 1=0", "duckdb", MagicMock())
        }

        from ob_driver_core.type_codes import STRING

        mock_cursor = MagicMock()
        mock_cursor.description = (("name", STRING, None, None, None, None, None),)
        mock_cursor.fetchmany.return_value = []  # no rows

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("ob_flight.server.db_connect", return_value=mock_conn):
            ticket = flight.Ticket(ticket_id.encode("utf-8"))
            stream = server.do_get(MagicMock(), ticket)
            assert stream is not None

        mock_conn.close.assert_called_once()

    def test_connection_closed_on_error(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._batch_size = 1024

        ticket_id = "error-ticket"
        server._pending = {ticket_id: ("SELECT bad", "duckdb", MagicMock())}

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("SQL error")

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("ob_flight.server.db_connect", return_value=mock_conn):
            ticket = flight.Ticket(ticket_id.encode("utf-8"))
            with pytest.raises(RuntimeError, match="SQL error"):
                server.do_get(MagicMock(), ticket)

        mock_conn.close.assert_called_once()


class TestListFlights:
    def test_with_model(self, mock_session_manager):
        col = MagicMock()
        col.label = "ID"
        col.abstract_type = "int"
        obj = MagicMock()
        obj.columns = {"ID": col}

        model = MagicMock()
        model.data_objects = {"Orders": obj}

        mock_session_manager.get_session.return_value.model_store.get_model.return_value = (
            model
        )

        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "duckdb"

        # list_flights returns a generator
        infos = list(server.list_flights(MagicMock(), b""))
        assert len(infos) == 1

    def test_no_model_returns_empty(self):
        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = None
        server._default_dialect = "duckdb"

        infos = list(server.list_flights(MagicMock(), b""))
        assert len(infos) == 0

    def test_multiple_objects_listed(self, mock_session_manager):
        col = MagicMock()
        col.label = "X"
        col.abstract_type = "string"

        obj1 = MagicMock()
        obj1.columns = {"X": col}
        obj2 = MagicMock()
        obj2.columns = {"X": col}

        model = MagicMock()
        model.data_objects = {"A": obj1, "B": obj2}

        mock_session_manager.get_session.return_value.model_store.get_model.return_value = (
            model
        )

        server = OBFlightServer.__new__(OBFlightServer)
        server._session_manager = mock_session_manager
        server._default_dialect = "duckdb"

        infos = list(server.list_flights(MagicMock(), b""))
        assert len(infos) == 2
