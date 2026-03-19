"""Arrow Flight SQL server for OrionBelt Semantic Layer."""

from __future__ import annotations

import logging
import uuid
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight

from ob_driver_core.detection import is_obml, parse_obml

from ob_flight.catalog import model_to_flight_infos
from ob_flight.converters import rows_to_batch, schema_from_description
from ob_flight.db_router import connect as db_connect

logger = logging.getLogger("ob_flight.server")


class OBFlightServer(flight.FlightServerBase):
    """Arrow Flight server that compiles OBML queries via the OrionBelt pipeline.

    Runs inside the orionbelt-api process with direct access to
    CompilationPipeline and SessionManager — no HTTP hop.
    """

    def __init__(
        self,
        location: str = "grpc://0.0.0.0:8815",
        *,
        auth_handler: flight.ServerAuthHandler | None = None,
        session_manager: Any = None,
        default_dialect: str = "duckdb",
        batch_size: int = 1024,
    ) -> None:
        super().__init__(location, auth_handler=auth_handler)
        self._session_manager = session_manager
        self._default_dialect = default_dialect
        self._batch_size = batch_size
        # Pending queries: ticket_id -> (sql, dialect, model)
        self._pending: dict[str, tuple[str, str, Any]] = {}

    def _get_model(self) -> tuple[Any, str]:
        """Get the default model from the session manager.

        Returns (model, dialect) tuple.
        Uses the default session's first model (single-model mode).
        """
        if self._session_manager is None:
            raise flight.FlightUnavailableError("No session manager configured")

        try:
            session = self._session_manager.get_session("__default__")
        except Exception:
            raise flight.FlightUnavailableError("No default session available")

        models = session.model_store.list_models()
        if not models:
            raise flight.FlightUnavailableError("No models loaded")

        model_id = models[0].model_id
        model = session.model_store.get_model(model_id)
        return model, self._default_dialect

    def _compile_obml(self, obml: dict[str, Any], model: Any, dialect: str) -> str:
        """Compile OBML to SQL using the OrionBelt pipeline directly."""
        from orionbelt.compiler.pipeline import CompilationPipeline
        from orionbelt.models.query import QueryObject

        query = QueryObject.model_validate(obml)
        result = CompilationPipeline().compile(query, model, dialect)
        return result.sql

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        """Handle a query request — compile OBML if needed, return ticket."""
        query_str = descriptor.command.decode("utf-8")
        model, dialect = self._get_model()

        if is_obml(query_str):
            obml = parse_obml(query_str)
            sql = self._compile_obml(obml, model, dialect)
            logger.info("Compiled OBML to SQL: %s", sql[:200])
        else:
            sql = query_str

        # Store pending query with a ticket
        ticket_id = str(uuid.uuid4())
        self._pending[ticket_id] = (sql, dialect, model)

        # Build a placeholder schema (actual schema comes from execution)
        # We return a minimal schema — real schema is in the RecordBatches
        schema = pa.schema([pa.field("result", pa.utf8())])
        ticket = flight.Ticket(ticket_id.encode("utf-8"))
        endpoint = flight.FlightEndpoint(ticket, [])
        return flight.FlightInfo(schema, descriptor, [endpoint], -1, -1)

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.RecordBatchStream:
        """Execute the query and stream results as Arrow RecordBatches."""
        ticket_id = ticket.ticket.decode("utf-8")

        if ticket_id not in self._pending:
            raise flight.FlightServerError(f"Unknown ticket: {ticket_id}")

        sql, dialect, model = self._pending.pop(ticket_id)

        # Execute on vendor DB
        conn = db_connect(dialect)
        try:
            cursor = conn.cursor()
            cursor.execute(sql)

            if cursor.description is None:
                # DDL or no-result query
                schema = pa.schema([pa.field("status", pa.utf8())])
                batch = rows_to_batch([("OK",)], schema)
                table = pa.Table.from_batches([batch])
                return flight.RecordBatchStream(table)

            # Build Arrow schema from cursor description
            schema = schema_from_description(cursor.description)

            # Fetch all results as batches
            batches: list[pa.RecordBatch] = []
            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break
                batches.append(rows_to_batch(rows, schema))

            if not batches:
                # Query returned no rows — return empty table with schema
                batches = [rows_to_batch([], schema)]

            table = pa.Table.from_batches(batches)
            return flight.RecordBatchStream(table)
        finally:
            conn.close()

    def list_flights(
        self, context: flight.ServerCallContext, criteria: bytes
    ) -> Any:
        """List available data objects as Flight tables (for DBeaver schema browser)."""
        try:
            model, _ = self._get_model()
        except Exception:
            return

        # Use a simple model_id for the default model
        for info in model_to_flight_infos(model, "default"):
            yield info
