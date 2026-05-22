"""Embedded ``pg_catalog`` / ``information_schema`` emulator (Step 3).

Builds an in-memory DuckDB and registers each loaded OBSL model as an
empty TABLE so DuckDB's native ``pg_catalog`` (auto-populated from the
schema) answers Postgres introspection probes — ``\\dt``, DBeaver schema
trees, Tableau's pre-flight checks — without us hand-rolling pg_class /
pg_namespace / pg_attribute.

Why a TABLE and not a VIEW: ``psql \\dt`` filters
``c.relkind IN ('r','p','')`` so views (``relkind='v'``) are skipped.
The tables hold zero rows; they exist purely so the catalog views
describe them. Semantic queries against the same model never reach
this connection — they're routed to the real warehouse by the router.

Caveat: DuckDB's ``pg_attribute.atttypid`` returns DuckDB's internal
type ids, not real Postgres OIDs. ``\\d <table>`` in psql will show
mislabeled types; clients that consult ``information_schema.columns``
instead (DBeaver, Tableau, Power BI) get the correct DuckDB SQL types.
Step 5 of design/PLAN_postgres_wire.md addresses this for BI-tool
fidelity.
"""

from __future__ import annotations

import contextlib
import logging
import re
import threading
import time
from collections.abc import Iterator
from typing import Any

import duckdb

from orionbelt.models.semantic import DataType, Dimension, Measure, Metric, SemanticModel
from orionbelt.service.db_executor import ColumnMeta, ExecutionResult
from orionbelt.service.session_manager import SessionManager

logger = logging.getLogger(__name__)


# OBML DataType → DuckDB SQL type. Coarse mapping; column-level
# ``dataType`` overrides (e.g. ``decimal(18,2)``) are ignored on
# purpose — pg_attribute's mis-typing makes finer types invisible
# anyway and the catalog only needs to round-trip the *column* shape.
_DATATYPE_TO_DUCKDB: dict[DataType, str] = {
    DataType.STRING: "VARCHAR",
    DataType.JSON: "JSON",
    DataType.INT: "BIGINT",
    DataType.FLOAT: "DOUBLE",
    DataType.DATE: "DATE",
    DataType.TIME: "TIME",
    DataType.TIME_TZ: "TIMETZ",
    DataType.TIMESTAMP: "TIMESTAMP",
    DataType.TIMESTAMP_TZ: "TIMESTAMPTZ",
    DataType.BOOLEAN: "BOOLEAN",
}

# DuckDB identifier safety. Postgres allows arbitrary quoted names, so
# any printable Unicode is fair game inside the model.  We pre-validate
# the *model* name (used as the table name) more strictly because BI
# tools sometimes refuse quoted identifiers; column names stay quoted.
_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

# Branded schema name surfaced to BI tools.  Replaces the Postgres
# default ``public`` so OBSL models show up under a recognisable label
# in Tableau / Power BI / DBeaver schema browsers. Kept as a constant
# (not configurable) so the canned ``search_path`` / ``current_schema``
# replies and the catalog DDL stay in lockstep.
CATALOG_SCHEMA = "orionbelt"


# Stub Postgres catalog functions referenced by psql ``\\dt`` / ``\\d``.
# DuckDB exposes ``pg_class`` / ``pg_namespace`` / ``pg_attribute`` as
# native views but stops short of these helper scalars; we hand-roll
# them as macros in the main schema and pair with a SQL pre-processor
# that strips ``pg_catalog.`` from function references (DuckDB rejects
# qualified function lookups into a system schema).
_STUB_MACROS: tuple[str, ...] = (
    "CREATE OR REPLACE MACRO pg_get_userbyid(uid) AS 'obsl'",
    "CREATE OR REPLACE MACRO pg_table_is_visible(oid) AS true",
    "CREATE OR REPLACE MACRO pg_type_is_visible(oid) AS true",
    "CREATE OR REPLACE MACRO pg_get_partkeydef(oid) AS NULL",
    "CREATE OR REPLACE MACRO pg_get_indexdef(oid) AS NULL",
    "CREATE OR REPLACE MACRO pg_get_constraintdef(oid) AS NULL",
    "CREATE OR REPLACE MACRO pg_get_expr(expr, oid) AS NULL",
    # 3-arg form psql / DBeaver emit for ``pg_get_expr(adbin, adrelid, true)``
    # — DuckDB needs an explicit overload because its macro dispatch
    # is arity-strict.
    "CREATE OR REPLACE MACRO pg_get_expr(expr, oid, pretty) AS NULL",
    "CREATE OR REPLACE MACRO pg_get_keywords() AS 'keyword'",
    "CREATE OR REPLACE MACRO pg_get_function_arguments(oid) AS ''",
    "CREATE OR REPLACE MACRO pg_get_function_identity_arguments(oid) AS ''",
    "CREATE OR REPLACE MACRO pg_get_function_result(oid) AS ''",
    "CREATE OR REPLACE MACRO pg_get_serial_sequence(table_name, column_name) AS NULL",
    "CREATE OR REPLACE MACRO pg_size_pretty(bytes) AS '0 bytes'",
    "CREATE OR REPLACE MACRO pg_relation_size(oid) AS 0",
    "CREATE OR REPLACE MACRO pg_total_relation_size(oid) AS 0",
    "CREATE OR REPLACE MACRO pg_database_size(oid) AS 0",
    "CREATE OR REPLACE MACRO pg_indexes_size(oid) AS 0",
    "CREATE OR REPLACE MACRO pg_table_size(oid) AS 0",
    "CREATE OR REPLACE MACRO pg_relation_is_publishable(oid) AS false",
    "CREATE OR REPLACE MACRO obj_description(oid, catalog) AS NULL",
    "CREATE OR REPLACE MACRO col_description(oid, col) AS NULL",
    "CREATE OR REPLACE MACRO format_type(oid, typemod) AS 'unknown'",
    "CREATE OR REPLACE MACRO pg_encoding_to_char(enc) AS 'UTF8'",
    # JDBC's ``getPrimaryKeys`` calls
    # ``(information_schema._pg_expandarray(i.indkey)).n`` to enumerate
    # PK column positions. Our virtual tables have no PKs so the
    # surrounding JOIN against ``pg_index`` is always empty; the macro
    # only needs to be resolvable. Return a STRUCT so ``(call).n``
    # parses; the actual SELECT never executes.
    "CREATE OR REPLACE MACRO _pg_expandarray(arr) AS {'x': NULL, 'n': NULL}",
)


# Shadow views that translate DuckDB's internal type-id numbering in
# ``pg_attribute`` / ``pg_type`` to real Postgres OIDs. DuckDB stores
# ``atttypid = 23`` for a DOUBLE column — but 23 in Postgres is INT4.
# Tableau's pgjdbc reads that 23 and allocates a 4-byte integer reader
# for what we send as an 8-byte FLOAT8, producing NULL / 0 measures.
#
# We can't write into the ``pg_catalog`` schema (DuckDB rejects it as a
# system catalog), so we expose translated views under the orionbelt
# schema and rewrite ``pg_attribute`` / ``pg_type`` references in
# incoming SQL to use them.
#
# Mapping below derived empirically from ``CREATE TABLE t (a VARCHAR,
# b BIGINT, c DOUBLE, …)`` + ``SELECT atttypid FROM pg_attribute``:
# DuckDB id → Postgres OID
#   10 → 16   (BOOL)
#   12 → 21   (INT2)
#   13 → 23   (INT4)
#   14 → 20   (INT8)
#   15 → 1082 (DATE)
#   16 → 1083 (TIME)
#   19 → 1114 (TIMESTAMP)
#   21 → 1700 (NUMERIC)
#   22 → 700  (FLOAT4)
#   23 → 701  (FLOAT8)
#   25 → 25   (TEXT — already matches)
#   26 → 17   (BYTEA)
#   27 → 1186 (INTERVAL)
_OID_TRANSLATION_CASE = """
        CASE atttypid
            WHEN 10 THEN 16    WHEN 12 THEN 21    WHEN 13 THEN 23
            WHEN 14 THEN 20    WHEN 15 THEN 1082  WHEN 16 THEN 1083
            WHEN 19 THEN 1114  WHEN 21 THEN 1700  WHEN 22 THEN 700
            WHEN 23 THEN 701   WHEN 26 THEN 17    WHEN 27 THEN 1186
            ELSE atttypid::INTEGER
        END
"""

_SHADOW_VIEWS: tuple[str, ...] = (
    # Shadow pg_attribute that translates atttypid to real Postgres OIDs.
    # All other columns pass through unchanged so the rest of the JDBC
    # getColumns query keeps working.
    #
    # ``TEMP VIEW`` is intentional: DuckDB puts temp objects in a special
    # catalog where ``pg_class.relnamespace`` is NULL. Tableau's
    # ``getTables`` query filters ``WHERE nspname NOT IN ('pg_catalog',
    # 'information_schema')`` — and ``NULL NOT IN (…)`` evaluates to
    # NULL, excluding the row. The views remain queryable by name so
    # the SQL rewrites below still resolve.
    # ``attidentity`` (Postgres 10+) and ``attgenerated`` (Postgres 12+)
    # don't exist in DuckDB's pg_attribute view. Dremio's
    # ``DatabaseMetaData.getColumns()`` probe references both, so we
    # synthesize them as the Postgres sentinels for "not an identity
    # column" / "not a generated column" (empty string).
    f"""CREATE OR REPLACE TEMP VIEW _obsl_pg_attribute AS
        SELECT
            attrelid,
            attname,
            {_OID_TRANSLATION_CASE} AS atttypid,
            attstattarget,
            attlen,
            attnum,
            attndims,
            attcacheoff,
            atttypmod,
            attbyval,
            attstorage,
            attalign,
            attnotnull,
            atthasdef,
            attisdropped,
            attislocal,
            attinhcount,
            attcollation,
            ''::VARCHAR AS attidentity,
            ''::VARCHAR AS attgenerated
        FROM pg_catalog.pg_attribute""",
    # Shadow pg_type with real Postgres OIDs + the columns pgjdbc reads
    # from DatabaseMetaData.getColumns / getTypeInfo. TEMP for the same
    # invisibility-to-Tableau reasons as ``_obsl_pg_attribute`` above.
    # Dremio's Postgres JDBC connector probe from
    # `DatabaseMetaData.getColumns()` references `typtypmod` and
    # `typbasetype`; without them the binder fails with
    # 'Values list "t" does not have a column named "<col>"'. -1 is the
    # Postgres sentinel for "no type modifier", 0 is the sentinel for
    # "not a domain over another type" — both are correct for every base
    # type we expose here.
    """CREATE OR REPLACE TEMP VIEW _obsl_pg_type AS
        SELECT * FROM (VALUES
            -- (oid, typname, typcategory, typlen, typtype, typnotnull, typtypmod, typbasetype)
            (16,   'bool',        'B', 1,   'b', false, -1, 0),
            (17,   'bytea',       'U', -1,  'b', false, -1, 0),
            (18,   'char',        'S', 1,   'b', false, -1, 0),
            (19,   'name',        'S', 64,  'b', false, -1, 0),
            (20,   'int8',        'N', 8,   'b', false, -1, 0),
            (21,   'int2',        'N', 2,   'b', false, -1, 0),
            (23,   'int4',        'N', 4,   'b', false, -1, 0),
            (25,   'text',        'S', -1,  'b', false, -1, 0),
            (26,   'oid',         'N', 4,   'b', false, -1, 0),
            (700,  'float4',      'N', 4,   'b', false, -1, 0),
            (701,  'float8',      'N', 8,   'b', false, -1, 0),
            (1042, 'bpchar',      'S', -1,  'b', false, -1, 0),
            (1043, 'varchar',     'S', -1,  'b', false, -1, 0),
            (1082, 'date',        'D', 4,   'b', false, -1, 0),
            (1083, 'time',        'D', 8,   'b', false, -1, 0),
            (1114, 'timestamp',   'D', 8,   'b', false, -1, 0),
            (1184, 'timestamptz', 'D', 8,   'b', false, -1, 0),
            (1186, 'interval',    'T', 16,  'b', false, -1, 0),
            (1266, 'timetz',      'D', 12,  'b', false, -1, 0),
            (1700, 'numeric',     'N', -1,  'b', false, -1, 0),
            (2950, 'uuid',        'U', 16,  'b', false, -1, 0)
        ) AS t(oid, typname, typcategory, typlen, typtype, typnotnull, typtypmod, typbasetype)""",
)


# Rewrites we apply to the SQL before handing it to DuckDB.  Each
# pattern targets a specific psql / pgAdmin quirk.  Order matters: the
# more specific rules (``OPERATOR(pg_catalog.~)``) run before the
# generic ``pg_catalog.<ident>`` prefix strip.
_REWRITES: tuple[tuple[re.Pattern[str], str], ...] = (
    # CREATE [LOCAL|GLOBAL] TEMPORARY TABLE → CREATE TEMPORARY TABLE.
    # DuckDB rejects the LOCAL / GLOBAL modifiers Postgres allows;
    # they're meaningless for our single-process catalog connection.
    (
        re.compile(
            r"\bcreate\s+(?:local|global)\s+(temp(?:orary)?\s+table)\b",
            re.IGNORECASE,
        ),
        r"CREATE \1",
    ),
    # ``ON COMMIT PRESERVE ROWS`` / ``DROP`` modifiers — meaningless
    # for an in-memory DuckDB connection that has no transactions in
    # the Postgres sense. Strip so the surrounding DDL parses.
    (
        re.compile(r"\bON\s+COMMIT\s+(?:PRESERVE|DELETE|DROP)\s+ROWS\b", re.IGNORECASE),
        "",
    ),
    # ``SELECT … INTO [TEMP[ORARY]] TABLE "name" FROM …`` is Postgres
    # syntax DuckDB doesn't accept. Rewrite to ``CREATE [TEMPORARY]
    # TABLE "name" AS SELECT … FROM …``. Tableau's connect-check uses
    # this shape to test temp-table support.
    (
        re.compile(
            r"\bSELECT\b(?P<cols>.+?)\bINTO\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+"
            r'(?P<name>"[^"]+"|\w+)\s+FROM\b(?P<rest>.+)$',
            re.IGNORECASE | re.DOTALL,
        ),
        r"CREATE TEMPORARY TABLE \g<name> AS SELECT \g<cols> FROM \g<rest>",
    ),
    # COLLATE pg_catalog.default → drop entirely.  DuckDB has no notion
    # of named collations and the default collation is implicit anyway.
    (re.compile(r"\bCOLLATE\s+pg_catalog\.\w+\b", re.IGNORECASE), ""),
    # OPERATOR(pg_catalog.~) → OPERATOR(~), and the same for other
    # comparison / regex operators psql wraps with the qualifier.
    (re.compile(r"OPERATOR\(\s*pg_catalog\.", re.IGNORECASE), "OPERATOR("),
    # ::pg_catalog.text / ::pg_catalog.regtype / ::pg_catalog.name —
    # collapse Postgres-specific type names that don't exist in DuckDB
    # to VARCHAR. The cast result type is only used for display, so the
    # loose coercion is fine for catalog probes.
    (
        re.compile(
            r"::\s*pg_catalog\.(text|name|regtype|regclass|regprocedure|regnamespace|oid|char)\b",
            re.IGNORECASE,
        ),
        "::VARCHAR",
    ),
    # Bare ::regclass / ::regtype / ::oid / ::name (without the
    # ``pg_catalog.`` qualifier) — same collapse, same rationale.
    # Tableau and pgAdmin emit both forms depending on the probe.
    (
        re.compile(
            r"::\s*(regclass|regtype|regprocedure|regnamespace|name)\b",
            re.IGNORECASE,
        ),
        "::VARCHAR",
    ),
    # information_schema._pg_expandarray → bare _pg_expandarray. JDBC's
    # getPrimaryKeys query uses the qualified form; DuckDB can't create
    # macros inside ``information_schema`` so we strip the prefix and
    # resolve against the stub macro defined above.
    (
        re.compile(r"\binformation_schema\._pg_expandarray\b", re.IGNORECASE),
        "_pg_expandarray",
    ),
    # pg_catalog.pg_attribute / pg_attribute → _obsl_pg_attribute. The
    # DuckDB pg_attribute view stores DuckDB's internal type-id numbering
    # in atttypid (e.g. 23 for DOUBLE, where 23 is Postgres INT4). The
    # shadow view translates to real Postgres OIDs so Tableau's pgjdbc
    # allocates the right-width column reader.
    (
        re.compile(r"\bpg_catalog\s*\.\s*pg_attribute\b", re.IGNORECASE),
        "_obsl_pg_attribute",
    ),
    (
        re.compile(r"(?<![.\w])pg_attribute\b", re.IGNORECASE),
        "_obsl_pg_attribute",
    ),
    # Same story for pg_type — the shadow view exposes real Postgres
    # OIDs + typname so the JOIN ``a.atttypid = t.oid`` lines up after
    # the atttypid translation above.
    (
        re.compile(r"\bpg_catalog\s*\.\s*pg_type\b", re.IGNORECASE),
        "_obsl_pg_type",
    ),
    (
        re.compile(r"(?<![.\w])pg_type\b", re.IGNORECASE),
        "_obsl_pg_type",
    ),
    # Function / operator references prefixed with pg_catalog. — strip
    # the prefix so DuckDB resolves against the unqualified built-in or
    # our stub macros.  We deliberately don't touch table references
    # like ``pg_catalog.pg_class`` because DuckDB handles those itself.
    (
        re.compile(
            r"pg_catalog\.(pg_[a-z_]+|format_type|obj_description|col_description)\s*\(",
            re.IGNORECASE,
        ),
        r"\1(",
    ),
)


def _rewrite_for_duckdb(sql: str) -> str:
    """Best-effort SQL rewrite so psql introspection runs on DuckDB.

    Touches only the patterns documented in ``_REWRITES``.  Unrecognised
    constructs are left alone — the catalog branch is best-effort by
    design, and the caller's error response will surface anything we
    miss so we can extend the rule list incrementally.
    """

    out = sql
    for pattern, replacement in _REWRITES:
        out = pattern.sub(replacement, out)
    return out


class CatalogEmulator:
    """Wraps an in-memory DuckDB connection used only for catalog probes.

    The emulator is intended to be created once and shared across all
    pgwire connections.  ``refresh()`` rebuilds the schema from the
    current :class:`SessionManager` state; ``execute()`` runs an
    arbitrary SQL string against the connection and returns an
    :class:`ExecutionResult` so the router can encode rows uniformly
    with the semantic path.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._con: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")
        # Tracked as (database_name, table_name) — each model gets its
        # own ATTACHed in-memory database so ``table_catalog`` in
        # information_schema matches the Postgres ``database`` parameter
        # BI tools filter on.
        self._registered_tables: list[tuple[str, str]] = []
        self._attached_dbs: set[str] = set()
        for ddl in _STUB_MACROS:
            with contextlib.suppress(Exception):
                self._con.execute(ddl)

    # ------------------------------------------------------------------
    # Refresh — rebuild the in-memory schema from a SessionManager.
    # ------------------------------------------------------------------

    def refresh(self, session_manager: SessionManager) -> None:
        """Drop and recreate one empty TABLE per loaded model.

        Called on every catalog probe — the cost is dominated by the
        DDL round-trip (~microseconds for a handful of models) and the
        simpler design is worth more than a stale-cache invalidation
        protocol.
        """

        with self._lock:
            # Drop everything we registered last time first.
            for db_name, table_name in self._registered_tables:
                with contextlib.suppress(Exception):
                    self._con.execute(
                        f'DROP TABLE IF EXISTS "{db_name}".{CATALOG_SCHEMA}."{table_name}"'
                    )
            self._registered_tables = []

            for store_target, model in _iter_loaded_models(session_manager):
                db_name = _safe_model_table_name(store_target)
                # Each model lives in its own ATTACHed in-memory DB so
                # ``information_schema.tables.table_catalog`` reports
                # the model's addressing name — what BI tools filter on.
                if db_name not in self._attached_dbs:
                    try:
                        self._con.execute(f"ATTACH ':memory:' AS \"{db_name}\"")
                    except duckdb.Error:
                        logger.exception("Failed to ATTACH catalog DB for model '%s'", store_target)
                        continue
                    self._attached_dbs.add(db_name)
                    with contextlib.suppress(Exception):
                        self._con.execute(
                            f'CREATE SCHEMA IF NOT EXISTS "{db_name}".{CATALOG_SCHEMA}'
                        )
                    # Stub macros live in DuckDB's default ``memory`` DB
                    # but ``execute()`` switches the current DB to the
                    # one matching the Postgres ``database`` parameter,
                    # putting the macros out of scope. Recreate them in
                    # every attached DB so JDBC catalog probes resolve.
                    self._con.execute(f'USE "{db_name}"')
                    for macro_ddl in _STUB_MACROS:
                        with contextlib.suppress(Exception):
                            self._con.execute(macro_ddl)
                    for view_ddl in _SHADOW_VIEWS:
                        with contextlib.suppress(Exception):
                            self._con.execute(view_ddl)
                    with contextlib.suppress(Exception):
                        self._con.execute("USE memory")
                table_name = db_name
                ddl = _build_table_ddl(db_name, table_name, model)
                if ddl is None:
                    continue
                try:
                    self._con.execute(ddl)
                except duckdb.Error:  # pragma: no cover — defensive guard
                    logger.exception(
                        "Failed to register catalog table for model '%s'", store_target
                    )
                    continue
                self._registered_tables.append((db_name, table_name))

    # ------------------------------------------------------------------
    # Execute — run a catalog/info-schema query through DuckDB.
    # ------------------------------------------------------------------

    def execute(self, sql: str, database: str = "") -> ExecutionResult:
        """Run ``sql`` against the embedded DuckDB.

        DuckDB's pg_catalog and information_schema are auto-populated
        from the schema we registered in :meth:`refresh`, so the
        caller doesn't need to special-case which table is being
        queried.  Errors bubble as ``duckdb.Error``.

        ``database`` is the Postgres ``database`` parameter the client
        connected with; we ``USE`` the matching ATTACHed DuckDB database
        so ``pg_class`` / ``pg_namespace`` (which only enumerate the
        currently-connected DuckDB DB) scope to the right model.
        """

        t0 = time.monotonic()
        rewritten = _rewrite_for_duckdb(sql)
        with self._lock:
            target_db = database if database in self._attached_dbs else None
            if target_db is None and len(self._attached_dbs) == 1:
                # No explicit database (callers like tests, REST API
                # probes) — fall back to the single attached DB so
                # ``pg_class`` / ``pg_namespace`` enumerate something
                # rather than empty.
                target_db = next(iter(self._attached_dbs))
            if target_db is not None:
                with contextlib.suppress(Exception):
                    self._con.execute(f'USE "{target_db}"')
            cursor = self._con.execute(rewritten)
            rows_raw = cursor.fetchall()
            description = cursor.description or []
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        columns = [
            ColumnMeta(name=str(d[0]), type_hint=_duckdb_desc_to_hint(d)) for d in description
        ]
        rows = [list(row) for row in rows_raw]
        return ExecutionResult(
            columns=columns,
            raw_rows=rows,
            row_count=len(rows),
            execution_time_ms=elapsed_ms,
        )

    # ------------------------------------------------------------------

    def close(self) -> None:
        with self._lock, contextlib.suppress(Exception):
            self._con.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_loaded_models(session_manager: SessionManager) -> Iterator[tuple[str, SemanticModel]]:
    """Yield ``(target_name, SemanticModel)`` for every loaded model.

    ``target_name`` is the addressing name BI tools use as the Postgres
    ``database`` parameter:

    * multi-model (``MODEL_FILES``): each preload sits in its own
      *protected* session whose id IS the OBML name; iterated via
      :meth:`SessionManager.list_protected_session_ids`.
    * single-model legacy (``MODEL_FILE``): the model lives in the
      ``__default__`` session, exposed under that name.
    * user-created sessions: iterated via
      :meth:`SessionManager.list_sessions` so models loaded over REST
      still light up the catalog.

    ``list_sessions()`` filters out the default + protected sets, so we
    union all three sources manually to cover production layouts.
    """

    candidate_ids: list[str] = []
    candidate_ids.extend(session_manager.list_protected_session_ids())
    candidate_ids.append("__default__")
    candidate_ids.extend(s.session_id for s in session_manager.list_sessions())

    seen_names: set[str] = set()
    for session_id in candidate_ids:
        if session_id in seen_names:
            continue
        try:
            store = session_manager.get_store(session_id)
        except Exception:
            continue
        models = store.list_models()
        if not models:
            continue
        try:
            model = store.get_model(models[0].model_id)
        except KeyError:
            continue
        seen_names.add(session_id)
        yield session_id, model


def _safe_model_table_name(name: str) -> str:
    """Coerce a model addressing name into a DuckDB-safe table name.

    The pgwire surface accepts arbitrary Postgres database parameters,
    but DuckDB DDL is friendlier with simple identifiers. Names that
    don't match the canonical pattern fall through unchanged inside
    double quotes — DuckDB tolerates that fine; we just pre-check so
    common cases produce predictable bare identifiers.
    """

    if _SAFE_TABLE_NAME.match(name):
        return name
    return name.replace('"', '""')


def _build_table_ddl(db_name: str, table_name: str, model: SemanticModel) -> str | None:
    """Build the ``CREATE TABLE`` for a model.

    Columns: every dimension, measure, and metric exposed by the
    model.  Names are quoted because OBSL labels routinely contain
    spaces and punctuation.  Returns ``None`` if no columns survive
    deduplication — DuckDB rejects empty column lists.
    """

    columns: list[str] = []
    seen: set[str] = set()
    for label, sql_type in _model_columns(model):
        if label in seen:
            continue
        seen.add(label)
        quoted = label.replace('"', '""')
        columns.append(f'"{quoted}" {sql_type}')
    if not columns:
        return None
    quoted_db = db_name.replace('"', '""')
    quoted_table = table_name.replace('"', '""')
    return f'CREATE TABLE "{quoted_db}".{CATALOG_SCHEMA}."{quoted_table}" ({", ".join(columns)})'


def _model_columns(model: SemanticModel) -> Iterator[tuple[str, str]]:
    for label, dim in model.dimensions.items():
        yield label, _dim_sql_type(dim)
    for label, measure in model.measures.items():
        yield label, _measure_sql_type(measure)
    for label, metric in model.metrics.items():
        yield label, _metric_sql_type(metric)


def _dim_sql_type(dim: Dimension) -> str:
    return _DATATYPE_TO_DUCKDB.get(dim.result_type, "VARCHAR")


def _measure_sql_type(measure: Measure) -> str:
    return _DATATYPE_TO_DUCKDB.get(measure.result_type, "DOUBLE")


def _metric_sql_type(_metric: Metric) -> str:
    # Metrics produce a single derived value — float is the safe
    # default the OBSL compiler also uses.  Step 7's finer type story
    # can revisit per-metric output typing.
    return "DOUBLE"


def _duckdb_desc_to_hint(description_row: tuple[Any, ...]) -> str:
    """Coarse DuckDB type-code → executor type_hint.

    DuckDB's cursor description carries a string type name in slot 1.
    We collapse it onto the same four-hint vocabulary the executor
    uses so the encoder in pgwire/types.py is shared between the
    catalog and semantic paths.
    """

    if len(description_row) < 2 or description_row[1] is None:
        return "string"
    name = str(description_row[1]).lower()
    if any(token in name for token in ("int", "decimal", "numeric", "float", "double", "real")):
        return "number"
    if any(token in name for token in ("timestamp", "date", "time")):
        return "datetime"
    if name == "boolean" or name == "bool":
        return "string"  # text-format bool encoder picks 't'/'f' from python bool
    if "blob" in name or "binary" in name:
        return "binary"
    return "string"
