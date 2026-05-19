"""Hand-written replies for Postgres protocol-level probes (Step 3).

Many Postgres clients and BI tools fire a flurry of trivial queries
before they let the user run anything:

* ``SELECT 1``                                    — connectivity probe
* ``SELECT version()``                            — server identity
* ``SHOW <param>`` / ``SET <param> = <value>``    — session state
* ``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` / ``SAVEPOINT``
                                                  — transaction wrappers
* ``SELECT current_schema()`` / ``current_database()`` / ``current_user``
* ``SELECT pg_catalog.set_config(...)``           — search_path tweaks

OBSL is read-only and doesn't keep session state, so we accept these
no-ops with a clean reply rather than letting them fall through to the
semantic translator (which would treat them as a model query and
error). DDL / DML / writes still hit the semantic router and bounce
there per design/PLAN_postgres_wire.md §10.
"""

from __future__ import annotations

import re

from orionbelt.pgwire import protocol
from orionbelt.pgwire.catalog import OBSL_DATABASE_NAME as _OBSL_DATABASE_NAME

# Connectivity probes that don't need a model loaded.
_CONNECTIVITY_PATTERNS: dict[str, tuple[str, str, str]] = {
    # normalised SQL → (column name, postgres-typed OID name, value)
    "select 1": ("?column?", "int4", "1"),
}


# Parameter values surfaced via ``SHOW``.  Mirrors the ParameterStatus
# frames the server sends after AuthenticationOk so clients see a
# consistent view of session state.
_SHOW_VALUES: dict[str, str] = {
    "server_version": "15.0 (orionbelt-pgwire 0.3)",
    "server_encoding": "UTF8",
    "client_encoding": "UTF8",
    "datestyle": "ISO, MDY",
    "timezone": "UTC",
    "integer_datetimes": "on",
    "standard_conforming_strings": "on",
    "application_name": "",
    "is_superuser": "off",
    "session_authorization": "obsl",
    "search_path": '"$user", orionbelt',
    "transaction_isolation": "read committed",
    "transaction_read_only": "off",
    "default_transaction_isolation": "read committed",
    "default_transaction_read_only": "off",
    "extra_float_digits": "3",
    "max_index_keys": "32",
    "max_identifier_length": "63",
    "block_size": "8192",
    # Locale parameters — Tableau and other BI tools probe these during
    # connect to detect collation behaviour. UTF-8 / en_US matches the
    # values reported by a default Postgres install on Linux.
    "lc_collate": "en_US.UTF-8",
    "lc_ctype": "en_US.UTF-8",
    "lc_messages": "en_US.UTF-8",
    "lc_monetary": "en_US.UTF-8",
    "lc_numeric": "en_US.UTF-8",
    "lc_time": "en_US.UTF-8",
    "server_version_num": "150000",
}


_VERSION_LITERAL = "PostgreSQL 15.0 on x86_64-pc-linux-gnu (OrionBelt pgwire 0.3)"


_RE_SHOW = re.compile(r'^show\s+"?([a-z_][a-z0-9_]*)"?\s*$', re.IGNORECASE)
_RE_SET = re.compile(r"^set\s+", re.IGNORECASE)
_RE_RESET = re.compile(r"^reset\s+", re.IGNORECASE)
_RE_DISCARD = re.compile(r"^discard\s+", re.IGNORECASE)
_RE_BEGIN = re.compile(r"^(begin|start\s+transaction)\b", re.IGNORECASE)
_RE_COMMIT = re.compile(r"^(commit|end)\b", re.IGNORECASE)
_RE_ROLLBACK = re.compile(r"^rollback\b", re.IGNORECASE)
_RE_SAVEPOINT = re.compile(r"^(savepoint|release\s+savepoint)\b", re.IGNORECASE)


def match_canned(sql: str, database: str = "") -> bytes | None:
    """Return wire bytes for a recognised protocol probe, else ``None``.

    The caller appends ``ReadyForQuery``; we only emit the per-statement
    reply (RowDescription + DataRow + CommandComplete or a bare
    CommandComplete).  Returns ``None`` so the router falls through to
    catalog / semantic dispatch.

    ``database`` is the value the client sent in StartupMessage —
    used for routing to the right model. Catalog probes
    (``current_database`` / ``current_catalog``) return the fixed
    ``orionbelt`` logical database name so BI-tool schema trees stay
    consistent with the one-row ``pg_database`` view.
    """

    normalised = _strip_terminator(sql).lower()
    if not normalised:
        # Empty / whitespace-only query — Postgres replies with
        # EmptyQueryResponse; we approximate with a bare
        # CommandComplete.  Behaviour matches what jdbc / psql expect.
        return protocol.build_command_complete("")

    # Connectivity probes (SELECT 1, etc.)
    canned = _CONNECTIVITY_PATTERNS.get(normalised)
    if canned is not None:
        name, type_name, value = canned
        oid = protocol.OID_INT4 if type_name == "int4" else protocol.OID_TEXT
        return (
            protocol.build_row_description([(name, oid)])
            + protocol.build_data_row([value])
            + protocol.build_command_complete("SELECT 1")
        )

    if normalised == "select version()":
        return (
            protocol.build_row_description([("version", protocol.OID_TEXT)])
            + protocol.build_data_row([_VERSION_LITERAL])
            + protocol.build_command_complete("SELECT 1")
        )

    if normalised in {
        "select current_schema()",
        "select current_schema",
    }:
        return _single_text_row("current_schema", "orionbelt")

    if normalised in {
        "select current_database()",
        "select current_database",
        "select current_catalog",
        "select current_catalog()",
    }:
        # OBSL surfaces every loaded model under a single "orionbelt"
        # logical database — each model is a schema (per pg_namespace).
        # current_catalog is the SQL-standard alias for current_database;
        # both return the brand name so BI-tool schema trees stay
        # consistent with the one-row pg_database view, regardless of
        # which model the client selected via StartupMessage `database=`.
        del database  # routing uses it; the catalog name does not
        column = "current_catalog" if "catalog" in normalised else "current_database"
        return _single_text_row(column, _OBSL_DATABASE_NAME)

    if normalised in {
        "select current_user",
        "select current_user()",
        "select user",
        "select current_role",
        "select session_user",
    }:
        column = (
            "current_role"
            if "current_role" in normalised
            else "session_user"
            if "session_user" in normalised
            else "current_user"
        )
        return _single_text_row(column, _SHOW_VALUES["session_authorization"])

    show_match = _RE_SHOW.match(normalised)
    if show_match is not None:
        name = show_match.group(1).lower()
        value = _SHOW_VALUES.get(name, "")
        return _single_text_row(name, value)

    # SET / RESET / DISCARD / transaction wrappers — accept and ignore.
    if _RE_SET.match(normalised) is not None:
        return protocol.build_command_complete("SET")
    if _RE_RESET.match(normalised) is not None:
        return protocol.build_command_complete("RESET")
    if _RE_DISCARD.match(normalised) is not None:
        return protocol.build_command_complete("DISCARD ALL")
    if _RE_BEGIN.match(normalised) is not None:
        return protocol.build_command_complete("BEGIN")
    if _RE_COMMIT.match(normalised) is not None:
        return protocol.build_command_complete("COMMIT")
    if _RE_ROLLBACK.match(normalised) is not None:
        return protocol.build_command_complete("ROLLBACK")
    if _RE_SAVEPOINT.match(normalised) is not None:
        return protocol.build_command_complete("SAVEPOINT")

    # Tableau connect-check temp-table dance is NOT handled here — the
    # router routes those to the catalog DuckDB so CREATE / INSERT /
    # SELECT / DROP actually execute. See ``references_temp_table`` in
    # router.py. Stubbing the SELECT with zero rows broke Tableau,
    # which inserts one row and expects to read it back.

    return None


def _strip_terminator(sql: str) -> str:
    text = sql.strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    return text


def _single_text_row(column_name: str, value: str) -> bytes:
    return (
        protocol.build_row_description([(column_name, protocol.OID_TEXT)])
        + protocol.build_data_row([value])
        + protocol.build_command_complete("SELECT 1")
    )
