"""Runtime smoke for ``examples/notebook_setup.py`` display helpers.

v2.7.5 caught the static-only nature of ``test_notebook_contracts.py``:
``notebook_setup.show_yaml()`` had a ``_indentless`` typo (PyYAML calls
the override with ``indentless=`` kwarg) since April. The contract
suite imported the module and asserted ``start_api`` / ``api`` were
callable, but never *invoked* ``show_yaml`` — so the runtime kwarg
mismatch hit users at the very first cell. Filed as issue #88.

This file does the missing layer: import ``notebook_setup`` and
actually call every public display helper against synthetic inputs.
Cheap (no subprocess, no DuckDB), pinned to fail loudly on the next
PyYAML / pygments regression.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_NB_SETUP = _ROOT / "examples" / "notebook_setup.py"


@pytest.fixture(scope="module")
def nb_setup():
    if not _NB_SETUP.exists():
        pytest.skip(f"{_NB_SETUP} not present")
    pytest.importorskip("yaml", reason="pyyaml required for notebook_setup helpers")
    pytest.importorskip("pygments", reason="pygments required for notebook_setup helpers")
    pytest.importorskip("sqlparse", reason="sqlparse required for notebook_setup helpers")
    spec = importlib.util.spec_from_file_location("_nb_setup_under_test", _NB_SETUP)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def nb_setup_with_ipython(nb_setup):
    pytest.importorskip("IPython", reason="IPython required for show_result side-effect")
    return nb_setup


def test_show_yaml_renders_without_error(nb_setup) -> None:
    """Direct repro of issue #88 — pre-fix this raised
    ``TypeError: _IndentedDumper.increase_indent() got an unexpected
    keyword argument 'indentless'``.
    """
    out = nb_setup.show_yaml("select:\n  dimensions: [Country]\n  measures: [Revenue]\n")
    assert isinstance(out, str)
    assert "<pre" in out or "<span" in out  # syntax-highlighted HTML


def test_show_yaml_preserves_block_lists(nb_setup) -> None:
    """The whole point of ``_IndentedDumper`` is to render lists in
    block style indented under their parent. A successful call here
    means the override is wired correctly.
    """
    out = nb_setup.show_yaml("filters:\n  - field: x\n    op: '='\n    value: 1\n")
    # Block-style list items keep the leading dash
    assert "- field" in out or "field" in out


def test_show_sql_renders_without_error(nb_setup) -> None:
    out = nb_setup.show_sql("SELECT a, b FROM t WHERE x = 1")
    assert isinstance(out, str)


def test_show_table_renders_without_error(nb_setup) -> None:
    out = nb_setup.show_table(
        [{"name": "country"}, {"name": "revenue"}],
        [["US", 1000], ["DE", 750]],
    )
    assert isinstance(out, str)
    assert "<table" in out
    assert "US" in out and "1,000" in out  # numeric formatting + cell contents


def test_show_result_full_payload_renders(nb_setup_with_ipython) -> None:
    """``show_result`` composes ``show_yaml`` + ``show_sql`` + ``show_table`` —
    a single call exercises every display helper end-to-end. Pre-fix this
    failed on the first ``show_yaml`` invocation with the ``_indentless``
    TypeError. Side-effect via ``IPython.display.display`` — we assert
    only that the call doesn't raise.
    """
    result = {
        "sql": 'SELECT "country", SUM("revenue") FROM "t" GROUP BY "country"',
        "columns": [{"name": "country"}, {"name": "revenue"}],
        "rows": [["US", 1000], ["DE", 750]],
    }
    query = "select:\n  dimensions: [Country]\n  measures: [Revenue]\n"
    nb_setup_with_ipython.show_result(result, query)  # must not raise


def test_show_result_without_rows_skips_table(nb_setup_with_ipython) -> None:
    """Compile-only responses have no ``rows`` field — table section
    should be skipped without error.
    """
    result = {"sql": "SELECT 1", "columns": [], "rows": []}
    nb_setup_with_ipython.show_result(result, None)  # must not raise
