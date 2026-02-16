"""Gradio demo UI — thin HTTP client for the OrionBelt REST API."""

from __future__ import annotations

import contextlib

import httpx
import sqlparse
import yaml

_DEFAULT_API_URL = "http://localhost:8000"
_FALLBACK_DIALECTS = ["postgres", "snowflake", "clickhouse", "dremio", "databricks"]

_DEFAULT_QUERY = """\
select:
  dimensions:
    - Product Name
    - Client Name
  measures:
    - Total Sales
    - Total Returns
    - Return Rate
limit: 100
"""

_CHROME_PX = 160  # header + settings + button + gaps

_CSS = f"""\
/* ── Layout: full-width, fit viewport ── */
.gradio-container {{
  max-width: 100% !important;
  padding: 8px 16px !important;
}}
/* compact header */
.header-row {{ min-height: 0 !important; padding: 0 !important; }}
.header-row h2 {{ margin: 0 !important; }}
/* compact settings row */
.settings-row {{ min-height: 0 !important; }}

/* Proportional editor heights — subtract fixed chrome, split remainder.
   Editors (60 %) and SQL output (40 %) shrink together. */
.code-editor .cm-editor {{
  max-height: calc((100dvh - {_CHROME_PX}px) * 0.55) !important;
}}
.sql-output .cm-editor {{
  max-height: calc((100dvh - {_CHROME_PX}px) * 0.38) !important;
}}

/* purple primary button */
.purple-btn {{
  background: linear-gradient(135deg, #7c3aed, #9333ea) !important;
  border: none !important;
  color: white !important;
}}
.purple-btn:hover {{
  background: linear-gradient(135deg, #6d28d9, #7c3aed) !important;
}}

/* ── YAML / SQL syntax highlighting (dark-mode optimised) ──
   Uses high-specificity selectors to override any built-in CM theme.
   Class names from CodeMirror 5 legacy YAML mode bundled in Gradio:
     cm-atom=keys  cm-string=values  cm-comment  cm-number
     cm-keyword=booleans  cm-meta=structural  cm-def=doc-markers */

/* keys / property names — cyan */
.cm-editor .cm-atom     {{ color: #7dcfff !important; }}
/* string values (data types, source names) — warm orange */
.cm-editor .cm-string   {{ color: #ce9178 !important; }}
/* comments — bright green, italic */
.cm-editor .cm-comment  {{ color: #6a9955 !important; font-style: italic; }}
/* numbers — soft green */
.cm-editor .cm-number   {{ color: #b5cea8 !important; }}
/* booleans (true/false/yes/no) — purple */
.cm-editor .cm-keyword  {{ color: #c586c0 !important; }}
/* structural chars  :  -  |  >  [ ] — muted */
.cm-editor .cm-meta     {{ color: #858585 !important; }}
/* document markers --- ... — muted blue */
.cm-editor .cm-def      {{ color: #9cdcfe !important; }}
/* anchors & aliases — light teal */
.cm-editor .cm-variable {{ color: #4ec9b0 !important; }}

/* SQL output: make keywords pop */
.sql-output .cm-editor .cm-keyword {{ color: #569cd6 !important; }}
.sql-output .cm-editor .cm-builtin {{ color: #4ec9b0 !important; }}
"""

_DARK_MODE_INIT_JS = """
() => {
    if (!window.location.search.includes('__theme=')) {
        const url = new URL(window.location);
        url.searchParams.set('__theme', 'dark');
        window.location.replace(url.href);
    }
}
"""

_DARK_MODE_TOGGLE_JS = """
() => {
    const url = new URL(window.location);
    const current = url.searchParams.get('__theme');
    url.searchParams.set('__theme', current === 'dark' ? 'light' : 'dark');
    window.location.replace(url.href);
}
"""


def _format_sql(sql: str) -> str:
    """Pretty-print SQL with keyword-per-line formatting."""
    import re

    formatted = sqlparse.format(
        sql,
        reindent=True,
        keyword_case="upper",
        indent_width=2,
        wrap_after=80,
    )
    # sqlparse doesn't break after UNION ALL — ensure newline before next SELECT
    # Capture leading indentation so the new SELECT line keeps alignment
    formatted = re.sub(
        r"^(\s*)(UNION ALL(?:\s+BY NAME)?)\s+(SELECT\b)",
        r"\1\2\n\1\3",
        formatted,
        flags=re.MULTILINE,
    )
    return formatted


def _load_example_model() -> str:
    """Load the bundled example OBML model, or return a placeholder."""
    from pathlib import Path

    candidates = [
        Path(__file__).resolve().parents[3] / "examples" / "sem-layer.obml.yml",
        Path.cwd() / "examples" / "sem-layer.obml.yml",
    ]
    for p in candidates:
        if p.is_file():
            return p.read_text(encoding="utf-8")
    return "# Place your OBML model YAML here\n"


def _fetch_dialects(api_url: str) -> list[str]:
    """Fetch dialect names from the API, falling back to hardcoded list."""
    try:
        resp = httpx.get(f"{api_url.rstrip('/')}/dialects", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        names = [d["name"] for d in data.get("dialects", [])]
        return names if names else _FALLBACK_DIALECTS
    except Exception:
        return _FALLBACK_DIALECTS


def compile_sql(model_yaml: str, query_yaml: str, dialect: str, api_url: str) -> str:
    """Compile SQL by calling the OrionBelt REST API.

    Returns the generated SQL string, or an error message prefixed with ``-- Error:``.
    """
    api_url = api_url.rstrip("/") if api_url else _DEFAULT_API_URL
    session_id: str | None = None

    try:
        client = httpx.Client(base_url=api_url, timeout=30)

        # 1. Create session
        resp = client.post("/sessions")
        resp.raise_for_status()
        session_id = resp.json()["session_id"]

        # 2. Load model
        resp = client.post(
            f"/sessions/{session_id}/models",
            json={"model_yaml": model_yaml},
        )
        if resp.status_code == 422:
            detail = resp.json().get("detail", resp.text)
            return f"-- Error: Model validation failed\n-- {detail}"
        resp.raise_for_status()
        model_id = resp.json()["model_id"]

        # 3. Parse query YAML
        try:
            query_dict = yaml.safe_load(query_yaml)
        except yaml.YAMLError as exc:
            return f"-- Error: Invalid query YAML\n-- {exc}"

        if not isinstance(query_dict, dict):
            return "-- Error: Query YAML must be a mapping (dict), not a scalar or list"

        # 4. Compile query
        resp = client.post(
            f"/sessions/{session_id}/query/sql",
            json={"model_id": model_id, "query": query_dict, "dialect": dialect},
        )
        if resp.status_code in (400, 422):
            detail = resp.json().get("detail", resp.text)
            return f"-- Error: Query compilation failed\n-- {detail}"
        resp.raise_for_status()
        data = resp.json()
        sql: str = data["sql"]
        formatted = _format_sql(sql)

        # Surface validation state and warnings above the SQL output
        warnings: list[str] = data.get("warnings", [])
        sql_valid: bool = data.get("sql_valid", True)
        header_lines: list[str] = []
        if not sql_valid:
            header_lines.append("-- ⚠ SQL validation failed")
        for w in warnings:
            header_lines.append(f"-- WARNING: {w}")
        if header_lines:
            header_lines.append("")  # blank line before SQL
            return "\n".join(header_lines) + "\n" + formatted
        return formatted

    except httpx.ConnectError:
        return (
            f"-- Error: Cannot connect to API at {api_url}\n"
            "-- Make sure the server is running: uv run orionbelt-api"
        )
    except httpx.HTTPStatusError as exc:
        return f"-- Error: HTTP {exc.response.status_code}\n-- {exc.response.text}"
    except Exception as exc:
        return f"-- Error: {exc}"
    finally:
        if session_id is not None:
            with contextlib.suppress(Exception):
                httpx.Client(base_url=api_url, timeout=5).delete(f"/sessions/{session_id}")


def create_ui() -> None:
    """Build and launch the Gradio interface."""
    import gradio as gr

    example_model = _load_example_model()
    dialects = _fetch_dialects(_DEFAULT_API_URL)

    with gr.Blocks(
        title="OrionBelt Semantic Layer",
        css=_CSS,
        js=_DARK_MODE_INIT_JS,
    ) as demo:
        with gr.Row(elem_classes=["header-row"]):
            gr.Markdown("## OrionBelt Semantic Layer")
            dark_btn = gr.Button("Light / Dark", size="sm", scale=0, min_width=120)

        dark_btn.click(fn=None, js=_DARK_MODE_TOGGLE_JS)

        with gr.Row(elem_classes=["settings-row"]):
            dialect = gr.Dropdown(
                choices=dialects,
                value=dialects[0] if dialects else "postgres",
                label="SQL Dialect",
                scale=1,
            )
            api_url = gr.Textbox(
                value=_DEFAULT_API_URL,
                label="API Base URL",
                scale=2,
            )

        with gr.Row(equal_height=True):
            model_input = gr.Code(
                value=example_model,
                language="yaml",
                label="OBML Model (YAML)",
                lines=15,
                scale=3,
                interactive=True,
                elem_classes=["code-editor"],
            )
            query_input = gr.Code(
                value=_DEFAULT_QUERY,
                language="yaml",
                label="Query (YAML)",
                lines=15,
                scale=2,
                interactive=True,
                elem_classes=["code-editor"],
            )

        compile_btn = gr.Button("Compile SQL", variant="primary", elem_classes=["purple-btn"])

        sql_output = gr.Code(
            language="sql",
            label="Generated SQL",
            interactive=False,
            lines=10,
            elem_classes=["sql-output"],
        )

        compile_btn.click(
            fn=compile_sql,
            inputs=[model_input, query_input, dialect, api_url],
            outputs=sql_output,
        )

    demo.launch()


def main() -> None:
    """Entry point for ``orionbelt-ui`` console script."""
    create_ui()
