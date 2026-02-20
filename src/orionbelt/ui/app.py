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

_CSS = """\
/* ── Layout: full-width, fit viewport ── */
.gradio-container {
  max-width: 100% !important;
  padding: 4px 16px !important;
}
/* compact header */
.header-row { min-height: 0 !important; padding: 0 !important; }
.header-row h2 { margin: 0 !important; }
/* compact settings row */
.settings-row { min-height: 0 !important; }

/* Code editors + SQL output: viewport-percentage heights */
.code-editor .cm-editor { max-height: 45dvh !important; }
.sql-output .cm-editor { max-height: 20dvh !important; }

/* purple primary button — compact */
.purple-btn {
  background: linear-gradient(135deg, #7c3aed, #9333ea) !important;
  border: none !important;
  color: white !important;
  padding-top: 6px !important;
  padding-bottom: 6px !important;
  margin: 0 !important;
}
.purple-btn:hover {
  background: linear-gradient(135deg, #6d28d9, #7c3aed) !important;
}

/* ── YAML / SQL syntax highlighting (dark-mode optimised) ── */
.cm-editor .cm-atom     { color: #7dcfff !important; }
.cm-editor .cm-string   { color: #ce9178 !important; }
.cm-editor .cm-comment  { color: #6a9955 !important; font-style: italic; }
.cm-editor .cm-number   { color: #b5cea8 !important; }
.cm-editor .cm-keyword  { color: #c586c0 !important; }
.cm-editor .cm-meta     { color: #858585 !important; }
.cm-editor .cm-def      { color: #9cdcfe !important; }
.cm-editor .cm-variable { color: #4ec9b0 !important; }
.sql-output .cm-editor .cm-keyword { color: #569cd6 !important; }
.sql-output .cm-editor .cm-builtin { color: #4ec9b0 !important; }

/* ── Upload icon button ── */
.ob-upload-btn {
  background: transparent;
  border: none;
  cursor: pointer;
  color: var(--body-text-color, #fff);
  padding: 4px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  transition: opacity 0.15s ease;
}
.ob-upload-btn:hover { opacity: 0.7; }

/* Bridge textboxes: rendered but removed from layout flow */
.ob-bridge {
  position: absolute !important;
  width: 1px !important;
  height: 1px !important;
  overflow: hidden !important;
  clip: rect(0,0,0,0) !important;
  padding: 0 !important;
  margin: -1px !important;
  border: 0 !important;
}

/* ── ER Diagram tab ── */
#er-diagram {
  overflow: auto;
  max-height: calc(100dvh - 220px);
  border: 1px solid var(--border-color-primary);
  border-radius: 8px;
  padding: 8px;
}
#er-diagram svg {
  transform-origin: top left;
  transition: transform 0.15s ease;
}
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

# Simple redirect — used as .then() after saving state.
_THEME_REDIRECT_JS = """
() => {
    setTimeout(() => {
        const url = new URL(window.location);
        const current = url.searchParams.get('__theme');
        url.searchParams.set('__theme', current === 'dark' ? 'light' : 'dark');
        window.location.replace(url.href);
    }, 50);
}
"""

# JS pre-processor: detect the active Gradio colour scheme from the URL
# and inject the matching Mermaid theme into the last argument slot.
_DETECT_THEME_JS = """
(...args) => {
    const p = new URLSearchParams(window.location.search);
    const isDark = p.get('__theme') !== 'light';
    args[args.length - 1] = isDark ? 'dark' : 'default';
    return args;
}
"""

# SVG icon: upload (Lucide style, matches Gradio's 16x16 toolbar icons)
_UPLOAD_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16"'
    ' viewBox="0 0 24 24" fill="none" stroke="currentColor"'
    ' stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
    '<polyline points="17 8 12 3 7 8"/>'
    '<line x1="12" y1="3" x2="12" y2="15"/></svg>'
)

_INJECT_UPLOAD_JS = (
    """
() => {
    const SVG = '"""
    + _UPLOAD_SVG.replace("'", "\\'")
    + """';
    function setBridge(bridgeId, content) {
        var el = document.getElementById(bridgeId);
        if (!el) return;
        var ta = el.querySelector('textarea') || el.querySelector('input');
        if (!ta) return;
        ta.value = content;
        ta.dispatchEvent(new Event('input', {bubbles: true}));
        ta.dispatchEvent(new Event('change', {bubbles: true}));
    }

    function addUploadBtn(codeId, bridgeId) {
        const root = document.getElementById(codeId);
        if (!root || root.querySelector('.ob-upload-btn')) return;

        /* Find the toolbar: locate an SVG-icon button (download/copy) */
        /* and use its parent as the toolbar container.               */
        var svgInBtn = root.querySelector('button svg');
        if (!svgInBtn) return;
        var toolbar = svgInBtn.closest('button').parentElement;

        const btn = document.createElement('button');
        btn.className = 'ob-upload-btn';
        btn.title = 'Load YAML file';
        btn.innerHTML = SVG;

        btn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            var fi = document.createElement('input');
            fi.type = 'file';
            fi.accept = '.yaml,.yml';
            fi.addEventListener('change', function() {
                var f = fi.files[0];
                if (!f) return;
                var reader = new FileReader();
                reader.addEventListener('load', function() {
                    setBridge(bridgeId, reader.result);
                });
                reader.readAsText(f);
            });
            fi.click();
        });

        /* Prepend to toolbar — places it left of download/copy */
        toolbar.style.display = 'flex';
        toolbar.style.flexWrap = 'nowrap';
        toolbar.style.alignItems = 'center';
        toolbar.insertBefore(btn, toolbar.firstChild);
    }

    /* Retry a few times — components render asynchronously. */
    var attempts = 0;
    var iv = setInterval(function() {
        addUploadBtn('ob-model', 'ob-model-bridge');
        addUploadBtn('ob-query', 'ob-query-bridge');
        if (++attempts >= 10) clearInterval(iv);
    }, 300);
}
"""
)


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


def _fetch_diagram_er(
    model_yaml: str, show_columns: bool, api_url: str, theme: str = "dark"
) -> str:
    """Fetch a Mermaid ER diagram via the REST API.

    Falls back to local generation (using ``service.diagram``) when the API
    is not reachable.  *theme* is the Mermaid theme name (``"dark"`` or
    ``"default"``), injected by JS based on the active Gradio colour scheme.
    Returns a Markdown string with a ``mermaid`` code fence.
    """
    if not model_yaml or not model_yaml.strip():
        return "*No model YAML provided.*"

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
            return f"**Model validation failed:** {detail}"
        resp.raise_for_status()
        model_id = resp.json()["model_id"]

        # 3. Fetch ER diagram
        resp = client.get(
            f"/sessions/{session_id}/models/{model_id}/diagram/er",
            params={"show_columns": show_columns, "theme": theme},
        )
        resp.raise_for_status()
        mermaid: str = resp.json()["mermaid"]
        return f"```mermaid\n{mermaid}\n```"

    except httpx.ConnectError:
        # API not available — fall back to local generation
        return _generate_mermaid_er_local(model_yaml, show_columns, theme=theme)
    except httpx.HTTPStatusError as exc:
        return f"**Error:** HTTP {exc.response.status_code} — {exc.response.text}"
    except Exception as exc:
        return f"**Error:** {exc}"
    finally:
        if session_id is not None:
            with contextlib.suppress(Exception):
                httpx.Client(base_url=api_url, timeout=5).delete(f"/sessions/{session_id}")


def _generate_mermaid_er_local(
    model_yaml: str, show_columns: bool = True, *, theme: str = "dark"
) -> str:
    """Generate a Mermaid ER diagram locally from raw OBML YAML (no API)."""
    from orionbelt.parser.loader import TrackedLoader
    from orionbelt.parser.resolver import ReferenceResolver
    from orionbelt.service.diagram import generate_mermaid_er

    try:
        loader = TrackedLoader()
        raw, source_map = loader.load_string(model_yaml)
        resolver = ReferenceResolver()
        model, result = resolver.resolve(raw, source_map)
        if not result.valid:
            msgs = "; ".join(e.message for e in result.errors)
            return f"**Model validation failed:** {msgs}"
        mermaid = generate_mermaid_er(model, show_columns=show_columns, theme=theme)
        return f"```mermaid\n{mermaid}\n```"
    except Exception as exc:
        return f"**Error:** {exc}"


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

    from orionbelt import __version__

    example_model = _load_example_model()
    dialects = _fetch_dialects(_DEFAULT_API_URL)
    default_dialect = dialects[0] if dialects else "postgres"

    with gr.Blocks(
        title="OrionBelt Semantic Layer",
    ) as demo:
        # ── Browser-persisted state (localStorage via Gradio BrowserState) ──
        saved_model = gr.BrowserState("", storage_key="ob_model_yaml")
        saved_query = gr.BrowserState("", storage_key="ob_query_yaml")
        saved_api = gr.BrowserState(_DEFAULT_API_URL, storage_key="ob_api_url")
        saved_dialect = gr.BrowserState(default_dialect, storage_key="ob_dialect")
        saved_tab = gr.BrowserState(0, storage_key="ob_active_tab")
        saved_zoom = gr.BrowserState(100, storage_key="ob_zoom")
        active_tab = gr.State(0)

        with gr.Row(elem_classes=["header-row"]):
            gr.Markdown(f"## OrionBelt Semantic Layer <small>v{__version__}</small>")
            dark_btn = gr.Button("Light / Dark", size="sm", scale=0, min_width=120)

        with gr.Tabs() as tabs:
            with gr.Tab("SQL Compiler", id=0) as sql_tab:
                with gr.Row(elem_classes=["settings-row"]):
                    dialect = gr.Dropdown(
                        choices=dialects,
                        value=default_dialect,
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
                        label="OBML Model (YAML) \u2014 schema/obml-schema.json",
                        lines=11,
                        scale=3,
                        interactive=True,
                        elem_classes=["code-editor"],
                        elem_id="ob-model",
                    )
                    query_input = gr.Code(
                        value=_DEFAULT_QUERY,
                        language="yaml",
                        label="Query (YAML) \u2014 schema/query-schema.json",
                        lines=11,
                        scale=2,
                        interactive=True,
                        elem_classes=["code-editor"],
                        elem_id="ob-query",
                    )

                # Hidden textboxes: JS writes file content here → Python
                # forwards to Code editors (bridges JS↔Gradio state).
                model_bridge = gr.Textbox(
                    elem_id="ob-model-bridge",
                    container=False,
                    elem_classes=["ob-bridge"],
                )
                query_bridge = gr.Textbox(
                    elem_id="ob-query-bridge",
                    container=False,
                    elem_classes=["ob-bridge"],
                )
                model_bridge.change(
                    fn=lambda x: x,
                    inputs=[model_bridge],
                    outputs=[model_input],
                )
                query_bridge.change(
                    fn=lambda x: x,
                    inputs=[query_bridge],
                    outputs=[query_input],
                )

                compile_btn = gr.Button(
                    "Compile SQL", variant="primary", elem_classes=["purple-btn"]
                )

                sql_output = gr.Code(
                    language="sql",
                    label="Generated SQL",
                    interactive=False,
                    lines=3,
                    elem_classes=["sql-output"],
                )

                compile_btn.click(
                    fn=compile_sql,
                    inputs=[model_input, query_input, dialect, api_url],
                    outputs=sql_output,
                )

            with gr.Tab("ER Diagram", id=1) as er_tab:
                with gr.Row():
                    show_columns_cb = gr.Checkbox(value=True, label="Show columns")
                    zoom_slider = gr.Slider(
                        minimum=10,
                        maximum=200,
                        value=100,
                        step=10,
                        label="Zoom %",
                        scale=1,
                    )
                    er_btn = gr.Button(
                        "Refresh Diagram",
                        variant="primary",
                        elem_classes=["purple-btn"],
                    )

                # Hidden input — JS injects the Mermaid theme at call time
                theme_input = gr.Textbox(value="dark", visible=False)

                mermaid_output = gr.Markdown(
                    value="*Click 'Refresh Diagram' to generate the ER diagram "
                    "from the model YAML.*",
                    elem_id="er-diagram",
                )

                _apply_zoom_js = """(zoom) => {
                    const el = document.querySelector('#er-diagram svg');
                    if (el) el.style.transform = 'scale(' + (zoom / 100) + ')';
                }"""

                # After diagram generation, Mermaid renders the SVG asynchronously.
                # Poll until the SVG appears, then apply the zoom transform.
                _apply_zoom_deferred_js = """(zoom) => {
                    let tries = 0;
                    const t = setInterval(() => {
                        const el = document.querySelector('#er-diagram svg');
                        if (el) {
                            el.style.transform = 'scale(' + (zoom / 100) + ')';
                            clearInterval(t);
                        }
                        if (++tries > 30) clearInterval(t);
                    }, 100);
                }"""

                er_btn.click(
                    fn=_fetch_diagram_er,
                    inputs=[model_input, show_columns_cb, api_url, theme_input],
                    outputs=mermaid_output,
                    js=_DETECT_THEME_JS,
                ).then(
                    fn=None,
                    inputs=[zoom_slider],
                    js=_apply_zoom_deferred_js,
                )

                er_tab.select(
                    fn=_fetch_diagram_er,
                    inputs=[model_input, show_columns_cb, api_url, theme_input],
                    outputs=mermaid_output,
                    js=_DETECT_THEME_JS,
                ).then(
                    fn=None,
                    inputs=[zoom_slider],
                    js=_apply_zoom_deferred_js,
                )

                zoom_slider.change(
                    fn=None,
                    inputs=[zoom_slider],
                    js=_apply_zoom_js,
                )

        # ── Track active tab ──
        sql_tab.select(fn=lambda: 0, outputs=[active_tab])
        er_tab.select(fn=lambda: 1, outputs=[active_tab])

        # ── Toggle: Python reads all inputs → BrowserState, then JS redirects ──
        dark_btn.click(
            fn=lambda m, q, a, d, t, z: (m, q, a, d, t, z),
            inputs=[model_input, query_input, api_url, dialect, active_tab, zoom_slider],
            outputs=[saved_model, saved_query, saved_api, saved_dialect, saved_tab, saved_zoom],
        ).then(
            fn=None,
            js=_THEME_REDIRECT_JS,
        )

        # ── On page load: restore from BrowserState → visible components ──
        def _restore(sm, sq, sa, sd, st, sz):
            return (
                sm if sm else example_model,
                sq if sq else _DEFAULT_QUERY,
                sa if sa else _DEFAULT_API_URL,
                sd if sd else default_dialect,
                gr.Tabs(selected=st if st else 0),
                sz if sz else 100,
            )

        demo.load(
            fn=_restore,
            inputs=[saved_model, saved_query, saved_api, saved_dialect, saved_tab, saved_zoom],
            outputs=[model_input, query_input, api_url, dialect, tabs, zoom_slider],
        ).then(fn=None, js=_INJECT_UPLOAD_JS)

    demo.launch(css=_CSS, js=_DARK_MODE_INIT_JS)


def main() -> None:
    """Entry point for ``orionbelt-ui`` console script."""
    create_ui()
