"""Integration tests for the FastAPI REST API."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from orionbelt.api.app import create_app
from orionbelt.api.deps import init_session_manager, reset_session_manager
from orionbelt.service.session_manager import SessionManager
from orionbelt.settings import Settings
from tests.conftest import SAMPLE_MODEL_YAML


@pytest.fixture
def app():
    settings = Settings(session_ttl_seconds=3600, session_cleanup_interval=9999)
    app = create_app(settings=settings)
    # Manually init SessionManager (ASGITransport doesn't trigger lifespan)
    mgr = SessionManager(
        ttl_seconds=settings.session_ttl_seconds,
        cleanup_interval=settings.session_cleanup_interval,
    )
    init_session_manager(mgr)
    yield app
    reset_session_manager()


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Health & Dialects
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    async def test_health(self, client: AsyncClient) -> None:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data


class TestDialectsEndpoint:
    async def test_list_dialects(self, client: AsyncClient) -> None:
        response = await client.get("/dialects")
        assert response.status_code == 200
        data = response.json()
        names = [d["name"] for d in data["dialects"]]
        assert "postgres" in names
        assert "snowflake" in names
        assert "clickhouse" in names
        assert "dremio" in names
        assert "databricks" in names


class TestSettingsEndpoint:
    async def test_settings_default(self, client: AsyncClient) -> None:
        response = await client.get("/settings")
        assert response.status_code == 200
        data = response.json()
        assert data["single_model_mode"] is False
        assert data["model_yaml"] is None
        assert data["session_ttl_seconds"] == 3600  # from fixture

    async def test_settings_single_model(self, single_model_client: AsyncClient) -> None:
        response = await single_model_client.get("/settings")
        assert response.status_code == 200
        data = response.json()
        assert data["single_model_mode"] is True
        assert data["model_yaml"] is not None
        assert "dataObjects" in data["model_yaml"]
        assert data["session_ttl_seconds"] == 3600  # from fixture


# ---------------------------------------------------------------------------
# Session endpoints
# ---------------------------------------------------------------------------


class TestSessionEndpoints:
    async def test_create_session(self, client: AsyncClient) -> None:
        response = await client.post("/sessions")
        assert response.status_code == 201
        data = response.json()
        assert "session_id" in data
        assert data["model_count"] == 0

    async def test_create_session_with_metadata(self, client: AsyncClient) -> None:
        response = await client.post("/sessions", json={"metadata": {"env": "test"}})
        assert response.status_code == 201
        data = response.json()
        assert data["metadata"] == {"env": "test"}

    async def test_list_sessions(self, client: AsyncClient) -> None:
        await client.post("/sessions")
        await client.post("/sessions")
        response = await client.get("/sessions")
        assert response.status_code == 200
        data = response.json()
        assert len(data["sessions"]) == 2

    async def test_get_session(self, client: AsyncClient) -> None:
        create = await client.post("/sessions")
        sid = create.json()["session_id"]
        response = await client.get(f"/sessions/{sid}")
        assert response.status_code == 200
        assert response.json()["session_id"] == sid

    async def test_get_missing_session(self, client: AsyncClient) -> None:
        response = await client.get("/sessions/nonexist123")
        assert response.status_code == 404

    async def test_delete_session(self, client: AsyncClient) -> None:
        create = await client.post("/sessions")
        sid = create.json()["session_id"]
        response = await client.delete(f"/sessions/{sid}")
        assert response.status_code == 204
        # Verify it's gone
        response = await client.get(f"/sessions/{sid}")
        assert response.status_code == 404

    async def test_delete_missing_session(self, client: AsyncClient) -> None:
        response = await client.delete("/sessions/nonexist123")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Session model flow
# ---------------------------------------------------------------------------


class TestSessionModelFlow:
    async def test_load_model(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        response = await client.post(
            f"/sessions/{sid}/models",
            json={"model_yaml": SAMPLE_MODEL_YAML},
        )
        assert response.status_code == 201
        data = response.json()
        assert "model_id" in data
        assert data["data_objects"] == 2
        assert data["dimensions"] == 1
        assert data["measures"] == 3

    async def test_list_models(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        await client.post(f"/sessions/{sid}/models", json={"model_yaml": SAMPLE_MODEL_YAML})
        response = await client.get(f"/sessions/{sid}/models")
        assert response.status_code == 200
        models = response.json()
        assert len(models) == 1

    async def test_describe_model(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        load = await client.post(f"/sessions/{sid}/models", json={"model_yaml": SAMPLE_MODEL_YAML})
        mid = load.json()["model_id"]
        response = await client.get(f"/sessions/{sid}/models/{mid}")
        assert response.status_code == 200
        data = response.json()
        assert data["model_id"] == mid
        assert len(data["data_objects"]) == 2

    async def test_describe_missing_model(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        response = await client.get(f"/sessions/{sid}/models/nonexist")
        assert response.status_code == 404

    async def test_remove_model(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        load = await client.post(f"/sessions/{sid}/models", json={"model_yaml": SAMPLE_MODEL_YAML})
        mid = load.json()["model_id"]
        response = await client.delete(f"/sessions/{sid}/models/{mid}")
        assert response.status_code == 204
        # Verify it's gone
        response = await client.get(f"/sessions/{sid}/models/{mid}")
        assert response.status_code == 404

    async def test_validate_in_session(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        response = await client.post(
            f"/sessions/{sid}/validate",
            json={"model_yaml": SAMPLE_MODEL_YAML},
        )
        assert response.status_code == 200
        assert response.json()["valid"] is True

    async def test_compile_query_in_session(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        load = await client.post(f"/sessions/{sid}/models", json={"model_yaml": SAMPLE_MODEL_YAML})
        mid = load.json()["model_id"]
        response = await client.post(
            f"/sessions/{sid}/query/sql",
            json={
                "model_id": mid,
                "query": {
                    "select": {
                        "dimensions": ["Customer Country"],
                        "measures": ["Total Revenue"],
                    },
                },
                "dialect": "postgres",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "SELECT" in data["sql"]
        assert data["dialect"] == "postgres"

    async def test_load_invalid_model(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        response = await client.post(
            f"/sessions/{sid}/models",
            json={"model_yaml": "}{bad"},
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Session isolation
# ---------------------------------------------------------------------------


class TestDiagramEndpoint:
    async def test_diagram_er(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        load = await client.post(f"/sessions/{sid}/models", json={"model_yaml": SAMPLE_MODEL_YAML})
        mid = load.json()["model_id"]
        response = await client.get(f"/sessions/{sid}/models/{mid}/diagram/er")
        assert response.status_code == 200
        data = response.json()
        assert "mermaid" in data
        mermaid = data["mermaid"]
        assert "erDiagram" in mermaid
        assert "Orders" in mermaid
        assert "Customers" in mermaid
        # Relationship line
        assert "}o--||" in mermaid

    async def test_diagram_er_hide_columns(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        load = await client.post(f"/sessions/{sid}/models", json={"model_yaml": SAMPLE_MODEL_YAML})
        mid = load.json()["model_id"]
        response = await client.get(
            f"/sessions/{sid}/models/{mid}/diagram/er",
            params={"show_columns": False},
        )
        assert response.status_code == 200
        mermaid = response.json()["mermaid"]
        # Should NOT contain column attribute blocks
        assert "{" not in mermaid.split("\n", 1)[-1].split("}o")[0]

    async def test_diagram_er_missing_model(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        response = await client.get(f"/sessions/{sid}/models/nonexist/diagram/er")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Session isolation
# ---------------------------------------------------------------------------


class TestRequestBodyLimit:
    """Verify RequestBodyLimitMiddleware rejects oversized payloads."""

    async def test_model_endpoint_rejects_body_over_5mb(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        oversized = "x" * (5 * 1024 * 1024 + 1)
        response = await client.post(
            f"/sessions/{sid}/models",
            json={"model_yaml": oversized},
        )
        assert response.status_code == 413
        assert "too large" in response.json()["detail"]

    async def test_validate_endpoint_rejects_body_over_5mb(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        oversized = "x" * (5 * 1024 * 1024 + 1)
        response = await client.post(
            f"/sessions/{sid}/validate",
            json={"model_yaml": oversized},
        )
        assert response.status_code == 413
        assert "too large" in response.json()["detail"]

    async def test_query_endpoint_rejects_body_over_1mb(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        oversized = "x" * (1 * 1024 * 1024 + 1)
        response = await client.post(
            f"/sessions/{sid}/query/sql",
            content=oversized,
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 413
        assert "too large" in response.json()["detail"]

    async def test_small_payload_passes(self, client: AsyncClient) -> None:
        sid = (await client.post("/sessions")).json()["session_id"]
        response = await client.post(
            f"/sessions/{sid}/validate",
            json={"model_yaml": "version: '1'"},
        )
        # Not 413 — body is small enough
        assert response.status_code != 413

    async def test_content_length_header_checked_first(self, client: AsyncClient) -> None:
        """A spoofed Content-Length > limit triggers 413 before body is read."""
        sid = (await client.post("/sessions")).json()["session_id"]
        response = await client.post(
            f"/sessions/{sid}/query/sql",
            content=b"small",
            headers={
                "content-type": "application/json",
                "content-length": "99999999",
            },
        )
        assert response.status_code == 413


class TestSessionIsolation:
    async def test_models_in_session_a_not_visible_in_b(self, client: AsyncClient) -> None:
        sid_a = (await client.post("/sessions")).json()["session_id"]
        sid_b = (await client.post("/sessions")).json()["session_id"]

        await client.post(f"/sessions/{sid_a}/models", json={"model_yaml": SAMPLE_MODEL_YAML})

        models_a = (await client.get(f"/sessions/{sid_a}/models")).json()
        models_b = (await client.get(f"/sessions/{sid_b}/models")).json()

        assert len(models_a) == 1
        assert len(models_b) == 0


# ---------------------------------------------------------------------------
# Single-model mode
# ---------------------------------------------------------------------------


@pytest.fixture
def single_model_app(tmp_path):
    """Create an app in single-model mode with SAMPLE_MODEL_YAML on disk."""
    model_file = tmp_path / "model.yaml"
    model_file.write_text(SAMPLE_MODEL_YAML)
    settings = Settings(
        session_ttl_seconds=3600,
        session_cleanup_interval=9999,
        model_file=str(model_file),
    )
    app = create_app(settings=settings)
    # Manually init (ASGITransport doesn't trigger lifespan)
    from orionbelt.api.app import _read_model_file

    preload_yaml = _read_model_file(str(model_file))
    mgr = SessionManager(
        ttl_seconds=settings.session_ttl_seconds,
        cleanup_interval=settings.session_cleanup_interval,
    )
    init_session_manager(mgr, preload_model_yaml=preload_yaml)
    yield app
    reset_session_manager()


@pytest.fixture
async def single_model_client(single_model_app):
    transport = ASGITransport(app=single_model_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestSingleModelMode:
    async def test_session_created_with_preloaded_model(
        self, single_model_client: AsyncClient
    ) -> None:
        response = await single_model_client.post("/sessions")
        assert response.status_code == 201
        data = response.json()
        assert data["model_count"] == 1

    async def test_model_upload_blocked(self, single_model_client: AsyncClient) -> None:
        sid = (await single_model_client.post("/sessions")).json()["session_id"]
        response = await single_model_client.post(
            f"/sessions/{sid}/models",
            json={"model_yaml": SAMPLE_MODEL_YAML},
        )
        assert response.status_code == 403
        assert "model upload is disabled" in response.json()["detail"]

    async def test_model_removal_blocked(self, single_model_client: AsyncClient) -> None:
        sid = (await single_model_client.post("/sessions")).json()["session_id"]
        models = (await single_model_client.get(f"/sessions/{sid}/models")).json()
        mid = models[0]["model_id"]
        response = await single_model_client.delete(f"/sessions/{sid}/models/{mid}")
        assert response.status_code == 403
        assert "model removal is disabled" in response.json()["detail"]

    async def test_query_works_with_preloaded_model(
        self, single_model_client: AsyncClient
    ) -> None:
        sid = (await single_model_client.post("/sessions")).json()["session_id"]
        models = (await single_model_client.get(f"/sessions/{sid}/models")).json()
        mid = models[0]["model_id"]
        response = await single_model_client.post(
            f"/sessions/{sid}/query/sql",
            json={
                "model_id": mid,
                "query": {
                    "select": {
                        "dimensions": ["Customer Country"],
                        "measures": ["Total Revenue"],
                    },
                },
                "dialect": "postgres",
            },
        )
        assert response.status_code == 200
        assert "SELECT" in response.json()["sql"]

    async def test_sessions_still_independent(
        self, single_model_client: AsyncClient
    ) -> None:
        """Each session gets its own copy of the preloaded model."""
        sid_a = (await single_model_client.post("/sessions")).json()["session_id"]
        sid_b = (await single_model_client.post("/sessions")).json()["session_id"]
        models_a = (await single_model_client.get(f"/sessions/{sid_a}/models")).json()
        models_b = (await single_model_client.get(f"/sessions/{sid_b}/models")).json()
        assert len(models_a) == 1
        assert len(models_b) == 1
        # Different model IDs (separate ModelStore instances)
        assert models_a[0]["model_id"] != models_b[0]["model_id"]

    async def test_validate_still_works(self, single_model_client: AsyncClient) -> None:
        sid = (await single_model_client.post("/sessions")).json()["session_id"]
        response = await single_model_client.post(
            f"/sessions/{sid}/validate",
            json={"model_yaml": SAMPLE_MODEL_YAML},
        )
        assert response.status_code == 200
        assert response.json()["valid"] is True

    async def test_delete_session_still_works(
        self, single_model_client: AsyncClient
    ) -> None:
        sid = (await single_model_client.post("/sessions")).json()["session_id"]
        response = await single_model_client.delete(f"/sessions/{sid}")
        assert response.status_code == 204
