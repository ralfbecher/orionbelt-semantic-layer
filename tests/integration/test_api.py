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


class TestSessionIsolation:
    async def test_models_in_session_a_not_visible_in_b(self, client: AsyncClient) -> None:
        sid_a = (await client.post("/sessions")).json()["session_id"]
        sid_b = (await client.post("/sessions")).json()["session_id"]

        await client.post(f"/sessions/{sid_a}/models", json={"model_yaml": SAMPLE_MODEL_YAML})

        models_a = (await client.get(f"/sessions/{sid_a}/models")).json()
        models_b = (await client.get(f"/sessions/{sid_b}/models")).json()

        assert len(models_a) == 1
        assert len(models_b) == 0
