"""Unit tests for SessionManager."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from orionbelt.service.session_manager import SessionManager, SessionNotFoundError


class TestSessionLifecycle:
    def test_create_session(self, session_manager: SessionManager) -> None:
        info = session_manager.create_session()
        assert len(info.session_id) == 12
        assert info.model_count == 0
        assert info.metadata == {}

    def test_create_with_metadata(self, session_manager: SessionManager) -> None:
        info = session_manager.create_session(metadata={"user": "alice"})
        assert info.metadata == {"user": "alice"}

    def test_get_store(self, session_manager: SessionManager) -> None:
        info = session_manager.create_session()
        store = session_manager.get_store(info.session_id)
        assert store is not None
        assert store.list_models() == []

    def test_get_store_missing_raises(self, session_manager: SessionManager) -> None:
        with pytest.raises(SessionNotFoundError, match="not found"):
            session_manager.get_store("nonexist123")

    def test_get_session(self, session_manager: SessionManager) -> None:
        info = session_manager.create_session()
        retrieved = session_manager.get_session(info.session_id)
        assert retrieved.session_id == info.session_id

    def test_close_session(self, session_manager: SessionManager) -> None:
        info = session_manager.create_session()
        session_manager.close_session(info.session_id)
        with pytest.raises(SessionNotFoundError):
            session_manager.get_store(info.session_id)

    def test_close_missing_raises(self, session_manager: SessionManager) -> None:
        with pytest.raises(SessionNotFoundError, match="not found"):
            session_manager.close_session("nonexist123")

    def test_list_sessions(self, session_manager: SessionManager) -> None:
        session_manager.create_session()
        session_manager.create_session()
        sessions = session_manager.list_sessions()
        assert len(sessions) == 2

    def test_active_count(self, session_manager: SessionManager) -> None:
        assert session_manager.active_count == 0
        session_manager.create_session()
        session_manager.create_session()
        assert session_manager.active_count == 2


class TestSessionExpiration:
    def test_expired_session_raises(self) -> None:
        mgr = SessionManager(ttl_seconds=0, cleanup_interval=9999)
        info = mgr.create_session()
        time.sleep(0.05)  # ensure TTL has passed
        with pytest.raises(SessionNotFoundError, match="expired"):
            mgr.get_store(info.session_id)

    def test_get_session_expired_raises(self) -> None:
        mgr = SessionManager(ttl_seconds=0, cleanup_interval=9999)
        info = mgr.create_session()
        time.sleep(0.05)
        with pytest.raises(SessionNotFoundError, match="expired"):
            mgr.get_session(info.session_id)


class TestDefaultSession:
    def test_get_or_create_default(self, session_manager: SessionManager) -> None:
        store1 = session_manager.get_or_create_default()
        store2 = session_manager.get_or_create_default()
        assert store1 is store2

    def test_default_not_in_list(self, session_manager: SessionManager) -> None:
        session_manager.get_or_create_default()
        assert session_manager.list_sessions() == []


class TestCleanup:
    def test_purge_expired(self) -> None:
        mgr = SessionManager(ttl_seconds=0, cleanup_interval=9999)
        mgr.create_session()
        mgr.create_session()
        time.sleep(0.05)
        mgr._purge_expired()
        assert mgr.active_count == 0

    def test_cleanup_thread(self) -> None:
        mgr = SessionManager(ttl_seconds=0, cleanup_interval=0.05)
        mgr.start()
        try:
            mgr.create_session()
            time.sleep(0.2)  # wait for cleanup to run
            assert mgr.active_count == 0
        finally:
            mgr.stop()


class TestThreadSafety:
    def test_concurrent_creates(self, session_manager: SessionManager) -> None:
        def create() -> str:
            info = session_manager.create_session()
            return info.session_id

        with ThreadPoolExecutor(max_workers=10) as pool:
            ids = list(pool.map(lambda _: create(), range(50)))

        assert len(set(ids)) == 50
        assert session_manager.active_count == 50


class TestSessionIsolation:
    def test_stores_are_independent(self, session_manager: SessionManager) -> None:
        """Models loaded in one session are not visible in another."""
        from tests.conftest import SAMPLE_MODEL_YAML

        info_a = session_manager.create_session()
        info_b = session_manager.create_session()

        store_a = session_manager.get_store(info_a.session_id)
        store_b = session_manager.get_store(info_b.session_id)

        store_a.load_model(SAMPLE_MODEL_YAML)

        assert len(store_a.list_models()) == 1
        assert len(store_b.list_models()) == 0
