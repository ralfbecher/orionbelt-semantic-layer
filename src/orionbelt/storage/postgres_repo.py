"""PostgreSQL-backed storage implementation (placeholder for Phase 6)."""

from __future__ import annotations

from uuid import UUID

from orionbelt.models.artifact import Artifact
from orionbelt.models.project import ModelVersion, Project, Tenant
from orionbelt.storage.repository import (
    ArtifactRepository,
    ModelVersionRepository,
    ProjectRepository,
    TenantRepository,
)


class InMemoryTenantRepository(TenantRepository):
    """In-memory tenant repository for development/testing."""

    def __init__(self) -> None:
        self._store: dict[UUID, Tenant] = {}

    async def create(self, tenant: Tenant) -> Tenant:
        self._store[tenant.id] = tenant
        return tenant

    async def get(self, tenant_id: UUID) -> Tenant | None:
        return self._store.get(tenant_id)

    async def list(self) -> list[Tenant]:
        return list(self._store.values())


class InMemoryProjectRepository(ProjectRepository):
    """In-memory project repository for development/testing."""

    def __init__(self) -> None:
        self._store: dict[UUID, Project] = {}

    async def create(self, project: Project) -> Project:
        self._store[project.id] = project
        return project

    async def get(self, project_id: UUID) -> Project | None:
        return self._store.get(project_id)

    async def list(self, tenant_id: UUID | None = None) -> list[Project]:
        projects = list(self._store.values())
        if tenant_id:
            projects = [p for p in projects if p.tenant_id == tenant_id]
        return projects


class InMemoryModelVersionRepository(ModelVersionRepository):
    """In-memory model version repository for development/testing."""

    def __init__(self) -> None:
        self._store: dict[UUID, ModelVersion] = {}

    async def create(self, version: ModelVersion) -> ModelVersion:
        self._store[version.id] = version
        return version

    async def get(self, version_id: UUID) -> ModelVersion | None:
        return self._store.get(version_id)

    async def list(self, project_id: UUID | None = None) -> list[ModelVersion]:
        versions = list(self._store.values())
        if project_id:
            versions = [v for v in versions if v.project_id == project_id]
        return versions

    async def update(self, version: ModelVersion) -> ModelVersion:
        self._store[version.id] = version
        return version


class InMemoryArtifactRepository(ArtifactRepository):
    """In-memory artifact repository for development/testing."""

    def __init__(self) -> None:
        self._store: dict[UUID, Artifact] = {}

    async def create(self, artifact: Artifact) -> Artifact:
        self._store[artifact.id] = artifact
        return artifact

    async def get(self, artifact_id: UUID) -> Artifact | None:
        return self._store.get(artifact_id)

    async def list(self, model_version_id: UUID | None = None) -> list[Artifact]:
        artifacts = list(self._store.values())
        if model_version_id:
            artifacts = [a for a in artifacts if a.model_version_id == model_version_id]
        return artifacts
