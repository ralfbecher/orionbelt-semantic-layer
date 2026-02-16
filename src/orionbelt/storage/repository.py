"""Abstract repository interfaces for persistence."""

from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from orionbelt.models.artifact import Artifact
from orionbelt.models.project import ModelVersion, Project, Tenant


class TenantRepository(ABC):
    @abstractmethod
    async def create(self, tenant: Tenant) -> Tenant: ...

    @abstractmethod
    async def get(self, tenant_id: UUID) -> Tenant | None: ...

    @abstractmethod
    async def list(self) -> list[Tenant]: ...


class ProjectRepository(ABC):
    @abstractmethod
    async def create(self, project: Project) -> Project: ...

    @abstractmethod
    async def get(self, project_id: UUID) -> Project | None: ...

    @abstractmethod
    async def list(self, tenant_id: UUID | None = None) -> list[Project]: ...


class ModelVersionRepository(ABC):
    @abstractmethod
    async def create(self, version: ModelVersion) -> ModelVersion: ...

    @abstractmethod
    async def get(self, version_id: UUID) -> ModelVersion | None: ...

    @abstractmethod
    async def list(self, project_id: UUID | None = None) -> list[ModelVersion]: ...

    @abstractmethod
    async def update(self, version: ModelVersion) -> ModelVersion: ...


class ArtifactRepository(ABC):
    @abstractmethod
    async def create(self, artifact: Artifact) -> Artifact: ...

    @abstractmethod
    async def get(self, artifact_id: UUID) -> Artifact | None: ...

    @abstractmethod
    async def list(self, model_version_id: UUID | None = None) -> list[Artifact]: ...
