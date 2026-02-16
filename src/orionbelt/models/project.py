"""Tenant, project, and model version models."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


class ModelStatus(StrEnum):
    DRAFT = "draft"
    PUBLISHED = "published"


class Tenant(BaseModel):
    """A tenant / workspace — the top-level isolation boundary."""

    id: UUID
    name: str
    created_at: datetime = Field(default_factory=_utcnow)


class Project(BaseModel):
    """A project container for semantic models."""

    id: UUID
    tenant_id: UUID
    name: str
    description: str = ""
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class ModelVersion(BaseModel):
    """A versioned semantic model within a project."""

    id: UUID
    project_id: UUID
    version: int
    status: ModelStatus = ModelStatus.DRAFT
    raw_yaml: str = ""
    created_at: datetime = Field(default_factory=_utcnow)
    published_at: datetime | None = None
