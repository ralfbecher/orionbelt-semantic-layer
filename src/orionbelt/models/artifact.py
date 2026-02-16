"""Artifact envelope and validation result models."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class ArtifactType(StrEnum):
    FACT = "fact"
    DIMENSION = "dimension"
    MEASURE = "measure"
    MACRO = "macro"
    POLICY = "policy"
    MODEL = "model"


class Artifact(BaseModel):
    """An artifact envelope wrapping a semantic model component."""

    id: UUID
    model_version_id: UUID
    artifact_type: ArtifactType
    name: str
    content_yaml: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
