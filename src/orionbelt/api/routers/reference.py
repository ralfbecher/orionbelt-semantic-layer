"""Reference endpoint: GET /reference/obml."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from orionbelt.obml_reference import OBML_REFERENCE

router = APIRouter()


class ReferenceResponse(BaseModel):
    """Response for GET /reference/obml."""

    reference: str = Field(description="OBML format reference text")


@router.get("/obml", response_model=ReferenceResponse)
async def get_obml_reference() -> ReferenceResponse:
    """Return the full OBML format reference."""
    return ReferenceResponse(reference=OBML_REFERENCE)
