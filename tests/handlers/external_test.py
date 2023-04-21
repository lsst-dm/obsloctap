"""Tests for the obsloctap.handlers.external module and routes."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from obsloctap.config import config


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /obsloctap/``"""
    response = await client.get("/")
    assert response.status_code == 200
    metadata = response.json()
    assert metadata["name"] == config.name
    assert isinstance(metadata["version"], str)
    assert isinstance(metadata["description"], str)
    assert isinstance(metadata["repository_url"], str)
    assert isinstance(metadata["documentation_url"], str)
