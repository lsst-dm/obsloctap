"""Tests for the obsloctap.handlers.external module and routes."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from obsloctap.config import config
from obsloctap.db import DbHelpProvider, MockDbHelp


@pytest.mark.asyncio
async def test_get_exernal_index(client: AsyncClient) -> None:
    """Test ``GET /``"""
    response = await client.get(f"/{config.path_prefix}")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == config.name
    assert isinstance(data["version"], str)
    assert isinstance(data["description"], str)
    assert isinstance(data["repository_url"], str)
    assert isinstance(data["documentation_url"], str)


@pytest.mark.asyncio
async def test_get_schedule(client: AsyncClient) -> None:
    """will use mock for local Testing"""
    DbHelpProvider.clear()
    dbhelp = await DbHelpProvider.getHelper()
    assert isinstance(dbhelp, MockDbHelp)

    response = await client.get(f"{config.path_prefix}/schedule")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    obs = data[0]
    assert "rubin_nexp" in obs
    assert "t_planning" in obs
    assert data[0]["t_planning"] == 60032.194918981484
