# This file is part of obsloctap.
#
# Developed for the Rubin Data Management System.
# This product includes software developed by the Rubin Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# Use of this source code is governed by a 3-clause BSD-style
# license that can be found in the LICENSE file.

# consdbhelp.py

__all__ = ["ConsDbHelp", "ConsDbHelpProvider", "EXPOSURE_FIELDS"]

from typing import Any, Sequence

import structlog
from pydantic import ValidationError
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)

from obsloctap.models import Exposure

from .config import Configuration

EXPOSURE_FIELDS = [
    "exposure_id",
    "obs_start_mjd",
    "obs_end_mjd",
    "band",
    "physical_filter",
    "s_ra",
    "s_dec",
    "target_name",
    "science_program",
    "scheduler_note",
    "sky_rotation",
]

# Configure logging
log = structlog.getLogger(__name__)


class ConsDbHelp:
    def __init__(self, engine: AsyncEngine | None) -> None:
        self.engine = engine
        self.fields = ",".join(EXPOSURE_FIELDS)
        self.schema = ""

    def get_session(self) -> AsyncSession:
        return AsyncSession(self.engine)

    def process(self, result: Sequence[Row[Any]]) -> list[Exposure]:
        exposures = []
        for row in result:
            data = {key: row[i] for i, key in enumerate(EXPOSURE_FIELDS)}
            if not (data["s_ra"] and data["s_dec"]):
                log.error(
                    f"Exposure {data['exposure_id']} has no "
                    f"RA and/or DEC \n {data}"
                )
            else:
                try:
                    exposures.append(Exposure(**data))
                except ValidationError as e:
                    log.warning(
                        "Exposure validation failed", error=str(e), data=data
                    )

        return exposures

    async def get_exposure_rows_between(
        self, start: float, end: float
    ) -> Sequence[Row]:
        statement = (
            f'SELECT {self.fields} FROM {self.schema}"exposure" '
            f"WHERE obs_start_mjd >= {start} AND obs_start_mjd <= {end} "
            f"and can_see_sky = True "
            f"ORDER BY obs_start_mjd"
        )
        session = AsyncSession(self.engine)
        result = await session.execute(text(statement))
        rows = result.all()
        await session.close()
        return rows

    async def get_exposures_between(
        self, start: float, end: float
    ) -> list[Exposure]:
        rows = await self.get_exposure_rows_between(start, end)
        return self.process(rows)


class ConsDbHelpProvider:
    consdb_helper: ConsDbHelp | None = None

    @staticmethod
    async def getHelper() -> ConsDbHelp:
        if ConsDbHelpProvider.consdb_helper is None:
            config = Configuration()
            driver = "postgresql+asyncpg"
            full_url = (
                f"{driver}://{config.consdb_username}:"  # noqa: E231
                f"{config.consdb_password}@"
                f"{config.consdb_url}/{config.consdb_database}"
            )
            if "memory" in config.consdb_url:
                driver = "sqlite+aiosqlite"
                full_url = (  # just using one db for sqllite
                    f"{driver}:///file:obloctabdb"  # noqa: E231
                    "?mode=memory&cache=shared&uri=true"  # noqa: E231
                )
                log.info(
                    f"Creating SQlAlchemy engine For CONSDB with "
                    f"{full_url}"
                )
            else:
                log.info(
                    f"Creating SQlAlchemy for CONSDB engine with "
                    f"{config.consdb_username}@{config.database_url}"
                    f"/config.consdb"
                    f" and schema: '{config.consdb_schema}'."
                )

            engine = create_async_engine(full_url)
            helper = ConsDbHelp(engine=engine)
            helper.schema = config.consdb_schema
            if helper.schema and not helper.schema.endswith("."):
                helper.schema = f"{helper.schema}."
            ConsDbHelpProvider.consdb_helper = helper
        return ConsDbHelpProvider.consdb_helper
