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

__all__ = [
    "ConsDbHelp",
    "ConsDbHelpProvider",
    "EXPOSURE_FIELDS",
    "do_exp_updates",
]

import asyncio
from typing import Any, Sequence

import structlog
from astropy.time import Time, TimeDelta
from pydantic import ValidationError
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)

from obsloctap.db import DbHelp, DbHelpProvider
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


async def do_exp_updates(stopafter: int = 0) -> int:
    """this will get the consdb entries for scheduled observations
    it never exits .. but sleeps for a few minutes
    returns the number of times the loop executed"""
    config = Configuration()
    db: DbHelp = await DbHelpProvider.getHelper()
    # config hours - sleep is in seconds
    stime = config.exp_sleeptime * 60
    sleeptime = stime
    log.info("Starting updates from consdb ")
    count = 1  # not equal to default stopafter which is only for test
    exec = 0
    entries = 0

    now = Time.now().to_value("mjd")
    lastconsdb = now
    # look for the last update so NOT scheduled
    try:
        fillin = await db.find_oldest_plan(negate=True)
        if fillin == 0:
            # go back 24 hours anyway
            h24 = TimeDelta("24hr")
            fillin = (Time.now() - h24).to_value("mjd")
            log.info(f"Did not find any observered plan going to {fillin}")

        cdb: ConsDbHelp = await ConsDbHelpProvider.getHelper()
        if fillin < now:
            log.info(f"Doign consdb fillin from {fillin} to {now} ")
            exposures = await cdb.get_exposures_between(fillin, now)
            session = db.get_session()
            for exp in exposures:
                await db.insert_exposure(exp, session)
            await session.commit()
            await session.close()
            log.info(
                f"Inserted {len(exposures or [])} exp going back to {fillin}"
            )
            if exposures:
                lastconsdb = exposures[-1].obs_start_mjd
        else:
            log.info(f" Last plan {fillin} is in the future now: {now} ")
        lastconsdb = fillin
    except Exception:
        log.exception("exposure update error in fillin")
    # this will be true always unless we pass in a number which is for test
    while stopafter != count:
        try:
            # oldest scheduled job if it should have happened ..
            sched = await db.find_oldest_plan()
            now = Time.now().to_value("mjd")
            if sched < now:  # we may have something to do it is in the past
                cdb = await ConsDbHelpProvider.getHelper()
                # just go back to last condb entry we got if its earlier
                prior = min(sched, lastconsdb)
                exposures = await cdb.get_exposures_between(prior, now)
                if exposures:
                    lastconsdb = exposures[-1].obs_start_mjd
                entries += await db.update_entries(exposures)
                exec += 1
                sleeptime = stime
            else:  # it is in the future
                sleeptime = round(sched - now, 1) * 86400
                log.debug(
                    f"Oldest obs MJD is {sched} it is now {now}, "
                    f"exposure update will sleep {sleeptime} "
                )
            if count % 100 == 0:
                log.info(
                    f"Update exposures {count} runs "
                    f"executed {exec} updates."
                    f"Updated {entries} total planning lines"
                    f"Sleeping {sleeptime}s"
                )
            count += 1
            # if we  have a scheduled observation could sleep until then.
            await asyncio.sleep(sleeptime)
        except Exception:
            # back off
            sleeptime = 2 * sleeptime
            ConsDbHelpProvider.consdb_helper = (
                None  # make it get a new connection
            )
            log.exception("exposure update error")
    return count


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
        log.debug(f"Get exposures with {statement}")
        session = self.get_session()
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
