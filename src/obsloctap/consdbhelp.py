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
    "MockConsDbHelp",
    "do_exp_updates",
    "validate_exposure_columns",
    "validate_exposure_predicate",
]

import asyncio
import re
from typing import Any, Sequence

import structlog
from astropy.time import Time
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
    "can_see_sky",
    "observation_reason",
    "group_id",
]


def validate_exposure_columns(columns: list[str]) -> list[str]:
    """Validate a list of column names are valid EXPOSURE_FIELDS.

    Raises ValueError if invalid column names are found.
    Returns the columns list if valid.
    """
    invalid = [c for c in columns if c not in EXPOSURE_FIELDS]
    if invalid:
        raise ValueError(
            f"Invalid column(s): {', '.join(invalid)}. "
            f"Valid columns: {', '.join(EXPOSURE_FIELDS)}"
        )
    return columns


def validate_exposure_predicate(predicate: str) -> str:
    """Validate a predicate string and check column names are valid.

    Raises ValueError if invalid column names are found.
    Returns the predicate string if valid.
    """
    pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|!=|<>|<=|>=|<|>|LIKE)"
    matches = re.findall(pattern, predicate, re.IGNORECASE)

    sql_keywords = {"AND", "OR", "NOT", "and", "or", "not"}
    columns_used = [m for m in matches if m.upper() not in sql_keywords]

    invalid = [c for c in columns_used if c not in EXPOSURE_FIELDS]
    if invalid:
        raise ValueError(
            f"Invalid column(s) in predicate: {', '.join(invalid)}. "
            f"Valid columns: {', '.join(EXPOSURE_FIELDS)}"
        )

    return predicate


# Configure logging
log = structlog.getLogger(__name__)


async def do_exp_updates(lastconsdb: float, stopafter: int = 0) -> int:
    """this will get the consdb entries for scheduled observations
    it never exits if stop after is 0
    but sleeps for a few minutes after each iteration
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

    sec = 20
    if stopafter == 0:
        log.info(
            f"Exposure(from ConsDB) update waiting {sec}s for other updates "
        )
        await asyncio.sleep(sec)

    # this will be true always unless we pass in a number which is for test
    while stopafter != count:
        count += 1
        try:
            # oldest scheduled job if it should have happened ..
            sched = await db.find_oldest_plan()
            now = Time.now().utc.to_value("mjd")
            log.debug(
                f"Oldest plan (sched):{sched}, now:{now}, "
                f"lastconsdb:{lastconsdb}."
            )
            # there may be no scheduled item
            if sched == 0:
                # go get the last performed/aborted plan element
                sched = await db.find_oldest_plan(negate=True)
            if (
                0 < sched < now
            ):  # we may have something to do it is in the past
                cdb = await ConsDbHelpProvider.getHelper()
                # just go back to last condb entry we got if its earlier
                prior = min(sched, lastconsdb)
                exposures = await cdb.get_exposures_between(prior, now)
                if exposures:
                    lastconsdb = exposures[-1].obs_start_mjd
                updated, inserted = await db.update_insert_exposures(exposures)
                entries += updated
                exec += 1
                sleeptime = stime
            if sched > now:  # it is in the future so wait that long
                sleeptime = round(sched - now, 1) * 86400
                log.debug(
                    f"Oldest scheduled obs MJD is {sched} it is now {now}, "
                    f"exposure update will sleep {sleeptime} "
                )
            log.info(
                f"Update exposures {count} runs "
                f"executed {exec} updates."
                f"Updated {entries} total planning lines"
                f"Sleeping {sleeptime}s, lastconsdb:{lastconsdb}"
            )
            # anything not now performed is aborted
            await db.mark_aborted_older(now)
            # if we  have a scheduled observation could sleep until then.
            await asyncio.sleep(sleeptime)
        except Exception:
            # back off - we really just want this to keep running
            sleeptime = 2 * sleeptime
            ConsDbHelpProvider.consdb_helper = (
                None  # make it get a new connection
            )
            log.exception("exposure update error - backing off")
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
            f"ORDER BY obs_start_mjd ASC"
        )
        log.debug(f"Get exposures with {statement}")
        rows: Sequence[Row] = []
        session = self.get_session()
        try:
            result = await session.execute(text(statement))
            rows = result.all()
        except Exception as e:
            log.error(f"Failed to get exposures {type(e).__name__} : {e}")
        finally:
            await session.close()
        return rows

    async def get_exposures_between(
        self, start: float, end: float
    ) -> list[Exposure]:
        rows = await self.get_exposure_rows_between(start, end)
        return self.process(rows)

    async def get_exposures(
        self,
        columns: list[str] | None = None,
        predicate: str | None = None,
        limit: int = 1000,
    ) -> list[Exposure] | list[dict[str, Any]]:
        """Return exposures from ConsDB.

        If columns is provided, only those columns are returned as dicts.
        If predicate is provided, it is added to the WHERE clause.
        """
        # Use subset of columns if specified
        if columns:
            select_fields = ",".join(columns)
        else:
            select_fields = self.fields

        where_conditions: list[str] = []
        if predicate:
            where_conditions.append(f"({predicate})")

        whereclause = ""
        if where_conditions:
            whereclause = " WHERE " + " AND ".join(where_conditions)

        statement = (
            f'SELECT {select_fields} FROM {self.schema}"exposure"'
            f"{whereclause} "
            f"ORDER BY obs_start_mjd DESC "
            f"LIMIT {limit}"
        )
        log.debug(f"Get exposures with {statement}")
        rows: Sequence[Row] = []
        session = self.get_session()
        try:
            result = await session.execute(text(statement))
            rows = result.all()
        except Exception as e:
            log.error(f"Failed to get exposures {type(e).__name__} : {e}")
        finally:
            await session.close()

        # Return dicts if columns specified, else Exposure objects
        if columns:
            return [dict(zip(columns, row)) for row in rows]
        return self.process(rows)


class MockConsDbHelp(ConsDbHelp):
    """Mock ConsDbHelp that returns sample data for local testing."""

    async def get_exposures_between(
        self, start: float, end: float
    ) -> list[Exposure]:
        log.warning(f"Using MockConsDbHelp: start {start}, end {end} ignored")
        return [self._sample_exposure()]

    async def get_exposures(
        self,
        columns: list[str] | None = None,
        predicate: str | None = None,
        limit: int = 1000,
    ) -> list[Exposure] | list[dict[str, Any]]:
        log.warning(
            f"Using MockConsDbHelp: predicate {predicate}, "
            f"limit {limit} ignored"
        )
        exp = self._sample_exposure()
        if columns:
            return [{col: getattr(exp, col) for col in columns}]
        return [exp]

    def _sample_exposure(self) -> Exposure:
        return Exposure(
            exposure_id=2024010100001,
            obs_start_mjd=60310.5,
            obs_end_mjd=60310.501,
            band="r",
            physical_filter="r_03",
            s_ra=150.0,
            s_dec=-30.0,
            target_name="MOCK_TARGET",
            science_program="MOCK_PROGRAM",
            scheduler_note="mock note",
            sky_rotation=0.0,
            can_see_sky=1,
            observation_reason="science",
            group_id="MOCK_GROUP_001",
        )


class ConsDbHelpProvider:
    consdb_helper: ConsDbHelp | None = None

    @staticmethod
    def clear() -> None:
        ConsDbHelpProvider.consdb_helper = None

    @staticmethod
    async def getHelper() -> ConsDbHelp:
        if ConsDbHelpProvider.consdb_helper is None:
            config = Configuration()
            # If no consdb_url configured, use mock
            if not config.consdb_url:
                log.info("No consdb_url configured, using MockConsDbHelp")
                ConsDbHelpProvider.consdb_helper = MockConsDbHelp(engine=None)
                return ConsDbHelpProvider.consdb_helper

            driver = "postgresql+asyncpg"
            full_url = (
                f"{driver}://{config.consdb_username}:"
                f"{config.consdb_password}@"
                f"{config.consdb_url}/{config.consdb_database}"
            )
            if "memory" in config.consdb_url:
                driver = "sqlite+aiosqlite"
                full_url = (  # just using one db for sqllite
                    f"{driver}:///file:obloctabdb"
                    "?mode=memory&cache=shared&uri=true"
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
