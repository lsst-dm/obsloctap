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

"""Copied from consdb -and perhaps they could be combined in the future"""

import asyncio
import logging
import os

import structlog
from astropy.time import Time, TimeDelta

from obsloctap.config import Configuration
from obsloctap.consdbhelp import ConsDbHelp, ConsDbHelpProvider
from obsloctap.consumekafka import consume
from obsloctap.db import DbHelp, DbHelpProvider
from obsloctap.schedule24h import Schedule24

# Configure logging

level = logging.DEBUG
if "LOG_LEVEL" in os.environ:
    # Get the log level string from environment
    log_level_str = os.environ.get("LOG_LEVEL", "DEGUB").upper()
    # Convert string to logging level (e.g., "DEBUG" -> logging.DEBUG)
    level = getattr(logging, log_level_str, logging.INFO)
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(level))
print(f"Log level {level}")
log = structlog.getLogger(__name__)
log.info(f" sent to INFO - Log level {level}")

sched24 = Schedule24()


async def do_exp_updates(stopafter: int = 0) -> None:
    """this will get the consdb entries for scheduled observations
    it never exits .. but sleeps for a few minutes"""
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
            h24 = TimeDelta("24h")
            fillin = (Time.now() - h24).to_value("mjd")
            log.info(f"Did not find any observered plan going to {fillin}")

        cdb: ConsDbHelp = await ConsDbHelpProvider.getHelper()
        if fillin < now:
            log.info(f"Doign consdb fillin from {fillin} to {now} ")
            exposures = await cdb.get_exposures_between(fillin, now)
            session = db.get_session()
            for exp in exposures:
                await db.insert_exposure(exp, session)
            session.commit()
            session.close()
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


async def runall() -> None:
    try:
        await asyncio.gather(
            do_exp_updates(),
            sched24.do24hs(),
            consume(),
        )
    except Exception:
        log.exception("Encountered an error")


runner = asyncio.Runner()
try:
    runner.run(runall())
except Exception:
    log.exception("Runner failed")
finally:
    runner.close()
