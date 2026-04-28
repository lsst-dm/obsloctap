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


import asyncio
import logging
import os

import structlog
from astropy.time import Time

from obsloctap.consdbhelp import do_exp_updates
from obsloctap.consumekafka import consume
from obsloctap.schedule24h import Schedule24

# Configure logging

level = logging.DEBUG
if "LOG_LEVEL" in os.environ:
    # Get the log level string from environment
    log_level_str = os.environ.get("LOG_LEVEL", "DEGUB").upper()
    # Convert string to logging level (e.g., "DEBUG" -> logging.DEBUG)
    level = getattr(logging, log_level_str, logging.INFO)
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(level))
log = structlog.getLogger(__name__)
log.info(f" Log level {level}")

sched24 = Schedule24()


async def runall() -> None:
    # first get Consdb since last time we got a schedule
    # fillin can take a while so better not to do the other
    # loops before it's done - it caused deadlock
    lastcdb = Time.now().utc.to_value("mjd")
    try:
        log.info("Doign consdb fillin")
        await do_exp_updates(0, 1)
    except Exception:
        log.exception("Encountered an error in fillin")
    # ok now the normal loop
    try:
        log.info("Gathering threads ")
        await asyncio.gather(
            sched24.do24hs(),
            consume(),
            do_exp_updates(lastcdb),
        )
    except Exception:
        log.exception("Encountered an error in runall")


if __name__ == "__main__":
    runner = asyncio.Runner()
    try:
        runner.run(runall())
    except Exception:
        log.exception("Runner failed")
    finally:
        runner.close()
