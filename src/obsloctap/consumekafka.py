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
import json
import logging
import os
import random
import time

import aiokafka
import structlog
from astropy.time import Time
from lsst.resources import ResourcePath

from obsloctap.config import Configuration
from obsloctap.consdbhelp import ConsDbHelp, ConsDbHelpProvider
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
# Environment variables from deployment

kafka_cluster = os.environ["KAFKA_CLUSTER"]
kafka_user = os.environ["KAFKA_USERNAME"]
kafka_password = os.environ["KAFKA_PASSWORD"]
kafka_schema_url = os.environ["SCHEMA_URL"]
db_url = os.environ["database_url"]
kafka_group_id = 1
jaas = ("org.apache.kafka.common.security.scram.ScramLoginModule required",)

# TODO this needs to be LSSTCam but that doe snot exist yet
topic = "lsst.sal.MTHeaderService.logevent_largeFileObjectAvailable"
sched24 = Schedule24()


async def do_exp_updates(stopafter: int = 0) -> None:
    """this will get the consdb entries for scheduled observations
    it never exits .. but sleeps for a fe wmoinutes"""
    config = Configuration()
    db: DbHelp = await DbHelpProvider.getHelper()
    # config hours - sleep is in seconds
    stime = config.exp_sleeptime * 60
    # this will be true always unless we pass in a number which is for test
    log.info("Starting updates from consdb ")
    count = 0
    exec = 0
    entries = 0

    now = Time.now().to_value("mjd")
    fillin = db.find_last()
    cdb: ConsDbHelp = await ConsDbHelpProvider.getHelper()
    exposures = cdb.get_exposures_between(fillin, now)
    for exp in exposures:
        db.insert_exposure(exp)

    while stopafter != count:
        old = await db.find_oldest_obs()
        log.info(f"Oldest obs MJD is {old} - now {now}")
        if old > now:  # we may have something to do
            cdb = await ConsDbHelpProvider.getHelper()
            exposures = cdb.get_exposures_between(old, now)
            entries += db.update_entries(exposures)
        if count % 100 == 0:
            log.info(
                f"Update exposures {count} runs "
                f"executed {exec} updates."
                f"Updated {entries} total planning lines"
            )
        count += 1
        await asyncio.sleep(stime)


def process_resource(resource: ResourcePath) -> None:
    content = json.loads(resource.read())
    print(content)  # TODO remove
    print("Not processing yet - just printing")
    # TODO create Obsplan use dbhelp to do the insert ..


async def consume() -> None:
    consumer = aiokafka.AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_cluster,
        group_id=f"{kafka_group_id}",
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=kafka_user,
        sasl_plain_password=kafka_password,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value
            resource = ResourcePath(message.url)
            while not resource.exists():
                # only does it once per exec/day
                await sched24.get_update_schedule24()
                time.sleep(random.uniform(0.1, 2.0))
            process_resource(resource)

    finally:
        await consumer.stop()


runner = asyncio.Runner()
log.info("exposure update")
try:
    runner.run(do_exp_updates())
except Exception:
    log.exception("exposure update error")

log.info("24h schedule")
try:
    runner.run(sched24.do24hs())
except Exception:
    log.exception("24h schedule error")

try:
    log.info("Not running kafka")
    # runner.run(consume())
except Exception as e:
    log.error(e)

runner.close()
