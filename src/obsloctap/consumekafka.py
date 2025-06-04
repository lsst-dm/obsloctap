"""Copied from consdb -and perhaps they could be combined in the future"""

import asyncio
import json
import logging
import os
import random
import time

import aiokafka
from lsst.resources import ResourcePath

from obsloctap.schedule24h import Schedule24

# Configure logging
log = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s"
)
handler.setFormatter(formatter)
log.addHandler(handler)
if "LOG_LEVEL" in os.environ:
    log.setLevel(os.environ["LOG_LEVEL"].upper())
else:
    log.setLevel("DEBUG")
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
try:
    runner.run(sched24.do24hs())
except Exception as e:
    log.error(e)

print("now kafka")
runner.run(consume())
runner.close()
