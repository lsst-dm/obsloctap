"""Copied from consdb -and perhaps they could be combined in the future"""

import asyncio
import json
import os
import random
import time

import aiokafka
import httpx
import kafkit
from lsst.resources import ResourcePath

from obsloctap.db import DbHelpProvider
from obsloctap.schedule24h import Schedule24

# Environment variables from deployment

kafka_cluster = os.environ["KAFKA_CLUSTER"]
schema_url = os.environ["SCHEMA_URL"]
db_url = os.environ["database_url"]
kafka_group_id = 1

# TODO this needs to be LSSTCam but that doe snot exist yet
topic = "lsst.MTHeaderService.logevent_largeFileObjectAvailable"
dbhelp = DbHelpProvider().getHelper()
sched24 = Schedule24()


def process_resource(resource: ResourcePath) -> None:
    content = json.loads(resource.read())
    print(content)  # TODO remove
    print("Not processing yet - just printing")
    # TODO create Obsplan use dbhelp to do the insert ..


async def main() -> None:
    async with httpx.AsyncClient() as client:
        schema_registry = kafkit.registry.RegistryApi(
            client=client, url=schema_url
        )
        deserializer = kafkit.registry.Deserializer(registry=schema_registry)

        # Alternative 2:
        # Something like
        # asyncio.run(queue_check())

        consumer = aiokafka.AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka_cluster,
            group_id=f"{kafka_group_id}",
        )
        await consumer.start()
        try:
            async for msg in consumer:
                message = (await deserializer.deserialize(msg.value)).message
                resource = ResourcePath(message.url)
                # Alternative 1: block for file
                while not resource.exists():
                    # only does it once per exec/day
                    await sched24.get_update_schedule24()
                    time.sleep(random.uniform(0.1, 2.0))
                process_resource(resource)

                # Alternative 2: queue
                # r.sadd("EXPOSURE_QUEUE", str(resource))

        finally:
            await consumer.stop()


asyncio.run(main())
