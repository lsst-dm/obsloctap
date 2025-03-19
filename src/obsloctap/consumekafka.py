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

spectral_ranges = {
    "u": [3.3e-07, 4e-07],
    " u~nd": [3.3e-07, 4e-07],
    "g": [4.02e-07, 5.52e-07],
    "g~nd": [4.02e-07, 5.52e-07],
    "r": [5.52e-07, 6.91e-07],
    "r~nd": [5.52e-07, 6.91e-07],
    "i": [6.91e-07, 8.18e-07],
    "i~nd": [6.91e-07, 8.18e-07],
    "z": [8.18e-07, 9.22e-07],
    "z~nd": [8.18e-07, 9.22e-07],
    "y": [9.22e-07, 1.06e-06],
    "y~nd": [9.22e-07, 1.06e-06],
}
# Environment variables from deployment

kafka_cluster = os.environ["KAFKA_CLUSTER"]
schema_url = os.environ["SCHEMA_URL"]
db_url = os.environ["database_url"]
kafka_group_id = 1

# TODO this needs to be LSSTCam but that doe snot exist yet
topic = "lsst.MTHeaderService.logevent_largeFileObjectAvailable"
dbhelp = DbHelpProvider().getHelper()


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
                    time.sleep(random.uniform(0.1, 2.0))
                process_resource(resource)

                # Alternative 2: queue
                # r.sadd("EXPOSURE_QUEUE", str(resource))

        finally:
            await consumer.stop()


asyncio.run(main())
