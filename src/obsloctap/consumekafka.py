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

import io
import json
import struct

import aiokafka
import httpx
import structlog
from aiokafka import ConsumerRecord
from fastavro import parse_schema, schemaless_reader

from obsloctap.config import config
from obsloctap.db import DbHelp, DbHelpProvider
from obsloctap.PredictedSchedule import convert_predicted

# Configure logging
log = structlog.getLogger(__name__)
jaas = ("org.apache.kafka.common.security.scram.ScramLoginModule required",)

# need schedule updates
topic = "lsst.sal.Scheduler.logevent_predictedSchedule"

schema: dict | None = None


def get_schema(schema_id: int = 2191) -> dict:
    global schema
    if not schema:
        schema_url = config.kafka_schema_url
        with httpx.Client(timeout=10.0) as client:
            r = client.get(f"{schema_url}/schemas/ids/{schema_id}")
            r.raise_for_status()
            schema_json = r.json()["schema"]  # string
            schema_dict = json.loads(schema_json)  # dict
            schema = parse_schema(schema_dict)
            log.debug(schema)

    return schema


def unpack_message(msg: ConsumerRecord, schema: dict) -> dict:
    log.info(f"Unpack kafka message {msg.timestamp}")
    value = msg.value  # bytes from Kafka message
    magic = value[0]
    assert magic == 0, "Not Confluent Avro wire format"
    schema_id = struct.unpack(">I", value[1:5])[0]
    log.debug(f"Schema id {schema_id}")
    payload = value[5:]
    record = schemaless_reader(io.BytesIO(payload))
    return record


async def process_message(msg: ConsumerRecord) -> None:
    log.info(f"Processing kafka - {msg}")
    value = msg.value  # bytes from Kafka message
    magic = value[0]
    assert magic == 0, "Not Confluent Avro wire format"
    schema_id = struct.unpack(">I", value[1:5])[0]
    payload = value[5:]

    # 2) Fetch writer schema from Schema Registry
    schema_url = config.kafka_schema_url
    with httpx.Client(timeout=10.0) as client:
        r = client.get(f"{schema_url}/schemas/ids/{schema_id}")
        r.raise_for_status()

    schema_json = r.json()["schema"]  # string
    schema_dict = json.loads(schema_json)  # dict
    parsed = parse_schema(schema_dict)

    # 3) Decode Avro binary
    record = schemaless_reader(io.BytesIO(payload), parsed)
    log.debug(record)
    plan = convert_predicted(record)
    db: DbHelp = await DbHelpProvider.getHelper()
    await db.insert_obsplan(plan)


def get_consumer() -> aiokafka.AIOKafkaConsumer:
    return aiokafka.AIOKafkaConsumer(
        topic,
        bootstrap_servers=config.kafka_bootstrap,
        group_id=f"{config.kafka_group_id}",
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=config.kafka_user,
        sasl_plain_password=config.kafka_password,
    )


async def consume() -> None:
    try:
        consumer = get_consumer()
        log.info(f"Starting consumer for {topic}")
        await consumer.start()
        async for msg in consumer:
            await process_message(msg)

    except Exception:
        log.exception("Consumer error")
    finally:
        await consumer.stop()
