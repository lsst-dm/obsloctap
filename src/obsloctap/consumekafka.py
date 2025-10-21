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
import math
import struct
from typing import Any

import aiokafka
import httpx
import structlog
from aiokafka import ConsumerRecord
from fastavro import parse_schema, schemaless_reader
from pandas import DataFrame

from obsloctap.config import config
from obsloctap.db import DbHelp, DbHelpProvider
from obsloctap.models import Obsplan

# Configure logging
log = structlog.getLogger(__name__)
jaas = ("org.apache.kafka.common.security.scram.ScramLoginModule required",)

# need schedule updates
topic = "lsst.sal.Scheduler.logevent_predictedSchedule"
SCHEMA: dict | None = None

COLS = [  # as in the schedule message Kafka or EFD
    "mjd",
    "ra",
    "decl",
    "exptime",
    "nexp",
    "rotSkyPos",
]
TO_COLS = [  # as in the ObPlan object
    "t_planning",
    "s_ra",
    "s_dec",
    "t_exptime",
    "rubin_nexp",
    "rubin_rot_sky_pos",
]
SINGLE_COLS = [
    "instrumentConfiguration",
    "numberOfTargets",
    "private_efdStamp",
]


def convert_predicted(msg: DataFrame) -> list[Obsplan]:
    """The predicted schedule msg which is in EFD or comes from Kakfa
    contains all the columns defined in COLS but all in one row with e.g
    s_ra_1 s_ra_2. So need to parse that out to someting usefull"""

    plan = []
    count = 0
    while f"{COLS[0]}{count}" in msg.columns:
        p = Obsplan()
        for cc in range(0, len(COLS)):
            v = msg[f"{COLS[cc]}{count}"].head().values[0]
            p.__setattr__(TO_COLS[cc], v)
            cc += 1
        p.priority = 2  # more likely than 24 hr schedule
        if p.t_planning and p.t_planning > 0:
            plan.append(p)
        count += 1
    return plan


def convert_predicted_kafka(msg: dict) -> list[Obsplan]:
    plan = []
    max = msg["numberOfTargets"]
    # nexp is a float  , mjd can be 0.0
    for count in range(len(msg["mjd"])):
        if msg["mjd"][count] == 0.0:
            next
        p = Obsplan()
        nexp = msg["nexp"][count]
        msg["nexp"][count] = (
            0 if (isinstance(nexp, float) and math.isnan(nexp)) else int(nexp)
        )
        for cc in range(0, len(COLS)):
            v = msg[COLS[cc]][count]
            p.__setattr__(TO_COLS[cc], v)
            cc += 1
        p.priority = 1  # more likely than 24 hr schedule
        if p.t_planning and p.t_planning > 0:
            plan.append(p)
    if len(plan) < max:
        log.info(
            "Predicted message says {max} targets but only "
            "{len(plan)} have t_planning>0"
        )
    return plan


def get_schema(schema_id: int = 2191) -> dict:
    global SCHEMA
    if not SCHEMA:
        log.debug(f"Get schema id: {schema_id}")
        schema_url = config.kafka_schema_url
        with httpx.Client(timeout=10.0) as client:
            r = client.get(f"{schema_url}/schemas/ids/{schema_id}")
            r.raise_for_status()
            schema_json = r.json()["schema"]  # string
            schema_dict = json.loads(schema_json)  # dict
            SCHEMA = parse_schema(schema_dict)
    return SCHEMA


def unpack_value(value: Any, schema: dict) -> dict:
    payload = value[5:]
    record = schemaless_reader(io.BytesIO(payload), schema)
    return record


def unpack_message(msg: ConsumerRecord) -> dict:
    log.info(f"Unpack kafka message {msg.timestamp}")
    value = msg.value  # bytes from Kafka message
    magic = value[0]
    assert magic == 0, "Not Confluent Avro wire format"
    schema_id = struct.unpack(">I", value[1:5])[0]
    schema = get_schema(schema_id)
    return unpack_value(value, schema)


async def process_message(msg: ConsumerRecord) -> None:
    log.info(
        f"Processing kafka - {msg['topic']} {msg['timestamp']} "
        f"index: {[msg['salIndex']]}"
    )
    if msg["salIndex"] != config.salIndex:
        log.info(f"Skipping message - salIndex not {config.salIndex}")
        return
    record = unpack_message(msg)
    # log.debug(record)
    plan = convert_predicted_kafka(record)
    log.info(
        f"Got {record['numberOfTargets']} numberOfTargets "
        f"converted to {len(plan)} Obsplans"
    )
    db: DbHelp = await DbHelpProvider.getHelper()
    await db.remove_flag(plan)
    await db.insert_obsplan(plan)


def get_consumer() -> aiokafka.AIOKafkaConsumer:
    log.info(
        f"Setting up consumer with bootstrap {config.kafka_bootstrap}  "
        f"group {config.kafka_group_id} user {config.kafka_user}"
    )
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
        log.info(f"Starting consumer for {topic} ")
        consumer = get_consumer()
        await consumer.start()
        async for msg in consumer:
            await process_message(msg)

    except Exception:
        log.exception("Consumer error")
    finally:
        await consumer.stop()
