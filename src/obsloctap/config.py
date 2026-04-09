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

"""Configuration definition."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings
from safir.logging import LogLevel, Profile

__all__ = ["Configuration", "config"]


class Configuration(BaseSettings):
    """Configuration for obsloctap."""

    name: str = Field(
        "obsloctap",
        title="Name of application",
        validation_alias="SAFIR_NAME",
    )

    path_prefix: str = Field(
        "/obsloctap",
        title="URL prefix for application",
        validation_alias="SAFIR_PATH_PREFIX",
    )

    profile: Profile = Field(
        Profile.development,
        title="Application logging profile",
        validation_alias="SAFIR_PROFILE",
    )

    log_level: LogLevel = Field(
        LogLevel.INFO,
        title="Log level of the application's logger",
        validation_alias="SAFIR_LOG_LEVEL",
    )

    database: str = Field(
        "",
        title="postgres database name",
        validation_alias="database",
    )

    database_url: str = Field(
        "",
        title="URL for postgres database",
        validation_alias="database_url",
    )

    database_user: str = Field(
        "",
        title="user for postgres database",
        validation_alias="database_user",
    )

    database_password: str = Field(
        "",
        title="password for postgres database",
        validation_alias="database_password",
    )

    database_schema: str = Field(
        "obsloctap",
        title="schema for postgres database",
        validation_alias="database_schema",
    )

    obsplanLimit: int = Field(
        1000,
        title="Limit to use on obsplan query",
        validation_alias="obsplanLimit",
    )

    obsplanTimeSpan: int = Field(
        24,
        title="Time to look back in obsplan query with time in hours",
        validation_alias="obsplanTimeSpan",
    )

    sleeptime: float = Field(
        12,
        title="Hours to sleep before getting 24 hour scheulde again",
        validation_alias="sleeptime",
    )

    exp_sleeptime: float = Field(
        2,
        title="Minutes to sleep before looking for exposures to update plan",
        validation_alias="exp_sleeptime",
    )

    consdb_database: str = Field(
        "exposurelog",
        title="Consdb postgres database name",
        validation_alias="consdb_database",
    )

    consdb_url: str = Field(
        "usdf-summitdb-replica.slac.stanford.edu:5432",
        title="URL for postgres database",
        validation_alias="consdb_url",
    )

    consdb_username: str = Field(
        "usdf",
        title="Consdb user for postgres database",
        validation_alias="consdb_username",
    )

    consdb_password: str = Field(
        "",
        title="Consdb password for postgres database",
        validation_alias="consdb_password",
    )

    consdb_schema: str = Field(
        "cdb_lsstcam",
        title="Schema for consdb postgres database",
        validation_alias="consdb_schema",
    )

    kafka_schema_url: str = Field(
        "http://sasquatch-schema-registry.sasquatch:8081",
        title="Schemas for Kafka/Sasquatch",
        validation_alias="SCHEMA_URL",
    )

    kafka_bootstrap: str = Field(
        "sasquatch-kafka-bootstrap.sasquatch:9092",
        title="bootstrap url for sasquatch",
        validation_alias="KAFKA_BOOTSTRAP",
    )

    kafka_group_id: str = Field(
        "obsloctap-consumer",
        title="group id",
        validation_alias="KAFKA_GROUP_ID",
    )

    kafka_user: str = Field(
        "obsloctap",
        title="User for sasquatch ",
        validation_alias="KAFKA_USERNAME",
    )

    kafka_password: str = Field(
        "",
        title="password  for sasquatch user ",
        validation_alias="KAFKA_PASSWORD",
    )

    salIndex: int = Field(
        1,
        title="Which predicted schedule messages to lookat ",
        validation_alias="salIndex",
    )

    service_url: str = Field(
        "https://usdf-rsp.slac.stanford.edu/obsloctap",
        title="Base URL of the deployed obsloctap service",
        validation_alias="service_url",
    )


config = Configuration()
"""Configuration for obsloctap."""
