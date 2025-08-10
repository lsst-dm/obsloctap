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
        env="SAFIR_NAME",
    )

    path_prefix: str = Field(
        "/obsloctap",
        title="URL prefix for application",
        env="SAFIR_PATH_PREFIX",
    )

    profile: Profile = Field(
        Profile.development,
        title="Application logging profile",
        env="SAFIR_PROFILE",
    )

    log_level: LogLevel = Field(
        LogLevel.INFO,
        title="Log level of the application's logger",
        env="SAFIR_LOG_LEVEL",
    )

    database: str = Field(
        "",
        title="postgres database name",
        env="database",
    )

    database_url: str = Field(
        "",
        title="URL for postgres database",
        env="database_url",
    )

    database_user: str = Field(
        "",
        title="user for postgres database",
        env="database_user",
    )

    database_password: str = Field(
        "",
        title="password for postgres database",
        env="database_password",
    )

    database_schema: str = Field(
        "obsloctap",
        title="schema for postgres database",
        env="database_schema",
    )

    obsplanLimit: int = Field(
        1000,
        title="Limit to use on obsplan query",
        env="obsplanLimit",
    )

    obsplanTimeSpan: int = Field(
        24,
        title="Time to look back in obsplan query with time in hours",
        env="obsplanTimeSpan",
    )

    sleeptime: float = Field(
        12,
        title="Hours to sleep before getting 24 hour scheulde again",
        env="sleeptime",
    )

    exp_sleeptime: float = Field(
        2,
        title="Minutes to sleep before looking for exposures to update plan",
        env="exp_sleeptime",
    )

    consdb_database: str = Field(
        "consdb",
        title="Consdb postgres database name",
        env="consdb_database",
    )

    consdb_url: str = Field(
        "usdf-summitdb-replica.slac.stanford.edu:5432",
        title="URL for postgres database",
        env="consdb_url",
    )

    consdb_username: str = Field(
        "usdf",
        title="Consdb user for postgres database",
        env="consdb_username",
    )

    consdb_password: str = Field(
        "",
        title="Consdb password for postgres database",
        env="consdb_password",
    )

    consdb_schema: str = Field(
        "cdb_lsstcam",
        title="Schema for consdb postgres database",
        env="consdb_schema",
    )


config = Configuration()
"""Configuration for obsloctap."""
