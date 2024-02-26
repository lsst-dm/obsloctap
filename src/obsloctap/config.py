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

    database_url: str = Field(
        "",
        title="URL for postgres database",
        env="database_url",
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


config = Configuration()
"""Configuration for obsloctap."""
