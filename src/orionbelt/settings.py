"""Shared settings loaded from environment / .env file."""

from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for OrionBelt servers (API + MCP).

    Values are read from environment variables and from a ``.env`` file
    in the working directory.  See ``.env.example`` for all options.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Shared
    log_level: str = "INFO"

    # REST API
    api_server_host: str = "localhost"
    api_server_port: int = 8000

    # MCP
    mcp_transport: Literal["stdio", "http", "sse"] = "stdio"
    mcp_server_host: str = "localhost"
    mcp_server_port: int = 9000

    # Sessions
    session_ttl_seconds: int = 1800  # 30 min inactivity
    session_cleanup_interval: int = 60  # seconds between cleanup sweeps
