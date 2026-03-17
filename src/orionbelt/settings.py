"""Shared settings loaded from environment / .env file."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for OrionBelt REST API server.

    Values are read from environment variables and from a ``.env`` file
    in the working directory.  See ``.env.template`` for all options.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Shared
    log_level: str = "INFO"
    log_format: str = "console"  # "console" (pretty) or "json" (structured)

    # REST API
    api_server_host: str = "localhost"
    api_server_port: int = 8000
    port: int | None = None  # Cloud Run injects PORT; takes precedence over api_server_port

    @property
    def effective_port(self) -> int:
        """Return the port to listen on (Cloud Run PORT takes precedence)."""
        return self.port if self.port is not None else self.api_server_port

    # Sessions
    session_ttl_seconds: int = 1800  # 30 min inactivity
    session_cleanup_interval: int = 60  # seconds between cleanup sweeps
    disable_session_list: bool = False  # hide GET /sessions endpoint

    # Single-model mode — pre-loaded into every new session.
    # When set, model upload/removal endpoints return 403.
    model_dir: str | None = None  # base directory for MODEL_FILE (set by Docker)
    model_file: str | None = None  # filename or absolute path to OBML YAML

    # Arrow Flight SQL server (requires ob-flight-extension)
    flight_enabled: bool = False
    flight_port: int = 8815
    flight_auth_mode: str = "none"  # "none" or "token"
    flight_api_token: str | None = None
    db_vendor: str = "duckdb"  # default vendor driver for Flight query execution
