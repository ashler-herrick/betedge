"""
ThetaData configuration using Pydantic Settings.
"""

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings


class ThetaTerminalConfig(BaseSettings):
    """ThetaData specific configuration."""

    username: str = Field(..., description="ThetaData username (email)")
    password: str = Field(..., description="ThetaData password")
    jar_path: Path = Field(default=Path("./ThetaTerminal.jar"), description="Path to ThetaTerminal.jar")
    connection_timeout: int = Field(default=30, description="Connection timeout in seconds")

    model_config = {
        "env_prefix": "THETA_",
        "case_sensitive": False,
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


# Global configuration instance
config = ThetaTerminalConfig()
