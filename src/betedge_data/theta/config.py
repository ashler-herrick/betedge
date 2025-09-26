"""
ThetaData configuration using Pydantic Settings.
"""
import os
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load .env file before creating settings
load_dotenv()

class ThetaTerminalConfig(BaseSettings):
    """ThetaData specific configuration."""

    username: str = Field(default=os.getenv("THETA_USERNAME") or "", description="ThetaData username (email)")
    password: str = Field(default=os.getenv("THETA_PASSWORD") or "", description="ThetaData password")
    jar_path: Path = Field(default=Path("./ThetaTerminal.jar"), description="Path to ThetaTerminal.jar")
    connection_timeout: int = Field(default=30, description="Connection timeout in seconds")

    model_config = {
        "env_prefix": "THETA_",
        "case_sensitive": False,
        "extra": "ignore",
    }


# Global configuration instance
config = ThetaTerminalConfig()



