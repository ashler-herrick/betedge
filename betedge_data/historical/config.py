"""
Historical stock data configuration.
"""

import os
from pydantic import Field
from pydantic_settings import BaseSettings


class HistoricalClientConfig(BaseSettings):
    """Configuration for historical stock data client."""

    base_url: str = Field(
        default_factory=lambda: os.getenv("THETA_BASE_URL", "http://127.0.0.1:25510/v2"),
        description="ThetaData API base URL (set THETA_BASE_URL env var to override)"
    )
    timeout: int = Field(default=60, description="HTTP request timeout in seconds")
    retry_attempts: int = Field(default=3, description="Number of retry attempts for failed requests")
    retry_delay: float = Field(default=1.0, description="Delay between retry attempts in seconds")
    default_venue: str = Field(default="nqb", description="Default data venue")
    use_csv: bool = Field(default=False, description="Use CSV format instead of JSON")
    pretty_time: bool = Field(default=False, description="Use human-readable time format")
    stock_tier: str = Field(default="value", description="ThetaData subscription tier (value, standard, pro)")
    option_tier: str = Field(default="value", description="ThetaData subscription tier (value, standard, pro)")
    max_concurrent_requests: int = Field(
        default=4, description="Maximum concurrent HTTP requests (sync with ThetaData HTTP_CONCURRENCY)"
    )

    model_config = {
        "env_prefix": "THETA_",
        "case_sensitive": False,
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }
