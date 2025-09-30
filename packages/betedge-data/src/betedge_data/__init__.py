import logging

# Configure logging BEFORE importing any modules
# This ensures all loggers are properly configured
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,  # Override any existing configuration
)

# Control external library verbosity
logging.getLogger("httpx").setLevel(logging.WARNING)  # Reduce httpx noise
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("minio").setLevel(logging.WARNING)

# Ensure betedge_data modules log at INFO level
logging.getLogger("betedge_data").setLevel(logging.INFO)

# Now import your modules
from betedge_data.client.client import BetEdgeClient
from betedge_data.client.requests import (
    OptionRequest,
    StockRequest,
    EarningsRequest,
)

__all__ = [
    "BetEdgeClient",
    "OptionRequest",
    "StockRequest",
    "EarningsRequest",
]
