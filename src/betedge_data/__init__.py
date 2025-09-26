import logging
from betedge_data.client.client import BetEdgeClient
from betedge_data.client.client_requests import (
    ClientOptionRequest,
    ClientStockRequest,
    ClientEarningsRequest,
)

logging.basicConfig(
    format="%(asctime)s | Thread-(%(threadName)s) | %(name)s | %(levelname)s | %(message)s"
    ,level=logging.INFO
)

__all__ = [
    "BetEdgeClient",
    "ClientOptionRequest",
    "ClientStockRequest",
    "ClientEarningsRequest",
]
