import logging
import time
import pyarrow as pa
import pyarrow.csv as pv

from betedge_data.job import HTTPJob
from betedge_data.processing.theta.schemas import stock_quote

logger = logging.getLogger(__name__)


def process_stock(http_result: HTTPJob) -> pa.Table:
    """
    Process stock data from HTTP result.

    Args:
        http_result: HTTPJob containing CSV buffer with stock data

    Returns:
        PyArrow table with processed stock data
    """
    start_time = time.time()

    parse_start = time.time()
    table = pv.read_csv(http_result.csv_buffer, schema=stock_quote)
    parse_duration_ms = (time.time() - parse_start) * 1000

    duration_ms = (time.time() - start_time) * 1000
    row_count = len(table)

    logger.debug(f"Stock CSV parsed in {parse_duration_ms:.1f}ms")
    logger.info(f"Stock processing completed: {row_count} rows in {duration_ms:.1f}ms")

    return table
