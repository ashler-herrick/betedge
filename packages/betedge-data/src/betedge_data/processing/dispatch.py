import logging
import time
import pyarrow as pa

from betedge_data.processing.alt.earnings import process_earnings
from betedge_data.processing.theta.option import process_option
from betedge_data.processing.theta.stock import process_stock
from betedge_data.job import HTTPJob, FileWriteJob, Schema

logger = logging.getLogger(__name__)


def process_http_result(http_result: HTTPJob) -> FileWriteJob:
    """
    Process HTTP result and route to appropriate processor based on schema.

    Args:
        http_result: HTTPJob containing the response data and schema info

    Returns:
        FileWriteJob with processed table added
    """
    start_time = time.time()
    table = pa.table({})

    if http_result.schema in [Schema.OPTION_QUOTE, Schema.OPTION_EOD]:
        logger.debug(
            f"Routing to option processor for schema: {http_result.schema.value}"
        )
        table = process_option(http_result)
    elif http_result.schema in [Schema.STOCK_EOD, Schema.STOCK_QUOTE]:
        logger.debug(
            f"Routing to stock processor for schema: {http_result.schema.value}"
        )
        table = process_stock(http_result)
    elif http_result.schema == Schema.EARNINGS:
        logger.debug(
            f"Routing to earnings processor for schema: {http_result.schema.value}"
        )
        table = process_earnings(http_result)

    duration_ms = (time.time() - start_time) * 1000
    row_count = len(table)
    logger.info(
        f"Processed {http_result.schema.value} data: {row_count} rows in {duration_ms:.1f}ms"
    )

    fwj = http_result.file_write_job
    fwj.add_table(table)

    return fwj
