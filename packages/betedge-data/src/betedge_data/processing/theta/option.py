import logging
import time
import pyarrow as pa
import pyarrow.csv as pv

from urllib.parse import urlparse, parse_qs
from betedge_data.job import HTTPJob, Schema
from betedge_data.processing.theta.schemas import (
    stock_quote,
    option_quote,
    option_eod,
    stock_eod,
)

logger = logging.getLogger(__name__)


def _get_schema_map(schema: Schema) -> pa.schema:
    if schema == Schema.OPTION_QUOTE:
        return option_quote
    elif schema == Schema.OPTION_EOD:
        return option_eod
    else:
        raise NotImplementedError(
            f"Not matching PyArrow schema for '{schema}:{schema.value}'"
        )


def process_option(http_result: HTTPJob) -> pa.Table:
    """
    Process option data from HTTP result.

    Args:
        http_result: HTTPJob containing CSV buffer and schema info

    Returns:
        PyArrow table with processed option data
    """
    start_time = time.time()
    params = parse_qs(urlparse(http_result.url).query)

    if "stock" in http_result.url:
        logger.debug("Processing stock data within option request")
        if http_result.schema == Schema.OPTION_QUOTE:
            convert_options = pv.ConvertOptions(
                column_types={field.name: field.type for field in stock_quote}
            )
        elif http_result.schema == Schema.OPTION_EOD:
            convert_options = pv.ConvertOptions(
                column_types={field.name: field.type for field in stock_eod}
            )
        else:
            convert_options = pv.ConvertOptions()

        parse_start = time.time()
        table = pv.read_csv(http_result.csv_buffer, convert_options=convert_options)
        parse_duration_ms = (time.time() - parse_start) * 1000

        root = params.get("root")
        if root:
            root = root[0]

        # Add contract columns
        num_rows = len(table)

        table = (
            table.add_column(0, "right", pa.array(pa.nulls(num_rows), type=pa.string()))
            .add_column(0, "strike", pa.array(pa.nulls(num_rows), type=pa.int64()))
            .add_column(0, "expiration", pa.array([0] * num_rows, type=pa.int32()))
            .add_column(0, "root", pa.array([root] * num_rows, type=pa.string()))
        )

        logger.debug(f"Stock data CSV parsed in {parse_duration_ms:.1f}ms")
    else:
        logger.debug("Processing option data")
        convert_options = pv.ConvertOptions(
            column_types={field.name: field.type for field in option_quote}
        )

        parse_start = time.time()
        table = pv.read_csv(http_result.csv_buffer, convert_options=convert_options)
        parse_duration_ms = (time.time() - parse_start) * 1000

        logger.debug(f"Option data CSV parsed in {parse_duration_ms:.1f}ms")

    duration_ms = (time.time() - start_time) * 1000
    row_count = len(table)
    logger.info(f"Option processing completed: {row_count} rows in {duration_ms:.1f}ms")

    return table
