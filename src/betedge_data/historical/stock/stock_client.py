import io
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc

from betedge_data.common.http import get_http_client
from betedge_data.common.models import StockThetaDataResponse, TICK_SCHEMAS
from betedge_data.common.exceptions import NoDataAvailableError
from betedge_data.historical.config import HistoricalConfig
from betedge_data.historical.stock.hist_stock_request import HistStockRequest
from betedge_data.common.interface import IRequest


logger = logging.getLogger(__name__)


class HistoricalStockClient:
    """Client for fetching historical stock quote data from ThetaData API."""

    def __init__(self):
        """
        Initialize the historical stock client.
        """
        self.config = HistoricalConfig()
        self.http_client = get_http_client()

    def get_data(self, request: IRequest) -> io.BytesIO:
        """
        Get stock data and return as Parquet or IPC bytes for streaming.
        This is the single orchestrator method (the "dirty place").

        Args:
            request: IRequest with all parameters and validation

        Returns:
            BytesIO containing filtered Parquet or IPC data ready for streaming
        """
        # Validation
        if not isinstance(request, HistStockRequest):
            raise ValueError(f"Unsupported request type: {type(request)}")

        logger.info(f"Starting data fetch for {request.root} data_schema={request.data_schema}")

        try:
            # Step 1: Build URL (delegates to request object)
            url = request.get_url()

            # Step 2: Fetch data (delegates to pure function)
            stock_data = self._fetch_data(url)
            logger.debug(f"Data fetch complete - {len(stock_data.response)} records")

            # Step 3: Convert to Arrow table (delegates to pure function)
            table = self._convert_to_table(stock_data, request.data_schema)

            # Step 4: Serialize to requested format (orchestrate format selection)
            if request.return_format == "parquet":
                logger.debug(f"Converting to Parquet for {request.root}")
                buffer = self._write_parquet(table)
                logger.info(f"Parquet conversion complete: {len(buffer.getvalue())} bytes")
                return buffer
            elif request.return_format == "ipc":
                logger.debug(f"Converting to IPC for {request.root}")
                buffer = self._write_ipc(table)
                logger.info(f"IPC conversion complete: {len(buffer.getvalue())} bytes")
                return buffer
            else:
                raise ValueError(f"Unsupported return_format: {request.return_format}")

        except NoDataAvailableError:
            logger.info(f"No data available for {request.root}")
            raise
        except Exception as e:
            logger.error(f"Data processing failed for {request.root}: {str(e)}")
            logger.debug("Data processing error details", exc_info=True)
            raise RuntimeError(f"Data processing failed: {e}") from e

    def _fetch_data(self, url: str) -> StockThetaDataResponse:
        """Fetch stock data from URL (pure function, no error handling)."""
        return self.http_client.fetch_paginated(
            url=url, response_model=StockThetaDataResponse, stream_response=False, collect_items=True
        )

    def _convert_to_table(self, stock_data: StockThetaDataResponse, endpoint: str) -> pa.Table:
        """Convert stock data to Arrow table (pure function)."""
        # Get data_schema configuration
        data_schema = TICK_SCHEMAS[endpoint]
        field_names = data_schema["field_names"]
        arrow_types = data_schema["arrow_types"]

        response_data = stock_data.response
        if not response_data:
            raise RuntimeError("No stock data to convert")

        # Apply columnar transposition
        stock_columns = list(zip(*response_data))

        # Create typed arrays
        arrays = []
        for i, (field_name, arrow_type) in enumerate(zip(field_names, arrow_types)):
            if field_name in [
                "ms_of_day",
                "ms_of_day2",
                "date",
                "volume",
                "count",
                "bid_size",
                "ask_size",
                "bid_exchange",
                "ask_exchange",
                "bid_condition",
                "ask_condition",
            ]:
                arrays.append(pa.array([int(x) for x in stock_columns[i]], type=arrow_type))
            else:  # float fields
                arrays.append(pa.array([float(x) for x in stock_columns[i]], type=arrow_type))

        return pa.Table.from_arrays(arrays, names=field_names)

    def _write_parquet(self, table: pa.Table) -> io.BytesIO:
        """Write Arrow table to Parquet format (pure function)."""
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)
        return buffer

    def _write_ipc(self, table: pa.Table) -> io.BytesIO:
        """Write Arrow table to IPC format (pure function)."""
        buffer = io.BytesIO()
        with ipc.new_file(buffer, table.data_schema) as writer:
            writer.write_table(table)
        buffer.seek(0)
        return buffer
