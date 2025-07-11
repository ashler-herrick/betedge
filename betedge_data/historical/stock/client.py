import io
import logging
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc

from betedge_data.common.http import get_http_client
from betedge_data.common.models import StockThetaDataResponse, TICK_SCHEMAS
from betedge_data.common.exceptions import NoDataAvailableError
from betedge_data.historical.config import HistoricalClientConfig
from betedge_data.historical.stock.models import HistStockRequest

logger = logging.getLogger(__name__)

from betedge_data.common.interface import IClient


class HistoricalStockClient(IClient):
    """Client for fetching historical stock quote data from ThetaData API."""

    def __init__(self):
        """
        Initialize the historical stock client.
        """
        self.config = HistoricalClientConfig()
        self.http_client = get_http_client()

    def get_data(self, request: HistStockRequest) -> io.BytesIO:
        """
        Get stock data and return as Parquet or IPC bytes for streaming.

        Args:
            request: HistStockRequest with all parameters and validation

        Returns:
            BytesIO containing filtered Parquet or IPC data ready for streaming
        """
        logger.info(f"Starting stock data fetch for {request.root} on {request.date}")

        try:
            # Fetch validated stock data
            stock_data = self._fetch_stock_data(request)
            logger.debug(f"Data fetch complete - {len(stock_data.response)} records")

            # Convert to requested format
            if request.return_format == "parquet":
                logger.debug(f"Converting stock data to Parquet for {request.root}")
                buffer = self._convert_to_parquet(stock_data, endpoint=request.endpoint)
                logger.info(f"Parquet conversion complete: {len(buffer.getvalue())} bytes generated")
                return buffer
            elif request.return_format == "ipc":
                logger.debug(f"Converting stock data to IPC for {request.root}")
                buffer = self._convert_to_ipc(stock_data, endpoint=request.endpoint)
                logger.info(f"IPC conversion complete: {len(buffer.getvalue())} bytes generated")
                return buffer
            else:
                raise ValueError(f"Unsupported return_format: {request.return_format}")

        except NoDataAvailableError:
            logger.info(f"No data available for {request.root} on {request.date}")
            raise  # Re-raise to be handled by service layer
        except Exception as e:
            logger.error(f"Stock data processing failed for {request.root}: {str(e)}")
            logger.debug("Stock data processing error details", exc_info=True)
            raise RuntimeError(f"Stock data processing failed: {e}") from e

    def _fetch_stock_data(self, request: HistStockRequest) -> StockThetaDataResponse:
        """
        Fetch stock data and return as validated Pydantic model.

        Args:
            request: HistStockRequest with all parameters

        Returns:
            StockThetaDataResponse containing all stock data
        """
        url = self._build_stock_url(request)
        logger.info(f"Fetching stock data for {request.root}")

        try:
            # Use PaginatedHTTPClient with model validation
            result = self.http_client.fetch_paginated(
                url=url, response_model=StockThetaDataResponse, stream_response=False, collect_items=True
            )

            logger.info(f"Collected {len(result.response)} stock records")
            return result

        except NoDataAvailableError:
            raise  # Let NoDataAvailableError propagate
        except Exception as e:
            logger.error(f"Error fetching stock data: {e}")
            raise RuntimeError(f"Stock data fetch failed: {e}") from e

    def _build_stock_url(self, request: HistStockRequest) -> str:
        """Build URL for ThetaData historical stock endpoint."""
        params = {
            "root": request.root,
            "start_date": request.date,
            "end_date": request.date,
            "ivl": request.interval,
        }

        base_url = f"{self.config.base_url}/hist/stock/{request.endpoint}"
        return f"{base_url}?{urlencode(params)}"

    def _flatten_stock_data(self, stock_data: StockThetaDataResponse, endpoint: str = "quote") -> dict:
        """
        Flatten stock data into flat tick records.

        Args:
            stock_data: StockThetaDataResponse containing stock data
            endpoint: Endpoint type ("quote" or "ohlc") to determine schema

        Returns:
            Dict with lists of flattened tick data
        """
        logger.debug(f"Flattening {len(stock_data.response)} stock records using {endpoint} schema")

        # Get schema configuration for the endpoint
        schema = TICK_SCHEMAS[endpoint]
        field_count = schema["field_count"]
        field_names = schema["field_names"]

        response_data = stock_data.response
        if not response_data:
            logger.error("No data found in stock response")
            raise RuntimeError("No stock data to convert")

        # Apply columnar transposition (similar to option client approach)
        if response_data:
            stock_columns = list(zip(*response_data))
        else:
            stock_columns = [[] for _ in range(field_count)]

        # Create final data structure with batch type conversions using schema field names
        flattened_data = {}
        for i, field_name in enumerate(field_names):
            # Apply appropriate type conversion based on field type
            if field_name in ["ms_of_day", "date", "volume", "count"]:
                flattened_data[field_name] = [int(x) for x in stock_columns[i]]
            elif field_name in [
                "bid_size",
                "ask_size",
                "bid_exchange",
                "ask_exchange",
                "bid_condition",
                "ask_condition",
            ]:
                flattened_data[field_name] = [int(x) for x in stock_columns[i]]
            else:  # float fields like bid, ask, open, high, low, close
                flattened_data[field_name] = [float(x) for x in stock_columns[i]]

        logger.debug(f"Flattened to {len(flattened_data['ms_of_day'])} total tick records")
        return flattened_data

    def _create_arrow_table(self, flattened_data: dict, schema_type: str = "quote") -> pa.Table:
        """
        Create PyArrow table from flattened stock data using schema configuration.

        Args:
            flattened_data: Dict with lists of flattened tick data
            schema_type: Schema type ("quote" or "ohlc") to determine field structure

        Returns:
            PyArrow Table ready for serialization

        Raises:
            RuntimeError: If no data is provided
        """
        if not flattened_data or not any(flattened_data.values()):
            logger.error("No data found in flattened data")
            raise RuntimeError("No data to convert")

        # Get schema configuration
        tick_schema = TICK_SCHEMAS[schema_type]

        # Get field definitions
        field_names = tick_schema["field_names"]
        arrow_types = tick_schema["arrow_types"]

        # Create Arrow arrays with proper types
        arrays = []
        for field_name, arrow_type in zip(field_names, arrow_types):
            if field_name in flattened_data:
                arrays.append(pa.array(flattened_data[field_name], type=arrow_type))
            else:
                # Handle missing fields gracefully
                logger.warning(f"Field {field_name} not found in flattened data")
                arrays.append(pa.array([], type=arrow_type))

        # Create table
        table = pa.Table.from_arrays(arrays, names=field_names)
        total_records = len(flattened_data.get("ms_of_day", []))
        logger.debug(f"Created Arrow table with {total_records} records using {schema_type} schema")
        return table

    def _convert_to_parquet(self, stock_data: StockThetaDataResponse, endpoint: str = "quote") -> io.BytesIO:
        """
        Convert validated stock data to Parquet format.

        Args:
            stock_data: StockThetaDataResponse containing stock data
            endpoint: Endpoint type ("quote" or "ohlc") to determine schema

        Returns:
            BytesIO containing Parquet data
        """
        try:
            logger.debug(f"Converting stock data to Parquet using {endpoint} schema")

            # Flatten the data using schema-aware method
            flattened_data = self._flatten_stock_data(stock_data, endpoint)

            # Create Arrow table using schema-aware method
            table = self._create_arrow_table(flattened_data, schema_type=endpoint)

            # Write to Parquet with Snappy compression
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)

            logger.info(f"Converted {len(flattened_data['ms_of_day'])} records to Parquet")
            return buffer

        except Exception as e:
            logger.error(f"Failed to convert stock data to Parquet: {str(e)}")
            raise RuntimeError(f"Parquet conversion failed: {e}") from e

    def _convert_to_ipc(self, stock_data: StockThetaDataResponse, endpoint: str = "quote") -> io.BytesIO:
        """
        Convert validated stock data to IPC format.

        Args:
            stock_data: StockThetaDataResponse containing stock data
            endpoint: Endpoint type ("quote" or "ohlc") to determine schema

        Returns:
            BytesIO containing IPC data
        """
        try:
            logger.debug(f"Converting stock data to IPC using {endpoint} schema")

            # Flatten the data using schema-aware method
            flattened_data = self._flatten_stock_data(stock_data, endpoint)

            # Create Arrow table using schema-aware method
            table = self._create_arrow_table(flattened_data, schema_type=endpoint)

            # Write to IPC format
            buffer = io.BytesIO()
            with ipc.new_file(buffer, table.schema) as writer:
                writer.write_table(table)
            buffer.seek(0)

            logger.info(f"Converted {len(flattened_data['ms_of_day'])} records to IPC")
            return buffer

        except Exception as e:
            logger.error(f"Failed to convert stock data to IPC: {str(e)}")
            raise RuntimeError(f"IPC conversion failed: {e}") from e
