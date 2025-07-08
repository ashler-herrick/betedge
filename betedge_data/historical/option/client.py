import io
import logging
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc

from betedge_data.common.http import get_http_client
from betedge_data.common.models import OptionThetaDataResponse, StockThetaDataResponse, TICK_SCHEMAS, CONTRACT_SCHEMA
from betedge_data.historical.config import HistoricalClientConfig
from betedge_data.historical.option.models import HistOptionBulkRequest
from betedge_data.historical.stock.client import HistoricalStockClient
from betedge_data.historical.stock.models import HistStockRequest
from betedge_data.common.interface import IClient

logger = logging.getLogger(__name__)


class HistoricalOptionClient(IClient):
    """Client for fetching and filtering historical option data from ThetaData API."""

    def __init__(self):
        """
        Initialize the historical option client.

        Args:
            config: Configuration instance, uses default if None
        """
        self.config = HistoricalClientConfig()
        self.max_workers = self.config.max_concurrent_requests
        self.http_client = get_http_client()
        self.stock_client = HistoricalStockClient()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        if hasattr(self, "stock_client"):
            self.stock_client.close()

    def get_data(self, request: HistOptionBulkRequest) -> io.BytesIO:
        """
        Get option data as Parquet or IPC bytes for streaming.

        Args:
            request: HistOptionBulkRequest with all parameters and validation

        Returns:
            BytesIO containing Parquet or IPC data ready for streaming
        """
        logger.info(f"Starting data fetch for {request.root} for {request.date}")

        # Fetch option and stock data
        logger.info(f"Fetching option and stock data for {request.root}")
        option_data, stock_data = self._fetch_option_and_stock_data_models(request)
        logger.debug(
            f"Data fetch complete - option records: {len(option_data.response)}, stock records: {len(stock_data.response)}"
        )

        try:
            # Convert to requested format
            if request.return_format == "parquet":
                logger.debug(f"Converting option data to Parquet for {request.root}")
                buffer = self._convert_to_parquet(option_data, stock_data, endpoint=request.endpoint)
                logger.info(f"Parquet conversion complete: {len(buffer.getvalue())} bytes generated")
                return buffer
            elif request.return_format == "ipc":
                logger.debug(f"Converting option data to IPC for {request.root}")
                buffer = self._convert_to_ipc(option_data, stock_data, endpoint=request.endpoint)
                logger.info(f"IPC conversion complete: {len(buffer.getvalue())} bytes generated")
                return buffer
            else:
                raise ValueError(f"Unsupported return_format: {request.return_format}")

        except Exception as e:
            logger.error(f"Data conversion failed for {request.root}: {str(e)}", exc_info=True)
            raise RuntimeError(f"Option data conversion failed: {e}") from e

    def _fetch_option_and_stock_data_models(
        self, request: HistOptionBulkRequest
    ) -> tuple[OptionThetaDataResponse, StockThetaDataResponse]:
        """
        Fetch option and stock data in parallel and return as validated models.

        Args:
            request: HistoricalOptionRequest with all parameters

        Returns:
            Tuple of (option_data, stock_data)
        """
        logger.info(f"Fetching option and stock data for {request.root} exp={request.exp} date={request.date}")

        # Create stock request from option request
        stock_request = HistStockRequest(
            root=request.root, date=request.date, interval=request.interval, endpoint=request.endpoint
        )

        # Fetch stock and option data in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both tasks
            stock_future = executor.submit(self.stock_client._fetch_stock_data, stock_request)
            option_future = executor.submit(self._fetch_option_data, request)

            # Collect results
            try:
                stock_data = stock_future.result()
                logger.info(f"Successfully fetched stock data for {request.root}: {len(stock_data.response)} records")
            except Exception as e:
                logger.error(f"Failed to fetch stock data for {request.root}: {str(e)}", exc_info=True)
                raise RuntimeError(f"Stock data fetch failed: {e}") from e

            try:
                option_data = option_future.result()
                logger.info(
                    f"Successfully fetched option data for {request.root} exp {request.exp}: {len(option_data.response)} records"
                )
            except Exception as e:
                logger.error(
                    f"Failed to fetch option data for {request.root} exp {request.exp}: {str(e)}", exc_info=True
                )
                raise RuntimeError(f"Option data fetch failed: {e}") from e

        return option_data, stock_data

    def _flatten_option_data(
        self, option_data: OptionThetaDataResponse, stock_data: StockThetaDataResponse, endpoint: str = "quote"
    ) -> dict:
        """
        Flatten option and stock data into flat tick records with contract info.

        Args:
            option_data: OptionThetaDataResponse containing option data
            stock_data: StockThetaDataResponse containing stock data
            endpoint: Endpoint type ("quote" or "ohlc") to determine schema

        Returns:
            Dict with lists of flattened tick data
        """
        logger.debug(
            f"Flattening {len(option_data.response)} option contracts and {len(stock_data.response)} stock records using {endpoint} schema"
        )

        # Get schema configuration for the endpoint
        schema = TICK_SCHEMAS[endpoint]
        field_count = schema["field_count"]
        field_names = schema["field_names"]

        # Extract all option ticks and contract info using batch operations
        all_option_ticks = []
        contract_info = []
        for option_item in option_data.response:
            ticks = option_item.ticks
            contract = option_item.contract
            all_option_ticks.extend(ticks)
            # Repeat contract info for each tick
            contract_info.extend([contract] * len(ticks))

        # Apply columnar transposition (similar to stock client approach)
        if all_option_ticks:
            option_columns = list(zip(*all_option_ticks))
        else:
            option_columns = [[] for _ in range(field_count)]

        if stock_data.response:
            stock_columns = list(zip(*stock_data.response))
        else:
            stock_columns = [[] for _ in range(field_count)]

        # Combine option and stock columns efficiently
        combined_columns = []
        for i in range(field_count):
            combined_columns.append(list(option_columns[i]) + list(stock_columns[i]))

        root = contract_info[0].root
        # Create contract info columns with list comprehensions
        root_column = [root] * len(contract_info) + [root] * len(stock_data.response)
        expiration_column = [contract.expiration for contract in contract_info] + [0] * len(stock_data.response)
        strike_column = [contract.strike for contract in contract_info] + [0] * len(stock_data.response)
        right_column = [contract.right for contract in contract_info] + ["U"] * len(stock_data.response)

        # Create final data structure with batch type conversions using schema field names
        flattened_data = {}
        for i, field_name in enumerate(field_names):
            # Apply appropriate type conversion based on field type
            if field_name in ["ms_of_day", "date", "volume", "count"]:
                flattened_data[field_name] = [int(x) for x in combined_columns[i]]
            elif field_name in [
                "bid_size",
                "ask_size",
                "bid_exchange",
                "ask_exchange",
                "bid_condition",
                "ask_condition",
            ]:
                flattened_data[field_name] = [int(x) for x in combined_columns[i]]
            else:  # float fields like bid_price, ask_price, open, high, low, close
                flattened_data[field_name] = [float(x) for x in combined_columns[i]]

        # Add contract fields
        flattened_data["root"] = root_column
        flattened_data["expiration"] = expiration_column
        flattened_data["strike"] = strike_column
        flattened_data["right"] = right_column

        logger.debug(f"Flattened to {len(flattened_data['ms_of_day'])} total tick records")
        return flattened_data

    def _create_arrow_table(self, flattened_data: dict, schema_type: str = "quote") -> pa.Table:
        """
        Create PyArrow table from flattened option/stock data using schema configuration.

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

        # Get schema configurations
        tick_schema = TICK_SCHEMAS[schema_type]
        contract_schema = CONTRACT_SCHEMA

        # Combine tick and contract field definitions
        all_field_names = tick_schema["field_names"] + contract_schema["field_names"]
        all_arrow_types = tick_schema["arrow_types"] + contract_schema["arrow_types"]

        # Create Arrow arrays with proper types
        arrays = []
        for field_name, arrow_type in zip(all_field_names, all_arrow_types):
            if field_name in flattened_data:
                arrays.append(pa.array(flattened_data[field_name], type=arrow_type))
            else:
                # Handle missing fields gracefully
                logger.warning(f"Field {field_name} not found in flattened data")
                arrays.append(pa.array([], type=arrow_type))

        # Create table
        table = pa.Table.from_arrays(arrays, names=all_field_names)
        total_records = len(flattened_data.get("ms_of_day", []))
        logger.debug(f"Created Arrow table with {total_records} records using {schema_type} schema")
        return table

    def _convert_to_parquet(
        self, option_data: OptionThetaDataResponse, stock_data: StockThetaDataResponse, endpoint: str = "quote"
    ) -> io.BytesIO:
        """
        Convert option and stock data to Parquet format.

        Args:
            option_data: OptionThetaDataResponse containing option data
            stock_data: StockThetaDataResponse containing stock data
            endpoint: Endpoint type ("quote" or "ohlc") to determine schema

        Returns:
            BytesIO containing Parquet data
        """
        try:
            logger.debug(f"Converting option and stock data to Parquet using {endpoint} schema")

            # Flatten the data
            flattened_data = self._flatten_option_data(option_data, stock_data, endpoint)

            # Create Arrow table
            table = self._create_arrow_table(flattened_data, schema_type=endpoint)

            # Write to Parquet with Snappy compression
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)

            logger.info(f"Converted {len(flattened_data['ms_of_day'])} records to Parquet")
            return buffer

        except Exception as e:
            logger.error(f"Failed to convert option data to Parquet: {str(e)}")
            raise RuntimeError(f"Parquet conversion failed: {e}") from e

    def _convert_to_ipc(
        self, option_data: OptionThetaDataResponse, stock_data: StockThetaDataResponse, endpoint: str = "quote"
    ) -> io.BytesIO:
        """
        Convert option and stock data to IPC format.

        Args:
            option_data: OptionThetaDataResponse containing option data
            stock_data: StockThetaDataResponse containing stock data
            endpoint: Endpoint type ("quote" or "ohlc") to determine schema

        Returns:
            BytesIO containing IPC data
        """
        try:
            logger.debug(f"Converting option and stock data to IPC using {endpoint} schema")

            # Flatten the data
            flattened_data = self._flatten_option_data(option_data, stock_data, endpoint)

            # Create Arrow table
            table = self._create_arrow_table(flattened_data, schema_type=endpoint)

            # Write to IPC format
            buffer = io.BytesIO()
            with ipc.new_file(buffer, table.schema) as writer:
                writer.write_table(table)
            buffer.seek(0)

            logger.info(f"Converted {len(flattened_data['ms_of_day'])} records to IPC")
            return buffer

        except Exception as e:
            logger.error(f"Failed to convert option data to IPC: {str(e)}")
            raise RuntimeError(f"IPC conversion failed: {e}") from e

    def _fetch_option_data(self, request: HistOptionBulkRequest) -> OptionThetaDataResponse:
        """
        Fetch option data and return as validated Pydantic model.

        Args:
            request: HistoricalOptionRequest with all parameters

        Returns:
            OptionThetaDataResponse containing all option data
        """
        url = self._build_option_url(request)
        logger.info(f"Fetching option data for {request.root} exp {request.exp}")

        try:
            # Use PaginatedHTTPClient with streaming and model validation
            result = self.http_client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                stream_response=True,
                item_path="response.item",
                collect_items=True,
            )

            logger.info(f"Collected {len(result.response)} records for exp {request.exp}")

            # Return validated model directly
            return result

        except Exception as e:
            logger.error(f"Error fetching option data for exp {request.exp}: {e}")
            raise RuntimeError(f"Option data fetch failed: {e}") from e

    def _build_option_url(self, request: HistOptionBulkRequest) -> str:
        """Build URL for ThetaData bulk option quote endpoint."""
        params = {"root": request.root, "exp": request.exp, "start_date": request.date, "end_date": request.date}
        if request.interval is not None:
            params["ivl"] = request.interval

        base_url = f"{self.config.base_url}/bulk_hist/option/{request.endpoint}"
        return f"{base_url}?{urlencode(params)}"

    def close(self) -> None:
        """Close the HTTP client."""
        if hasattr(self, "http_client"):
            self.http_client.close()
        if hasattr(self, "stock_client"):
            self.stock_client.close()
