import io
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc

from betedge_data.common.http import get_http_client
from betedge_data.common.models import OptionThetaDataResponse, StockThetaDataResponse, TICK_SCHEMAS, CONTRACT_SCHEMA
from betedge_data.common.exceptions import NoDataAvailableError
from betedge_data.historical.config import get_hist_client_config
from betedge_data.historical.option.hist_option_bulk_request import HistOptionBulkRequest
from betedge_data.historical.stock.hist_stock_request import HistStockRequest
from betedge_data.historical.stock.stock_client import HistoricalStockClient

logger = logging.getLogger(__name__)


class HistoricalOptionClient:
    """Client for fetching and filtering historical option data from ThetaData API."""

    def __init__(self):
        """
        Initialize the historical option client.

        Args:
            config: Configuration instance, uses default if None
        """
        self.config = get_hist_client_config()
        self.max_workers = self.config.max_concurrent_requests
        self.http_client = get_http_client()
        self.stock_client = HistoricalStockClient()

    def get_data(self, request: HistOptionBulkRequest) -> io.BytesIO:
        """
        Get option data and return as Parquet or IPC bytes for streaming.
        This is the single orchestrator method (the "dirty place").

        Args:
            request: HistOptionBulkRequest with all parameters and validation

        Returns:
            BytesIO containing Parquet or IPC data ready for streaming
        """
        # Validation
        if not isinstance(request, HistOptionBulkRequest):
            raise ValueError(f"Unsupported request type: {type(request)}")

        logger.info(f"Starting data fetch for {request.root} data_schema={request.data_schema}")

        try:
            # Step 1: Build URL (delegates to pure function)
            url = request.get_url()

            # Step 2: Fetch data (delegates to pure function)
            option_data = self._fetch_data(url)
            logger.debug(f"Option data fetch complete - {len(option_data.response)} records")

            # Step 3: Handle stock data if needed
            stock_request = self._create_stock_request(request)
            stock_url = stock_request.get_url()
            stock_data = self.stock_client._fetch_data(stock_url)
            logger.debug(f"Stock data fetch complete - {len(stock_data.response)} records")

            # Step 4: Convert to Arrow table (delegates to pure function)
            table = self._convert_to_table(option_data, stock_data, request.data_schema)

            # Step 5: Serialize to requested format (orchestrate format selection)
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

    def _convert_to_table(
        self, option_data: OptionThetaDataResponse, stock_data: StockThetaDataResponse, endpoint: str
    ) -> pa.Table:
        """Convert option and stock data to Arrow table (for regular endpoints)."""
        # Use existing flattening logic for backward compatibility
        flattened_data = self._flatten_option_data(option_data, stock_data, endpoint)
        return self._create_arrow_table(flattened_data, data_schema_type=endpoint)

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

    def _flatten_option_data(
        self, option_data: OptionThetaDataResponse, stock_data: StockThetaDataResponse, endpoint: str = "quote"
    ) -> dict:
        """
        Flatten option and stock data into flat tick records with contract info.

        Args:
            option_data: OptionThetaDataResponse containing option data
            stock_data: StockThetaDataResponse containing stock data
            endpoint: Endpoint type ("quote" or "ohlc") to determine data_schema

        Returns:
            Dict with lists of flattened tick data
        """
        option_count = len(option_data.response)
        stock_count = len(stock_data.response)
        logger.debug(
            f"Flattening {option_count} option contracts and {stock_count} stock records using {endpoint} data_schema"
        )

        # Get data_schema configuration for the endpoint
        data_schema = TICK_SCHEMAS[endpoint]
        field_count = data_schema["field_count"]
        field_names = data_schema["field_names"]

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

        # Create final data structure with batch type conversions using data_schema field names
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

    def _create_arrow_table(self, flattened_data: dict, data_schema_type: str = "quote") -> pa.Table:
        """
        Create PyArrow table from flattened option/stock data using data_schema configuration.

        Args:
            flattened_data: Dict with lists of flattened tick data
            data_schema_type: data_schema type ("quote" or "ohlc") to determine field structure

        Returns:
            PyArrow Table ready for serialization

        Raises:
            RuntimeError: If no data is provided
        """
        if not flattened_data or not any(flattened_data.values()):
            logger.error("No data found in flattened data")
            raise RuntimeError("No data to convert")

        # Get data_schema configurations
        tick_data_schema = TICK_SCHEMAS[data_schema_type]
        contract_data_schema = CONTRACT_SCHEMA

        # Combine tick and contract field definitions
        all_field_names = tick_data_schema["field_names"] + contract_data_schema["field_names"]
        all_arrow_types = tick_data_schema["arrow_types"] + contract_data_schema["arrow_types"]

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
        logger.debug(f"Created Arrow table with {total_records} records using {data_schema_type} data_schema")
        return table

    def _create_stock_request(self, option_request: HistOptionBulkRequest) -> HistStockRequest:
        """
        Create a HistStockRequest from a HistOptionBulkRequest.
        
        Args:
            option_request: The option request to derive stock parameters from
            
        Returns:
            HistStockRequest with appropriate parameters for fetching underlying stock data
        """
        # For stock data, we typically want the same date and schema as the option request
        stock_request_params = {
            "root": option_request.root,
            "data_schema": option_request.data_schema,
            "return_format": option_request.return_format,
            "interval": option_request.interval,
        }
        
        # Handle date vs yearmo logic
        if option_request.date is not None:
            stock_request_params["date"] = option_request.date
        elif option_request.yearmo is not None:
            # For EOD with yearmo, we can extract year for stock request
            year = option_request.yearmo // 100
            stock_request_params["year"] = year
        else:
            raise ValueError("Option request must have either date or yearmo specified")
            
        return HistStockRequest(**stock_request_params)

    def _fetch_data(self, url: str) -> OptionThetaDataResponse:
        """Fetch option data from URL (pure function, no error handling)."""
        return self.http_client.fetch_paginated(
            url=url,
            response_model=OptionThetaDataResponse,
            stream_response=True,
            item_path="response.item",
            collect_items=True,
        )
