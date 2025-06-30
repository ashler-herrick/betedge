"""
Type stubs for fast_parser Rust module.

This module provides high-performance option contract filtering and processing
with Arrow-native data structures and serialization.
"""

def filter_contracts_to_ipc_bytes(
    option_json: str,
    stock_json: str,
    current_yyyymmdd: int,
    max_dte: int,
    base_pct: float,
) -> bytes:
    """
    Filter option contracts using time-matched underlying prices and return Arrow IPC bytes for streaming.
    
    This function processes tick-by-tick option data with combined stock data for time-matched
    underlying prices and returns Arrow IPC format bytes suitable for direct streaming to Kafka.
    
    Args:
        option_json: JSON string containing option contracts with tick arrays
        stock_json: JSON string containing underlying stock quotes for price matching
        current_yyyymmdd: Current date in YYYYMMDD format (e.g., 20231110)
        max_dte: Maximum days to expiration
        base_pct: Base percentage for moneyness filter (e.g., 0.1 for 10%)
    
    Returns:
        Arrow IPC bytes containing filtered tick-by-tick data with schema:
        - ms_of_day: Milliseconds since midnight (uint32)
        - bid_size, ask_size: Order sizes (uint16)
        - bid_exchange, ask_exchange: Exchange codes (uint8)
        - bid_price, ask_price: Prices (float32)
        - bid_condition, ask_condition: Condition codes (uint8)
        - date: Date of tick (uint32)
        - root: Underlying symbol (string)
        - expiration: Option expiration date (uint32)
        - strike: Strike price in minor units (uint32)
        - right: "C" for call, "P" for put (string)
    
    Example:
        >>> import fast_parser
        >>> option_data = '{"header": {...}, "response": [...]}'
        >>> stock_data = '{"header": {...}, "response": [...]}'
        >>> ipc_bytes = fast_parser.filter_contracts_to_ipc_bytes(
        ...     option_data, stock_data, 20231110, 30, 0.1
        ... )
        >>> # Stream ipc_bytes directly to Kafka
    """
    ...

def filter_contracts_to_parquet_bytes(
    option_json: str,
    stock_json: str,
    current_yyyymmdd: int,
    max_dte: int,
    base_pct: float,
) -> bytes:
    """
    Filter option contracts using time-matched underlying prices and return Parquet bytes for storage.
    
    This function processes tick-by-tick option data with combined stock data for time-matched
    underlying prices and returns compressed Parquet format bytes suitable for efficient storage.
    
    Args:
        option_json: JSON string containing option contracts with tick arrays
        stock_json: JSON string containing underlying stock quotes for price matching
        current_yyyymmdd: Current date in YYYYMMDD format (e.g., 20231110)
        max_dte: Maximum days to expiration
        base_pct: Base percentage for moneyness filter (e.g., 0.1 for 10%)
    
    Returns:
        Parquet bytes containing filtered tick-by-tick data with same schema as IPC:
        - Columnar storage optimized for analytics
        - Compatible with pandas, polars, DuckDB, etc.
    
    Example:
        >>> import fast_parser
        >>> option_data = '{"header": {...}, "response": [...]}'
        >>> stock_data = '{"header": {...}, "response": [...]}'
        >>> parquet_bytes = fast_parser.filter_contracts_to_parquet_bytes(
        ...     option_data, stock_data, 20231110, 30, 0.1
        ... )
        >>> # Write parquet_bytes to file or S3
    """
    ...
