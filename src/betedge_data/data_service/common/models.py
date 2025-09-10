"""Pydantic models for ThetaData API responses."""

from typing import List, Literal, Union, Optional, Any

import pyarrow as pa
from pydantic import BaseModel

from betedge_data.data_service.common.utils import generate_schema_from_model


class Header(BaseModel):
    """ThetaData API response header with metadata and pagination info."""

    latency_ms: int
    format: List[str]
    next_page: Optional[str] = None
    error_type: Optional[str] = None
    error_msg: Optional[str] = None


class Contract(BaseModel):
    """Option contract specification."""

    root: str
    expiration: int
    strike: int
    right: Literal["C", "P"]


class QuoteTick(BaseModel):
    """Quote tick data with bid/ask information."""

    ms_of_day: int
    bid_size: int
    bid_exchange: int
    bid: float
    bid_condition: int
    ask_size: int
    ask_exchange: int
    ask: float
    ask_condition: int
    date: int

    @classmethod
    def from_array(cls, tick_array: List) -> "QuoteTick":
        """Convert API array format to QuoteTick object."""
        return cls(
            ms_of_day=tick_array[0],
            bid_size=tick_array[1],
            bid_exchange=tick_array[2],
            bid=tick_array[3],
            bid_condition=tick_array[4],
            ask_size=tick_array[5],
            ask_exchange=tick_array[6],
            ask=tick_array[7],
            ask_condition=tick_array[8],
            date=tick_array[9],
        )


class OHLCTick(BaseModel):
    """OHLC tick data with volume and count."""

    ms_of_day: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    count: int
    date: int

    @classmethod
    def from_array(cls, tick_array: List) -> "OHLCTick":
        """Convert API array format to OHLCTick object."""
        return cls(
            ms_of_day=tick_array[0],
            open=tick_array[1],
            high=tick_array[2],
            low=tick_array[3],
            close=tick_array[4],
            volume=tick_array[5],
            count=tick_array[6],
            date=tick_array[7],
        )


class EODTick(BaseModel):
    """End-of-day tick data with comprehensive OHLC and quote information."""

    ms_of_day: int
    ms_of_day2: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    count: int
    bid_size: int
    bid_exchange: int
    bid: float
    bid_condition: int
    ask_size: int
    ask_exchange: int
    ask: float
    ask_condition: int
    date: int

    @classmethod
    def from_array(cls, tick_array: List) -> "EODTick":
        """Convert API array format to EODTick object."""
        return cls(
            ms_of_day=tick_array[0],
            ms_of_day2=tick_array[1],
            open=tick_array[2],
            high=tick_array[3],
            low=tick_array[4],
            close=tick_array[5],
            volume=tick_array[6],
            count=tick_array[7],
            bid_size=tick_array[8],
            bid_exchange=tick_array[9],
            bid=tick_array[10],
            bid_condition=tick_array[11],
            ask_size=tick_array[12],
            ask_exchange=tick_array[13],
            ask=tick_array[14],
            ask_condition=tick_array[15],
            date=tick_array[16],
        )


Tick = Union[QuoteTick, OHLCTick, EODTick]


class ThetaDataResponse(BaseModel):
    """Base class for ThetaData API responses."""

    header: Header
    response: List[Any]

    def has_next_page(self) -> bool:
        """Check if there's a next page."""
        return bool(self.header.next_page and self.header.next_page.lower() != "null")


class OptionResponseItem(BaseModel):
    """Single option contract with its tick data."""

    ticks: List[List]  # Raw arrays from API
    contract: Contract

    def get_parsed_ticks(self) -> List[QuoteTick]:
        """Convert raw tick arrays to QuoteTick objects."""
        return [QuoteTick.from_array(tick) for tick in self.ticks]


class OptionThetaDataResponse(ThetaDataResponse):
    """ThetaData option API response containing multiple contracts."""

    response: List[OptionResponseItem]


class StockThetaDataResponse(ThetaDataResponse):
    """ThetaData stock API response containing tick arrays."""

    response: List[List]  # Raw tick arrays for stock data


# Auto-generate schemas from Pydantic models
TICK_SCHEMAS = {
    "quote": generate_schema_from_model(QuoteTick),
    "ohlc": generate_schema_from_model(OHLCTick),
    "eod": generate_schema_from_model(EODTick),
}

# Contract field schema (same for all tick types)
CONTRACT_SCHEMA = {
    "field_names": ["root", "expiration", "strike", "right"],
    "arrow_types": [pa.string(), pa.uint32(), pa.uint32(), pa.string()],
}
