import pyarrow as pa

quote = [
    pa.field("ms_of_day", pa.int64()),
    pa.field("bid_size", pa.int32()),
    pa.field("bid_exchange", pa.int16()),
    pa.field("bid", pa.float64()),
    pa.field("bid_condition", pa.int16()),
    pa.field("ask_size", pa.int32()),
    pa.field("ask_exchange", pa.int16()),
    pa.field("ask", pa.float64()),
    pa.field("ask_condition", pa.int16()),
    pa.field("date", pa.int32()),
]

eod = [
    pa.field("ms_of_day", pa.int64()),
    pa.field("ms_of_day_2", pa.int64()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("count", pa.int64()),
    pa.field("bid_size", pa.int32()),
    pa.field("bid_exchange", pa.int16()),
    pa.field("bid", pa.float64()),
    pa.field("bid_condition", pa.int16()),
    pa.field("ask_size", pa.int32()),
    pa.field("ask_exchange", pa.int16()),
    pa.field("ask", pa.float64()),
    pa.field("ask_condition", pa.int16()),
    pa.field("date", pa.int32()),
]


contract = [
    pa.field("root", pa.string()),
    pa.field("expiration", pa.int32()),
    pa.field("strike", pa.int64()),
    pa.field("right", pa.string()),
]

stock_quote = pa.schema(quote)
stock_eod = pa.schema(eod)
option_quote = pa.schema(contract + quote)
option_eod = pa.schema(contract + eod)
