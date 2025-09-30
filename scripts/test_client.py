from betedge_data import BetEdgeClient
from betedge_data import OptionRequest

client = BetEdgeClient()

request = OptionRequest(
    root="SPY",
    endpoint="eod",
    start_date=20240101,
    end_date=20240131,
    force_refresh=False,
)

client.request_data(request)
