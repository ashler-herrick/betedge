
import requests 
from io import StringIO

import pandas as pd

from betedge_data import BetEdgeClient, OptionRequest


def get_index_tickers(index_name):
    urls = {
        "sp500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "dow": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        "nasdaq100": "https://en.wikipedia.org/wiki/NASDAQ-100",
        "russell1000": "https://en.wikipedia.org/wiki/Russell_1000_Index",
        "sp400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "sp600": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
    }

    if index_name not in urls:
        raise ValueError(f"Index {index_name} not supported")

    url = urls[index_name]

    # Add headers to avoid 403 error
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # Use requests to get the page content
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes

    # Read HTML tables from the response content
    tables = pd.read_html(StringIO(response.text))

    # Different pages have different structures
    if index_name == "sp500":
        return tables[0]["Symbol"].tolist()
    elif index_name == "dow":
        return tables[1]["Symbol"].tolist()
    elif index_name == "nasdaq100":
        return tables[4]["Ticker"].tolist()
    else:
        # Try common column names
        for col in ["Symbol", "Ticker", "Stock Symbol"]:
            if col in tables[0].columns:
                return tables[0][col].tolist()

    return []


def main():
    client = BetEdgeClient(num_threads=2, log_level="info")

    nq = get_index_tickers("sp500")
    
    for t in nq:
        req = OptionRequest(
            root=t,
            start_date=20200101,
            end_date=20250929,
            endpoint="eod",   
        )

        client.request_data(req)

if __name__ == "__main__":
    main()