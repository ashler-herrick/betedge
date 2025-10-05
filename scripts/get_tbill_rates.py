"""Quick script that runs in 15s and gets an approximation for risk free rates that I can run once month."""
from io import BytesIO
from collections import defaultdict
from typing import List
from dataclasses import dataclass

import httpx
import pyarrow as pa 
import pyarrow.parquet as pq 

from betedge_data import BetEdgeClient

CLIENT = BetEdgeClient()

@dataclass
class TBillEntry:
    year: str
    month: str
    rate: float

def get_tbill_responses() -> List[TBillEntry]:
    client = httpx.Client(
                timeout=60,
                limits=httpx.Limits(
                    max_connections=4,
                    max_keepalive_connections=4,
                )
    )

    base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates"

    tbill_responses = []
    next_link = "&page%5Bnumber%5D=1&page%5Bsize%5D=100"
    while next_link is not None:

        response = client.get(base_url + "?" + next_link)
        resp_json = response.json()
        data = resp_json["data"]
        for entry in data:
            if entry["security_desc"] == "Treasury Bills":
                tbill = TBillEntry(
                    year=entry["record_calendar_year"],
                    month=entry["record_calendar_month"],
                    rate=float(entry["avg_interest_rate_amt"])
                )
                tbill_responses.append(tbill)
        
        links = resp_json["links"]
        next_link = links["next"]

    return tbill_responses


def write_file_with_client(client: BetEdgeClient, buffer: BytesIO, object_key: str) -> None:
    minio_client = client.minio_client
    size = len(buffer.getvalue())

    minio_client.put_object(
        bucket_name=client.minio_config.bucket,
        object_name=object_key,
        data=buffer,
        length=size,
        content_type="application/octet-stream",
    )

def write_tables(tbill_responses: List[TBillEntry]) -> None:

    entries_by_year = defaultdict(list)
    for entry in tbill_responses:
        entries_by_year[entry.year].append(entry)

    yearly_tables = {}
    for year, year_entries in entries_by_year.items():
        months = [entry.month for entry in year_entries]
        rates = [entry.rate for entry in year_entries]
        years = [entry.year for entry in year_entries]
        
        # Create PyArrow table
        table = pa.table({
            'year': years,
            'month': months,
            'rate': rates
        })
        
        yearly_tables[year] = table

    for year, table in yearly_tables.items():
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        object_key = f"tbill-rates/{year}/data.parquet"
        write_file_with_client(CLIENT, buffer, object_key)

if __name__ == "__main__":
    resp = get_tbill_responses()
    write_tables(resp)