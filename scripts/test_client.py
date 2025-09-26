from betedge_data.data_service import BetEdgeClient
from betedge_data.data_service import OptionRequest

client = BetEdgeClient()

request = OptionRequest(
            root="AAPL",
            start_date="20240101", 
            end_date="20240131",
            data_schema="quote",
            interval=900000,  # 15 minutes in milliseconds
            force_refresh=False
        )

job_info = client.request_data(request)

print(job_info)