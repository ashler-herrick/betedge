from typing import Dict
import requests
import time

# Test configuration
API_BASE_URL = "http://localhost:8000"
TEST_SYMBOL = "AAPL"
TEST_INTERVAL = 450_000  # 1 minute intervals

def make_ohlc_request() -> Dict:
    """
    Make historical stock EOD API request.
    
    Args:
        start_year: Start year for EOD data
        end_year: End year for EOD data
        
    Returns:
        API response data
    """
    # Convert years to date format expected by unified endpoint
    start_date = f"20230101"
    end_date = f"20230201"
    
    payload = {
        "root": "AAPL",
        "start_date": start_date,
        "end_date": end_date,
        "endpoint": "ohlc",
        "interval": 4_500_000,
        "return_format": "parquet"
    }
    
    print("Making EOD API request:")
    print(f"  Endpoint: POST {API_BASE_URL}/historical/stock")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Date range: {start_date} to {end_date}")
    print("  Endpoint: ohlc")
    print("  Format: parquet")
    print()
    
    start_time = time.time()
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/historical/stock",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        
        request_time = time.time() - start_time
        
        print("API Response:")
        print(f"  Status: {response.status_code}")
        print(f"  Request time: {request_time:.2f}s")
        
        if response.status_code == 202:  # Accepted
            return response.json()
        else:
            print(f"  Error: {response.text}")
            return {"error": f"HTTP {response.status_code}: {response.text}"}
            
    except Exception as e:
        print(f"  Error: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    make_ohlc_request()