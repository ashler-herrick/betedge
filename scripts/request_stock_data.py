#!/usr/bin/env python3
"""
Helper script for requesting historical stock data.

Configure the parameters below and run the script to make a stock data request.
"""

import json
import time

import requests

# =============================================================================
# CONFIGURATION - Edit these parameters for your request
# =============================================================================

# Stock symbol to fetch data for
SYMBOL = "AAPL"

# Date range (YYYYMMDD format)
START_DATE = "20231201"  # December 1, 2023
END_DATE = "20231205"  # December 5, 2023

# Data parameters
VENUE = "nqb"  # Data venue: nqb, utp_ca, cboe, cqs
INTERVAL = 60000  # Interval in milliseconds (60000 = 1 minute, 0 = tick data)
RTH = True  # Regular trading hours only

# Optional time range (milliseconds since midnight ET)
START_TIME = None  # e.g., 32400000 for 9:00 AM ET
END_TIME = None  # e.g., 57600000 for 4:00 PM ET

# API configuration
API_URL = "http://localhost:8000"
REQUEST_TIMEOUT = 300  # 5 minutes timeout

# =============================================================================
# SCRIPT EXECUTION - No need to edit below this line
# =============================================================================


def print_header(title: str) -> None:
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def print_section(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n📋 {title}")
    print("-" * 40)


def format_time_param(ms: int) -> str:
    """Format milliseconds to human readable time."""
    if ms is None:
        return "Not specified"

    hours = ms // 3600000
    minutes = (ms % 3600000) // 60000
    seconds = (ms % 60000) // 1000
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def main():
    """Execute the stock data request."""
    print_header("HISTORICAL STOCK DATA REQUEST")

    # Display configuration
    print_section("Request Configuration")
    print(f"Symbol: {SYMBOL}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Venue: {VENUE}")
    print(
        f"Interval: {INTERVAL:,}ms ({INTERVAL // 60000}m {(INTERVAL % 60000) // 1000}s)"
        if INTERVAL > 0
        else "Tick data"
    )
    print(f"Regular Trading Hours: {'Yes' if RTH else 'No'}")
    print(f"Start Time: {format_time_param(START_TIME)}")
    print(f"End Time: {format_time_param(END_TIME)}")
    print(f"API URL: {API_URL}")

    # Build request payload
    payload = {
        "root": SYMBOL,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "venue": VENUE,
        "rth": RTH,
        "interval": INTERVAL,
    }

    # Add optional time parameters
    if START_TIME is not None:
        payload["start_time"] = START_TIME
    if END_TIME is not None:
        payload["end_time"] = END_TIME

    print_section("Request Payload")
    print(json.dumps(payload, indent=2))

    # Make API request
    print_section("Making API Request")
    print(f"🌐 Sending POST request to {API_URL}/historical/stock")
    print(f"⏱️  Timeout: {REQUEST_TIMEOUT} seconds")

    start_time = time.time()

    try:
        response = requests.post(
            f"{API_URL}/historical/stock",
            json=payload,
            timeout=REQUEST_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )

        request_duration = time.time() - start_time

        print_section("Response Information")
        print(f"⏱️  Request Duration: {request_duration:.2f} seconds")
        print(f"📊 HTTP Status Code: {response.status_code}")
        print(f"📏 Response Size: {len(response.content):,} bytes")

        # Parse and display response
        if response.status_code in [200, 202]:
            try:
                data = response.json()
                print_section("Response Data")
                print(json.dumps(data, indent=2, default=str))

                # Extract key metrics if available
                if "status" in data:
                    print_section("Processing Summary")
                    print(f"✅ Status: {data.get('status', 'Unknown')}")

                    if "request_id" in data:
                        print(f"🆔 Request ID: {data['request_id']}")

                    if "message" in data:
                        print(f"💬 Message: {data['message']}")

                    if "processing_time_ms" in data:
                        processing_time = data["processing_time_ms"]
                        print(f"⏱️  Processing Time: {processing_time:,}ms ({processing_time / 1000:.2f}s)")

                    if "storage_location" in data:
                        print(f"🗄️  Storage Location: {data['storage_location']}")

                    if "records_count" in data:
                        print(f"📊 Records Count: {data.get('records_count', 0):,}")

            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON response: {e}")
                print(f"Raw response: {response.text}")
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.Timeout:
        print(f"⏰ Request timed out after {REQUEST_TIMEOUT} seconds")
        print("💡 This might indicate:")
        print("   - Large date range requiring more processing time")
        print("   - ThetaTerminal connectivity issues")
        print("   - API server overload")

    except requests.exceptions.ConnectionError:
        print(f"🔌 Connection failed to {API_URL}")
        print("💡 Check that:")
        print("   - API server is running on localhost:8000")
        print("   - No firewall blocking the connection")
        print("   - URL is correct")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    print_header("REQUEST COMPLETED")
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
