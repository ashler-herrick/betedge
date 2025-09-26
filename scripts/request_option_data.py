#!/usr/bin/env python3
"""
Helper script for requesting historical option data.

Configure the parameters below and run the script to make an option data request.
"""

import json
import time

import requests

# =============================================================================
# CONFIGURATION - Edit these parameters for your request
# =============================================================================

# Option root symbol to fetch data for
SYMBOL = "AAPL"

# Date range (YYYYMMDD format)
START_DATE = "20231201"  # December 1, 2023
END_DATE = "20231205"  # December 5, 2023

# Option-specific parameters
EXPIRATION = 0  # 0 = all expirations, "20231215" = specific exp, ["20231215", "20231222"] = multiple
MAX_DTE = 30  # Maximum days to expiration (1-365)
BASE_PCT = 0.1  # Base percentage for moneyness filtering (0.1 = 10%)

# Data parameters
INTERVAL = 900000  # Interval in milliseconds (900000 = 15 minutes, 0 = tick data)

# Optional time range (milliseconds since midnight ET)
START_TIME = None  # e.g., 32400000 for 9:00 AM ET
END_TIME = None  # e.g., 57600000 for 4:00 PM ET

# Threading for multiple expirations
MAX_WORKERS = None  # Number of threads for parallel processing (1-20, None = auto)

# API configuration
API_URL = "http://localhost:8000"
REQUEST_TIMEOUT = 600  # 10 minutes timeout (options can take longer)

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


def format_expiration(exp):
    """Format expiration parameter for display."""
    if exp == 0:
        return "All available expirations"
    elif isinstance(exp, str):
        return f"Single expiration: {exp}"
    elif isinstance(exp, list):
        return f"Multiple expirations: {', '.join(exp)}"
    else:
        return str(exp)


def main():
    """Execute the option data request."""
    print_header("HISTORICAL OPTION DATA REQUEST")

    # Display configuration
    print_section("Request Configuration")
    print(f"Symbol: {SYMBOL}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Expiration: {format_expiration(EXPIRATION)}")
    print(f"Max DTE: {MAX_DTE} days")
    print(f"Base Percentage: {BASE_PCT * 100}% (moneyness filter)")
    print(
        f"Interval: {INTERVAL:,}ms ({INTERVAL // 60000}m {(INTERVAL % 60000) // 1000}s)"
        if INTERVAL > 0
        else "Tick data"
    )
    print(f"Start Time: {format_time_param(START_TIME)}")
    print(f"End Time: {format_time_param(END_TIME)}")
    print(f"Max Workers: {MAX_WORKERS if MAX_WORKERS else 'Auto'}")
    print(f"API URL: {API_URL}")

    # Build request payload
    payload = {
        "root": SYMBOL,
        "exp": EXPIRATION,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "max_dte": MAX_DTE,
        "base_pct": BASE_PCT,
        "interval": INTERVAL,
    }

    # Add optional parameters
    if START_TIME is not None:
        payload["start_time"] = START_TIME
    if END_TIME is not None:
        payload["end_time"] = END_TIME
    if MAX_WORKERS is not None:
        payload["max_workers"] = MAX_WORKERS

    print_section("Request Payload")
    print(json.dumps(payload, indent=2))

    # Make API request
    print_section("Making API Request")
    print(f"🌐 Sending POST request to {API_URL}/historical/option")
    print(f"⏱️  Timeout: {REQUEST_TIMEOUT} seconds")
    print("📝 Note: Option requests may take longer due to filtering and processing")

    start_time = time.time()

    try:
        response = requests.post(
            f"{API_URL}/historical/option",
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
                        message = data["message"]
                        print(f"💬 Message: {message}")

                        # Parse message for insights
                        if "days" in message:
                            print(
                                "📅 Date Processing: Message indicates daily processing"
                            )
                        if "expiration" in message.lower():
                            print(
                                "📊 Expiration Processing: Multiple expirations handled"
                            )

                    if "processing_time_ms" in data:
                        processing_time = data["processing_time_ms"]
                        print(
                            f"⏱️  Processing Time: {processing_time:,}ms ({processing_time / 1000:.2f}s)"
                        )

                    if "storage_location" in data:
                        storage_location = data["storage_location"]
                        print(f"🗄️  Storage Location: {storage_location}")
                        if "historical-options" in storage_location:
                            print("✅ Stored in option-specific bucket path")

                    if "records_count" in data:
                        records_count = data.get("records_count", 0)
                        print(f"📊 Records Count: {records_count:,}")
                        if records_count > 0:
                            print(
                                f"💾 Data Size: Processed {records_count:,} option records"
                            )

            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON response: {e}")
                print(f"Raw response: {response.text}")
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text}")

            # Common error guidance
            if response.status_code == 422:
                print("\n💡 Validation Error - Check:")
                print("   - Date format is YYYYMMDD")
                print("   - Expiration format is YYYYMMDD")
                print("   - MAX_DTE is between 1-365")
                print("   - BASE_PCT is between 0.01-1.0")
            elif response.status_code == 400:
                print("\n💡 Bad Request - Check:")
                print("   - Symbol exists and is valid")
                print("   - Date range is reasonable")
                print("   - Expiration dates are valid")

    except requests.exceptions.Timeout:
        print(f"⏰ Request timed out after {REQUEST_TIMEOUT} seconds")
        print("💡 This might indicate:")
        print("   - Large date range with many expirations")
        print("   - Complex filtering taking longer than expected")
        print("   - ThetaTerminal connectivity issues")
        print("   - Consider reducing date range or using specific expirations")

    except requests.exceptions.ConnectionError:
        print(f"🔌 Connection failed to {API_URL}")
        print("💡 Check that:")
        print("   - API server is running on localhost:8000")
        print("   - ThetaTerminal is running and authenticated")
        print("   - No firewall blocking the connection")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    print_header("REQUEST COMPLETED")
    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.2f} seconds")

    if total_time > 60:
        print(f"💡 Long processing time detected ({total_time / 60:.1f} minutes)")
        print("   Consider using smaller date ranges for faster results")


if __name__ == "__main__":
    main()
