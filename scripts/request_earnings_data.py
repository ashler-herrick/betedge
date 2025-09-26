#!/usr/bin/env python3
"""
Helper script for requesting earnings data.

Configure the parameters below and run the script to make an earnings data request.
"""

import json
import time
from datetime import datetime

import requests

# =============================================================================
# CONFIGURATION - Edit these parameters for your request
# =============================================================================

# Date range (YYYYMMDD format) - minimum 1 month required
START_DATE = "20231201"  # December 1, 2023
END_DATE = "20231231"  # December 31, 2023

# Return format
RETURN_FORMAT = "parquet"  # Currently only "parquet" is supported

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


def calculate_months_in_range(start_date: str, end_date: str) -> int:
    """Calculate the number of months in the date range."""
    try:
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")

        months = (end.year - start.year) * 12 + (end.month - start.month)
        if end.day >= start.day:
            months += 1

        return max(1, months)
    except ValueError:
        return 1


def format_date_range(start_date: str, end_date: str) -> str:
    """Format date range for display."""
    try:
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")

        start_formatted = start.strftime("%B %d, %Y")
        end_formatted = end.strftime("%B %d, %Y")

        return f"{start_formatted} to {end_formatted}"
    except ValueError:
        return f"{start_date} to {end_date}"


def main():
    """Execute the earnings data request."""
    print_header("EARNINGS DATA REQUEST")

    # Calculate months and display configuration
    months_count = calculate_months_in_range(START_DATE, END_DATE)

    print_section("Request Configuration")
    print(f"Date Range: {format_date_range(START_DATE, END_DATE)}")
    print(f"Raw Dates: {START_DATE} to {END_DATE}")
    print(f"Estimated Months: {months_count}")
    print(f"Return Format: {RETURN_FORMAT}")
    print(f"API URL: {API_URL}")

    # Build request payload
    payload = {
        "start_date": START_DATE,
        "end_date": END_DATE,
        "return_format": RETURN_FORMAT,
    }

    print_section("Request Payload")
    print(json.dumps(payload, indent=2))

    # Validate date range
    if months_count == 0:
        print("⚠️  Warning: Date range appears to be less than 1 month")
    elif months_count > 12:
        print(f"📊 Large request: {months_count} months of data will be processed")
        print("   This may take several minutes to complete")

    # Make API request
    print_section("Making API Request")
    print(f"🌐 Sending POST request to {API_URL}/earnings")
    print(f"⏱️  Timeout: {REQUEST_TIMEOUT} seconds")
    print("📝 Note: Earnings data is fetched from NASDAQ API and processed monthly")

    start_time = time.time()

    try:
        response = requests.post(
            f"{API_URL}/earnings",
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

                        # Parse message for monthly processing insights
                        if "month" in message.lower():
                            print(
                                "📅 Monthly Processing: Data processed in monthly chunks"
                            )
                        if "/" in message and "month" in message.lower():
                            print("📊 Progress: Message shows month-by-month progress")

                    if "processing_time_ms" in data:
                        processing_time = data["processing_time_ms"]
                        print(
                            f"⏱️  Processing Time: {processing_time:,}ms ({processing_time / 1000:.2f}s)"
                        )

                        # Performance insights
                        if processing_time > 60000:  # > 1 minute
                            print(
                                f"📈 Performance: {processing_time / 1000 / months_count:.1f}s per month average"
                            )

                    if "storage_location" in data:
                        storage_location = data["storage_location"]
                        print(f"🗄️  Storage Location: {storage_location}")
                        if "earnings" in storage_location:
                            print("✅ Stored in earnings-specific bucket path")

                    if "records_count" in data:
                        records_count = data.get("records_count", 0)
                        print(f"📊 Records Count: {records_count:,}")
                        if records_count > 0:
                            print(
                                f"💾 Data Processed: {records_count:,} earnings announcements"
                            )
                            if months_count > 0:
                                avg_per_month = records_count / months_count
                                print(
                                    f"📈 Average per Month: {avg_per_month:.0f} earnings announcements"
                                )

                    if "data_type" in data:
                        data_type = data["data_type"]
                        print(f"📋 Data Type: {data_type}")
                        if data_type == "parquet":
                            print("✅ Data stored in efficient Parquet format")

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
                print("   - end_date >= start_date")
                print("   - Date range spans at least 1 month")
                print("   - return_format is 'parquet'")
            elif response.status_code == 400:
                print("\n💡 Bad Request - Check:")
                print("   - Date range is reasonable (not too far in future)")
                print("   - Dates are valid calendar dates")

    except requests.exceptions.Timeout:
        print(f"⏰ Request timed out after {REQUEST_TIMEOUT} seconds")
        print("💡 This might indicate:")
        print("   - Large date range requiring more processing time")
        print("   - NASDAQ API being slow or unresponsive")
        print("   - Network connectivity issues")
        print(f"   - Consider reducing date range (currently {months_count} months)")

    except requests.exceptions.ConnectionError:
        print(f"🔌 Connection failed to {API_URL}")
        print("💡 Check that:")
        print("   - API server is running on localhost:8000")
        print("   - Internet connection is available for NASDAQ API")
        print("   - No firewall blocking the connection")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    print_header("REQUEST COMPLETED")
    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.2f} seconds")

    if total_time > 60:
        print(
            f"💡 Processing took {total_time / 60:.1f} minutes for {months_count} months"
        )
        print(f"   Average: {total_time / months_count:.1f} seconds per month")


if __name__ == "__main__":
    main()
