import httpx
import csv
import io


def fetch_option_quotes(
    root: str = "AAPL",
    exp: int = 0,
    start_date: str = "20250922",
    end_date: str = "20250922",
    ivl: int = 3_600_000,
    base_url: str = "http://127.0.0.1:25510",
) -> tuple[list[dict], dict]:
    """
    Fetch option quotes CSV data and return parsed data plus response info.

    Returns:
        tuple: (parsed_csv_data, response_info)
    """

    url = f"{base_url}/v2/hist/stock/quote"
    params = {
        "root": root,
        # "exp": exp,
        "start_date": start_date,
        "end_date": end_date,
        "ivl": ivl,
        "use_csv": "true",
    }

    with httpx.Client() as client:
        response = client.get(url, params=params, timeout=30.0)

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            print(f"HTTP Error {e.response.status_code} for URL: {e.request.url}")
            print(f"Response: {e.response.text}")
            print(f"Headers: {dict(e.response.headers)}")
            raise

        # Collect response metadata for pagination analysis
        response_info = {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "content_length": len(response.content),
            "url": str(response.url),
        }

        # Parse CSV content
        csv_content = response.text
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        data = list(csv_reader)

        # Add data analysis to response info
        response_info["row_count"] = len(data)
        response_info["columns"] = list(data[0].keys()) if data else []

        return data, response_info


if __name__ == "__main__":
    print("Fetching option quotes data...")

    data, response_info = fetch_option_quotes()

    print(response_info)
    if data:
        print("\nFirst few rows:")
        for i, row in enumerate(data[:3]):
            print(f"Row {i + 1}: {row}")
