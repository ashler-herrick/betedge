import io
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from betedge_data.alternative.earnings.models import EarningsRequest, EarningsRecord
from betedge_data.common.interface import IClient
from betedge_data.common.http import get_http_client
from betedge_data.manager.utils import generate_trading_date_list

logger = logging.getLogger(__name__)


class EarningsClient(IClient):
    """Client for fetching and normalizing NASDAQ earnings data."""

    def __init__(self):
        """Initialize the earnings client."""
        self.base_url = "https://api.nasdaq.com/api/calendar/earnings"
        self.headers = {
            "authority": "api.nasdaq.com",
            "accept": "application/json, text/plain, */*",
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
            ),
            "origin": "https://www.nasdaq.com",
            "sec-fetch-site": "same-site",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https://www.nasdaq.com/",
            "accept-language": "en-US,en;q=0.9",
        }
        self.client = get_http_client()

    def get_data(self, request: EarningsRequest) -> io.BytesIO:
        """
        Get monthly earnings data as Parquet bytes.

        Args:
            request: EarningsRequest with year, month and parameters

        Returns:
            BytesIO containing Parquet data ready for streaming
        """
        logger.info(f"Starting monthly earnings data fetch for {request.year}-{request.month:02d}")

        # Validate return format
        if request.return_format != "parquet":
            raise ValueError(f"Expected return_format='parquet', got '{request.return_format}'")

        try:
            # Generate trading dates for the month using proper holiday-aware function
            start_date = datetime(request.year, request.month, 1)
            if request.month == 12:
                end_date = datetime(request.year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(request.year, request.month + 1, 1) - timedelta(days=1)

            # Convert to YYYYMMDD format for generate_trading_date_list
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")

            # Get trading dates as integers, then convert to YYYY-MM-DD format for API
            trading_dates_int = generate_trading_date_list(start_date_str, end_date_str)
            trading_dates = [
                datetime.strptime(str(date_int), "%Y%m%d").strftime("%Y-%m-%d") for date_int in trading_dates_int
            ]

            logger.info(f"Generated {len(trading_dates)} trading dates for {request.year}-{request.month:02d}")

            # Fetch earnings data for all dates
            all_records = []
            failed_dates = []

            for i, date_str in enumerate(trading_dates, 1):
                try:
                    logger.debug(f"Fetching earnings for {date_str} ({i}/{len(trading_dates)})")
                    daily_records = self._fetch_daily_earnings(date_str)
                    all_records.extend(daily_records)

                    if i % 50 == 0:  # Log progress every 50 requests
                        logger.info(
                            f"Progress: {i}/{len(trading_dates)} dates processed, {len(all_records)} total records"
                        )

                except Exception as e:
                    logger.warning(f"Failed to fetch earnings for {date_str}: {e}")
                    failed_dates.append(date_str)
                    continue

            successful_dates = len(trading_dates) - len(failed_dates)
            total_dates = len(trading_dates)
            logger.info(
                f"Data collection complete: {len(all_records)} records from {successful_dates}/{total_dates} dates"
            )

            if failed_dates:
                failed_preview = failed_dates[:5]
                ellipsis = "..." if len(failed_dates) > 5 else ""
                logger.warning(f"Failed to fetch data for {len(failed_dates)} dates: {failed_preview}{ellipsis}")

            if not all_records:
                logger.error(f"No earnings data found for {request.year}-{request.month:02d}")
                raise RuntimeError

            # Convert to Parquet
            parquet_buffer = self._convert_to_parquet(all_records)
            logger.info(f"Conversion complete: {parquet_buffer.getvalue().__len__()} bytes Parquet data generated")
            return parquet_buffer

        except Exception as e:
            logger.error(f"Monthly earnings data fetch failed for {request.year}-{request.month:02d}: {str(e)}")
            logger.debug("Monthly earnings fetch error details", exc_info=True)
            raise RuntimeError(f"Monthly earnings data fetch failed: {e}") from e

    def _fetch_daily_earnings(self, date_str: str) -> List[EarningsRecord]:
        """
        Fetch earnings data for a specific date.

        Args:
            date_str: Date in YYYY-MM-DD format

        Returns:
            List of normalized EarningsRecord objects
        """
        try:
            # Use shared HTTP client's internal httpx client for the request
            response = self.client.client.get(self.base_url, params={"date": date_str}, headers=self.headers)
            response.raise_for_status()

            data = response.json()

            # Extract earnings data
            if "data" not in data or not data["data"]:
                logger.debug(f"No earnings data available for {date_str}")
                return []

            earnings_data = data["data"]
            if "rows" not in earnings_data or not earnings_data["rows"]:
                logger.debug(f"No earnings rows for {date_str}")
                return []

            # Normalize the data
            records = []
            for row in earnings_data["rows"]:
                try:
                    record = self._normalize_earnings_record(row, date_str)
                    records.append(record)
                except Exception as e:
                    logger.warning(f"Failed to normalize earnings record for {date_str}: {e}, row: {row}")
                    continue

            logger.debug(f"Fetched {len(records)} earnings records for {date_str}")
            return records

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug(f"No earnings data found for {date_str} (404)")
                return []
            else:
                logger.error(f"HTTP error fetching earnings for {date_str}: {e.response.status_code}")
                raise
        except Exception as e:
            logger.error(f"Error fetching earnings for {date_str}: {e}")
            raise

    def _normalize_earnings_record(self, row: Dict, date_str: str) -> EarningsRecord:
        """
        Normalize a single earnings record from API response.

        Args:
            row: Raw earnings data row
            date_str: Date in YYYY-MM-DD format

        Returns:
            Normalized EarningsRecord
        """
        return EarningsRecord(
            date=date_str,
            symbol=row.get("symbol", "").strip(),
            name=row.get("name", "").strip(),
            time=self._parse_time(row.get("time")),
            eps=self._parse_currency(row.get("eps")),
            eps_forecast=self._parse_currency(row.get("epsForecast")),
            surprise_pct=self._parse_percentage(row.get("surprise")),
            market_cap=self._parse_market_cap(row.get("marketCap")),
            fiscal_quarter_ending=row.get("fiscalQuarterEnding", "").strip() or None,
            num_estimates=self._parse_int(row.get("noOfEsts")),
        )

    def _parse_currency(self, value: str) -> Optional[float]:
        """Parse currency values like '$0.56', '($2.55)', 'N/A', or empty string."""
        if not value or value == "N/A":
            return None

        try:
            # Remove $ and commas, handle parentheses for negative values
            cleaned = value.strip().replace("$", "").replace(",", "")

            if cleaned.startswith("(") and cleaned.endswith(")"):
                # Negative value in parentheses
                cleaned = "-" + cleaned[1:-1]

            return float(cleaned)
        except (ValueError, AttributeError):
            return None

    def _parse_percentage(self, value: str) -> Optional[float]:
        """Parse percentage values like '12', 'N/A', or empty string."""
        if not value or value == "N/A":
            return None

        try:
            return float(str(value).strip())
        except (ValueError, AttributeError):
            return None

    def _parse_market_cap(self, value: str) -> Optional[int]:
        """Parse market cap values like '$899,395,987', 'N/A', or empty string."""
        if not value or value == "N/A":
            return None

        try:
            # Remove $ and commas
            cleaned = str(value).strip().replace("$", "").replace(",", "")
            return int(float(cleaned))
        except (ValueError, AttributeError):
            return None

    def _parse_int(self, value: str) -> Optional[int]:
        """Parse integer values, handling 'N/A' and empty strings."""
        if not value or value == "N/A":
            return None

        try:
            return int(str(value).strip())
        except (ValueError, AttributeError):
            return None

    def _parse_time(self, value: str) -> Optional[str]:
        """Parse time values, handling 'time-not-supplied' and empty strings."""
        if not value or value == "time-not-supplied":
            return None

        try:
            return str(value).strip()
        except (ValueError, AttributeError):
            return None

    def _convert_to_parquet(self, records: List[EarningsRecord]) -> io.BytesIO:
        """
        Convert earnings records to Parquet format.

        Args:
            records: List of EarningsRecord objects

        Returns:
            BytesIO containing Parquet data
        """
        try:
            logger.debug(f"Converting {len(records)} earnings records to Parquet")

            # Convert records to dictionary format
            data = {
                "date": [r.date for r in records],
                "symbol": [r.symbol for r in records],
                "name": [r.name for r in records],
                "time": [r.time for r in records],
                "eps": [r.eps for r in records],
                "eps_forecast": [r.eps_forecast for r in records],
                "surprise_pct": [r.surprise_pct for r in records],
                "market_cap": [r.market_cap for r in records],
                "fiscal_quarter_ending": [r.fiscal_quarter_ending for r in records],
                "num_estimates": [r.num_estimates for r in records],
            }

            # Define schema with proper data types
            schema = pa.schema(
                [
                    pa.field("date", pa.string()),
                    pa.field("symbol", pa.string()),
                    pa.field("name", pa.string()),
                    pa.field("time", pa.string()),
                    pa.field("eps", pa.float64()),
                    pa.field("eps_forecast", pa.float64()),
                    pa.field("surprise_pct", pa.float64()),
                    pa.field("market_cap", pa.int64()),
                    pa.field("fiscal_quarter_ending", pa.string()),
                    pa.field("num_estimates", pa.int64()),
                ]
            )

            # Create PyArrow table
            table = pa.table(data, schema=schema)

            # Write to Parquet with Snappy compression
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)

            logger.info(f"Converted {len(records)} earnings records to Parquet")
            return buffer

        except Exception as e:
            logger.error(f"Failed to convert earnings data to Parquet: {str(e)}")
            raise RuntimeError(f"Parquet conversion failed: {e}") from e
