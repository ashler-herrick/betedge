from typing import List, Dict
from enum import Enum
from urllib.parse import urlencode
from uuid import uuid4
from betedge_data.datetime import (
    map_trading_days_to_yearmo,
    interval_ms_to_string,
    DateParts,
)
from betedge_data.client.validations import (
    val_interval,
    val_start_date_before_end_date,
)


THETA_BASE_URL = "http://127.0.0.1:25510/v2"


class FileGranularity(Enum):
    DAILY = "daily"
    MONTHLY = "monthly"


def convert_fg(fg: str | FileGranularity) -> FileGranularity:
    if isinstance(fg, str):
        return FileGranularity(fg)
    return fg


class StockRequest:
    headers = None

    def __init__(
        self,
        *,
        root: str,
        start_date: int,
        end_date: int,
        endpoint: str,
        interval: int = 3_600_000,
        force_refresh: bool = False,
        file_granularity: FileGranularity = FileGranularity.MONTHLY,
    ) -> None:
        """
        Args:
            root(str): Underlying symbol.
            start_date(int): Start data in integer format YYYYMMDD
            end_date(int): End date in integer format YYYYMMDD
            endpoint(str): API endpoint to hit, either 'quote' or 'eod'
            interval(int): Response interval in ms. Default is 3,600,000 corresponding to 1 hour.
            file_format(Formats): Format to use when writing to the lake. Default is 'parquet'
            file_granularity(FileGranularity): Granularity to concatenate response to.
        """
        val_start_date_before_end_date(start_date, end_date)
        val_interval(interval)
        self.root = root
        self.start_date = start_date
        self.end_date = end_date
        self.endpoint = endpoint
        self.interval = interval
        self.force_refresh = force_refresh
        self.file_granularity = convert_fg(file_granularity)
        self.id = uuid4()

    def _create_urls_per_day(self, days: List[DateParts]) -> List[str]:
        urls = []
        base_params = {
            "root": self.root,
            "exp": "0",
            "ivl": self.interval,
            "use_csv": "true",
        }
        base_url = f"{THETA_BASE_URL}/hist/stock/{self.endpoint}"

        if self.endpoint == "eod":
            base_params.pop("ivl")

        for d in days:
            # Create a string like YYYYMMDD
            date = str(d)
            params = base_params | {"start_date": date, "end_date": date}
            urls.append(f"{base_url}?{urlencode(params)}")

        return urls

    def get_key_map(self) -> Dict[str, List[str]]:
        int_str = (
            "1d" if self.endpoint == "eod" else interval_ms_to_string(self.interval)
        )
        base_key = f"historical-stock/{self.endpoint}/{self.file_granularity.value}/{int_str}/{self.root}"
        day_map = map_trading_days_to_yearmo(self.start_date, self.end_date)
        key_map: Dict[str, List[str]] = {}
        if self.file_granularity == FileGranularity.MONTHLY:
            key_map = {
                f"{base_key}/{k[0]}/{k[1]}/data.parquet": self._create_urls_per_day(v)
                for k, v in day_map.items()
            }
        elif self.file_granularity == FileGranularity.DAILY:
            key_map = {
                f"{base_key}/{d[0]}/{d[1]}/{d[2]}/data.parquet": self._create_urls_per_day(
                    d
                )
                for d in day_map.values()
            }

        return key_map


class OptionRequest:
    headers = None

    def __init__(
        self,
        *,
        root: str,
        start_date: int,
        end_date: int,
        endpoint: str,
        interval: int = 3_600_000,
        force_refresh: bool = False,
        file_granularity: str | FileGranularity = FileGranularity.MONTHLY,
    ) -> None:
        """
        Args:
            root(str): Underlying symbol.
            start_date(int): Start data in integer format YYYYMMDD
            end_date(int): End date in integer format YYYYMMDD
            endpoint(str): API endpoint to hit, either 'quote' or 'eod'
            interval(int): Response interval in ms. Default is 3,600,000 corresponding to 1 hour.
            file_format(Formats): Format to use when writing to the lake. Default is 'parquet'
            file_granularity(FileGranularity): Granularity to concatenate response to.
        """
        val_start_date_before_end_date(start_date, end_date)
        val_interval(interval)
        self.root = root
        self.start_date = start_date
        self.end_date = end_date
        self.endpoint = endpoint
        self.interval = interval
        self.force_refresh = force_refresh
        self.file_granularity = convert_fg(file_granularity)
        self.id = uuid4()

        self.stock_request = StockRequest(
            root=self.root,
            start_date=self.start_date,
            end_date=self.end_date,
            endpoint=self.endpoint,
            interval=self.interval,
            file_granularity=self.file_granularity,
        )

    def get_key_map(self) -> Dict[str, List[str]]:
        int_str = (
            "1d" if self.endpoint == "eod" else interval_ms_to_string(self.interval)
        )
        base_key = f"historical-options/{self.endpoint}/{self.file_granularity.value}/{int_str}/{self.root}"
        day_map = map_trading_days_to_yearmo(self.start_date, self.end_date)
        key_map: Dict[str, List[str]] = {}
        if self.file_granularity == FileGranularity.MONTHLY:
            key_map = {
                f"{base_key}/{k[0]}/{k[1]}/data.parquet": self._create_urls_per_day(v)
                for k, v in day_map.items()
            }
        elif self.file_granularity == FileGranularity.DAILY:
            key_map = {
                f"{base_key}/{d[0]}/{d[1]}/{d[2]}/data.parquet": self._create_urls_per_day(
                    d
                )
                for d in day_map.values()
            }

        return key_map

    def _create_urls_per_day(self, days: List[DateParts]) -> List[str]:
        # Request stock along with the options
        urls = self.stock_request._create_urls_per_day(days)
        base_params = {
            "root": self.root,
            "exp": "0",
            "ivl": self.interval,
            "use_csv": "true",
        }
        base_url = f"{THETA_BASE_URL}/bulk_hist/option/{self.endpoint}"

        if self.endpoint == "eod":
            base_params.pop("ivl")

        for d in days:
            # Create a string like YYYYMMDD
            date = str(d)
            params = base_params | {"start_date": date, "end_date": date}
            urls.append(f"{base_url}?{urlencode(params)}")

        return urls


class EarningsRequest:
    headers = {
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

    def __init__(
        self, *, start_yearmo: int, end_yearmo: int, force_refresh: bool = False
    ) -> None:
        """
        Args:
            start_yearmo(int): Start yearmo as an integer, like 202509.
            end_yearmo(int): End yearmo as an integer.
        """
        val_start_date_before_end_date(start_yearmo, end_yearmo)
        self.start_yearmo = start_yearmo
        self.end_yearmo = end_yearmo
        self.force_refresh = force_refresh
        self.id = uuid4()
        self.key_map: Dict[str, List[str]] = {}

    def _create_urls_per_day(self, days: List[DateParts]) -> List[str]:
        urls = []

        base_url = "https://api.nasdaq.com/api/calendar/earnings"

        for d in days:
            # Create a string like YYYY-MM-DD
            params = {"date": d.to_dash_format()}
            urls.append(f"{base_url}?{urlencode(params)}")

        return urls

    def get_key_map(self) -> Dict[str, List[str]]:
        base_key = "earnings"
        # Add a day to the yearmo to use the function
        day_map = map_trading_days_to_yearmo(
            self.start_yearmo * 100 + 1, self.end_yearmo * 100 + 1
        )
        return {
            f"{base_key}/{k[0]}/{k[1]}/data.parquet": self._create_urls_per_day(v)
            for k, v in day_map.items()
        }
