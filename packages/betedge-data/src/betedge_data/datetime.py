from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Tuple


@dataclass
class DateParts:
    year: str
    month: str
    day: str

    def __str__(self) -> str:
        """Return date in YYYYMMDD format"""
        return f"{self.year}{self.month}{self.day}"

    def to_dash_format(self) -> str:
        """Return date in YYYY-MM-DD format"""
        return f"{self.year}-{self.month}-{self.day}"

    @classmethod
    def from_datetime(cls, dt: datetime) -> "DateParts":
        """Create DateParts from datetime object"""
        return cls(year=f"{dt.year:04d}", month=f"{dt.month:02d}", day=f"{dt.day:02d}")

    @classmethod
    def from_int(cls, date_int: int) -> "DateParts":
        """Create DateParts from YYYYMMDD integer"""
        dt = datetime.strptime(str(date_int), "%Y%m%d")
        return cls.from_datetime(dt)


def map_trading_days_to_yearmo(
    start_date: int, end_date: int
) -> Dict[Tuple[str, str], List[DateParts]]:
    """
    Map trading days in a date range to year-month combinations.

    Args:
        start_date: Start date as integer (YYYYMMDD format)
        end_date: End date as integer (YYYYMMDD format)

    Returns:
        Dictionary with (YYYY, MM) string tuples as keys and lists of DateParts as values
    """
    start_dt = datetime.strptime(str(start_date), "%Y%m%d")
    end_dt = datetime.strptime(str(end_date), "%Y%m%d")

    yearmo_dict = defaultdict(list)
    current_dt = start_dt

    while current_dt <= end_dt:
        if is_market_day(current_dt):
            date_parts = DateParts.from_datetime(current_dt)
            yearmo_key = (date_parts.year, date_parts.month)
            yearmo_dict[yearmo_key].append(date_parts)
        current_dt += timedelta(days=1)

    return dict(yearmo_dict)


def generate_month_list(start_month: str, end_month: str) -> List[Tuple[int, int]]:
    """
    Generate list of (year, month) tuples between start and end months (inclusive).

    Args:
        start_month: Start month in YYYYMM format
        end_month: End month in YYYYMM format

    Returns:
        List of (year, month) tuples
    """
    # Parse months
    start = datetime.strptime(start_month, "%Y%m")
    end = datetime.strptime(end_month, "%Y%m")

    # Generate month list
    months = []
    current = start
    while current <= end:
        months.append((current.year, current.month))

        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def is_weekend(date: datetime) -> bool:
    """
    Check if a date falls on a weekend (Saturday or Sunday).

    Args:
        date: datetime object to check

    Returns:
        True if the date is Saturday or Sunday, False otherwise
    """
    return date.weekday() >= 5  # 5=Saturday, 6=Sunday


def get_us_market_holidays(year: int) -> List[datetime]:
    """
    Get list of US market holidays for a given year.

    Args:
        year: Year to get holidays for

    Returns:
        List of datetime objects representing market holidays
    """
    holidays = []

    # New Year's Day (January 1)
    holidays.append(datetime(year, 1, 1))

    # Martin Luther King Jr. Day (3rd Monday in January)
    jan_1 = datetime(year, 1, 1)
    days_to_first_monday = (7 - jan_1.weekday()) % 7
    first_monday = jan_1 + timedelta(days=days_to_first_monday)
    mlk_day = first_monday + timedelta(days=14)  # 3rd Monday
    holidays.append(mlk_day)

    # Presidents Day (3rd Monday in February)
    feb_1 = datetime(year, 2, 1)
    days_to_first_monday = (7 - feb_1.weekday()) % 7
    first_monday = feb_1 + timedelta(days=days_to_first_monday)
    presidents_day = first_monday + timedelta(days=14)  # 3rd Monday
    holidays.append(presidents_day)

    # Memorial Day (last Monday in May)
    may_31 = datetime(year, 5, 31)
    days_back_to_monday = (may_31.weekday() - 0) % 7
    memorial_day = may_31 - timedelta(days=days_back_to_monday)
    holidays.append(memorial_day)

    # Independence Day (July 4)
    independence_day = datetime(year, 7, 4)
    # If it falls on weekend, observed on Friday or Monday
    if independence_day.weekday() == 5:  # Saturday
        holidays.append(independence_day - timedelta(days=1))  # Friday
    elif independence_day.weekday() == 6:  # Sunday
        holidays.append(independence_day + timedelta(days=1))  # Monday
    else:
        holidays.append(independence_day)

    # Labor Day (1st Monday in September)
    sep_1 = datetime(year, 9, 1)
    days_to_first_monday = (7 - sep_1.weekday()) % 7
    labor_day = sep_1 + timedelta(days=days_to_first_monday)
    holidays.append(labor_day)

    # Thanksgiving (4th Thursday in November)
    nov_1 = datetime(year, 11, 1)
    days_to_first_thursday = (3 - nov_1.weekday()) % 7
    first_thursday = nov_1 + timedelta(days=days_to_first_thursday)
    thanksgiving = first_thursday + timedelta(days=21)  # 4th Thursday
    holidays.append(thanksgiving)

    # Christmas (December 25)
    christmas = datetime(year, 12, 25)
    # If it falls on weekend, observed on Friday or Monday
    if christmas.weekday() == 5:  # Saturday
        holidays.append(christmas - timedelta(days=1))  # Friday
    elif christmas.weekday() == 6:  # Sunday
        holidays.append(christmas + timedelta(days=1))  # Monday
    else:
        holidays.append(christmas)

    return holidays


def is_market_day(date: datetime) -> bool:
    """
    Check if a date is a trading day (not weekend or holiday).

    Args:
        date: datetime object to check

    Returns:
        True if the date is a trading day, False otherwise
    """
    if is_weekend(date):
        return False

    holidays = get_us_market_holidays(date.year)
    return date.date() not in [h.date() for h in holidays]


def interval_ms_to_string(interval_ms: int) -> str:
    """
    Convert interval in milliseconds to human-readable format.

    Args:
        interval_ms: Interval in milliseconds

    Returns:
        Human-readable interval string (e.g., "15m", "1h", "1d")
    """
    if interval_ms == 0:
        return "tick"
    elif interval_ms < 60000:  # Less than 1 minute
        return f"{interval_ms // 1000}s"
    elif interval_ms < 3600000:  # Less than 1 hour
        minutes = interval_ms // 60000
        return f"{minutes}m"
    elif interval_ms < 86400000:  # Less than 1 day
        hours = interval_ms // 3600000
        return f"{hours}h"
    else:
        days = interval_ms // 86400000
        return f"{days}d"
