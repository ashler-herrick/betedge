from datetime import datetime, timedelta
from typing import List, Tuple


def generate_date_list(start_date: str, end_date: str) -> List[int]:
    """
    Generate list of dates between start and end (inclusive).

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        List of dates as integers in YYYYMMDD format
    """
    # Parse dates
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    # Generate date list
    dates = []
    current = start
    while current <= end:
        dates.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)

    return dates


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
