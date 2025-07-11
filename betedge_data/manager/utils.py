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


def generate_trading_date_list(start_date: str, end_date: str) -> List[int]:
    """
    Generate list of trading dates between start and end (inclusive).
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        List of trading dates as integers in YYYYMMDD format
    """
    # Parse dates
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    # Generate trading date list
    dates = []
    current = start
    while current <= end:
        if is_market_day(current):
            dates.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    
    return dates
