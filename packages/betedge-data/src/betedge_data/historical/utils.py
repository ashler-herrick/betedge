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


def expiration_to_string(exp: int) -> str:
    """
    Convert expiration to human-readable format.

    Args:
        exp: Expiration string (e.g., "0" or "20231117")

    Returns:
        Human-readable expiration ("all" for 0, otherwise unchanged)
    """
    return "all" if exp == 0 else str(exp)
