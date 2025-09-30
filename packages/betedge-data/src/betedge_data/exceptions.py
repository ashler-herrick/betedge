"""Custom exceptions for the betedge_data package."""


class NoDataAvailableError(Exception):
    """Raised when ThetaData API returns no data for the specified timeframe."""

    pass
