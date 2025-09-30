def val_start_date_before_end_date(start_date: int, end_date: int) -> None:
    """Check that end_date >= start_start and raise an insightful error if not."""
    if end_date < start_date:
        raise ValueError("end_date must be later than start date.")


def val_interval(interval: int) -> None:
    if interval not in [3_600_000, 60_000]:
        raise UserWarning(
            "Intervals not equal to 3_600_000 or 60_000 have degraded performance due to the ThetaData API."
        )
