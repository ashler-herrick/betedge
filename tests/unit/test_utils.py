"""
Unit tests for manager models module.
"""

from datetime import datetime
import pytest

from betedge_data.manager.utils import generate_date_list


@pytest.mark.unit
class TestUtilityFunctions:
    """Test utility functions in models module."""

    def test_generate_date_list_single_day(self):
        """Test generating date list for a single day."""
        dates = generate_date_list("20231117", "20231117")
        assert dates == [20231117]

    def test_generate_date_list_multiple_days(self):
        """Test generating date list for multiple days."""
        dates = generate_date_list("20231113", "20231117")
        expected = [20231113, 20231114, 20231115, 20231116, 20231117]
        assert dates == expected

    def test_generate_date_list_across_months(self):
        """Test generating date list across month boundary."""
        dates = generate_date_list("20231130", "20231202")
        expected = [20231130, 20231201, 20231202]
        assert dates == expected

    def test_generate_date_list_across_years(self):
        """Test generating date list across year boundary."""
        dates = generate_date_list("20231230", "20240102")
        expected = [20231230, 20231231, 20240101, 20240102]
        assert dates == expected

    def test_generate_date_list_leap_year(self):
        """Test generating date list including leap day."""
        dates = generate_date_list("20240228", "20240301")
        expected = [20240228, 20240229, 20240301]  # 2024 is a leap year
        assert dates == expected

    def test_generate_date_list_returns_integers(self):
        """Test that generate_date_list returns integers in YYYYMMDD format."""
        dates = generate_date_list("20231113", "20231115")

        # Verify return type is list of integers
        assert isinstance(dates, list)
        assert all(isinstance(date, int) for date in dates)

        # Verify YYYYMMDD format correctness
        expected = [20231113, 20231114, 20231115]
        assert dates == expected

        # Verify each date is properly formatted as YYYYMMDD integer
        for date in dates:
            # Convert back to string to verify format
            date_str = str(date)
            assert len(date_str) == 8, f"Date {date} should be 8 digits"

            # Verify it's a valid date by parsing
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])

            assert 2000 <= year <= 2100, f"Year {year} should be reasonable"
            assert 1 <= month <= 12, f"Month {month} should be 1-12"
            assert 1 <= day <= 31, f"Day {day} should be 1-31"

            # Verify it can be parsed as a valid date
            try:
                datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                pytest.fail(f"Date {date} is not a valid YYYYMMDD format")
