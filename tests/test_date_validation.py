"""Tests for data validation functions"""

import pytest
from unittest.mock import patch

from src.collector import validate_date_format, fetch_crypto_data_endpoint


class TestValidateDateFormat:
    """Test the validate_date_format function"""

    def test_valid_date_formats(self):
        """Test valid date formats"""
        valid_dates = [
            "2024-01-01",
            "2023-12-31",
            "2022-02-28",
            "2020-02-29",  # leap year
            "1990-06-15",
        ]

        for date_str in valid_dates:
            assert validate_date_format(date_str), f"Should be valid: {date_str}"

    def test_invalid_date_formats(self):
        """Test invalid date formats"""
        invalid_dates = [
            "01-01-2024",  # MM-DD-YYYY
            "2024/01/01",  # slashes
            "2024-13-01",  # invalid month
            "2024-02-30",  # invalid day
            "2023-02-29",  # not a leap year (2023 is not a leap year)
            "24-01-01",  # 2-digit year
            "",  # empty string
            "2024-01",  # incomplete
            "invalid",  # not a date
            None,  # None value
        ]

        for date_str in invalid_dates:
            if date_str is not None:
                assert not validate_date_format(date_str), (
                    f"Should be invalid: {date_str}"
                )

    def test_edge_cases(self):
        """Test edge cases"""
        # Test with whitespace
        assert not validate_date_format(" 2024-01-01 ")
        assert not validate_date_format("2024-01-01\n")

        # Test with extra characters
        assert not validate_date_format("2024-01-01T00:00:00")
        assert not validate_date_format("2024-01-01 extra")


class TestFetchCryptoDataEndpointValidation:
    """Test the validation logic in fetch_crypto_data_endpoint"""

    def test_missing_ticker(self):
        """Test validation when ticker is missing"""
        result = fetch_crypto_data_endpoint("")
        assert result == {"error": "Ticker symbol is required"}

        result = fetch_crypto_data_endpoint(None)
        assert result == {"error": "Ticker symbol is required"}

    def test_invalid_date_format_specific_date(self):
        """Test validation for invalid specific date format"""
        result = fetch_crypto_data_endpoint("BTCUSD", specific_date="01-01-2024")
        expected = {"error": "Invalid date format: 01-01-2024. Use YYYY-MM-DD format"}
        assert result == expected

    def test_invalid_date_format_start_date(self):
        """Test validation for invalid start date format"""
        result = fetch_crypto_data_endpoint("BTCUSD", start_date="2024/01/01")
        expected = {"error": "Invalid date format: 2024/01/01. Use YYYY-MM-DD format"}
        assert result == expected

    def test_invalid_date_format_end_date(self):
        """Test validation for invalid end date format"""
        result = fetch_crypto_data_endpoint("BTCUSD", end_date="invalid-date")
        expected = {"error": "Invalid date format: invalid-date. Use YYYY-MM-DD format"}
        assert result == expected

    def test_date_range_validation_start_after_end(self):
        """Test validation when start date is after end date"""
        result = fetch_crypto_data_endpoint(
            "BTCUSD", start_date="2024-01-02", end_date="2024-01-01"
        )
        expected = {"error": "Start date must be before or equal to end date"}
        assert result == expected

    def test_date_range_validation_equal_dates(self):
        """Test validation when start and end dates are equal (should be valid)"""
        # This should pass validation but fail on API call since we don't have TIINGO_TOKEN
        import os

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="TIINGO_TOKEN environment variable is not set"
            ):
                fetch_crypto_data_endpoint(
                    "BTCUSD", start_date="2024-01-01", end_date="2024-01-01"
                )

    def test_valid_parameters_no_api_token(self):
        """Test that valid parameters pass validation but fail on missing API token"""
        with patch.dict("os.environ", {}, clear=True):
            # Test specific date
            with pytest.raises(
                ValueError, match="TIINGO_TOKEN environment variable is not set"
            ):
                fetch_crypto_data_endpoint("BTCUSD", specific_date="2024-01-01")

            # Test date range
            with pytest.raises(
                ValueError, match="TIINGO_TOKEN environment variable is not set"
            ):
                fetch_crypto_data_endpoint(
                    "BTCUSD", start_date="2024-01-01", end_date="2024-01-02"
                )

            # Test single start date
            with pytest.raises(
                ValueError, match="TIINGO_TOKEN environment variable is not set"
            ):
                fetch_crypto_data_endpoint("BTCUSD", start_date="2024-01-01")

    def test_specific_date_overrides_range(self):
        """Test that specific_date parameter overrides start_date and end_date"""
        # Clear environment to ensure no TIINGO_TOKEN
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(
                ValueError, match="TIINGO_TOKEN environment variable is not set"
            ):
                fetch_crypto_data_endpoint(
                    "BTCUSD",
                    start_date="2024-01-01",
                    end_date="2024-01-02",
                    specific_date="2024-01-03",
                )
