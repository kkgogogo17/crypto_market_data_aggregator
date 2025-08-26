import os
from typing import Final, Optional, Dict, Any
from datetime import datetime
import requests

from src.parquet_storage import save_crypto_data_to_parquet


def get_crypto_historical_data(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    specific_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch historical crypto data from Tiingo API with 1-minute resampling.

    Args:
        ticker: Crypto ticker symbol (e.g., 'BTCUSD')
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        specific_date: Single date in 'YYYY-MM-DD' format (overrides date range)

    Returns:
        Dict containing the API response with historical data
    """
    TIINGO_TOKEN: Final[str] = os.environ.get("TIINGO_TOKEN")

    if not TIINGO_TOKEN:
        raise ValueError("TIINGO_TOKEN environment variable is not set")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {TIINGO_TOKEN}",
    }

    # Build the API endpoint
    base_url = "https://api.tiingo.com/tiingo/crypto/prices"

    # Prepare query parameters
    params = {"tickers": ticker, "resampleFreq": "1Min"}

    # Handle date parameters
    if specific_date:
        params["startDate"] = specific_date
    else:
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date

    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        return {
            "error": f"API request failed: {str(e)}",
            "status_code": getattr(e.response, "status_code", None)
            if hasattr(e, "response")
            else None,
        }


def validate_date_format(date_str: str) -> bool:
    """
    Validate if date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def fetch_crypto_data_endpoint(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    specific_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Main endpoint function for fetching crypto historical data.
    Includes validation and error handling.

    Args:
        ticker: Crypto ticker symbol (e.g., 'BTCUSD')
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        specific_date: Single date in 'YYYY-MM-DD' format

    Returns:
        Dict containing the data or error information
    """
    # Validate inputs
    if not ticker:
        return {"error": "Ticker symbol is required"}

    # Validate date formats
    dates_to_validate = []
    if specific_date:
        dates_to_validate.append(specific_date)
    else:
        if start_date:
            dates_to_validate.append(start_date)
        if end_date:
            dates_to_validate.append(end_date)

    for date_str in dates_to_validate:
        if not validate_date_format(date_str):
            return {"error": f"Invalid date format: {date_str}. Use YYYY-MM-DD format"}

    # Validate date range logic
    if start_date and end_date and not specific_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        if start > end:
            return {"error": "Start date must be before or equal to end date"}

    # Fetch the data
    return get_crypto_historical_data(ticker, start_date, end_date, specific_date)


def fetch_and_save_crypto_data(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    specific_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch crypto data from API and save to Parquet file"""

    # Fetch data from API
    api_result = fetch_crypto_data_endpoint(ticker, start_date, end_date, specific_date)

    # Only proceed with save if API call was successful
    if "error" in api_result:
        return {
            "api_result": api_result,
            "storage_result": {"error": "API call failed, save operation skipped"},
        }

    # Determine the date for file organization
    if specific_date:
        file_date = specific_date
    elif start_date:
        file_date = start_date
    else:
        file_date = datetime.now().strftime("%Y-%m-%d")

    # Save to parquet
    storage_result = save_crypto_data_to_parquet(api_result, ticker, file_date)

    return {"api_result": api_result, "storage_result": storage_result}
