from typing import Dict, Any
from src.collector import fetch_and_save_crypto_data, fetch_crypto_data_endpoint, fetch_historical_range
from src.database import DataCollectionDB
from datetime import datetime, timedelta


def initialize_ticker_data(
    ticker: str,
    exchange: str
) -> Dict[str, Any]:
    """
    Initialize complete historical data collection for a ticker
    Uses early start date without end date to get all available data
    
    Args:
        ticker: Crypto ticker symbol (e.g., 'BTCUSD')
        exchange: Exchange name (e.g., 'tiingo')
        
    Returns:
        Dict containing collection results and statistics
    """
    db = DataCollectionDB()
    
    # Add to monitored assets
    db.add_monitored_asset(ticker, exchange)
    
    # Use very early date, no end date - Tiingo will fetch all available data
    start_date = "2012-01-01"
    
    print(f"Initializing complete historical data for {ticker} from {exchange}")
    print(f"Fetching from {start_date} (all available data)")
    
    try:
        # Fetch all available historical data
        result = fetch_and_save_crypto_data(
            ticker=ticker,
            exchange=exchange,
            start_date=start_date
        )
        
        if result["storage_result"].get("success"):
            # Log successful collection
            db.log_collection_run(
                ticker=ticker,
                exchange=exchange,
                start_date=datetime.strptime(start_date, "%Y-%m-%d").date(),
                end_date=datetime.now().date(),
                status='completed',
                records_fetched=result["storage_result"].get("total_records", 0)
            )
            
        else:
            # Log failed collection
            db.log_collection_run(
                ticker=ticker,
                exchange=exchange,
                start_date=datetime.strptime(start_date, "%Y-%m-%d").date(),
                end_date=datetime.now().date(),
                status='failed',
                error_message=result["storage_result"].get("error")
            )
            
        return result
        
    except Exception as e:
        error_msg = f"Initialization failed: {str(e)}"
        db.log_collection_run(
            ticker=ticker,
            exchange=exchange,
            start_date=datetime.strptime(start_date, "%Y-%m-%d").date(),
            end_date=datetime.now().date(),
            status='failed',
            error_message=error_msg
        )
        return {"error": error_msg}


def collect_historical_data(
    ticker: str,
    exchange: str, 
    months_back: int = 12
) -> Dict[str, Any]:
    """
    Collect specific months of historical crypto data (for incremental updates)
    
    Args:
        ticker: Crypto ticker symbol (e.g., 'BTCUSD')
        exchange: Exchange name (e.g., 'tiingo')
        months_back: Number of months of historical data to collect
        
    Returns:
        Dict containing collection results and statistics
    """
    db = DataCollectionDB()
    
    # Add to monitored assets
    db.add_monitored_asset(ticker, exchange)
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = (datetime.now() - timedelta(days=months_back * 30)).date()
    
    print(f"Collecting {months_back} months of {ticker} data from {exchange}")
    print(f"Date range: {start_date} to {end_date}")
    
    try:
        # Fetch historical data
        result = fetch_historical_range(
            ticker=ticker,
            exchange=exchange,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        )
        
        if result["storage_result"].get("success"):
            # Log successful collection
            db.log_collection_run(
                ticker=ticker,
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                status='completed',
                records_fetched=result["storage_result"].get("total_records", 0)
            )
            
        else:
            # Log failed collection
            db.log_collection_run(
                ticker=ticker,
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                status='failed',
                error_message=result["storage_result"].get("error")
            )
            
        return result
        
    except Exception as e:
        error_msg = f"Collection failed: {str(e)}"
        db.log_collection_run(
            ticker=ticker,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            status='failed',
            error_message=error_msg
        )
        return {"error": error_msg}


def main():
    print("Crypto Market Data Aggregator - Historical Data Collection")

    # Example 1: Initialize complete historical data for BTCUSD
    print("\n=== Initializing Complete BTCUSD Data ===")
    result = initialize_ticker_data("BTCUSD", "tiingo")
    print("Initialization Result:", result.get("storage_result", {}).get("message"))

    # Example 2: Test single date fetch
    print("\n=== Single Date Test ===")
    single_result = fetch_and_save_crypto_data("ETHUSD", "tiingo", specific_date="2024-01-01")
    print("Single Date Result:", single_result["storage_result"])

    # Example 3: Test validation
    print("\n=== Validation Test ===")
    invalid_result = fetch_crypto_data_endpoint("BTCUSD", specific_date="01-01-2024")
    print("Invalid Date Result:", invalid_result)


if __name__ == "__main__":
    main()
