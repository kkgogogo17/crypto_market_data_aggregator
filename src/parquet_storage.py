import os
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
import time
from src.database import DataCollectionDB


class ParquetStorage:
    """Monthly aggregated Parquet storage with exchange support"""

    def __init__(self):
        self.local_data_dir = Path(os.environ.get("LOCAL_DATA_DIR", "./data"))

        # Cloudflare R2 configuration
        self.r2_config = {
            "endpoint_url": os.environ.get("R2_ENDPOINT_URL"),
            "aws_access_key_id": os.environ.get("R2_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.environ.get("R2_SECRET_ACCESS_KEY"),
            "region_name": "auto",
        }
        self.bucket_name = os.environ.get("R2_BUCKET_NAME", "crypto-data-tiingo")

    def create_s3_client(self):
        """Create S3 client for Cloudflare R2"""
        try:
            return boto3.client("s3", **self.r2_config)
        except Exception as e:
            print(f"Error creating R2 client: {e}")
            return None

    def get_monthly_file_path(
        self, ticker: str, exchange: str, year: int, month: int
    ) -> Path:
        """Generate monthly file path: data/ticker/exchange/YYYY/MM/ticker_exchange_YYYYMM.parquet"""
        ticker_dir = self.local_data_dir / ticker
        exchange_dir = ticker_dir / exchange
        year_dir = exchange_dir / str(year)
        month_dir = year_dir / f"{month:02d}"
        
        filename = f"{ticker}_{exchange}_{year}{month:02d}.parquet"
        return month_dir / filename

    def append_to_monthly_file(
        self, existing_file: Path, new_data: pd.DataFrame
    ) -> None:
        """Append new data to existing monthly file, handling deduplication and sorting"""
        if not existing_file.exists():
            raise FileNotFoundError(f"Existing file not found: {existing_file}")
        
        # Read existing data
        existing_data = pd.read_parquet(existing_file)
        
        # Combine data
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        
        # Handle deduplication based on timestamp (assuming timestamp column exists)
        if "timestamp" in combined_data.columns:
            # Remove duplicates based on timestamp, keeping last occurrence
            combined_data = combined_data.drop_duplicates(subset=["timestamp"], keep="last")
            # Sort by timestamp
            combined_data = combined_data.sort_values("timestamp").reset_index(drop=True)
        else:
            # If no timestamp column, just remove exact duplicates
            combined_data = combined_data.drop_duplicates().reset_index(drop=True)
        
        # Save back to file
        combined_data.to_parquet(existing_file, compression="snappy", index=False)

    def save_to_monthly_parquet(
        self, data: List[Dict], ticker: str, exchange: str, year: int, month: int
    ) -> str:
        """Save crypto data for a SINGLE month, append if exists. 
        For multi-month data, use save_multi_month_data() instead.
        Returns file path."""
        if not data:
            raise ValueError("No data to save")

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Ensure consistent schema: always use 'timestamp' column
        if "date" in df.columns:
            df["timestamp"] = pd.to_datetime(df["date"])
            df = df.drop("date", axis=1)
        elif "timestamp" not in df.columns:
            raise ValueError("Data must contain either 'date' or 'timestamp' column")

        # Add ticker and exchange columns
        df["ticker"] = ticker
        df["exchange"] = exchange
        
        # Get monthly file path
        monthly_file = self.get_monthly_file_path(ticker, exchange, year, month)
        
        if monthly_file.exists():
            # File exists - append to it
            self.append_to_monthly_file(monthly_file, df)
        else:
            # File doesn't exist - create new monthly file
            monthly_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(monthly_file, compression="snappy", index=False)

        return str(monthly_file)

    def get_r2_monthly_key(
        self, ticker: str, exchange: str, year: int, month: int
    ) -> str:
        """Generate R2 key for monthly file: ticker/exchange/YYYY/MM/ticker_exchange_YYYYMM.parquet"""
        return f"{ticker}/{exchange}/{year}/{month:02d}/{ticker}_{exchange}_{year}{month:02d}.parquet"

    def read_from_monthly_parquet(
        self, ticker: str, exchange: str, year: int, month: int
    ) -> pd.DataFrame:
        """Read crypto data from monthly parquet file. Returns DataFrame."""
        monthly_file = self.get_monthly_file_path(ticker, exchange, year, month)
        
        if not monthly_file.exists():
            raise FileNotFoundError(f"Monthly file not found: {monthly_file}")
        
        return pd.read_parquet(monthly_file)

    def group_data_by_month(self, data: List[Dict]) -> Dict[tuple, List[Dict]]:
        """Group data by (year, month) based on timestamp"""
        if not data:
            return {}
        
        grouped = {}
        for record in data:
            # Handle both 'date' and 'timestamp' fields
            timestamp_str = record.get('date') or record.get('timestamp')
            if not timestamp_str:
                raise ValueError("Data record must contain either 'date' or 'timestamp' field")
            
            # Parse timestamp to get year/month
            timestamp = pd.to_datetime(timestamp_str)
            
            year_month = (timestamp.year, timestamp.month)
            
            if year_month not in grouped:
                grouped[year_month] = []
            grouped[year_month].append(record)
        
        return grouped

    def save_multi_month_data(self, data: List[Dict], ticker: str, exchange: str) -> Dict[str, Any]:
        """Save data that may span multiple months and upload to R2"""
        try:
            grouped = self.group_data_by_month(data)
            
            if not grouped:
                raise ValueError("No valid data to save")
            
            db = DataCollectionDB()
            saved_files = []
            total_records = 0
            r2_failed = 0
            
            for (year, month), month_data in grouped.items():
                # Save local file
                file_path = self.save_to_monthly_parquet(month_data, ticker, exchange, year, month)
                file_size = Path(file_path).stat().st_size
                
                # Update database with local file info
                db.update_collection_progress(
                    ticker=ticker,
                    exchange=exchange,
                    year=year,
                    month=month,
                    last_date=datetime.now().date(),
                    records_count=len(month_data),
                    file_info={
                        "local_file_path": file_path,
                        "r2_key": self.get_r2_monthly_key(ticker, exchange, year, month),
                        "file_size_bytes": file_size
                    }
                )
                
                # Upload to R2 with retry
                r2_result = self.upload_to_r2_with_retry(ticker, exchange, year, month)
                if not r2_result.get("success"):
                    r2_failed += 1
                
                saved_files.append({
                    "file_path": file_path,
                    "year": year,
                    "month": month,
                    "records": len(month_data)
                })
                total_records += len(month_data)
            
            message = f"Successfully saved {total_records} records across {len(saved_files)} monthly files"
            if r2_failed > 0:
                message += f" ({r2_failed} R2 uploads failed, will retry later)"
            
            return {
                "success": True,
                "message": message,
                "total_records": total_records,
                "months_saved": len(saved_files),
                "files": saved_files,
                "ticker": ticker,
                "exchange": exchange,
            }
            
        except Exception as e:
            return {"error": f"Failed to save multi-month data: {str(e)}"}

    def upload_to_r2(self, local_file_path: str, r2_key: str) -> Dict[str, Any]:
        """Upload local file to Cloudflare R2"""

        s3_client = self.create_s3_client()
        if not s3_client:
            return {"error": "Failed to create R2 client"}

        try:
            s3_client.upload_file(local_file_path, self.bucket_name, r2_key)

            return {
                "success": True,
                "message": f"Successfully uploaded {local_file_path} to R2",
                "r2_key": r2_key,
                "bucket": self.bucket_name,
            }

        except ClientError as e:
            return {"error": f"Failed to upload to R2: {str(e)}"}

    def upload_to_r2_with_retry(
        self, 
        ticker: str,
        exchange: str,
        year: int, 
        month: int,
        max_attempts: int = 3
    ) -> Dict[str, Any]:
        """Upload monthly file to R2 with retry logic and database tracking"""

        db = DataCollectionDB()
        
        # Get file path and R2 key
        monthly_file = self.get_monthly_file_path(ticker, exchange, year, month)
        if not monthly_file.exists():
            error_msg = f"Monthly file not found: {monthly_file}"
            db.update_r2_upload_status(ticker, exchange, year, month, False, error_msg)
            return {"error": error_msg}
        
        r2_key = self.get_r2_monthly_key(ticker, exchange, year, month)
        
        # Retry with exponential backoff: 0s, 30s, 5min
        backoff_delays = [0, 30, 300]
        
        for attempt in range(max_attempts):
            try:
                # Add delay for retry attempts
                if attempt > 0:
                    print(f"Retrying R2 upload (attempt {attempt + 1}/{max_attempts}) after {backoff_delays[attempt]}s delay...")
                    time.sleep(backoff_delays[attempt])
                
                # Attempt upload
                result = self.upload_to_r2(str(monthly_file), r2_key)
                
                if result.get("success"):
                    # Success - update database
                    db.update_r2_upload_status(ticker, exchange, year, month, True)
                    result["attempts"] = attempt + 1
                    return result
                else:
                    # Failed attempt
                    error_msg = result.get("error", "Unknown upload error")
                    print(f"R2 upload attempt {attempt + 1} failed: {error_msg}")
                    
                    if attempt == max_attempts - 1:
                        # Final attempt failed
                        db.update_r2_upload_status(ticker, exchange, year, month, False, error_msg)
                        return {
                            "error": f"R2 upload failed after {max_attempts} attempts: {error_msg}",
                            "attempts": max_attempts
                        }
                    else:
                        # Record failed attempt but continue retrying
                        db.update_r2_upload_status(ticker, exchange, year, month, False, error_msg)
                        
            except Exception as e:
                error_msg = f"Upload exception: {str(e)}"
                print(f"R2 upload attempt {attempt + 1} exception: {error_msg}")
                
                if attempt == max_attempts - 1:
                    # Final attempt failed
                    db.update_r2_upload_status(ticker, exchange, year, month, False, error_msg)
                    return {
                        "error": f"R2 upload failed after {max_attempts} attempts: {error_msg}",
                        "attempts": max_attempts
                    }
                else:
                    # Record failed attempt but continue retrying
                    db.update_r2_upload_status(ticker, exchange, year, month, False, error_msg)
        
        # Should not reach here
        return {"error": "Unexpected error in retry logic"}

    def list_local_files(self, pattern: str = "*.parquet") -> List[str]:
        """List all local parquet files"""
        return [str(p) for p in self.local_data_dir.rglob(pattern)]

    def get_monthly_files_for_upload(self, months_old: int = 0) -> List[Dict[str, str]]:
        """Get monthly files that should be uploaded to R2"""
        files_to_upload = []
        
        for file_path in self.list_local_files():
            path_obj = Path(file_path)
            
            # Parse monthly file path: data/ticker/exchange/YYYY/MM/ticker_exchange_YYYYMM.parquet
            try:
                parts = path_obj.parts
                if len(parts) >= 5 and path_obj.name.endswith('.parquet'):
                    # Extract from path: [...]/ticker/exchange/YYYY/MM/filename.parquet
                    ticker = parts[-5]
                    exchange = parts[-4] 
                    year = int(parts[-3])
                    month = int(parts[-2])
                    
                    # Check if file is old enough (compare months)
                    current_date = datetime.now()
                    file_months_old = (current_date.year - year) * 12 + (current_date.month - month)
                    
                    if file_months_old >= months_old:
                        # Generate R2 key using monthly structure
                        r2_key = self.get_r2_monthly_key(ticker, exchange, year, month)
                        
                        files_to_upload.append({
                            "local_path": str(path_obj),
                            "r2_key": r2_key,
                            "ticker": ticker,
                            "exchange": exchange,
                            "year": year,
                            "month": month,
                        })
            except (ValueError, IndexError):
                continue
                
        return files_to_upload


def save_crypto_data_to_parquet(
    api_result: Dict[str, Any], ticker: str, exchange: str = "tiingo"
) -> None:
    """Save crypto data to monthly parquet file. Auto-handles multi-month data."""
    if "error" in api_result:
        raise ValueError(f"API result contains error: {api_result['error']}")

    # Extract price data from API response
    if not (api_result and isinstance(api_result, list) and len(api_result) > 0):
        raise ValueError("Invalid API response format")
        
    price_data = api_result[0].get("priceData", [])
    if not price_data:
        raise ValueError("No price data found in API response")

    storage = ParquetStorage()
    storage.save_multi_month_data(price_data, ticker, exchange)



def upload_monthly_parquet_to_r2(
    ticker: str, exchange: str, year: int, month: int
) -> None:
    """Upload monthly parquet file to R2"""
    storage = ParquetStorage()
    
    # Get file path
    monthly_file = storage.get_monthly_file_path(ticker, exchange, year, month)
    if not monthly_file.exists():
        raise FileNotFoundError(f"Monthly file not found: {monthly_file}")
    
    # Generate R2 key and upload
    r2_key = storage.get_r2_monthly_key(ticker, exchange, year, month)
    result = storage.upload_to_r2(str(monthly_file), r2_key)
    
    if "error" in result:
        raise RuntimeError(f"Failed to upload to R2: {result['error']}")


def batch_upload_monthly_to_r2(months_old: int = 1) -> int:
    """Upload monthly files older than specified months to R2. Returns count of uploaded files."""
    storage = ParquetStorage()
    db = DataCollectionDB()

    # Get files needing upload from database (respects is_active status)
    files_needing_upload = db.get_files_needing_r2_upload()

    # Filter by age if needed
    files_to_upload = []
    current_date = datetime.now()

    for file_info in files_needing_upload:
        file_months_old = (current_date.year - file_info["year"]) * 12 + (current_date.month - file_info["month"])

        if file_months_old >= months_old:
            files_to_upload.append(file_info)

    if not files_to_upload:
        return 0

    uploaded_count = 0
    failed_files = []

    for file_info in files_to_upload:
        try:
            result = storage.upload_to_r2(file_info["local_file_path"], file_info["r2_key"])
            if result.get("success"):
                # Update database on successful upload
                db.update_r2_upload_status(
                    file_info["ticker"],
                    file_info["exchange"],
                    file_info["year"],
                    file_info["month"],
                    True
                )
                uploaded_count += 1
            else:
                # Update database on failed upload
                error_msg = result.get("error", "Unknown error")
                db.update_r2_upload_status(
                    file_info["ticker"],
                    file_info["exchange"],
                    file_info["year"],
                    file_info["month"],
                    False,
                    error_msg
                )
                failed_files.append(f"{file_info['local_file_path']}: {error_msg}")
        except Exception as e:
            # Update database on exception
            db.update_r2_upload_status(
                file_info["ticker"],
                file_info["exchange"],
                file_info["year"],
                file_info["month"],
                False,
                str(e)
            )
            failed_files.append(f"{file_info['local_file_path']}: {str(e)}")

    if failed_files:
        raise RuntimeError(f"Failed to upload {len(failed_files)} files: {'; '.join(failed_files)}")

    return uploaded_count
