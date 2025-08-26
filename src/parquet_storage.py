import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
import boto3
from botocore.exceptions import ClientError
from pathlib import Path


class ParquetStorage:
    """Local Parquet storage and Cloudflare R2 operations"""

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

    def save_to_parquet(
        self, data: List[Dict], ticker: str, date_str: str
    ) -> Dict[str, Any]:
        """Save crypto data to local Parquet file organized by date and ticker"""

        if not data:
            return {"error": "No data to save"}

        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Ensure proper datetime conversion
            if "date" in df.columns:
                df["timestamp"] = pd.to_datetime(df["date"])
                df = df.drop("date", axis=1)

            # Add ticker column
            df["ticker"] = ticker

            # Organize by date: data/YYYY/MM/DD/ticker_YYYYMMDD.parquet
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year_dir = self.local_data_dir / str(date_obj.year)
            month_dir = year_dir / f"{date_obj.month:02d}"
            day_dir = month_dir / f"{date_obj.day:02d}"

            # Create directories (including parent local_data_dir)
            day_dir.mkdir(parents=True, exist_ok=True)

            # File path
            filename = f"{ticker}_{date_obj.strftime('%Y%m%d')}.parquet"
            file_path = day_dir / filename

            # Save to parquet
            df.to_parquet(file_path, compression="snappy", index=False)

            return {
                "success": True,
                "message": f"Successfully saved {len(data)} records for {ticker}",
                "records_count": len(data),
                "file_path": str(file_path),
                "file_size": file_path.stat().st_size,
            }

        except Exception as e:
            return {"error": f"Failed to save parquet file: {str(e)}"}

    def read_from_parquet(self, ticker: str, date_str: str) -> Dict[str, Any]:
        """Read crypto data from local Parquet file"""

        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            filename = f"{ticker}_{date_obj.strftime('%Y%m%d')}.parquet"

            file_path = (
                self.local_data_dir
                / str(date_obj.year)
                / f"{date_obj.month:02d}"
                / f"{date_obj.day:02d}"
                / filename
            )

            if not file_path.exists():
                return {"error": f"File not found: {file_path}"}

            df = pd.read_parquet(file_path)

            return {
                "success": True,
                "data": df.to_dict("records"),
                "count": len(df),
                "file_path": str(file_path),
            }

        except Exception as e:
            return {"error": f"Failed to read parquet file: {str(e)}"}

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

    def list_local_files(self, pattern: str = "*.parquet") -> List[str]:
        """List all local parquet files"""
        return [str(p) for p in self.local_data_dir.rglob(pattern)]

    def get_files_for_upload(self, days_old: int = 0) -> List[Dict[str, str]]:
        """Get files that should be uploaded to R2 (older than specified days)"""
        files_to_upload = []

        for file_path in self.list_local_files():
            path_obj = Path(file_path)

            # Extract date info from path structure: data/YYYY/MM/DD/ticker_YYYYMMDD.parquet
            try:
                parts = path_obj.parts
                if len(parts) >= 4:
                    year, month, day = parts[-4], parts[-3], parts[-2]
                    file_date = datetime(int(year), int(month), int(day))

                    # Check if file is old enough
                    days_diff = (datetime.now() - file_date).days
                    if days_diff >= days_old:
                        # Create R2 key: crypto-data/YYYY/MM/DD/ticker_YYYYMMDD.parquet
                        r2_key = f"crypto-data/{year}/{month}/{day}/{path_obj.name}"

                        files_to_upload.append(
                            {
                                "local_path": str(path_obj),
                                "r2_key": r2_key,
                                "file_date": file_date.strftime("%Y-%m-%d"),
                            }
                        )
            except (ValueError, IndexError):
                continue

        return files_to_upload


def save_crypto_data_to_parquet(
    api_result: Dict[str, Any], ticker: str, date_str: str
) -> Dict[str, Any]:
    """Save crypto data to Parquet file"""

    if "error" in api_result:
        return api_result

    storage = ParquetStorage()

    try:
        # Extract price data from API response
        if api_result and isinstance(api_result, list) and len(api_result) > 0:
            price_data = api_result[0].get("priceData", [])

            if price_data:
                return storage.save_to_parquet(price_data, ticker, date_str)
            else:
                return {"error": "No price data found in API response"}
        else:
            return {"error": "Invalid API response format"}

    except Exception as e:
        return {"error": f"Failed to save data: {str(e)}"}


def read_crypto_data_from_parquet(ticker: str, date_str: str) -> Dict[str, Any]:
    """Read crypto data from Parquet file"""

    storage = ParquetStorage()
    return storage.read_from_parquet(ticker, date_str)


def upload_parquet_to_r2(
    local_file_path: str, r2_key: Optional[str] = None
) -> Dict[str, Any]:
    """Upload a specific parquet file to Cloudflare R2"""

    storage = ParquetStorage()

    if not r2_key:
        # Generate R2 key from file path
        path_obj = Path(local_file_path)
        try:
            parts = path_obj.parts
            if len(parts) >= 4:
                year, month, day = parts[-4], parts[-3], parts[-2]
                r2_key = f"crypto-data/{year}/{month}/{day}/{path_obj.name}"
            else:
                r2_key = f"crypto-data/{path_obj.name}"
        except (ValueError, IndexError):
            r2_key = f"crypto-data/{path_obj.name}"

    return storage.upload_to_r2(local_file_path, r2_key)


def batch_upload_to_r2(days_old: int = 1) -> Dict[str, Any]:
    """Upload files older than specified days to Cloudflare R2 (for cron job use)"""

    storage = ParquetStorage()
    files_to_upload = storage.get_files_for_upload(days_old)

    if not files_to_upload:
        return {
            "success": True,
            "message": f"No files older than {days_old} days found for upload",
            "uploaded_count": 0,
        }

    uploaded_files = []
    failed_files = []

    for file_info in files_to_upload:
        result = storage.upload_to_r2(file_info["local_path"], file_info["r2_key"])

        if result.get("success"):
            uploaded_files.append(file_info)
        else:
            failed_files.append({**file_info, "error": result.get("error")})

    return {
        "success": True,
        "message": "Batch upload completed",
        "uploaded_count": len(uploaded_files),
        "failed_count": len(failed_files),
        "uploaded_files": uploaded_files,
        "failed_files": failed_files,
    }
