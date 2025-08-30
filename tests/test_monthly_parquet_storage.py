import pandas as pd
from unittest.mock import patch, MagicMock, Mock
from pathlib import Path
from datetime import datetime
import pytest
from botocore.exceptions import ClientError

from src.parquet_storage import (
    ParquetStorage,
    save_crypto_data_to_parquet,
    upload_monthly_parquet_to_r2,
    batch_upload_monthly_to_r2,
)


class TestMonthlyParquetStorage:
    """Test the ParquetStorage class with monthly storage"""

    def test_init_sets_local_data_dir_path(self, temp_data_dir):
        """Test that initialization sets the local data directory path"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()
            assert storage.local_data_dir == temp_data_dir

    def test_get_monthly_file_path(self, temp_data_dir):
        """Test monthly file path generation"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()
            
            path = storage.get_monthly_file_path("BTCUSD", "tiingo", 2024, 1)
            
            expected = temp_data_dir / "BTCUSD" / "tiingo" / "2024" / "01" / "BTCUSD_tiingo_202401.parquet"
            assert path == expected

    def test_save_to_monthly_parquet_new_file(self, temp_data_dir):
        """Test saving data to new monthly file"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            data = [
                {
                    "date": "2024-01-01T00:00:00.000Z",
                    "open": 45000.5,
                    "close": 45050.0,
                },
                {
                    "date": "2024-01-01T00:01:00.000Z", 
                    "open": 45050.0,
                    "close": 45060.0,
                },
            ]

            file_path = storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)
            
            # Verify file was created
            path = Path(file_path)
            assert path.exists()
            assert path.name == "BTCUSD_tiingo_202401.parquet"

            # Verify data integrity
            df = pd.read_parquet(path)
            assert len(df) == 2
            assert "timestamp" in df.columns
            assert "date" not in df.columns
            assert "ticker" in df.columns
            assert "exchange" in df.columns
            assert df["ticker"].iloc[0] == "BTCUSD"
            assert df["exchange"].iloc[0] == "tiingo"

    def test_save_to_monthly_parquet_empty_data(self, temp_data_dir):
        """Test error handling for empty data"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            with pytest.raises(ValueError, match="No data to save"):
                storage.save_to_monthly_parquet([], "BTCUSD", "tiingo", 2024, 1)

    def test_save_to_monthly_parquet_missing_timestamp(self, temp_data_dir):
        """Test error handling for data without date or timestamp"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            data = [{"open": 45000.5, "close": 45050.0}]

            with pytest.raises(ValueError, match="Data must contain either 'date' or 'timestamp' column"):
                storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)

    def test_append_to_monthly_file(self, temp_data_dir):
        """Test appending data to existing monthly file"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create initial file
            initial_data = [
                {
                    "date": "2024-01-01T00:00:00.000Z",
                    "open": 45000.5,
                    "close": 45050.0,
                }
            ]
            file_path = storage.save_to_monthly_parquet(initial_data, "BTCUSD", "tiingo", 2024, 1)

            # Append new data
            new_data = [
                {
                    "date": "2024-01-01T00:02:00.000Z",
                    "open": 45060.0,
                    "close": 45070.0,
                }
            ]
            storage.save_to_monthly_parquet(new_data, "BTCUSD", "tiingo", 2024, 1)

            # Verify combined data
            df = pd.read_parquet(file_path)
            assert len(df) == 2
            assert df.iloc[0]["close"] == 45050.0
            assert df.iloc[1]["close"] == 45070.0

    def test_append_to_monthly_file_with_duplicates(self, temp_data_dir):
        """Test appending data with duplicate timestamps"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create initial file
            initial_data = [
                {
                    "date": "2024-01-01T00:00:00.000Z",
                    "open": 45000.5,
                    "close": 45050.0,
                }
            ]
            file_path = storage.save_to_monthly_parquet(initial_data, "BTCUSD", "tiingo", 2024, 1)

            # Append data with same timestamp (should replace)
            new_data = [
                {
                    "date": "2024-01-01T00:00:00.000Z",
                    "open": 45000.5,
                    "close": 45055.0,  # Different close price
                }
            ]
            storage.save_to_monthly_parquet(new_data, "BTCUSD", "tiingo", 2024, 1)

            # Verify deduplication (should keep latest)
            df = pd.read_parquet(file_path)
            assert len(df) == 1
            assert df.iloc[0]["close"] == 45055.0

    def test_append_to_monthly_file_not_found(self, temp_data_dir):
        """Test error handling when trying to append to non-existent file"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            fake_path = Path(temp_data_dir) / "nonexistent.parquet"
            df = pd.DataFrame([{"timestamp": "2024-01-01", "value": 100}])

            with pytest.raises(FileNotFoundError):
                storage.append_to_monthly_file(fake_path, df)

    def test_read_from_monthly_parquet_success(self, temp_data_dir):
        """Test reading from monthly parquet file"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Save data first
            data = [
                {
                    "date": "2024-01-01T00:00:00.000Z",
                    "open": 45000.5,
                    "close": 45050.0,
                }
            ]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)

            # Read it back
            df = storage.read_from_monthly_parquet("BTCUSD", "tiingo", 2024, 1)

            assert len(df) == 1
            assert df.iloc[0]["open"] == 45000.5
            assert df.iloc[0]["close"] == 45050.0
            assert df.iloc[0]["ticker"] == "BTCUSD"
            assert df.iloc[0]["exchange"] == "tiingo"

    def test_read_from_monthly_parquet_not_found(self, temp_data_dir):
        """Test reading from non-existent monthly file"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            with pytest.raises(FileNotFoundError):
                storage.read_from_monthly_parquet("NONEXISTENT", "tiingo", 2024, 1)

    def test_group_data_by_month(self, temp_data_dir):
        """Test grouping data by month"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            data = [
                {"date": "2024-01-15T00:00:00.000Z", "value": 100},
                {"date": "2024-01-20T00:00:00.000Z", "value": 200},
                {"date": "2024-02-05T00:00:00.000Z", "value": 300},
                {"date": "2024-02-10T00:00:00.000Z", "value": 400},
            ]

            grouped = storage.group_data_by_month(data)

            assert len(grouped) == 2
            assert (2024, 1) in grouped
            assert (2024, 2) in grouped
            assert len(grouped[(2024, 1)]) == 2
            assert len(grouped[(2024, 2)]) == 2

    def test_group_data_by_month_with_timestamp(self, temp_data_dir):
        """Test grouping data that uses 'timestamp' field instead of 'date'"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            data = [
                {"timestamp": "2024-01-15T00:00:00.000Z", "value": 100},
                {"timestamp": "2024-02-05T00:00:00.000Z", "value": 200},
            ]

            grouped = storage.group_data_by_month(data)

            assert len(grouped) == 2
            assert (2024, 1) in grouped
            assert (2024, 2) in grouped

    def test_group_data_by_month_empty_data(self, temp_data_dir):
        """Test grouping empty data"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            grouped = storage.group_data_by_month([])
            assert grouped == {}

    def test_group_data_by_month_missing_timestamp(self, temp_data_dir):
        """Test error handling for data without timestamp"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            data = [{"value": 100}]  # No date or timestamp field

            with pytest.raises(ValueError, match="Data record must contain either 'date' or 'timestamp' field"):
                storage.group_data_by_month(data)

    def test_save_multi_month_data_success(self, temp_data_dir, mock_external_services):
        """Test saving data spanning multiple months"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            data = [
                {"date": "2024-01-15T00:00:00.000Z", "open": 45000, "close": 45100},
                {"date": "2024-01-20T00:00:00.000Z", "open": 45100, "close": 45200},
                {"date": "2024-02-05T00:00:00.000Z", "open": 45200, "close": 45300},
                {"date": "2024-02-10T00:00:00.000Z", "open": 45300, "close": 45400},
            ]

            result = storage.save_multi_month_data(data, "BTCUSD", "tiingo")

            assert result["success"] is True
            assert result["total_records"] == 4
            assert result["months_saved"] == 2
            assert len(result["files"]) == 2
            assert result["ticker"] == "BTCUSD"
            assert result["exchange"] == "tiingo"

            # Verify files were created
            jan_file = temp_data_dir / "BTCUSD" / "tiingo" / "2024" / "01" / "BTCUSD_tiingo_202401.parquet"
            feb_file = temp_data_dir / "BTCUSD" / "tiingo" / "2024" / "02" / "BTCUSD_tiingo_202402.parquet"

            assert jan_file.exists()
            assert feb_file.exists()

            # Verify data integrity
            jan_df = pd.read_parquet(jan_file)
            feb_df = pd.read_parquet(feb_file)

            assert len(jan_df) == 2
            assert len(feb_df) == 2

    def test_save_multi_month_data_empty(self, temp_data_dir):
        """Test saving empty multi-month data"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            result = storage.save_multi_month_data([], "BTCUSD", "tiingo")

            assert "error" in result
            assert "No valid data to save" in result["error"]

    def test_get_r2_monthly_key(self, temp_data_dir):
        """Test R2 monthly key generation"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            key = storage.get_r2_monthly_key("BTCUSD", "tiingo", 2024, 1)

            assert key == "BTCUSD/tiingo/2024/01/BTCUSD_tiingo_202401.parquet"

    def test_get_monthly_files_for_upload(self, temp_data_dir):
        """Test getting monthly files for upload"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create test file
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)

            # Mock current date to make files appear old enough
            with patch("src.parquet_storage.datetime") as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 3, 1)  # 2 months later

                files = storage.get_monthly_files_for_upload(months_old=1)

                assert len(files) == 1

                # Check structure of returned data
                btc_file = files[0]
                assert btc_file["ticker"] == "BTCUSD"
                assert btc_file["exchange"] == "tiingo"
                assert btc_file["year"] == 2024
                assert btc_file["month"] == 1
                assert "BTCUSD_tiingo_202401.parquet" in btc_file["r2_key"]

    def test_save_crypto_data_to_parquet_success(self, sample_api_response, temp_data_dir):
        """Test successful saving of API response to parquet"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            # Should not raise any exception
            save_crypto_data_to_parquet(sample_api_response, "BTCUSD", "tiingo")
            
            # Verify file was created
            expected_path = temp_data_dir / "BTCUSD" / "tiingo" / "2024" / "01" / "BTCUSD_tiingo_202401.parquet"
            assert expected_path.exists()

    def test_save_crypto_data_to_parquet_error_response(self, error_api_response):
        """Test handling of error API response"""
        with pytest.raises(ValueError, match="API result contains error"):
            save_crypto_data_to_parquet(error_api_response, "BTCUSD", "tiingo")

    def test_save_crypto_data_to_parquet_empty_price_data(self, empty_api_response):
        """Test handling of API response with empty price data"""
        with pytest.raises(ValueError, match="No price data found"):
            save_crypto_data_to_parquet(empty_api_response, "BTCUSD", "tiingo")

    def test_save_crypto_data_to_parquet_invalid_format(self):
        """Test handling of invalid API response format"""
        invalid_response = {"invalid": "format"}

        with pytest.raises(ValueError, match="Invalid API response format"):
            save_crypto_data_to_parquet(invalid_response, "BTCUSD", "tiingo")

    def test_upload_monthly_parquet_to_r2_success(self, temp_data_dir, mock_external_services):
        """Test successful upload of monthly parquet to R2"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            # Create a test file
            storage = ParquetStorage()
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)

            # Should not raise any exception
            upload_monthly_parquet_to_r2("BTCUSD", "tiingo", 2024, 1)

            # Verify upload was called with correct parameters
            mock_client = mock_external_services["boto3_client"]
            mock_client.upload_file.assert_called_once()
            args = mock_client.upload_file.call_args[0]
            assert "BTCUSD_tiingo_202401.parquet" in args[0]  # local file path
            assert args[1] == "crypto-data-tiingo"  # bucket name (default)
            assert args[2] == "BTCUSD/tiingo/2024/01/BTCUSD_tiingo_202401.parquet"  # R2 key

    def test_upload_monthly_parquet_to_r2_file_not_found(self, temp_data_dir):
        """Test upload when file doesn't exist"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            with pytest.raises(FileNotFoundError):
                upload_monthly_parquet_to_r2("NONEXISTENT", "tiingo", 2024, 1)

    def test_upload_monthly_parquet_to_r2_upload_failed(self, temp_data_dir, mock_external_services):
        """Test upload when R2 upload fails"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            # Create a test file
            storage = ParquetStorage()
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)

            # Configure mock to simulate upload failure
            mock_client = mock_external_services["boto3_client"]
            mock_client.upload_file.side_effect = ClientError(
                error_response={
                    "Error": {
                        "Code": "ServiceUnavailable",
                        "Message": "Service down",
                    }
                },
                operation_name="upload_file",
            )

            with pytest.raises(RuntimeError, match="Failed to upload to R2"):
                upload_monthly_parquet_to_r2("BTCUSD", "tiingo", 2024, 1)

    def test_batch_upload_monthly_to_r2_success(self, temp_data_dir, mock_external_services):
        """Test successful batch upload"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create actual test files that will be found by get_monthly_files_for_upload
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)
            storage.save_to_monthly_parquet(data, "ETHUSD", "coinbase", 2024, 1)

            # Mock datetime to make files appear old enough
            with patch("src.parquet_storage.datetime") as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 3, 1)  # 2 months later

                uploaded_count = batch_upload_monthly_to_r2(months_old=1)

                assert uploaded_count == 2
                assert mock_external_services["boto3_client"].upload_file.call_count == 2

    def test_batch_upload_monthly_to_r2_no_files(self, temp_data_dir):
        """Test batch upload with no files"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            # Don't create any files

            uploaded_count = batch_upload_monthly_to_r2(months_old=1)

            assert uploaded_count == 0

    def test_batch_upload_monthly_to_r2_with_failures(self, temp_data_dir, mock_external_services):
        """Test batch upload with some failures"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create actual test files
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)
            storage.save_to_monthly_parquet(data, "ETHUSD", "coinbase", 2024, 1)

            # Configure mock with mixed success/failure
            mock_client = mock_external_services["boto3_client"]
            # First upload succeeds, second fails
            mock_client.upload_file.side_effect = [
                None,  # Success
                ClientError(
                    error_response={
                        "Error": {
                            "Code": "ServiceUnavailable",
                            "Message": "Service down",
                        }
                    },
                    operation_name="upload_file",
                ),
            ]

            # Mock datetime to make files appear old enough
            with patch("src.parquet_storage.datetime") as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 3, 1)  # 2 months later

                with pytest.raises(RuntimeError, match="Failed to upload 1 files"):
                    batch_upload_monthly_to_r2(months_old=1)
