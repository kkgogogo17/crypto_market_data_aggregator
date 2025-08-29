"""Tests for Cloudflare R2 upload functionality"""

from datetime import datetime
from unittest.mock import patch, Mock
from botocore.exceptions import ClientError
import pytest

from src.parquet_storage import (
    ParquetStorage,
    save_crypto_data_to_parquet,
    upload_monthly_parquet_to_r2,
    batch_upload_monthly_to_r2,
)


class TestR2Upload:
    """Test Cloudflare R2 upload functionality"""

    def test_create_s3_client_success(self, mock_env_vars):
        """Test successful S3 client creation"""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client

            storage = ParquetStorage()
            client = storage.create_s3_client()

            assert client == mock_client
            mock_boto3.assert_called_once_with(
                "s3",
                endpoint_url="https://test.r2.cloudflarestorage.com",
                aws_access_key_id="test_access_key",
                aws_secret_access_key="test_secret_key",
                region_name="auto",
            )

    def test_create_s3_client_failure(self, mock_env_vars):
        """Test S3 client creation failure"""
        with patch("boto3.client") as mock_boto3:
            mock_boto3.side_effect = Exception("Connection failed")

            storage = ParquetStorage()
            client = storage.create_s3_client()

            assert client is None

    def test_upload_to_r2_success(self, mock_env_vars, temp_data_dir):
        """Test successful upload to R2"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create a test file
            test_file = temp_data_dir / "test_file.parquet"
            test_file.write_text("test content")

            with patch.object(storage, "create_s3_client") as mock_create_client:
                mock_client = Mock()
                mock_create_client.return_value = mock_client

                result = storage.upload_to_r2(str(test_file), "test/path/file.parquet")

                assert result["success"] is True
                assert result["message"] == f"Successfully uploaded {test_file} to R2"
                assert result["r2_key"] == "test/path/file.parquet"
                assert result["bucket"] == "test-crypto-bucket"

                # Verify upload_file was called
                mock_client.upload_file.assert_called_once_with(
                    str(test_file), "test-crypto-bucket", "test/path/file.parquet"
                )

    def test_upload_to_r2_client_creation_failure(self, mock_env_vars, temp_data_dir):
        """Test upload failure due to client creation error"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            with patch.object(storage, "create_s3_client") as mock_create_client:
                mock_create_client.return_value = (
                    None  # Simulate client creation failure
                )

                result = storage.upload_to_r2("/fake/path/file.parquet", "test/key")

                assert "error" in result
                assert result["error"] == "Failed to create R2 client"

    def test_upload_to_r2_upload_failure(self, mock_env_vars, temp_data_dir):
        """Test upload failure due to S3 client error"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            test_file = temp_data_dir / "test_file.parquet"
            test_file.write_text("test content")

            with patch.object(storage, "create_s3_client") as mock_create_client:
                mock_client = Mock()
                mock_client.upload_file.side_effect = ClientError(
                    error_response={
                        "Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}
                    },
                    operation_name="upload_file",
                )
                mock_create_client.return_value = mock_client

                result = storage.upload_to_r2(str(test_file), "test/path/file.parquet")

                assert "error" in result
                assert "Failed to upload to R2" in result["error"]
                assert "NoSuchBucket" in result["error"]

    def test_upload_to_r2_various_client_errors(self, mock_env_vars, temp_data_dir):
        """Test different types of client errors"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            test_file = temp_data_dir / "test_file.parquet"
            test_file.write_text("test content")

            # Test different error scenarios
            error_scenarios = [
                ("AccessDenied", "Access denied to bucket"),
                ("NoSuchKey", "Key does not exist"),
                ("InvalidRequest", "Invalid request parameters"),
            ]

            for error_code, error_message in error_scenarios:
                with patch.object(storage, "create_s3_client") as mock_create_client:
                    mock_client = Mock()
                    mock_client.upload_file.side_effect = ClientError(
                        error_response={
                            "Error": {"Code": error_code, "Message": error_message}
                        },
                        operation_name="upload_file",
                    )
                    mock_create_client.return_value = mock_client

                    result = storage.upload_to_r2(
                        str(test_file), "test/path/file.parquet"
                    )

                    assert "error" in result
                    assert "Failed to upload to R2" in result["error"]
                    assert error_code in result["error"]

    def test_upload_to_r2_with_different_bucket_names(self, temp_data_dir):
        """Test upload with different bucket configurations"""
        test_file = temp_data_dir / "test_file.parquet"
        test_file.write_text("test content")

        # Test default bucket name
        with patch.dict(
            "os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}, clear=True
        ):
            storage = ParquetStorage()
            assert storage.bucket_name == "crypto-data-tiingo"  # default

        # Test custom bucket name
        with patch.dict(
            "os.environ",
            {
                "LOCAL_DATA_DIR": str(temp_data_dir),
                "R2_BUCKET_NAME": "custom-bucket-name",
            },
        ):
            storage = ParquetStorage()
            assert storage.bucket_name == "custom-bucket-name"

    def test_upload_large_file_simulation(self, mock_env_vars, temp_data_dir):
        """Test upload behavior with large file simulation"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create a larger test file
            test_file = temp_data_dir / "large_file.parquet"
            test_file.write_text("x" * 10000)  # 10KB file

            with patch.object(storage, "create_s3_client") as mock_create_client:
                mock_client = Mock()
                mock_create_client.return_value = mock_client

                # Simulate slower upload (in real scenario)
                def slow_upload(*args, **kwargs):
                    pass

                mock_client.upload_file.side_effect = slow_upload

                result = storage.upload_to_r2(str(test_file), "test/large_file.parquet")

                assert result["success"] is True
                mock_client.upload_file.assert_called_once()


class TestR2Configuration:
    """Test R2 configuration and environment variable handling"""

    def test_missing_r2_credentials(self, temp_data_dir):
        """Test behavior when R2 credentials are missing"""
        with patch.dict(
            "os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}, clear=True
        ):
            storage = ParquetStorage()

            # Check that None values are set for missing env vars
            assert storage.r2_config["endpoint_url"] is None
            assert storage.r2_config["aws_access_key_id"] is None
            assert storage.r2_config["aws_secret_access_key"] is None
            assert storage.bucket_name == "crypto-data-tiingo"  # default value

    def test_partial_r2_credentials(self, temp_data_dir):
        """Test behavior with partial R2 credentials"""
        with patch.dict(
            "os.environ",
            {
                "LOCAL_DATA_DIR": str(temp_data_dir),
                "R2_ENDPOINT_URL": "https://test.r2.cloudflarestorage.com",
                # Missing access keys
            },
            clear=True,
        ):
            storage = ParquetStorage()

            assert (
                storage.r2_config["endpoint_url"]
                == "https://test.r2.cloudflarestorage.com"
            )
            assert storage.r2_config["aws_access_key_id"] is None
            assert storage.r2_config["aws_secret_access_key"] is None

    def test_r2_client_creation_with_missing_credentials(self, temp_data_dir):
        """Test S3 client creation fails gracefully with missing credentials"""
        with patch.dict(
            "os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}, clear=True
        ):
            storage = ParquetStorage()

            with patch("boto3.client") as mock_boto3:
                mock_boto3.side_effect = Exception("Missing credentials")

                client = storage.create_s3_client()

                assert client is None

    def test_upload_with_missing_credentials_fails_gracefully(self, temp_data_dir):
        """Test that upload fails gracefully when credentials are missing"""
        with patch.dict(
            "os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}, clear=True
        ):
            storage = ParquetStorage()

            # Create a temporary file for the upload test
            test_file = temp_data_dir / "test_file.parquet"
            test_file.write_text("test content")

            # Mock the create_s3_client to return None (simulating credential failure)
            with patch.object(storage, "create_s3_client") as mock_create_client:
                mock_create_client.return_value = None

                result = storage.upload_to_r2(str(test_file), "test/key")

                assert "error" in result
                assert result["error"] == "Failed to create R2 client"


class TestR2Integration:
    """Test integration scenarios for R2 uploads"""

    def test_end_to_end_upload_flow(
        self, mock_env_vars, temp_data_dir, sample_api_response
    ):
        """Test complete flow from data saving to R2 upload"""

        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            # Save data to parquet (this now uses monthly storage)
            save_crypto_data_to_parquet(sample_api_response, "BTCUSD", "tiingo")

            # Verify file was created in monthly structure
            expected_path = temp_data_dir / "BTCUSD" / "tiingo" / "2024" / "01" / "BTCUSD_tiingo_202401.parquet"
            assert expected_path.exists()

            # Upload to R2 using monthly upload function
            with patch("boto3.client") as mock_boto3:
                mock_client = Mock()
                mock_boto3.return_value = mock_client

                # Should not raise any exception
                upload_monthly_parquet_to_r2("BTCUSD", "tiingo", 2024, 1)

                # Verify upload was called
                mock_client.upload_file.assert_called_once()
                args = mock_client.upload_file.call_args[0]
                assert "BTCUSD_tiingo_202401.parquet" in args[0]
                assert args[1] == "test-crypto-bucket"  # bucket name
                assert args[2] == "BTCUSD/tiingo/2024/01/BTCUSD_tiingo_202401.parquet"  # R2 key

    def test_batch_upload_error_handling(self, mock_env_vars, temp_data_dir):
        """Test error handling in batch upload scenarios"""

        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create test files using monthly structure
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)
            storage.save_to_monthly_parquet(data, "ETHUSD", "coinbase", 2024, 1)

            with patch("boto3.client") as mock_boto3:
                mock_client = Mock()

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
                mock_boto3.return_value = mock_client

                # Mock datetime to make files appear old enough
                with patch("src.parquet_storage.datetime") as mock_datetime:
                    mock_datetime.now.return_value = datetime(2024, 3, 1)  # 2 months later

                    # Expect RuntimeError due to failed uploads
                    with pytest.raises(RuntimeError, match="Failed to upload"):
                        batch_upload_monthly_to_r2(months_old=1)

    def test_monthly_upload_function_success(self, mock_env_vars, temp_data_dir):
        """Test the monthly upload function specifically"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create a test monthly file
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)

            with patch("boto3.client") as mock_boto3:
                mock_client = Mock()
                mock_boto3.return_value = mock_client

                # Should not raise any exception
                upload_monthly_parquet_to_r2("BTCUSD", "tiingo", 2024, 1)

                # Verify correct parameters
                mock_client.upload_file.assert_called_once()
                args = mock_client.upload_file.call_args[0]
                assert "BTCUSD_tiingo_202401.parquet" in args[0]
                assert args[2] == "BTCUSD/tiingo/2024/01/BTCUSD_tiingo_202401.parquet"

    def test_monthly_upload_file_not_found(self, mock_env_vars, temp_data_dir):
        """Test monthly upload when file doesn't exist"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            with pytest.raises(FileNotFoundError):
                upload_monthly_parquet_to_r2("NONEXISTENT", "tiingo", 2024, 1)

    def test_batch_upload_success_scenario(self, mock_env_vars, temp_data_dir):
        """Test successful batch upload of multiple monthly files"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            storage = ParquetStorage()

            # Create multiple test files
            data = [{"date": "2024-01-15T00:00:00.000Z", "open": 45000}]
            storage.save_to_monthly_parquet(data, "BTCUSD", "tiingo", 2024, 1)
            storage.save_to_monthly_parquet(data, "ETHUSD", "coinbase", 2024, 1)

            with patch("boto3.client") as mock_boto3:
                mock_client = Mock()
                mock_boto3.return_value = mock_client

                # Mock datetime to make files appear old enough
                with patch("src.parquet_storage.datetime") as mock_datetime:
                    mock_datetime.now.return_value = datetime(2024, 3, 1)  # 2 months later

                    uploaded_count = batch_upload_monthly_to_r2(months_old=1)

                    assert uploaded_count == 2
                    assert mock_client.upload_file.call_count == 2

    def test_batch_upload_no_files(self, mock_env_vars, temp_data_dir):
        """Test batch upload when no files need uploading"""
        with patch.dict("os.environ", {"LOCAL_DATA_DIR": str(temp_data_dir)}):
            # Don't create any files

            uploaded_count = batch_upload_monthly_to_r2(months_old=1)

            assert uploaded_count == 0
