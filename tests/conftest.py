"""Pytest configuration and fixtures"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock
import os


@pytest.fixture(autouse=True)
def mock_external_services():
    """Automatically mock external services for all tests"""
    # Mock boto3 to prevent real R2 connections
    with patch("boto3.client") as mock_boto3:
        mock_s3_client = Mock()
        mock_boto3.return_value = mock_s3_client

        # Mock DataCollectionDB to prevent real database operations
        with patch("src.parquet_storage.DataCollectionDB") as mock_db_class:
            mock_db = Mock()
            mock_db_class.return_value = mock_db

            # Mock database init function to prevent file creation
            with patch("src.database.init_db") as mock_init_db:
                mock_init_db.return_value = None

                # Mock sqlite3.connect to prevent any database connections
                with patch("sqlite3.connect") as mock_sqlite_connect:
                    mock_conn = Mock()
                    mock_cursor = Mock()
                    mock_conn.cursor.return_value = mock_cursor
                    mock_sqlite_connect.return_value = mock_conn

                    # Mock the R2 upload methods to prevent actual uploads
                    with patch("src.parquet_storage.ParquetStorage.upload_to_r2_with_retry") as mock_upload:
                        mock_upload.return_value = {"success": True, "message": "Mocked upload"}

                        yield {
                            "boto3_client": mock_s3_client,
                            "db": mock_db,
                            "upload_retry": mock_upload,
                            "sqlite_connect": mock_sqlite_connect,
                            "init_db": mock_init_db
                        }


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing"""
    env_vars = {
        "TIINGO_TOKEN": "test_token_12345",
        "LOCAL_DATA_DIR": "./test-data",  # Use test-data directory
        "R2_ENDPOINT_URL": "https://test.r2.cloudflarestorage.com",
        "R2_ACCESS_KEY_ID": "test_access_key",
        "R2_SECRET_ACCESS_KEY": "test_secret_key",
        "R2_BUCKET_NAME": "test-crypto-bucket",
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture(autouse=True, scope="session")
def cleanup_test_directories():
    """Clean up test directories and database files before and after all tests"""
    # Clean up before tests start
    test_data_dir = Path("./test-data")
    test_db_file = Path("crypto_data.db")

    if test_data_dir.exists():
        print(f"Pre-cleanup: removing existing test-data directory")
        try:
            shutil.rmtree(test_data_dir)
            print("Pre-cleanup: removed test-data directory")
        except Exception as e:
            print(f"Pre-cleanup failed: {e}")

    if test_db_file.exists():
        print(f"Pre-cleanup: removing existing crypto_data.db")
        try:
            test_db_file.unlink()
            print("Pre-cleanup: removed crypto_data.db")
        except Exception as e:
            print(f"Pre-cleanup failed: {e}")

    yield
    
    # Clean up after all tests complete
    if test_data_dir.exists():
        print(f"Post-cleanup: found test-data directory")
        try:
            shutil.rmtree(test_data_dir)
            print("Post-cleanup: removed test-data directory")
        except Exception as e:
            print(f"Post-cleanup failed: {e}")
    else:
        print(f"Post-cleanup: no test-data directory found")

    if test_db_file.exists():
        print(f"Post-cleanup: found crypto_data.db")
        try:
            test_db_file.unlink()
            print("Post-cleanup: removed crypto_data.db")
        except Exception as e:
            print(f"Post-cleanup failed: {e}")
    else:
        print(f"Post-cleanup: no crypto_data.db found")


@pytest.fixture
def sample_api_response():
    """Sample Tiingo API response for testing"""
    return [
        {
            "ticker": "BTCUSD",
            "baseCurrency": "BTC",
            "quoteCurrency": "USD",
            "priceData": [
                {
                    "date": "2024-01-01T00:00:00.000Z",
                    "open": 45000.5,
                    "high": 45100.25,
                    "low": 44950.75,
                    "close": 45050.0,
                    "volume": 100.5,
                    "volumeNotional": 4520000.0,
                    "tradesDone": 150,
                },
                {
                    "date": "2024-01-01T00:01:00.000Z",
                    "open": 45050.0,
                    "high": 45075.0,
                    "low": 45025.0,
                    "close": 45060.0,
                    "volume": 50.25,
                    "volumeNotional": 2263000.0,
                    "tradesDone": 75,
                },
            ],
        }
    ]


@pytest.fixture
def empty_api_response():
    """Empty API response for testing edge cases"""
    return [
        {
            "ticker": "BTCUSD",
            "baseCurrency": "BTC",
            "quoteCurrency": "USD",
            "priceData": [],
        }
    ]


@pytest.fixture
def error_api_response():
    """Error API response for testing error handling"""
    return {"error": "API request failed: 404 Not Found", "status_code": 404}
