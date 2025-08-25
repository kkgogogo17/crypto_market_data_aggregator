"""Pytest configuration and fixtures"""
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
import os


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
        'TIINGO_TOKEN': 'test_token_12345',
        'LOCAL_DATA_DIR': './test-data',  # Use test-data directory
        'R2_ENDPOINT_URL': 'https://test.r2.cloudflarestorage.com',
        'R2_ACCESS_KEY_ID': 'test_access_key',
        'R2_SECRET_ACCESS_KEY': 'test_secret_key',
        'R2_BUCKET_NAME': 'test-crypto-bucket'
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture(autouse=True, scope="session")
def cleanup_test_directories():
    """Clean up test directories after all tests complete"""
    yield
    # Clean up test-data directory after all tests
    import shutil
    from pathlib import Path
    
    test_data_dir = Path('./test-data')
    if test_data_dir.exists():
        shutil.rmtree(test_data_dir)
        print("Cleaned up test-data directory")


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
                    "tradesDone": 150
                },
                {
                    "date": "2024-01-01T00:01:00.000Z",
                    "open": 45050.0,
                    "high": 45075.0,
                    "low": 45025.0,
                    "close": 45060.0,
                    "volume": 50.25,
                    "volumeNotional": 2263000.0,
                    "tradesDone": 75
                }
            ]
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
            "priceData": []
        }
    ]


@pytest.fixture
def error_api_response():
    """Error API response for testing error handling"""
    return {
        "error": "API request failed: 404 Not Found",
        "status_code": 404
    }