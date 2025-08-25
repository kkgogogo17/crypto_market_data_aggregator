"""Tests for API data fetching functionality"""
import pytest
import responses
import requests
from unittest.mock import patch, Mock

from src.main import (
    get_crypto_historical_data,
    fetch_crypto_data_endpoint,
    fetch_and_save_crypto_data
)


class TestGetCryptoHistoricalData:
    """Test the core API fetching function"""
    
    def test_missing_tiingo_token(self):
        """Test that function raises error when TIINGO_TOKEN is not set"""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="TIINGO_TOKEN environment variable is not set"):
                get_crypto_historical_data("BTCUSD")
    
    @responses.activate
    def test_successful_api_call_specific_date(self, mock_env_vars, sample_api_response):
        """Test successful API call with specific date"""
        # Mock the API response
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=sample_api_response,
            status=200
        )
        
        result = get_crypto_historical_data("BTCUSD", specific_date="2024-01-01")
        
        assert result == sample_api_response
        assert len(responses.calls) == 1
        
        # Verify request parameters
        request = responses.calls[0].request
        assert "tickers=BTCUSD" in request.url
        assert "resampleFreq=1Min" in request.url
        assert "startDate=2024-01-01" in request.url
        
        # Verify headers
        assert request.headers['Authorization'] == 'Token test_token_12345'
        assert request.headers['Content-Type'] == 'application/json'
    
    @responses.activate
    def test_successful_api_call_date_range(self, mock_env_vars, sample_api_response):
        """Test successful API call with date range"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=sample_api_response,
            status=200
        )
        
        result = get_crypto_historical_data("BTCUSD", start_date="2024-01-01", end_date="2024-01-02")
        
        assert result == sample_api_response
        
        # Verify request parameters
        request = responses.calls[0].request
        assert "startDate=2024-01-01" in request.url
        assert "endDate=2024-01-02" in request.url
    
    @responses.activate
    def test_successful_api_call_only_start_date(self, mock_env_vars, sample_api_response):
        """Test successful API call with only start date"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=sample_api_response,
            status=200
        )
        
        result = get_crypto_historical_data("BTCUSD", start_date="2024-01-01")
        
        assert result == sample_api_response
        
        # Verify request parameters
        request = responses.calls[0].request
        assert "startDate=2024-01-01" in request.url
        assert "endDate" not in request.url
    
    @responses.activate
    def test_api_error_404(self, mock_env_vars):
        """Test handling of 404 API error"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json={"error": "Ticker not found"},
            status=404
        )
        
        result = get_crypto_historical_data("INVALIDTICKER")
        
        assert 'error' in result
        assert 'API request failed' in result['error']
        assert result['status_code'] == 404
    
    @responses.activate
    def test_api_error_500(self, mock_env_vars):
        """Test handling of 500 API error"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json={"error": "Internal server error"},
            status=500
        )
        
        result = get_crypto_historical_data("BTCUSD", specific_date="2024-01-01")
        
        assert 'error' in result
        assert 'API request failed' in result['error']
        assert result['status_code'] == 500
    
    @responses.activate
    def test_network_error(self, mock_env_vars):
        """Test handling of network errors"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            body=requests.exceptions.ConnectionError("Network error")
        )
        
        result = get_crypto_historical_data("BTCUSD", specific_date="2024-01-01")
        
        assert 'error' in result
        assert 'API request failed' in result['error']
        assert result.get('status_code') is None
    
    @responses.activate
    def test_timeout_error(self, mock_env_vars):
        """Test handling of timeout errors"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            body=requests.exceptions.Timeout("Request timeout")
        )
        
        result = get_crypto_historical_data("BTCUSD", specific_date="2024-01-01")
        
        assert 'error' in result
        assert 'API request failed' in result['error']
    
    @responses.activate
    def test_empty_response(self, mock_env_vars):
        """Test handling of empty API response"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=[],
            status=200
        )
        
        result = get_crypto_historical_data("BTCUSD", specific_date="2024-01-01")
        
        assert result == []


class TestFetchCryptoDataEndpoint:
    """Test the main endpoint function with validation and API calls"""
    
    @responses.activate
    def test_successful_fetch_with_validation(self, mock_env_vars, sample_api_response):
        """Test successful fetch with all validation passed"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=sample_api_response,
            status=200
        )
        
        result = fetch_crypto_data_endpoint("BTCUSD", specific_date="2024-01-01")
        
        assert result == sample_api_response
    
    def test_validation_error_prevents_api_call(self):
        """Test that validation errors prevent API calls"""
        # This should return validation error without making API call
        result = fetch_crypto_data_endpoint("BTCUSD", specific_date="invalid-date")
        
        expected = {'error': 'Invalid date format: invalid-date. Use YYYY-MM-DD format'}
        assert result == expected
        
        # Verify no API call was made
        assert len(responses.calls) == 0


class TestFetchAndSaveCryptoData:
    """Test the combined fetch and save functionality"""
    
    @responses.activate
    @patch('src.main.save_crypto_data_to_parquet')
    def test_successful_fetch_and_save(self, mock_save, mock_env_vars, sample_api_response):
        """Test successful fetch and save operation"""
        # Mock API response
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=sample_api_response,
            status=200
        )
        
        # Mock save function
        mock_save.return_value = {
            'success': True,
            'records_count': 2,
            'file_path': '/test/path/BTCUSD_20240101.parquet'
        }
        
        result = fetch_and_save_crypto_data("BTCUSD", specific_date="2024-01-01")
        
        assert 'api_result' in result
        assert 'storage_result' in result
        assert result['api_result'] == sample_api_response
        assert result['storage_result']['success'] is True
        
        # Verify save function was called with correct parameters
        mock_save.assert_called_once_with(sample_api_response, "BTCUSD", "2024-01-01")
    
    @patch('src.main.save_crypto_data_to_parquet')
    def test_api_error_prevents_save(self, mock_save):
        """Test that API errors prevent save operations"""
        result = fetch_and_save_crypto_data("BTCUSD", specific_date="invalid-date")
        
        # Should return validation error in api_result
        assert 'api_result' in result
        assert 'error' in result['api_result']
        
        # Should have storage_result indicating save was skipped
        assert 'storage_result' in result
        assert 'error' in result['storage_result']
        assert 'save operation skipped' in result['storage_result']['error']
        
        # Save should not be called
        mock_save.assert_not_called()
    
    @responses.activate
    @patch('src.main.save_crypto_data_to_parquet')
    def test_save_error_handling(self, mock_save, mock_env_vars, sample_api_response):
        """Test handling of save errors"""
        # Mock successful API response
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=sample_api_response,
            status=200
        )
        
        # Mock save function to return error
        mock_save.return_value = {
            'error': 'Failed to save parquet file: Permission denied'
        }
        
        result = fetch_and_save_crypto_data("BTCUSD", specific_date="2024-01-01")
        
        assert result['api_result'] == sample_api_response
        assert 'error' in result['storage_result']
        assert 'Permission denied' in result['storage_result']['error']
    
    @responses.activate
    @patch('src.main.save_crypto_data_to_parquet')
    def test_date_determination_logic(self, mock_save, mock_env_vars, sample_api_response):
        """Test the logic for determining file date"""
        responses.add(
            responses.GET,
            "https://api.tiingo.com/tiingo/crypto/prices",
            json=sample_api_response,
            status=200
        )
        
        mock_save.return_value = {'success': True}
        
        # Test specific_date takes priority
        fetch_and_save_crypto_data("BTCUSD",
                                  start_date="2024-01-01",
                                  end_date="2024-01-02",
                                  specific_date="2024-01-03")
        mock_save.assert_called_with(sample_api_response, "BTCUSD", "2024-01-03")
        
        # Test start_date used when no specific_date
        mock_save.reset_mock()
        fetch_and_save_crypto_data("BTCUSD",
                                  start_date="2024-01-01",
                                  end_date="2024-01-02")
        mock_save.assert_called_with(sample_api_response, "BTCUSD", "2024-01-01")
        
        # Test current date used when no dates provided
        mock_save.reset_mock()
        with patch('src.main.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "2024-01-15"
            fetch_and_save_crypto_data("BTCUSD")
            mock_save.assert_called_with(sample_api_response, "BTCUSD", "2024-01-15")