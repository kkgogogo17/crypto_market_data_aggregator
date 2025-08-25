import pandas as pd
from unittest.mock import patch
from pathlib import Path
from datetime import datetime
from tests.conftest import temp_data_dir


from src.parquet_storage import (
    ParquetStorage, 
    save_crypto_data_to_parquet,
    read_crypto_data_from_parquet,
    upload_parquet_to_r2,
    batch_upload_to_r2
)


class TestParquetStorage:
    """Test the ParquetStorage class"""
    
    def test_init_sets_local_data_dir_path(self, temp_data_dir):
        """Test that initialization sets the local data directory path"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            assert storage.local_data_dir == temp_data_dir
    
    def test_init_with_default_data_dir(self):
        """Test initialization with default data directory"""
        with patch.dict('os.environ', {}, clear=True):
            storage = ParquetStorage()
            assert str(storage.local_data_dir) == 'data'  # Path constructor converts './data' to 'data'
    
    def test_init_sets_r2_config(self):
        """Test that R2 configuration is set from environment variables"""
        env_vars = {
            'R2_ENDPOINT_URL': 'https://test.r2.cloudflarestorage.com',
            'R2_ACCESS_KEY_ID': 'test_key',
            'R2_SECRET_ACCESS_KEY': 'test_secret',
            'R2_BUCKET_NAME': 'test-bucket'
        }
        
        with patch.dict('os.environ', env_vars):
            storage = ParquetStorage()
            
            assert storage.r2_config['endpoint_url'] == env_vars['R2_ENDPOINT_URL']
            assert storage.r2_config['aws_access_key_id'] == env_vars['R2_ACCESS_KEY_ID']
            assert storage.r2_config['aws_secret_access_key'] == env_vars['R2_SECRET_ACCESS_KEY']
            assert storage.bucket_name == env_vars['R2_BUCKET_NAME']
    
    def test_save_to_parquet_success(self, temp_data_dir):
        """Test successful saving to parquet file"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            
            # Sample data
            data = [
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
            
            result = storage.save_to_parquet(data, "BTCUSD", "2024-01-01")
            
            assert result['success'] is True
            assert result['records_count'] == 2
            assert 'file_path' in result
            assert 'file_size' in result
            
            # Verify file was created
            file_path = Path(result['file_path'])
            assert file_path.exists()
            assert file_path.name == "BTCUSD_20240101.parquet"
            
            # Verify directory structure
            assert file_path.parent.name == "01"  # day
            assert file_path.parent.parent.name == "01"  # month
            assert file_path.parent.parent.parent.name == "2024"  # year
    
    def test_save_to_parquet_empty_data(self, temp_data_dir):
        """Test saving empty data returns error"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            
            result = storage.save_to_parquet([], "BTCUSD", "2024-01-01")
            
            assert result == {'error': 'No data to save'}
    
    def test_save_to_parquet_data_transformation(self, temp_data_dir):
        """Test that data is properly transformed before saving"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            
            data = [{"date": "2024-01-01T00:00:00.000Z", "open": 45000.5}]
            result = storage.save_to_parquet(data, "BTCUSD", "2024-01-01")
            
            # Read the saved file to verify transformation
            df = pd.read_parquet(result['file_path'])
            
            # Should have timestamp column instead of date
            assert 'timestamp' in df.columns
            assert 'date' not in df.columns
            assert 'ticker' in df.columns
            assert df['ticker'].iloc[0] == "BTCUSD"
    
    def test_read_from_parquet_success(self, temp_data_dir):
        """Test successful reading from parquet file"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            
            # First save some data
            data = [{"date": "2024-01-01T00:00:00.000Z", "open": 45000.5, "close": 45050.0}]
            save_result = storage.save_to_parquet(data, "BTCUSD", "2024-01-01")
            
            # Then read it back
            read_result = storage.read_from_parquet("BTCUSD", "2024-01-01")
            
            assert read_result['success'] is True
            assert read_result['count'] == 1
            assert 'data' in read_result
            assert 'file_path' in read_result
            
            # Verify data integrity
            read_data = read_result['data'][0]
            assert read_data['open'] == 45000.5
            assert read_data['close'] == 45050.0
            assert read_data['ticker'] == "BTCUSD"
    
    def test_read_from_parquet_file_not_found(self, temp_data_dir):
        """Test reading from non-existent file"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            
            result = storage.read_from_parquet("NONEXISTENT", "2024-01-01")
            
            assert 'error' in result
            assert 'File not found' in result['error']
    
    def test_list_local_files(self, temp_data_dir):
        """Test listing local parquet files"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            
            # Create some test files
            data = [{"date": "2024-01-01T00:00:00.000Z", "open": 45000.5}]
            storage.save_to_parquet(data, "BTCUSD", "2024-01-01")
            storage.save_to_parquet(data, "ETHUSD", "2024-01-01")
            
            files = storage.list_local_files()
            
            assert len(files) == 2
            assert any("BTCUSD_20240101.parquet" in f for f in files)
            assert any("ETHUSD_20240101.parquet" in f for f in files)
    
    def test_get_files_for_upload(self, temp_data_dir):
        """Test getting files for upload based on age"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            storage = ParquetStorage()
            
            # Create a test file with specific date structure
            data = [{"date": "2024-01-01T00:00:00.000Z", "open": 45000.5}]
            storage.save_to_parquet(data, "BTCUSD", "2024-01-01")
            
            # Mock datetime.now to make the file appear older
            with patch('src.parquet_storage.datetime') as mock_datetime:
                # Need to mock both datetime() constructor and datetime.now()
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                mock_datetime.now.return_value = datetime(2024, 1, 3)  # 2 days later
                
                files = storage.get_files_for_upload(days_old=1)
                
                assert len(files) == 1
                assert files[0]['file_date'] == '2024-01-01'
                assert 'crypto-data/2024/01/01/' in files[0]['r2_key']
                assert 'BTCUSD_20240101.parquet' in files[0]['r2_key']


class TestParquetStorageFunctions:
    """Test the module-level functions"""
    
    def test_save_crypto_data_to_parquet_success(self, sample_api_response, temp_data_dir):
        """Test successful saving of API response to parquet"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            result = save_crypto_data_to_parquet(sample_api_response, "BTCUSD", "2024-01-01")
            
            assert result['success'] is True
            assert result['records_count'] == 2
            
            # Verify file was created
            file_path = Path(result['file_path'])
            assert file_path.exists()
    
    def test_save_crypto_data_to_parquet_error_response(self, error_api_response):
        """Test handling of error API response"""
        result = save_crypto_data_to_parquet(error_api_response, "BTCUSD", "2024-01-01")
        
        assert result == error_api_response  # Should return the error as-is
    
    def test_save_crypto_data_to_parquet_empty_price_data(self, empty_api_response, temp_data_dir):
        """Test handling of API response with empty price data"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            result = save_crypto_data_to_parquet(empty_api_response, "BTCUSD", "2024-01-01")
            
            assert 'error' in result
            assert 'No price data found' in result['error']
    
    def test_save_crypto_data_to_parquet_invalid_format(self, temp_data_dir):
        """Test handling of invalid API response format"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            invalid_response = {"invalid": "format"}
            result = save_crypto_data_to_parquet(invalid_response, "BTCUSD", "2024-01-01")
            
            assert 'error' in result
            assert 'Invalid API response format' in result['error']
    
    def test_read_crypto_data_from_parquet_success(self, sample_api_response, temp_data_dir):
        """Test successful reading from parquet"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            # First save data
            save_crypto_data_to_parquet(sample_api_response, "BTCUSD", "2024-01-01")
            
            # Then read it
            result = read_crypto_data_from_parquet("BTCUSD", "2024-01-01")
            
            assert result['success'] is True
            assert result['count'] == 2
    
    def test_read_crypto_data_from_parquet_not_found(self, temp_data_dir):
        """Test reading non-existent parquet file"""
        with patch.dict('os.environ', {'LOCAL_DATA_DIR': str(temp_data_dir)}):
            result = read_crypto_data_from_parquet("NONEXISTENT", "2024-01-01")
            
            assert 'error' in result
            assert 'File not found' in result['error']
    
    @patch('src.parquet_storage.ParquetStorage.upload_to_r2')
    def test_upload_parquet_to_r2_with_auto_key(self, mock_upload):
        """Test uploading parquet file with auto-generated key"""
        mock_upload.return_value = {'success': True}
        
        # Test with path that has proper structure
        test_path = "/data/2024/01/01/BTCUSD_20240101.parquet"
        
        result = upload_parquet_to_r2(test_path)
        
        mock_upload.assert_called_once_with(test_path, "crypto-data/2024/01/01/BTCUSD_20240101.parquet")
        assert result['success'] is True
    
    @patch('src.parquet_storage.ParquetStorage.upload_to_r2')
    def test_upload_parquet_to_r2_with_custom_key(self, mock_upload):
        """Test uploading parquet file with custom key"""
        mock_upload.return_value = {'success': True}
        
        test_path = "/some/path/file.parquet"
        custom_key = "custom/path/file.parquet"
        
        result = upload_parquet_to_r2(test_path, custom_key)
        
        mock_upload.assert_called_once_with(test_path, custom_key)
    
    @patch('src.parquet_storage.ParquetStorage')
    def test_batch_upload_to_r2_success(self, mock_storage_class):
        """Test successful batch upload"""
        mock_storage = mock_storage_class.return_value
        mock_storage.get_files_for_upload.return_value = [
            {
                'local_path': '/data/2024/01/01/BTCUSD_20240101.parquet',
                'r2_key': 'crypto-data/2024/01/01/BTCUSD_20240101.parquet',
                'file_date': '2024-01-01'
            }
        ]
        mock_storage.upload_to_r2.return_value = {'success': True}
        
        result = batch_upload_to_r2(days_old=1)
        
        assert result['success'] is True
        assert result['uploaded_count'] == 1
        assert result['failed_count'] == 0
        assert len(result['uploaded_files']) == 1
    
    @patch('src.parquet_storage.ParquetStorage')
    def test_batch_upload_to_r2_no_files(self, mock_storage_class):
        """Test batch upload with no files to upload"""
        mock_storage = mock_storage_class.return_value
        mock_storage.get_files_for_upload.return_value = []
        
        result = batch_upload_to_r2(days_old=1)
        
        assert result['success'] is True
        assert result['uploaded_count'] == 0
        assert 'No files older than 1 days found' in result['message']
    
    @patch('src.parquet_storage.ParquetStorage')
    def test_batch_upload_to_r2_with_failures(self, mock_storage_class):
        """Test batch upload with some failures"""
        mock_storage = mock_storage_class.return_value
        mock_storage.get_files_for_upload.return_value = [
            {
                'local_path': '/data/file1.parquet',
                'r2_key': 'crypto-data/file1.parquet',
                'file_date': '2024-01-01'
            },
            {
                'local_path': '/data/file2.parquet', 
                'r2_key': 'crypto-data/file2.parquet',
                'file_date': '2024-01-02'
            }
        ]
        
        # Mock first upload success, second failure
        mock_storage.upload_to_r2.side_effect = [
            {'success': True},
            {'error': 'Upload failed'}
        ]
        
        result = batch_upload_to_r2(days_old=1)
        
        assert result['success'] is True
        assert result['uploaded_count'] == 1
        assert result['failed_count'] == 1
        assert len(result['uploaded_files']) == 1
        assert len(result['failed_files']) == 1
        assert 'Upload failed' in result['failed_files'][0]['error']