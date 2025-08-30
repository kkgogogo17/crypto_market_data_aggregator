# AGENTS.md

This file provides guidelines for agentic coding agents operating in this repository.

## Commands

- **Run all tests**: `pytest`
- **Run a specific test file**: `pytest tests/test_api.py`
- **Run a single test**: `pytest tests/test_api.py::TestGetCryptoHistoricalData::test_successful_api_call_specific_date`
- **Linting**: `ruff check .`
- **Formatting**: `ruff format .`

## Code Style

- **Simplicity First**: Simplify the code style, never over engineer the code, only add complexity when there is a need.
- **Imports**: Group imports into three sections: standard library, third-party packages, and application-specific modules, separated by a blank line.
- **Formatting**: Adhere to `ruff` formatting.
- **Types**: Use type hints for function arguments and return values.
- **Naming Conventions**: Use `snake_case` for functions and variables, and `PascalCase` for classes.
- **Error Handling**: Use `try...except` blocks for operations that can fail, such as network requests or file I/O. Raise `ValueError` for invalid function arguments.
- **Docstrings**: Use docstrings for all public modules, classes, and functions.
- **Testing**: Use `pytest` for tests. Use classes to group related tests.

## Test Suite Overview

The test suite provides comprehensive coverage for the crypto market data aggregator, ensuring reliability across API
interactions, data storage, validation, and cloud upload functionality.

### Test Configuration (`tests/conftest.py`)

**Purpose**: Central pytest configuration with shared fixtures and mocking

- `mock_external_services`: Auto-mock for all external services (boto3, database)
- `temp_data_dir`: Temporary directory for file operations
- `mock_env_vars`: Mock environment variables for testing
- `cleanup_test_directories`: Session-level cleanup of test artifacts
- `sample_api_response`: Standard Tiingo API response for testing
- `empty_api_response` / `error_api_response`: Edge case API responses

### API Testing (`tests/test_api.py`)

**Purpose**: Test Tiingo API integration and data fetching functionality

#### TestGetCryptoHistoricalData

- `test_missing_tiingo_token`: Validates error when API token missing
- `test_successful_api_call_*`: Various successful API call scenarios
- `test_api_error_*`: HTTP error handling (404, 500, network issues)
- `test_empty_response`: Handle empty API responses

#### TestFetchCryptoDataEndpoint

- `test_successful_fetch_with_validation`: Complete fetch with validation
- `test_validation_error_prevents_api_call`: Input validation blocking API calls

#### TestFetchAndSaveCryptoData

- `test_successful_fetch_and_save`: End-to-end fetch + save operation
- `test_api_error_prevents_save`: Error handling in save pipeline
- `test_save_error_handling`: Storage failure scenarios
- `test_date_determination_logic`: Date parameter precedence logic

### Data Validation Testing (`tests/test_date_validation.py`)

**Purpose**: Validate date formats and input parameters

#### TestValidateDateFormat

- `test_valid_date_formats`: Accept YYYY-MM-DD formats including leap years
- `test_invalid_date_formats`: Reject malformed dates, wrong formats
- `test_edge_cases`: Whitespace, extra characters, special cases

#### TestFetchCryptoDataEndpointValidation

- `test_missing_ticker`: Require ticker symbol
- `test_invalid_date_format_*`: Date format validation for all parameters
- `test_date_range_validation_*`: Start/end date logic validation
- `test_specific_date_overrides_range`: Parameter precedence testing

### Monthly Parquet Storage Testing (`tests/test_monthly_parquet_storage.py`)

**Purpose**: Test monthly file storage system and data operations

#### TestMonthlyParquetStorage (Main class with 25+ tests)

- **File Path Management**: `test_get_monthly_file_path`
- **Data Saving**: `test_save_to_monthly_parquet_*` (new files, empty data, missing timestamps)
- **Data Appending**: `test_append_to_monthly_file_*` (existing files, duplicates, deduplication)
- **Data Reading**: `test_read_from_monthly_parquet_*` (success, file not found)
- **Data Grouping**: `test_group_data_by_month_*` (by month, different timestamp formats)
- **Multi-Month Operations**: `test_save_multi_month_data_*` (spanning months, empty data)
- **R2 Integration**: `test_get_monthly_files_for_upload`, upload success/failure scenarios
- **Batch Operations**: `test_batch_upload_*` (success, no files, partial failures)
- **Module Functions**: Test standalone functions like `save_crypto_data_to_parquet`

### R2 Cloud Storage Testing (`tests/test_r2_upload.py`)

**Purpose**: Test Cloudflare R2 upload functionality and cloud storage integration

#### TestR2Upload

- `test_create_s3_client_*`: S3 client creation (success/failure)
- `test_upload_to_r2_*`: Upload operations (success, client failure, upload errors)
- `test_upload_various_client_errors`: Different AWS/R2 error scenarios
- `test_upload_large_file_simulation`: Performance testing simulation

#### TestR2Configuration

- `test_missing_r2_credentials`: Handle missing environment variables
- `test_partial_r2_credentials`: Partial credential scenarios
- `test_r2_client_creation_*`: Client creation with credential issues

#### TestR2Integration

- `test_end_to_end_upload_flow`: Complete save → upload pipeline
- `test_batch_upload_error_handling`: Batch upload failure scenarios
- `test_monthly_upload_function_*`: Monthly upload functionality
- `test_batch_upload_*`: Multi-file upload scenarios

### Key Testing Patterns

1. **Mocking Strategy**: Global auto-mock prevents real external service calls
2. **File Operations**: Temporary directories ensure test isolation
3. **Error Coverage**: Comprehensive error scenario testing
4. **Integration Testing**: End-to-end workflows from API to storage
5. **Edge Case Testing**: Empty data, malformed inputs, missing files
6. **Configuration Testing**: Environment variable handling and defaults

### Test Execution Notes

- All tests use automatic mocking of external services (R2, database)
- Temporary directories ensure no test pollution
- Response mocking eliminates network dependencies
- Environment variable patching provides controlled test conditions
- 72+ total tests providing comprehensive coverage across all components

## Core Workflows

The application follows a layered architecture with distinct phases for data collection, processing, and storage.
Understanding these workflows is essential for development and debugging.

### 1. System Initialization Workflow

```
User Action → Load config.yaml → Sync to Database → Validate Tickers → Ready for Collection
```

**Key Components:**

- `ConfigManager` loads and validates YAML configuration
- `sync_to_database()` ensures monitored_assets table is synchronized
- Tickers not in config are soft-deleted (marked as `is_active = FALSE`)
- Environment variable overrides are applied for deployment flexibility

**Entry Points:**

- `ConfigManager.__init__()`
- `sync_config_to_database()`

### 2. Historical Data Collection Workflow

```
Initialize Ticker → API Validation → Fetch from Tiingo → Group by Month → Save Locally → Upload to R2 → Log Progress
```

**Data Flow:**

- `main.py` orchestrates the collection process
- `collector.py` handles API communication and validation
- `ParquetStorage` manages monthly file creation and deduplication
- `DataCollectionDB` tracks progress and upload status
- Automatic retry logic for failed R2 uploads

**Key Functions:**

- `initialize_ticker_data()` - Complete historical collection
- `fetch_and_save_crypto_data()` - Core collection logic
- `save_multi_month_data()` - Monthly file management

### 3. Soft Delete Management Workflow

```
Remove from config.yaml → Sync to Database → Set is_active=FALSE → Skip in Future Operations
```

**Behavior:**

- Configuration file serves as the single source of truth
- Database records are preserved for historical reference
- `is_active` flag controls operational behavior
- All queries respect active status to maintain data consistency

**Functions:**

- `sync_to_database(remove_orphans=False)` - Safe soft delete
- `get_files_needing_r2_upload()` - Only returns active assets
- `get_monitored_assets()` - Filters by active status

### 4. Batch Upload & Recovery Workflow

```
Scan Database → Filter Active Assets → Check Upload Status → Retry Failed Uploads → Update Status
```

**Process:**

- Database-driven approach ensures consistency
- Age-based filtering for upload prioritization
- Automatic retry logic with exponential backoff
- Status tracking prevents duplicate uploads
- Failed uploads are logged for manual review

**Entry Point:**

- `batch_upload_monthly_to_r2()` - Main batch upload function

### 5. File System Organization

```
data/ticker/exchange/YYYY/MM/ticker_exchange_YYYYMM.parquet
```

**Structure Benefits:**

- Hierarchical organization enables efficient querying
- Monthly sharding prevents large file issues
- Exchange separation supports multi-source data
- Consistent naming convention aids automation
- Direct mapping to R2 cloud storage structure

### Workflow Integration Points

**Configuration ↔ Database:**

- Bidirectional sync maintains consistency
- Config changes trigger database updates
- Soft delete preserves historical data

**Storage ↔ Database:**

- Progress tracking for each monthly file
- Upload status monitoring and retry logic
- Metadata storage for file management

**API ↔ Storage:**

- Multi-month data handling with automatic grouping
- Deduplication and timestamp-based sorting
- Error handling preserves data integrity

These workflows ensure reliable, scalable historical data collection with comprehensive error handling and recovery
mechanisms.

## UML System Diagrams

The following UML diagrams provide visual representations of the system architecture, component relationships, and data
flow patterns.

### Class Diagram - System Architecture

```mermaid
classDiagram
    class ConfigManager {
        -config_path: Path
        -_config_data: Dict[str, Any]
        +__init__(config_path: str)
        +_load_config(): None
        +get_tickers(): List[TickerConfig]
        +get_ticker_config(ticker: str): Optional[TickerConfig]
        +get_storage_config(): StorageConfig
        +sync_to_database(remove_orphans: bool): Dict[str, Any]
    }

    class TickerConfig {
        +ticker: str
        +exchange: str
        +start_date: Optional[str]
    }

    class CryptoDataConfig {
        +default_exchange: str
        +max_retries: int
    }

    class StorageConfig {
        +data_dir: str
    }

    class DataCollectionDB {
        -db_path: str
        +__init__(db_path: str)
        +update_collection_progress()
        +update_r2_upload_status()
        +get_files_needing_r2_upload(): List[Dict]
        +add_monitored_asset(ticker: str, exchange: str)
        +get_monitored_assets(): List[Dict]
        +deactivate_monitored_asset(): bool
        +get_all_monitored_assets(): List[Dict]
        +log_collection_run()
    }

    class ParquetStorage {
        -local_data_dir: Path
        -r2_config: Dict
        -bucket_name: str
        +__init__()
        +create_s3_client()
        +get_monthly_file_path(): Path
        +save_to_monthly_parquet(): str
        +save_multi_month_data(): Dict[str, Any]
        +group_data_by_month(): Dict[tuple, List]
        +upload_to_r2(): Dict[str, Any]
        +upload_to_r2_with_retry(): Dict[str, Any]
        +batch_upload_monthly_to_r2(): int
    }

    class Collector {
        <<module>>
        +get_crypto_historical_data(): Dict[str, Any]
        +validate_date_format(date_str: str): bool
        +fetch_crypto_data_endpoint(): Dict[str, Any]
        +fetch_and_save_crypto_data(): Dict[str, Any]
        +fetch_historical_range(): Dict[str, Any]
    }

    class MainApp {
        <<module>>
        +initialize_ticker_data(): Dict[str, Any]
        +collect_historical_data(): Dict[str, Any]
        +main(): None
    }

    class TiingoAPI {
        <<external>>
        +GET /crypto/prices
    }

    class CloudflareR2 {
        <<external>>
        +upload_file()
        +S3 Compatible API
    }

    class SQLiteDB {
        <<database>>
        +monitored_assets
        +data_collection_progress
        +collection_runs
    }

    class LocalFileSystem {
        <<storage>>
        +data/ticker/exchange/YYYY/MM/
    }

    %% Relationships
    ConfigManager --> TickerConfig : creates
    ConfigManager --> CryptoDataConfig : creates
    ConfigManager --> StorageConfig : creates
    ConfigManager --> DataCollectionDB : syncs with
    
    MainApp --> ConfigManager : uses
    MainApp --> Collector : calls
    MainApp --> DataCollectionDB : logs to
    
    Collector --> TiingoAPI : fetches from
    Collector --> ParquetStorage : saves via
    
    ParquetStorage --> DataCollectionDB : updates progress
    ParquetStorage --> LocalFileSystem : writes to
    ParquetStorage --> CloudflareR2 : uploads to
    
    DataCollectionDB --> SQLiteDB : operates on
    
    %% Composition relationships
    ParquetStorage *-- LocalFileSystem : manages
```

### Sequence Diagram - Complete Data Collection Flow

```mermaid
sequenceDiagram
    participant User
    participant ConfigManager
    participant MainApp
    participant Collector
    participant TiingoAPI
    participant ParquetStorage
    participant DataCollectionDB
    participant SQLiteDB
    participant LocalFileSystem
    participant CloudflareR2

    %% System Initialization
    rect rgb(240, 248, 255)
        Note over User, CloudflareR2: System Initialization Phase
        User->>ConfigManager: Load config.yaml
        ConfigManager->>ConfigManager: Parse YAML & apply env overrides
        ConfigManager->>DataCollectionDB: sync_to_database()
        DataCollectionDB->>SQLiteDB: INSERT/UPDATE monitored_assets
        DataCollectionDB-->>ConfigManager: Sync results
    end

    %% Data Collection Phase
    rect rgb(245, 255, 245)
        Note over User, CloudflareR2: Historical Data Collection Phase
        User->>MainApp: initialize_ticker_data(ticker, exchange)
        MainApp->>ConfigManager: get_tickers()
        ConfigManager-->>MainApp: List[TickerConfig]
        
        MainApp->>DataCollectionDB: add_monitored_asset()
        DataCollectionDB->>SQLiteDB: INSERT OR IGNORE monitored_assets
        
        MainApp->>Collector: fetch_and_save_crypto_data()
        Collector->>Collector: validate_date_format()
        Collector->>TiingoAPI: GET /crypto/prices?tickers=BTCUSD&resampleFreq=1Min
        TiingoAPI-->>Collector: JSON response with historical data
    end

    %% Storage Phase
    rect rgb(255, 248, 240)
        Note over User, CloudflareR2: Data Storage & Upload Phase
        Collector->>ParquetStorage: save_multi_month_data(data, ticker, exchange)
        ParquetStorage->>ParquetStorage: group_data_by_month(data)
        
        loop For each month
            ParquetStorage->>ParquetStorage: save_to_monthly_parquet()
            ParquetStorage->>LocalFileSystem: Write Parquet file
            LocalFileSystem-->>ParquetStorage: File path
            
            ParquetStorage->>DataCollectionDB: update_collection_progress()
            DataCollectionDB->>SQLiteDB: INSERT OR REPLACE data_collection_progress
            
            ParquetStorage->>ParquetStorage: upload_to_r2_with_retry()
            ParquetStorage->>CloudflareR2: upload_file(local_path, r2_key)
            
            alt Upload Success
                CloudflareR2-->>ParquetStorage: Success response
                ParquetStorage->>DataCollectionDB: update_r2_upload_status(success=True)
            else Upload Failed
                CloudflareR2-->>ParquetStorage: Error response
                ParquetStorage->>DataCollectionDB: update_r2_upload_status(success=False)
            end
            
            DataCollectionDB->>SQLiteDB: UPDATE data_collection_progress
        end
        
        ParquetStorage-->>Collector: Storage result with file info
    end

    %% Logging Phase
    rect rgb(248, 248, 255)
        Note over User, CloudflareR2: Logging & Cleanup Phase
        Collector-->>MainApp: Collection result
        MainApp->>DataCollectionDB: log_collection_run()
        DataCollectionDB->>SQLiteDB: INSERT collection_runs
        MainApp-->>User: Final result with statistics
    end
```

### Component Diagram - System Architecture Layers

```mermaid
graph TB
    subgraph "Presentation Layer"
        CLI[Command Line Interface]
        MAIN[main.py - Application Entry]
    end

    subgraph "Business Logic Layer"
        CONFIG[config.py - Configuration Management]
        COLLECTOR[collector.py - Data Collection Logic]
        WORKFLOW[Workflow Orchestration]
    end

    subgraph "Data Access Layer"
        STORAGE[parquet_storage.py - File Storage]
        DATABASE[database.py - SQLite Operations]
        DATAMODELS[Data Models & Validation]
    end

    subgraph "Infrastructure Layer"
        FS[Local File System<br/>Monthly Parquet Files]
        DB[(SQLite Database<br/>Progress Tracking)]
        R2[Cloudflare R2<br/>Cloud Storage]
        API[Tiingo API<br/>External Data Source]
    end

    %% Connections
    CLI --> MAIN
    MAIN --> CONFIG
    MAIN --> COLLECTOR
    MAIN --> WORKFLOW

    CONFIG --> DATABASE
    COLLECTOR --> STORAGE
    COLLECTOR --> API
    WORKFLOW --> DATABASE

    STORAGE --> FS
    STORAGE --> R2
    DATABASE --> DB

    %% Styling
    classDef presentation fill:#e3f2fd
    classDef business fill:#f3e5f5
    classDef data fill:#e8f5e8
    classDef infra fill:#fff3e0

    class CLI,MAIN presentation
    class CONFIG,COLLECTOR,WORKFLOW business
    class STORAGE,DATABASE,DATAMODELS data
    class FS,DB,R2,API infra
```

### State Diagram - Data Collection State Management

```mermaid
stateDiagram-v2
    [*] --> ConfigLoaded : Load config.yaml
    ConfigLoaded --> DatabaseSynced : sync_to_database()
    DatabaseSynced --> TickerInitialized : initialize_ticker_data()
    
    TickerInitialized --> APIValidation : Validate parameters
    APIValidation --> APICall : Parameters valid
    APIValidation --> Error : Invalid parameters
    
    APICall --> DataReceived : API success
    APICall --> APIRetry : API failure
    APIRetry --> APICall : Retry attempt
    APIRetry --> Error : Max retries exceeded
    
    DataReceived --> DataGrouped : group_by_month()
    DataGrouped --> LocalSave : save_to_monthly_parquet()
    LocalSave --> LocalSaved : File written
    LocalSaved --> R2Upload : upload_to_r2_with_retry()
    
    R2Upload --> R2Success : Upload successful
    R2Upload --> R2Retry : Upload failed
    R2Retry --> R2Upload : Retry with backoff
    R2Retry --> R2Failed : Max retries exceeded
    
    R2Success --> ProgressLogged : update_progress()
    R2Failed --> ProgressLogged : update_progress()
    ProgressLogged --> Complete : All months processed
    
    Complete --> [*]
    Error --> [*]
```
These UML diagrams provide comprehensive visual documentation of the system architecture, showing class relationships,
data flow, component layering, and state management for effective system understanding and maintenance.

