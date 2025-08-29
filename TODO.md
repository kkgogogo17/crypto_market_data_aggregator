# Crypto Market Data Aggregator - Monthly Storage & Exchange Support Implementation Plan

## Overview
Transform the current daily file-based storage system to a monthly aggregation system with multi-exchange support, SQLite tracking, and automated data collection.

## Current State Analysis

### Existing Structure

- **Storage Pattern**: `data/YYYY/MM/DD/ticker_YYYYMMDD.parquet` (daily files) ~~[MIGRATED TO MONTHLY]~~
- **R2 Pattern**: `crypto-data/YYYY/MM/DD/ticker_YYYYMMDD.parquet` ~~[UPDATED TO MONTHLY]~~
- **Data Source**: Single Tiingo API endpoint
- **No Exchange Differentiation**: All data treated as single source ~~[IMPLEMENTED]~~
- **No Tracking System**: No persistence of what data has been collected ~~[PLANNED]~~

### Current Components

- `ParquetStorage` class: Handles local storage and R2 uploads ~~[REFACTORED TO MONTHLY]~~
- `collector.py`: API data fetching functions
- Daily file generation and upload system ~~[MIGRATED TO MONTHLY]~~

## Target Architecture

### New Storage Hierarchy
```
Local: data/ticker/exchange/YYYY/MM/ticker_exchange_YYYYMM.parquet
R2: ticker/exchange/YYYY/MM/ticker_exchange_YYYYMM.parquet
```

### Example Structure
```
data/
├── BTCUSD/
│   ├── tiingo/
│   │   ├── 2024/
│   │   │   ├── 01/
│   │   │   │   └── BTCUSD_tiingo_202401.parquet
│   │   │   └── 02/
│   │   │       └── BTCUSD_tiingo_202402.parquet
│   │   └── coinbase/  # Future exchange support
│   │       └── 2024/
│   │           └── 01/
│   │               └── BTCUSD_coinbase_202401.parquet
└── ETHUSD/
    └── tiingo/
        └── 2024/
            └── 01/
                └── ETHUSD_tiingo_202401.parquet
```

## Implementation Plan

## Phase 1: Core Infrastructure Refactoring

### 1.1 Database Schema Design
**File**: `src/database.py`

Create SQLite database with tables:

```sql
-- Track data collection progress per ticker/exchange
CREATE TABLE data_collection_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    last_date_collected DATE NOT NULL,
    is_month_complete BOOLEAN DEFAULT FALSE,
    records_count INTEGER DEFAULT 0,
    file_size_bytes INTEGER DEFAULT 0,
    local_file_path TEXT,
    r2_key TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, exchange, year, month)
);

-- Track individual data collection runs
CREATE TABLE collection_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed')) DEFAULT 'pending',
    records_fetched INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Track what tickers and exchanges we monitor
CREATE TABLE monitored_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    fetch_frequency_hours INTEGER DEFAULT 24,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, exchange)
);
```

### 1.2 Monthly Storage System Refactor

**Files Modified**: `src/parquet_storage.py`

#### Key Changes:
1. **New Storage Pattern**: Change from daily to monthly files
2. **Exchange Support**: Add exchange parameter to all functions
3. **Monthly Aggregation**: Implement append/update mechanism for monthly files
4. **Database Integration**: Track progress in SQLite

#### New Class Structure:
```python
class MonthlyParquetStorage:
    def __init__(self):
        # Current init + database connection
        
    def save_to_monthly_parquet(
        self, 
        data: List[Dict], 
        ticker: str, 
        exchange: str,
        date_str: str
    ) -> Dict[str, Any]:
        # Save data to monthly file, append if exists
        
    def get_monthly_file_path(
        self, 
        ticker: str, 
        exchange: str, 
        year: int, 
        month: int
    ) -> Path:
        # Generate monthly file path
        
    def append_to_monthly_file(
        self,
        existing_file: Path,
        new_data: pd.DataFrame
    ) -> Dict[str, Any]:
        # Append new data to existing monthly file
        
    def upload_monthly_file_to_r2(
        self,
        local_path: str,
        ticker: str,
        exchange: str,
        year: int,
        month: int
    ) -> Dict[str, Any]:
        # Upload with new R2 key structure
```

### 1.3 Database Management System
**File**: `src/database.py`

```python
class DataCollectionDB:
    def __init__(self, db_path: str = "crypto_data.db"):
        # Initialize SQLite connection
        
    def get_last_collected_date(
        self, 
        ticker: str, 
        exchange: str, 
        year: int, 
        month: int
    ) -> Optional[date]:
        # Get last collected date for ticker/exchange/month
        
    def update_collection_progress(
        self,
        ticker: str,
        exchange: str, 
        year: int,
        month: int,
        last_date: date,
        records_count: int,
        file_info: Dict[str, Any]
    ):
        # Update or insert collection progress
        
    def mark_month_complete(
        self,
        ticker: str,
        exchange: str,
        year: int, 
        month: int
    ):
        # Mark month as fully collected
        
    def get_incomplete_months(self) -> List[Dict[str, Any]]:
        # Get months that need data collection
        
    def get_monitored_assets(self) -> List[Dict[str, str]]:
        # Get list of ticker/exchange pairs to monitor
```

## Phase 2: Data Collection Strategy

### 2.1 Enhanced Collector System
**Files to Modify**: `src/collector.py`

#### Key Enhancements:
1. **Exchange Parameter**: Add exchange support to all functions
2. **Incremental Collection**: Fetch only missing dates
3. **Monthly Aggregation**: Collect full months when possible
4. **Database Integration**: Track collection progress

#### New Functions:
```python
def get_crypto_historical_data_with_exchange(
    ticker: str,
    exchange: str,
    start_date: str,
    end_date: str
) -> Dict[str, Any]:
    # Enhanced API call with exchange support
    
def collect_missing_data_for_month(
    ticker: str,
    exchange: str,
    year: int,
    month: int
) -> Dict[str, Any]:
    # Collect missing data for a specific month
    
def get_next_collection_dates(
    ticker: str,
    exchange: str
) -> List[date]:
    # Determine what dates need to be collected
```

### 2.2 Incremental Update Mechanism
**File**: `src/incremental_updater.py`

```python
class IncrementalUpdater:
    def __init__(self):
        self.db = DataCollectionDB()
        self.storage = MonthlyParquetStorage()
        
    def update_current_month_data(
        self,
        ticker: str,
        exchange: str
    ) -> Dict[str, Any]:
        # Update current month with latest data
        
    def backfill_missing_data(
        self,
        ticker: str,
        exchange: str,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        # Backfill historical data gaps
        
    def process_daily_updates(self) -> List[Dict[str, Any]]:
        # Process daily updates for all monitored assets
```

## Phase 3: Automation & Scheduling

### 3.1 Cron Job System
**File**: `src/scheduler.py`

```python
class DataCollectionScheduler:
    def __init__(self):
        self.db = DataCollectionDB()
        self.updater = IncrementalUpdater()
        
    def daily_collection_job(self):
        # Main daily collection job
        
    def weekly_backfill_job(self):
        # Weekly job to catch missed data
        
    def monthly_completion_job(self):
        # Monthly job to finalize completed months
```

### 3.2 Configuration Management
**File**: `src/config.py`

```python
@dataclass
class CollectionConfig:
    # Monitored tickers and exchanges
    # Collection frequencies
    # Retry policies
    # R2 upload settings
```

## Phase 4: Migration & Validation

### 4.1 Migration Tools
**File**: `src/migration.py`

```python
class DataMigrator:
    def migrate_daily_to_monthly(self):
        # Migrate existing daily files to monthly format
        
    def validate_migration(self):
        # Validate migrated data integrity
```

### 4.2 Validation & Monitoring
**File**: `src/validator.py`

```python
class DataValidator:
    def validate_monthly_file_integrity(self):
        # Validate monthly file completeness
        
    def check_data_freshness(self):
        # Ensure data is fresh (< 24 hours old)
        
    def generate_collection_report(self):
        # Generate collection status report
```

## Implementation Timeline

### Week 1: Database & Core Infrastructure
- [ ] Create SQLite schema and database management system
- [ ] Implement `DataCollectionDB` class
- [ ] Create configuration management system
- [ ] Set up project structure for new modules

### Week 2: Monthly Storage System
- [ ] Refactor `ParquetStorage` to `MonthlyParquetStorage`
- [ ] Implement monthly file aggregation logic
- [ ] Update R2 upload system with new hierarchy
- [ ] Create comprehensive tests for storage system

### Week 3: Enhanced Collection System  
- [ ] Enhance collector with exchange support
- [ ] Implement incremental data collection
- [ ] Create `IncrementalUpdater` class
- [ ] Add missing data detection and backfill

### Week 4: Automation & Migration
- [ ] Build scheduling and cron job system
- [ ] Create migration tools for existing data
- [ ] Implement data validation and monitoring
- [ ] Create deployment and operational scripts

## Key Technical Decisions

### Exchange Support Strategy
- **Current**: Tiingo API only
- **Future**: Pluggable exchange adapters
- **Exchange Detection**: From ticker format or explicit parameter

### Monthly File Management
- **Append Strategy**: Read existing, append new data, deduplicate, sort by timestamp
- **Partitioning**: Consider partitioning large monthly files by day within parquet
- **Compression**: Use Snappy compression with optimized schemas

### Data Freshness Policy  
- **Target**: Data less than 24 hours old
- **Collection Window**: Run daily at 2 AM UTC
- **Retry Policy**: 3 attempts with exponential backoff
- **Alerting**: Notify if collection fails for 48+ hours

### R2 Upload Strategy
- **Current Month**: Upload after each update
- **Completed Months**: Upload once and mark as final
- **Versioning**: Use R2 object versioning for completed months
- **Lifecycle**: Archive data older than 2 years to cheaper storage

## Success Metrics

### Data Quality
- [ ] 99.9% data completeness for monitored assets
- [ ] < 24 hour data freshness
- [ ] Zero data duplication
- [ ] Successful historical data migration

### System Performance
- [ ] Collection jobs complete within 2 hours
- [ ] R2 upload success rate > 99%
- [ ] Storage cost reduction of 60%+ (monthly vs daily files)
- [ ] Query performance improvement for monthly aggregations

### Operational
- [ ] Automated monitoring and alerting
- [ ] Self-healing for temporary failures
- [ ] Clear operational runbooks
- [ ] Comprehensive logging and observability

## Risk Mitigation

### Data Loss Prevention
- [ ] Database backups before major operations
- [ ] Staging environment for testing migrations
- [ ] Rollback procedures for failed deployments
- [ ] Data validation at each step

### API Rate Limiting
- [ ] Implement exponential backoff
- [ ] Respect API rate limits
- [ ] Queue-based collection for high-volume periods
- [ ] Multiple API key rotation if needed

### Storage Costs
- [ ] Monitor R2 storage usage and costs
- [ ] Implement data lifecycle policies
- [ ] Optimize parquet file sizes and compression
- [ ] Regular cost analysis and optimization

## Future Enhancements (Post-MVP)

### Multi-Exchange Support
- [ ] Coinbase, Binance, Kraken API integrations
- [ ] Cross-exchange data validation
- [ ] Exchange-specific data normalization

### Advanced Analytics
- [ ] Data quality scoring
- [ ] Market data completeness analytics  
- [ ] Performance benchmarking across exchanges

### Real-time Streaming (Optional)
- [ ] WebSocket integration for real-time data
- [ ] Stream processing for live updates
- [ ] Hybrid batch + streaming architecture