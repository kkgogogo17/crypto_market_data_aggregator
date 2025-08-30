# Crypto Market Data Aggregator - Core Implementation Plan

## Overview
Complete the core historical data fetching and monthly storage functionality with Cloudflare R2 upload capabilities.

## Current State Analysis

### ✅ Already Implemented
- **Monthly Storage Pattern**: `data/ticker/exchange/YYYY/MM/ticker_exchange_YYYYMM.parquet` 
- **R2 Upload**: Monthly file structure with R2 integration
- **ParquetStorage Class**: Monthly aggregation with exchange support
- **Basic Collector**: Tiingo API data fetching
- **Database Schema**: SQLite schema defined

### Current Components
- `ParquetStorage` class: ✅ Monthly storage and R2 uploads implemented
- `collector.py`: ✅ Basic API fetching, needs enhancement for exchange support
- `database.py`: ✅ Schema defined, needs implementation of methods

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

## Core Implementation Tasks (Focus: Historical Data Fetching & R2 Storage)

### 🎯 Immediate Goals
Make the application work for fetching historical crypto data and storing it in monthly chunks on Cloudflare R2.

### Phase 1: Complete Core Functionality

#### 1.1 ✅ Already Working
- Monthly parquet storage with exchange support
- R2 upload functionality  
- Basic API fetching from Tiingo
- Database schema defined

#### 1.2 🔧 Need to Complete

**A. Enhanced Collector Functions** (`src/collector.py`)
- [ ] Add exchange parameter support to existing functions
- [ ] Fix `fetch_and_save_crypto_data()` to work with monthly storage
- [ ] Create bulk historical data collection function

**B. Database Implementation** (`src/database.py`) 
- [ ] Implement `DataCollectionDB` methods for tracking progress
- [ ] Add functions to insert/update collection records

**C. Main Application** (`src/main.py`)
- [ ] Create working examples for historical data collection
- [ ] Test multi-month data fetching and storage
- [ ] Verify R2 upload workflow

#### 1.3 Core Functions Needed

```python
# In collector.py
def fetch_historical_range(
    ticker: str, 
    exchange: str,
    start_date: str, 
    end_date: str
) -> Dict[str, Any]:
    # Fetch large date ranges and save to monthly files
    
# In main.py  
def collect_historical_data(
    ticker: str,
    exchange: str = "tiingo", 
    months_back: int = 12
):
    # Main function to collect historical data
```

### ✅ Implementation Completed

**Core functionality now working:**

1. **Enhanced Collector Integration** (`src/collector.py`)
   - ✅ Added exchange parameter to `fetch_and_save_crypto_data()`
   - ✅ Created `fetch_historical_range()` function for bulk data collection
   - ✅ Fixed integration with monthly storage system

2. **Database Implementation** (`src/database.py`)
   - ✅ Added basic CRUD methods to `DataCollectionDB`
   - ✅ Implemented progress tracking functions
   - ✅ Added monitored assets management

3. **Main Application** (`src/main.py`) 
   - ✅ Created `initialize_ticker_data()` function for complete historical data fetch
   - ✅ Created `collect_historical_data()` function for targeted data collection
   - ✅ End-to-end workflow: API → Monthly Storage → R2 Upload → Database Tracking

## Quick Start Guide

The application now has two main functions for data collection:

### 1. Complete Historical Data Initialization
```python
from src.main import initialize_ticker_data

# Get ALL available historical data for a ticker (recommended for first run)
initialize_ticker_data("BTCUSD", "tiingo")

# This will:
# 1. Fetch ALL available data from 2012-01-01 to now
# 2. Save to monthly parquet files locally  
# 3. Upload to Cloudflare R2
# 4. Track progress in SQLite database
```

### 2. Targeted Historical Data Collection
```python
from src.main import collect_historical_data

# Collect specific months of data (useful for updates/backfills)
collect_historical_data("BTCUSD", "tiingo", months_back=12)

# This will:
# 1. Fetch last 12 months of data
# 2. Save to monthly parquet files locally
# 3. Upload to Cloudflare R2  
# 4. Track progress in SQLite database
```