import sqlite3
from pathlib import Path

DB_PATH = Path("crypto_data.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS data_collection_progress (
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

CREATE TABLE IF NOT EXISTS collection_runs (
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

CREATE TABLE IF NOT EXISTS monitored_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    fetch_frequency_hours INTEGER DEFAULT 24,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, exchange)
);
"""

def init_db(db_path=DB_PATH):
    """Create the SQLite database and required tables"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()

class DataCollectionDB:
    def __init__(self, db_path: str = str(DB_PATH)):
        self.db_path = db_path
        self._ensure_schema()

    def _ensure_schema(self):
        init_db(self.db_path)

    # Future: Add methods here for interacting with the DB
