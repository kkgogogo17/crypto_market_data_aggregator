import sqlite3
from pathlib import Path
from datetime import date, datetime
from typing import Optional, Dict, Any, List

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
    r2_uploaded BOOLEAN DEFAULT FALSE,
    r2_upload_attempts INTEGER DEFAULT 0,
    r2_last_upload_attempt TIMESTAMP,
    r2_upload_error TEXT,
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
        """Update or insert collection progress (local data collection only)"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
            INSERT OR REPLACE INTO data_collection_progress 
            (ticker, exchange, year, month, last_date_collected, records_count, 
             file_size_bytes, local_file_path, r2_key, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            ticker, exchange, year, month, last_date.isoformat(),
            records_count, file_info.get('file_size_bytes', 0),
            file_info.get('local_file_path'), file_info.get('r2_key')
        ))
        
        conn.commit()
        conn.close()

    def update_r2_upload_status(
        self,
        ticker: str,
        exchange: str,
        year: int,
        month: int,
        success: bool,
        error_message: Optional[str] = None
    ):
        """Update R2 upload status for a specific month"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        if success:
            cur.execute("""
                UPDATE data_collection_progress 
                SET r2_uploaded = TRUE,
                    r2_upload_attempts = r2_upload_attempts + 1,
                    r2_last_upload_attempt = CURRENT_TIMESTAMP,
                    r2_upload_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE ticker = ? AND exchange = ? AND year = ? AND month = ?
            """, (ticker, exchange, year, month))
        else:
            cur.execute("""
                UPDATE data_collection_progress 
                SET r2_uploaded = FALSE,
                    r2_upload_attempts = r2_upload_attempts + 1,
                    r2_last_upload_attempt = CURRENT_TIMESTAMP,
                    r2_upload_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE ticker = ? AND exchange = ? AND year = ? AND month = ?
            """, (error_message, ticker, exchange, year, month))
        
        conn.commit()
        conn.close()

    def get_files_needing_r2_upload(self) -> List[Dict[str, Any]]:
        """Get files that need R2 upload (not uploaded or failed with < 3 attempts)
        Only returns files for active monitored assets."""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
                    SELECT p.ticker,
                           p.exchange,
                           p.year,
                           p.month,
                           p.local_file_path,
                           p.r2_key,
                           p.r2_upload_attempts,
                           p.r2_upload_error
                    FROM data_collection_progress p
                             INNER JOIN monitored_assets m ON p.ticker = m.ticker AND p.exchange = m.exchange
                    WHERE (p.r2_uploaded = FALSE OR p.r2_uploaded IS NULL)
                      AND p.r2_upload_attempts < 3
                      AND p.local_file_path IS NOT NULL
                      AND m.is_active = TRUE
        """)
        
        results = []
        for row in cur.fetchall():
            results.append({
                "ticker": row[0],
                "exchange": row[1], 
                "year": row[2],
                "month": row[3],
                "local_file_path": row[4],
                "r2_key": row[5],
                "r2_upload_attempts": row[6],
                "r2_upload_error": row[7]
            })
        
        conn.close()
        return results

    def add_monitored_asset(self, ticker: str, exchange: str):
        """Add a ticker/exchange pair to monitor"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
            INSERT OR IGNORE INTO monitored_assets (ticker, exchange)
            VALUES (?, ?)
        """, (ticker, exchange))
        
        conn.commit()
        conn.close()

    def get_monitored_assets(self) -> List[Dict[str, str]]:
        """Get list of ticker/exchange pairs to monitor"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT ticker, exchange FROM monitored_assets 
            WHERE is_active = TRUE
        """)
        
        results = [{"ticker": row[0], "exchange": row[1]} for row in cur.fetchall()]
        conn.close()
        return results

    def log_collection_run(
        self,
        ticker: str,
        exchange: str,
        start_date: date,
        end_date: date,
        status: str = 'completed',
        records_fetched: int = 0,
        error_message: Optional[str] = None
    ):
        """Log a collection run"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO collection_runs 
            (ticker, exchange, start_date, end_date, status, records_fetched, 
             error_message, started_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (
            ticker, exchange, start_date.isoformat(), end_date.isoformat(),
            status, records_fetched, error_message
        ))
        
        conn.commit()
        conn.close()

    def deactivate_monitored_asset(self, ticker: str, exchange: str) -> bool:
        """Deactivate a monitored asset (soft delete)"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute("""
                    UPDATE monitored_assets
                    SET is_active  = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE ticker = ?
                      AND exchange = ?
                    """, (ticker, exchange))

        rows_affected = cur.rowcount
        conn.commit()
        conn.close()

        return rows_affected > 0

    def reactivate_monitored_asset(self, ticker: str, exchange: str) -> bool:
        """Reactivate a monitored asset"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute("""
                    UPDATE monitored_assets
                    SET is_active  = TRUE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE ticker = ?
                      AND exchange = ?
                    """, (ticker, exchange))

        rows_affected = cur.rowcount
        conn.commit()
        conn.close()

        return rows_affected > 0

    def get_all_monitored_assets(self) -> List[Dict[str, Any]]:
        """Get all monitored assets (both active and inactive)"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute("""
                    SELECT ticker, exchange, is_active, created_at
                    FROM monitored_assets
                    ORDER BY ticker, exchange
                    """)

        results = []
        for row in cur.fetchall():
            results.append({
                "ticker": row[0],
                "exchange": row[1],
                "is_active": bool(row[2]),
                "created_at": row[3]
            })

        conn.close()
        return results
