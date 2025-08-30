"""Configuration management for crypto market data aggregator - MVP Version"""

import os
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class TickerConfig:
    """Configuration for a single ticker"""
    ticker: str
    exchange: str
    start_date: Optional[str] = None


@dataclass
class CryptoDataConfig:
    """Global crypto data settings"""
    default_exchange: str
    max_retries: int


@dataclass
class StorageConfig:
    """Storage settings"""
    data_dir: str


class ConfigManager:
    """Simple configuration manager"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        self._config_data: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from YAML file"""
        if self.config_path.exists():
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config_data = yaml.safe_load(f)
        else:
            self._config_data = self._get_default_config()

        # Apply environment variable overrides
        self._apply_env_overrides()

    def _get_default_config(self) -> Dict[str, Any]:
        """Default configuration if no file exists"""
        return {
            "crypto_data": {
                "default_exchange": "Binance",
                "max_retries": 3
            },
            "default_start_date": "2012-01-01",
            "storage": {
                "data_dir": "./data"
            },
            "tickers": [
                {
                    "ticker": "btcusdt",
                    "exchange": "Binance",
                    "start_date": "2012-01-01"
                }
            ]
        }

    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides"""
        if os.getenv("LOCAL_DATA_DIR"):
            self._config_data["storage"]["data_dir"] = os.getenv("LOCAL_DATA_DIR")

    def get_crypto_data_config(self) -> CryptoDataConfig:
        """Get crypto data configuration"""
        config = self._config_data.get("crypto_data", {})
        return CryptoDataConfig(
            default_exchange=config.get("default_exchange", "tiingo"),
            max_retries=config.get("max_retries", 3)
        )

    def get_storage_config(self) -> StorageConfig:
        """Get storage configuration"""
        config = self._config_data.get("storage", {})
        return StorageConfig(
            data_dir=config.get("data_dir", "./data")
        )

    def get_default_start_date(self) -> str:
        """Get default start date"""
        return self._config_data.get("default_start_date", "2012-01-01")

    def get_tickers(self) -> List[TickerConfig]:
        """Get list of all configured tickers"""
        tickers = self._config_data.get("tickers", [])
        return [
            TickerConfig(
                ticker=t.get("ticker"),
                exchange=t.get("exchange"),
                start_date=t.get("start_date")
            )
            for t in tickers
        ]

    def get_ticker_config(self, ticker: str) -> Optional[TickerConfig]:
        """Get configuration for a specific ticker"""
        for ticker_data in self._config_data.get("tickers", []):
            if ticker_data.get("ticker") == ticker:
                return TickerConfig(
                    ticker=ticker_data.get("ticker"),
                    exchange=ticker_data.get("exchange"),
                    start_date=ticker_data.get("start_date")
                )
        return None

    def sync_to_database(self, remove_orphans: bool = False) -> Dict[str, Any]:
        """Sync configuration to database with soft delete support
        
        Args:
            remove_orphans: If True, completely remove assets not in config (dangerous!)
                          If False, just deactivate them (recommended)
        
        Returns:
            Dict with sync results
        """
        from src.database import DataCollectionDB

        db = DataCollectionDB()
        config_tickers = {(t.ticker, t.exchange) for t in self.get_tickers()}

        results = {
            "added": [],
            "deactivated": [],
            "removed": [],
            "errors": []
        }

        try:
            # 1. Add/ensure all configured tickers are in database
            for ticker in self.get_tickers():
                try:
                    db.add_monitored_asset(ticker.ticker, ticker.exchange)
                    # Also ensure it's active (in case it was deactivated before)
                    db.reactivate_monitored_asset(ticker.ticker, ticker.exchange)
                    results["added"].append(f"{ticker.ticker} ({ticker.exchange})")
                except Exception as e:
                    results["errors"].append(f"Failed to add {ticker.ticker}: {str(e)}")

            # 2. Handle assets not in current configuration
            all_db_assets = db.get_all_monitored_assets()

            for asset in all_db_assets:
                key = (asset['ticker'], asset['exchange'])

                if key not in config_tickers and asset['is_active']:
                    # Asset is in database but not in config, and currently active
                    ticker_name = f"{asset['ticker']} ({asset['exchange']})"

                    try:
                        if remove_orphans:
                            # Dangerous: This would require cascade delete from other tables
                            results["errors"].append(f"Hard delete not implemented for {ticker_name}")
                        else:
                            # Safe: Soft delete (deactivate)
                            if db.deactivate_monitored_asset(asset['ticker'], asset['exchange']):
                                results["deactivated"].append(ticker_name)
                            else:
                                results["errors"].append(f"Failed to deactivate {ticker_name}")
                    except Exception as e:
                        results["errors"].append(f"Failed to process {ticker_name}: {str(e)}")

            return results

        except Exception as e:
            results["errors"].append(f"Sync failed: {str(e)}")
            return results


# Global configuration instance
config = ConfigManager()


# Simple convenience functions
def get_tickers() -> List[TickerConfig]:
    """Get list of all tickers"""
    return config.get_tickers()


def get_ticker_config(ticker: str) -> Optional[TickerConfig]:
    """Get configuration for specific ticker"""
    return config.get_ticker_config(ticker)


def get_storage_config() -> StorageConfig:
    """Get storage configuration"""
    return config.get_storage_config()


def get_default_start_date() -> str:
    """Get default start date"""
    return config.get_default_start_date()


def sync_config_to_database(remove_orphans: bool = False) -> Dict[str, Any]:
    """Sync configuration to database
    
    Args:
        remove_orphans: If True, completely remove assets not in config (dangerous!)
                      If False, just deactivate them (recommended, default)
    
    Returns:
        Dict with sync results showing what was added/deactivated
    """
    return config.sync_to_database(remove_orphans)
