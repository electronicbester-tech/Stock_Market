"""
TSE (Tehran Stock Exchange) API Connector
==========================================

اتصال به منابع داده بورس ایران
"""
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TSEConnector:
    """
    Connect to Tehran Stock Exchange data sources.
    
    Supported sources:
    1. tsetmc.com (official TSE website)
    2. tsetmc-client library (if installed)
    3. Alternative APIs
    """
    
    BASE_URLs = {
        'tsetmc': 'https://tsetmc.com/tsev2/data',
        'instinfo': 'https://cdn.tsetmc.com/api/Instrument',
    }
    
    def __init__(self):
        """Initialize TSE connector"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.logger = logger
    
    # ========================================================================
    # Method 1: Using tsetmc-client library (Recommended if available)
    # ========================================================================
    
    def try_tsetmc_client(self, symbol: str, days: int = 260) -> Optional[pd.DataFrame]:
        """
        Try to load data using tsetmc-client library.
        
        Install with: pip install tsetmc-client
        
        Args:
            symbol: Symbol name (نماد)
            days: Number of days to retrieve
        
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        try:
            from tsetmc_client import TSEClient
            from tsetmc_client.models import SymbolModel
            
            self.logger.info(f"Using tsetmc-client for {symbol}...")
            
            client = TSEClient()
            
            # Get symbol info
            symbols = client.get_symbol(symbol)
            if not symbols:
                self.logger.warning(f"Symbol {symbol} not found")
                return None
            
            symbol_model = symbols[0]
            
            # Get daily data
            data = client.get_daily_data(symbol_model.isin_code)
            
            if not data:
                self.logger.warning(f"No data for {symbol}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame([
                {
                    'Date': row.date,
                    'Open': row.open,
                    'High': row.high,
                    'Low': row.low,
                    'Close': row.close,
                    'Volume': row.volume
                }
                for row in data
            ])
            
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date').sort_index()
            df = df.tail(days)
            
            self.logger.info(f"✓ Retrieved {len(df)} days for {symbol}")
            return df
        
        except ImportError:
            self.logger.debug("tsetmc-client not installed")
            return None
        except Exception as e:
            self.logger.warning(f"Error with tsetmc-client: {e}")
            return None
    
    # ========================================================================
    # Method 2: Using tsetmc.com API directly
    # ========================================================================
    
    def get_symbol_isin(self, symbol: str) -> Optional[str]:
        """
        Get ISIN code for a symbol from tsetmc.com
        
        Args:
            symbol: Symbol name
        
        Returns:
            ISIN code or None
        """
        try:
            # This API returns available symbols
            url = f"{self.BASE_URLs['instinfo']}/GetInstSymbol/{symbol}"
            
            response = self.session.get(url, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            
            if data and isinstance(data, list) and len(data) > 0:
                return data[0].get('insCode') or data[0].get('isin')
            
            self.logger.warning(f"Could not find ISIN for {symbol}")
            return None
        
        except Exception as e:
            self.logger.debug(f"Error getting ISIN: {e}")
            return None
    
    def get_daily_data_tsetmc(self, symbol: str, days: int = 260) -> Optional[pd.DataFrame]:
        """
        Get daily data from tsetmc.com
        
        Args:
            symbol: Symbol name or ISIN code
            days: Number of days to retrieve
        
        Returns:
            DataFrame with OHLCV data or None
        """
        try:
            # Try to get ISIN first
            isin = self.get_symbol_isin(symbol)
            if not isin:
                self.logger.warning(f"Cannot get data without ISIN for {symbol}")
                return None
            
            # Build URL for daily data
            url = f"{self.BASE_URLs['tsetmc']}/dailytradesdata/{isin}/latest.json"
            
            self.logger.info(f"Fetching {symbol} from tsetmc.com...")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or 'data' not in data:
                self.logger.warning(f"No data returned for {symbol}")
                return None
            
            records = data.get('data', [])
            
            if not records:
                self.logger.warning(f"Empty data for {symbol}")
                return None
            
            # Parse records
            df_data = []
            for record in records:
                try:
                    # Date format: YYYYMMDD
                    date_str = str(record.get('date', ''))
                    if len(date_str) == 8:
                        date = datetime.strptime(date_str, '%Y%m%d')
                    else:
                        continue
                    
                    df_data.append({
                        'Date': date,
                        'Open': float(record.get('open', 0)),
                        'High': float(record.get('high', 0)),
                        'Low': float(record.get('low', 0)),
                        'Close': float(record.get('close', 0)),
                        'Volume': int(record.get('volume', 0))
                    })
                except Exception as e:
                    self.logger.debug(f"Error parsing record: {e}")
                    continue
            
            if not df_data:
                self.logger.warning(f"No valid records for {symbol}")
                return None
            
            df = pd.DataFrame(df_data)
            df = df.set_index('Date').sort_index()
            df = df.tail(days)
            
            self.logger.info(f"✓ Retrieved {len(df)} days for {symbol}")
            return df
        
        except requests.RequestException as e:
            self.logger.error(f"Network error for {symbol}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving data for {symbol}: {e}")
            return None
    
    # ========================================================================
    # Method 3: Batch data retrieval
    # ========================================================================
    
    def get_multiple_symbols(self, symbols: list, days: int = 260) -> Dict[str, pd.DataFrame]:
        """
        Retrieve data for multiple symbols.
        
        Args:
            symbols: List of symbol names
            days: Number of days per symbol
        
        Returns:
            dict: {symbol: DataFrame}
        """
        data_dict = {}
        
        for symbol in symbols:
            # Try tsetmc-client first (if available)
            df = self.try_tsetmc_client(symbol, days)
            
            # Fallback to direct API
            if df is None:
                df = self.get_daily_data_tsetmc(symbol, days)
            
            if df is not None and len(df) > 100:  # At least 100 days
                data_dict[symbol] = df
            else:
                self.logger.warning(f"Skipped {symbol}: insufficient data")
        
        self.logger.info(f"✓ Retrieved data for {len(data_dict)}/{len(symbols)} symbols")
        return data_dict


class CachedTSEConnector(TSEConnector):
    """
    TSE Connector with file-based caching.
    Cache is invalidated daily.
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        """Initialize with cache directory"""
        super().__init__()
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_age_days = 1  # Invalidate after 1 day
    
    def _get_cache_path(self, symbol: str) -> Path:
        """Get cache file path for symbol"""
        return self.cache_dir / f"{symbol}_daily.csv"
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache is still valid"""
        if not cache_path.exists():
            return False
        
        file_age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
        return file_age < timedelta(days=self.cache_age_days)
    
    def get_daily_data_cached(self, symbol: str, days: int = 260) -> Optional[pd.DataFrame]:
        """Get data with caching"""
        cache_path = self._get_cache_path(symbol)
        
        # Check cache
        if self._is_cache_valid(cache_path):
            try:
                df = pd.read_csv(cache_path, index_col='Date', parse_dates=True)
                self.logger.info(f"✓ Loaded {symbol} from cache")
                return df
            except Exception as e:
                self.logger.warning(f"Cache read error for {symbol}: {e}")
        
        # Fetch from API
        df = self.try_tsetmc_client(symbol, days)
        if df is None:
            df = self.get_daily_data_tsetmc(symbol, days)
        
        # Save to cache
        if df is not None:
            try:
                df.to_csv(cache_path)
                self.logger.info(f"✓ Cached {symbol}")
            except Exception as e:
                self.logger.warning(f"Cache write error: {e}")
        
        return df


# ==============================================================================
# Example Usage & Testing
# ==============================================================================

if __name__ == "__main__":
    print("TSE Connector Examples\n" + "="*60)
    
    # Initialize connector
    print("\n1️⃣  Direct TSE API Connector:")
    tse = TSEConnector()
    
    # Example symbols (these are real TSE symbols)
    test_symbols = ['اساتید', 'شبندر', 'غدیر', 'وبملی']
    
    print(f"   Attempting to retrieve data for: {test_symbols}")
    
    # Try to get data
    for symbol in test_symbols[:2]:  # Test first 2
        df = tse.try_tsetmc_client(symbol)
        if df is None:
            df = tse.get_daily_data_tsetmc(symbol)
        
        if df is not None:
            print(f"   ✓ {symbol}: {len(df)} days, Latest: {df['Close'].iloc[-1]:.0f}")
            break
    else:
        print("   ⚠️  Could not retrieve live data (may need internet/tsetmc-client)")
    
    # Example 2: Cached connector
    print("\n2️⃣  Cached TSE Connector:")
    cached_tse = CachedTSEConnector()
    print(f"   Cache directory: {cached_tse.cache_dir}")
    
    print("\n" + "="*60)
    print("Note: Requires internet connection and working TSE API access")
    print("For production use, combine with data_loader.py for fallback to CSV")
    print("="*60)
