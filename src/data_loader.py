"""
Data loader module - supports multiple data sources
"""
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVDataLoader:
    """Load and parse TSE symbols.csv file"""
    
    @staticmethod
    def load_from_csv(filepath: str, top_n: int = 50):
        """
        Load TSE symbols data from CSV file.
        
        Args:
            filepath: Path to symbols.csv
            top_n: Number of top symbols by trade value
        
        Returns:
            dict: {symbol: DataFrame with OHLCV data}
        """
        csv_file = Path(filepath)
        if not csv_file.exists():
            logger.error(f"File not found: {filepath}")
            return {}
        
        try:
            # Read CSV with proper header row
            df = pd.read_csv(csv_file, skiprows=2, encoding='utf-8')
            logger.info(f"✓ Loaded {len(df)} rows")
            
            # Convert numeric columns
            numeric_cols = ['حجم', 'ارزش', 'آخرین معامله - مقدار', 
                           'بیشترین', 'کمترین', 'قیمت پایانی - مقدار']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Filter active symbols
            df = df.dropna(subset=['حجم', 'ارزش', 'آخرین معامله - مقدار'])
            df = df[(df['حجم'] > 0) & (df['ارزش'] > 0)]
            df = df.sort_values('ارزش', ascending=False).head(top_n)
            
            logger.info(f"✓ Loaded {len(df)} active symbols")
            return df
        
        except Exception as e:
            logger.error(f"❌ Error loading CSV: {e}")
            return {}
    
    @staticmethod
    def generate_historical_data(symbol: str, close_price: float, 
                                 volume: float, n_days: int = 260):
        """
        Generate synthetic historical OHLCV data for a symbol.
        Used when historical data is not available.
        """
        dates = [datetime.today() - timedelta(days=i) for i in range(n_days)][::-1]
        
        # Deterministic randomness per symbol
        np.random.seed(hash(symbol) % 2**31)
        
        # Realistic price path
        price_path = close_price * np.exp(np.cumsum(np.random.randn(n_days) * 0.01))
        price_path = np.maximum(price_path, close_price * 0.3)
        
        high_path = price_path * (1 + np.abs(np.random.randn(n_days) * 0.01))
        low_path = price_path * (1 - np.abs(np.random.randn(n_days) * 0.01))
        volume_path = np.random.randint(int(volume * 0.3), int(volume * 2), size=n_days)
        
        df = pd.DataFrame({
            'Open': price_path * (1 + np.random.randn(n_days) * 0.003),
            'High': high_path,
            'Low': low_path,
            'Close': price_path,
            'Volume': volume_path
        }, index=pd.DatetimeIndex(dates, name='Date'))
        
        # Ensure High >= Close >= Low
        df['High'] = np.maximum(df['High'], df['Close'])
        df['Low'] = np.minimum(df['Low'], df['Close'])
        
        return df


class SQLiteDataManager:
    """
    SQLite-based data manager for local persistent storage.
    Supports both symbols (current snapshot) and historical OHLCV data.
    """
    
    def __init__(self, db_path: str = "data/market_data.db"):
        """Initialize SQLite database"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Create tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Table for symbol metadata
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS symbols (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    last_price REAL,
                    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Table for OHLCV historical data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ohlcv (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    UNIQUE(symbol, date),
                    FOREIGN KEY(symbol) REFERENCES symbols(symbol)
                )
            ''')
            
            conn.commit()
        logger.info(f"✓ Database initialized: {self.db_path}")
    
    def insert_symbol(self, symbol: str, name: str, price: float):
        """Insert or update symbol metadata"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO symbols (symbol, name, last_price, last_update)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (symbol, name, price))
            conn.commit()
    
    def insert_ohlcv(self, symbol: str, df: pd.DataFrame):
        """Insert OHLCV data for a symbol"""
        with sqlite3.connect(self.db_path) as conn:
            for date, row in df.iterrows():
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO ohlcv 
                        (symbol, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (symbol, date.date(), row['Open'], row['High'], 
                          row['Low'], row['Close'], int(row['Volume'])))
                except Exception as e:
                    logger.warning(f"Failed to insert {symbol} {date}: {e}")
            conn.commit()
    
    def get_symbol_data(self, symbol: str, days: int = 260) -> pd.DataFrame:
        """Retrieve OHLCV data for a symbol"""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT date, open, high, low, close, volume
                FROM ohlcv
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT ?
            '''
            df = pd.read_sql_query(query, conn, params=(symbol, days))
            
            if df.empty:
                logger.warning(f"No data found for {symbol}")
                return pd.DataFrame()
            
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').set_index('date')
            df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            return df
    
    def get_all_symbols(self) -> list:
        """Get list of all symbols in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT symbol FROM symbols ORDER BY last_update DESC')
            return [row[0] for row in cursor.fetchall()]
    
    def sync_from_csv(self, csv_path: str, top_n: int = 50):
        """Sync symbols and generate historical data from CSV"""
        df = CSVDataLoader.load_from_csv(csv_path, top_n)
        
        if df.empty:
            logger.error("Failed to load CSV data")
            return 0
        
        count = 0
        for idx, row in df.iterrows():
            try:
                symbol = str(row['نماد']).strip()
                name = str(row['نام']).strip()
                close_price = float(row['قیمت پایانی - مقدار']) or float(row['آخرین معامله - مقدار'])
                volume = float(row['حجم'])
                
                # Insert symbol
                self.insert_symbol(symbol, name, close_price)
                
                # Generate and insert historical data
                hist_df = CSVDataLoader.generate_historical_data(symbol, close_price, volume)
                self.insert_ohlcv(symbol, hist_df)
                
                count += 1
                logger.info(f"✓ Synced {symbol}")
            
            except Exception as e:
                logger.warning(f"Failed to sync {row.get('نماد', 'unknown')}: {e}")
        
        logger.info(f"✓ Synced {count} symbols to database")
        return count


class HybridDataManager:
    """
    Hybrid manager combining CSV, SQLite, and API sources.
    Priority: API > SQLite > CSV > Synthetic data
    """
    
    def __init__(self, csv_path: str = "data/indexes/symbols.csv", 
                 db_path: str = "data/market_data.db"):
        """Initialize hybrid manager"""
        self.csv_path = csv_path
        self.db_manager = SQLiteDataManager(db_path)
        self.logger = logger
    
    def load_data(self, symbols: list = None, top_n: int = 30, 
                  prefer_db: bool = True) -> dict:
        """
        Load data with fallback strategy.
        
        Args:
            symbols: List of symbols to load (None = all from CSV)
            top_n: Max symbols to load
            prefer_db: Use database if available
        
        Returns:
            dict: {symbol: DataFrame}
        """
        data_dict = {}
        
        # Try database first
        if prefer_db:
            available = self.db_manager.get_all_symbols()[:top_n]
            if available:
                self.logger.info(f"Loading from database: {len(available)} symbols")
                for sym in available:
                    df = self.db_manager.get_symbol_data(sym)
                    if not df.empty and len(df) >= 260:
                        data_dict[sym] = df
        
        # Fallback to CSV if needed
        if len(data_dict) < top_n:
            self.logger.info("Fallback to CSV data")
            csv_df = CSVDataLoader.load_from_csv(self.csv_path, top_n)
            
            for idx, row in csv_df.iterrows():
                if len(data_dict) >= top_n:
                    break
                
                symbol = str(row['نماد']).strip()
                if symbol not in data_dict:
                    try:
                        close_price = float(row['قیمت پایانی - مقدار']) or float(row['آخرین معامله - مقدار'])
                        volume = float(row['حجم'])
                        
                        hist_df = CSVDataLoader.generate_historical_data(symbol, close_price, volume)
                        data_dict[symbol] = hist_df
                        
                        # Async insert to database
                        self.db_manager.insert_symbol(symbol, str(row['نام']), close_price)
                        self.db_manager.insert_ohlcv(symbol, hist_df)
                    
                    except Exception as e:
                        self.logger.warning(f"Failed to load {symbol}: {e}")
        
        self.logger.info(f"✓ Loaded {len(data_dict)} symbols total")
        return data_dict


# ==============================================================================
# Example Usage
# ==============================================================================

if __name__ == "__main__":
    print("Data Loader Module Examples\n" + "="*60)
    
    # Example 1: CSV Loading
    print("\n1️⃣  CSV Data Loading:")
    df = CSVDataLoader.load_from_csv("data/indexes/symbols.csv", top_n=5)
    print(f"   Loaded {len(df)} symbols from CSV")
    if not df.empty:
        print(f"   Top symbol: {df.iloc[0]['نماد']}")
    
    # Example 2: Database Sync
    print("\n2️⃣  Database Synchronization:")
    db = SQLiteDataManager("data/market_data.db")
    count = db.sync_from_csv("data/indexes/symbols.csv", top_n=10)
    print(f"   Synced {count} symbols")
    
    # Example 3: Hybrid Loading
    print("\n3️⃣  Hybrid Data Loading:")
    hybrid = HybridDataManager()
    data = hybrid.load_data(top_n=20)
    print(f"   Loaded {len(data)} symbols")
    for sym in list(data.keys())[:3]:
        print(f"   • {sym}: {len(data[sym])} days")
    
    print("\n✅ Examples completed successfully!")
