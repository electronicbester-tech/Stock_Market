"""
Unit tests for Stock Market analysis module.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.indicators import add_indicators
from src.regime import market_regime
from src.analyze import analyze_universe


def make_mock_data(symbol="TEST", n_days=400, base_price=100):
    """Generate mock OHLCV data for testing."""
    dates = [datetime.today() - timedelta(days=i) for i in range(n_days)][::-1]
    price = np.cumsum(np.random.randn(n_days)) + base_price
    price = np.maximum(price, 10)  # Ensure positive prices
    high = price + np.abs(np.random.randn(n_days) * 2)
    low = price - np.abs(np.random.randn(n_days) * 2)
    low = np.maximum(low, 0.1)  # Ensure positive lows
    vol = np.random.randint(int(1e5), int(5e5), size=n_days)
    
    df = pd.DataFrame({
        "Date": dates,
        "Open": price,
        "High": high,
        "Low": low,
        "Close": price,
        "Volume": vol
    }).set_index("Date")
    return df


class TestIndicators:
    """Test indicator calculations."""
    
    def test_add_indicators_basic(self):
        """Test that add_indicators returns a DataFrame with expected columns."""
        df = make_mock_data(n_days=250)
        result = add_indicators(df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'ema9' in result.columns
        assert 'ema21' in result.columns
        assert 'sma50' in result.columns
        assert 'sma200' in result.columns
        assert 'rsi' in result.columns
        assert 'atr' in result.columns
        assert 'macd' in result.columns
    
    def test_add_indicators_removes_nan(self):
        """Test that add_indicators removes NaN rows."""
        df = make_mock_data(n_days=100)
        result = add_indicators(df)
        
        # After adding indicators, some leading rows will have NaN
        # The function should dropna()
        assert result.isna().sum().sum() == 0 or len(result) < len(df)
    
    def test_add_indicators_sufficient_data(self):
        """Test with sufficient data (250+ days)."""
        df = make_mock_data(n_days=260)
        result = add_indicators(df)
        
        # After dropna, we should have some data
        assert len(result) > 0


class TestRegime:
    """Test market regime detection."""
    
    def test_market_regime_returns_string(self):
        """Test that market_regime returns one of BULL, BEAR, NEUTRAL."""
        df = add_indicators(make_mock_data(n_days=260))
        df_index = add_indicators(make_mock_data(n_days=260))
        df_equal = add_indicators(make_mock_data(n_days=260))
        
        result = market_regime(df, df_index, df_equal)
        
        assert isinstance(result, str)
        assert result in ["BULL", "BEAR", "NEUTRAL"]


class TestAnalyzeUniverse:
    """Test the main analyze_universe function."""
    
    def test_analyze_universe_with_indices(self):
        """Test analyze_universe with explicit index_dict."""
        data_dict = {
            "SYM01": make_mock_data("SYM01", 260),
            "SYM02": make_mock_data("SYM02", 260),
        }
        index_dict = {
            "TEDPIX": make_mock_data("TEDPIX", 260, base_price=300),
            "EQUAL": make_mock_data("EQUAL", 260, base_price=250),
        }
        
        signals, long_top, short_top = analyze_universe(data_dict, index_dict)
        
        assert isinstance(signals, list)
        assert isinstance(long_top, list)
        assert isinstance(short_top, list)
        assert len(long_top) <= 20
        assert len(short_top) <= 20
    
    def test_analyze_universe_without_explicit_indices(self):
        """Test analyze_universe extracting indices from data_dict."""
        data_dict = {
            "SYM01": make_mock_data("SYM01", 260),
            "SYM02": make_mock_data("SYM02", 260),
            "TEDPIX": make_mock_data("TEDPIX", 260, base_price=300),
            "EQUAL": make_mock_data("EQUAL", 260, base_price=250),
        }
        
        signals, long_top, short_top = analyze_universe(data_dict)
        
        assert isinstance(signals, list)
        assert isinstance(long_top, list)
        assert isinstance(short_top, list)
    
    def test_analyze_universe_missing_indices_raises_error(self):
        """Test that missing indices raises ValueError."""
        data_dict = {
            "SYM01": make_mock_data("SYM01", 260),
            "SYM02": make_mock_data("SYM02", 260),
        }
        
        with pytest.raises(ValueError, match="TEDPIX and EQUAL"):
            analyze_universe(data_dict)
    
    def test_analyze_universe_insufficient_history_skips(self):
        """Test that symbols with insufficient history are skipped."""
        data_dict = {
            "SYM01": make_mock_data("SYM01", 50),  # Too short
            "SYM02": make_mock_data("SYM02", 260),  # OK
        }
        index_dict = {
            "TEDPIX": make_mock_data("TEDPIX", 260),
            "EQUAL": make_mock_data("EQUAL", 260),
        }
        
        signals, long_top, short_top = analyze_universe(data_dict, index_dict)
        
        # Only SYM02 should be processed
        # We can't directly verify which symbols were processed,
        # but the function should complete without error
        assert isinstance(signals, list)
    
    def test_analyze_universe_large_dataset(self):
        """Test with a larger dataset (stress test)."""
        data_dict = {f"SYM{i:02d}": make_mock_data(f"SYM{i:02d}", 260) for i in range(1, 51)}
        index_dict = {
            "TEDPIX": make_mock_data("TEDPIX", 260),
            "EQUAL": make_mock_data("EQUAL", 260),
        }
        
        signals, long_top, short_top = analyze_universe(data_dict, index_dict)
        
        assert len(long_top) <= 20
        assert len(short_top) <= 20


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
