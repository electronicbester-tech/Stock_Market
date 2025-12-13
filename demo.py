"""
Demo script to test analyze_universe with mock data.
This demonstrates the corrected API.
"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from src.analyze import analyze_universe


def make_mock(symbol="TEST", n=400, base=100):
    """Generate mock OHLCV data."""
    dates = [datetime.today() - timedelta(days=i) for i in range(n)][::-1]
    price = np.cumsum(np.random.randn(n)) + base
    price = np.maximum(price, 10)
    high = price + np.abs(np.random.randn(n) * 2)
    low = price - np.abs(np.random.randn(n) * 2)
    low = np.maximum(low, 0.1)
    vol = np.random.randint(int(5e5), int(5e6), size=n)
    df = pd.DataFrame({
        "Date": dates,
        "Open": price,
        "High": high,
        "Low": low,
        "Close": price,
        "Volume": vol
    }).set_index("Date")
    return df


if __name__ == "__main__":
    print("=" * 60)
    print("Demo: analyze_universe with corrected API")
    print("=" * 60)
    
    # Create sample data
    print("\n1. Creating mock data for 50 symbols + indices...")
    data_dict = {f"نماد{i:02d}": make_mock() for i in range(1, 51)}
    index_dict = {
        "TEDPIX": make_mock("TEDPIX", base=300),
        "EQUAL": make_mock("EQUAL", base=250),
    }
    print(f"   ✓ Created {len(data_dict)} symbols + 2 indices")
    
    # Test 1: Call with explicit index_dict (original behavior)
    print("\n2. Testing analyze_universe(data_dict, index_dict)...")
    try:
        signals, long_top, short_top = analyze_universe(data_dict, index_dict)
        print(f"   ✓ Success! Generated {len(signals)} signals")
        print(f"   ✓ Long top {len(long_top)}: {long_top[:3]}")
        print(f"   ✓ Short top {len(short_top)}: {short_top[:3]}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Test 2: Call without explicit index_dict (new behavior)
    print("\n3. Testing analyze_universe(data_dict) - indices extracted from data...")
    data_dict_with_indices = {**data_dict, "TEDPIX": index_dict["TEDPIX"], "EQUAL": index_dict["EQUAL"]}
    try:
        signals, long_top, short_top = analyze_universe(data_dict_with_indices)
        print(f"   ✓ Success! Generated {len(signals)} signals")
        print(f"   ✓ Long top {len(long_top)}: {long_top[:3]}")
        print(f"   ✓ Short top {len(short_top)}: {short_top[:3]}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Test 3: Error handling (missing indices)
    print("\n4. Testing error handling (missing indices)...")
    try:
        signals, long_top, short_top = analyze_universe(data_dict)  # No indices
        print(f"   ✗ Should have raised ValueError but didn't")
    except ValueError as e:
        print(f"   ✓ Correctly raised ValueError: {e}")
    except Exception as e:
        print(f"   ✗ Unexpected error: {e}")
    
    print("\n" + "=" * 60)
    print("✓ All tests completed successfully!")
    print("=" * 60)
