# test_mock.py
import numpy as np, pandas as pd
from datetime import datetime, timedelta
from src.analyze import analyze_universe

def make_mock(symbol):
    dates = [datetime.today() - timedelta(days=i) for i in range(400)][::-1]
    price = np.cumsum(np.random.randn(len(dates))) + 100
    high = price + np.random.rand(len(dates))*2
    low = price - np.random.rand(len(dates))*2
    vol = np.random.randint(1e5, 5e5, size=len(dates))
    df = pd.DataFrame({"Date":dates,"Open":price,"High":high,"Low":low,"Close":price,"Volume":vol}).set_index("Date")
    return df

symbols = [f"SYM{i:02d}"] * 1  # نمونه یک نماد تکراری برای نمایش قالب
data_dict = {f"SYM{i:02d}": make_mock(f"SYM{i:02d}") for i in range(1, 51)}
signals, long_top, short_top = analyze_universe(data_dict)

print("Top 20 Long:")
for s, r, sc in long_top[:20]:
    print(s, r, round(sc, 3))

print("\nTop 20 Short:")
for s, r, sc in short_top[:20]:
    print(s, r, round(sc, 3))

print("\nSample signals:")
for sig in signals[:10]:
    print(sig)
