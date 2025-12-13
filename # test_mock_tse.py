# test_mock_tse.py
import numpy as np, pandas as pd
from datetime import datetime, timedelta
from src.analyze import analyze_universe

def make_mock(n=400, base=100):
    dates = [datetime.today() - timedelta(days=i) for i in range(n)][::-1]
    price = np.cumsum(np.random.randn(n)) + base
    high = price + np.random.rand(n)*2
    low = price - np.random.rand(n)*2
    vol = np.random.randint(5e5, 5e6, size=n)
    df = pd.DataFrame({"Date":dates,"Open":price,"High":high,"Low":low,"Close":price,"Volume":vol}).set_index("Date")
    return df

symbols = [f"نماد{i:02d}" for i in range(1, 60)]
data_dict = {sym: make_mock() for sym in symbols}
index_dict = {"TEDPIX": make_mock(base=300), "EQUAL": make_mock(base=250)}

signals, long_top, short_top = analyze_universe(data_dict, index_dict)

print("Top 20 Long:")
for s, r, sc in long_top[:20]: print(s, r, round(sc, 3))

print("\nTop 20 Short:")
for s, r, sc in short_top[:20]: print(s, r, round(sc, 3))

print("\nSample signals:")
for sig in signals[:12]: print(sig)
