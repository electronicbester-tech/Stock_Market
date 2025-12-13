import pandas as pd

df = pd.read_csv('data/indexes/symbols.csv', skiprows=2, encoding='utf-8', nrows=1)
print('Column count:', len(df.columns))
cols = list(df.columns)
for i in range(min(12, len(cols))):
    print(f"Col {i}: {repr(cols[i])}")
