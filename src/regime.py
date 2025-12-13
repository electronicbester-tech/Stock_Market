import pandas as pd

def market_regime(df_symbol: pd.DataFrame, df_index: pd.DataFrame, df_equal: pd.DataFrame) -> str:
    s = df_symbol.iloc[-1]
    idx = df_index.iloc[-1]
    eq = df_equal.iloc[-1]
    idx_sma100 = df_index['Close'].rolling(100).mean().iloc[-1]
    eq_sma100 = df_equal['Close'].rolling(100).mean().iloc[-1]

    up = (s['Close'] > s['sma200']) and (s['trend_angle'] > 0) and (idx['Close'] > idx_sma100) and (eq['Close'] > eq_sma100)
    down = (s['Close'] < s['sma200']) and (s['trend_angle'] < 0) and (idx['Close'] < idx_sma100) and (eq['Close'] < eq_sma100)
    if up: return 'BULL'
    if down: return 'BEAR'
    return 'NEUTRAL'
