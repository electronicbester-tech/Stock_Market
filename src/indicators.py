import pandas as pd
import numpy as np
from ta.trend import EMAIndicator, SMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands

def true_range(df):
    prev_close = df['Close'].shift(1)
    tr1 = df['High'] - df['Low']
    tr2 = (df['High'] - prev_close).abs()
    tr3 = (df['Low'] - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
    df['ema9'] = EMAIndicator(close, 9).ema_indicator()
    df['ema21'] = EMAIndicator(close, 21).ema_indicator()
    df['sma50'] = SMAIndicator(close, 50).sma_indicator()
    df['sma200'] = SMAIndicator(close, 200).sma_indicator()
    macd = MACD(close)
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    df['rsi'] = RSIIndicator(close, 14).rsi()
    bb = BollingerBands(close, window=20, window_dev=2)
    df['bb_h'] = bb.bollinger_hband()
    df['bb_l'] = bb.bollinger_lband()
    df['donchian_h'] = high.rolling(20).max()
    df['donchian_l'] = low.rolling(20).min()
    df['vol_ma20'] = vol.rolling(20).mean()
    df['roc10'] = close.pct_change(10)
    df['trend_angle'] = df['sma200'].pct_change(5)
    # ATR واقعی (Wilder)
    tr = true_range(df)
    df['atr'] = tr.ewm(alpha=1/14, adjust=False).mean()
    # ADX
    adx = ADXIndicator(high, low, close, window=14)
    df['adx'] = adx.adx()
    return df.dropna()
