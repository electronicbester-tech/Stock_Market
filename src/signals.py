from dataclasses import dataclass

@dataclass
class Signal:
    symbol: str
    regime: str
    horizon: str
    direction: str
    entry: float
    stop: float
    take: float
    trailing_mult: float
    confidence: float
    notes: str

def not_on_limit(df_row, limit_buffer_pct=0.5):
    # اگر نزدیک سقف/کف دامنه باشد، ورود ممنوع
    # ساده‌سازی: بررسی نسبت فاصله High-Low به Close و جایگاه Close
    # بهتر: دریافت قیمت‌های آستانه دامنه از منبع داده
    high, low, close = df_row['High'], df_row['Low'], df_row['Close']
    range_pct = (high - low) / max(close, 1e-6) * 100
    # اگر دامنه روز بسیار کوچک باشد، ممکن است صف باشد
    near_limit_up = (high - close) / max(close, 1e-6) * 100 < limit_buffer_pct
    near_limit_down = (close - low) / max(close, 1e-6) * 100 < limit_buffer_pct
    return (not near_limit_up) and (not near_limit_down)

def swing(df, symbol, regime, cfg):
    s = df.iloc[-1]; sigs = []
    cond_long = (s['ema9'] > s['ema21']) and (s['rsi'] > 45) and (s['Close'] > df['High'].rolling(3).max().iloc[-2]) \
                and (s['Volume'] >= 1.2 * df['vol_ma20'].iloc[-1])
    if cond_long and regime != 'BEAR' and not_on_limit(s, cfg["filters"]["limit_buffer_pct"]):
        atr = max(s['atr'], 1e-6)
        sigs.append(Signal(symbol, regime, 'SWING', 'LONG', s['Close'], s['Close']-1.5*atr, s['Close']+2.5*atr, 1.0, 0.65,
                           'EMA9>EMA21, RSI>45, 3-day breakout, Volume confirm'))
    cond_short = (s['ema9'] < s['ema21']) and (s['rsi'] < 55) and (s['Close'] < df['Low'].rolling(3).min().iloc[-2]) \
                 and (s['Volume'] >= 1.2 * df['vol_ma20'].iloc[-1])
    if cond_short and regime != 'BULL' and not_on_limit(s, cfg["filters"]["limit_buffer_pct"]):
        atr = max(s['atr'], 1e-6)
        sigs.append(Signal(symbol, regime, 'SWING', 'SHORT', s['Close'], s['Close']+1.5*atr, s['Close']-2.5*atr, 1.0, 0.65,
                           'EMA9<EMA21, RSI<55, 3-day breakdown, Volume confirm'))
    return sigs

def trend(df, symbol, regime):
    s = df.iloc[-1]; sigs = []
    long_ok = (s['Close'] > s['sma50'] > s['sma200']) and (s['macd'] > s['macd_signal']) and (s['macd'] > 0) and (s['adx'] > 20)
    if long_ok and regime != 'BEAR':
        atr = max(s['atr'], 1e-6)
        sigs.append(Signal(symbol, regime, 'TREND', 'LONG', s['Close'], s['sma50']-1.2*atr, s['Close']+3.2*atr, 1.5, 0.7,
                           'Above SMA50/SMA200, MACD+, ADX>20'))
    short_ok = (s['Close'] < s['sma50'] < s['sma200']) and (s['macd'] < s['macd_signal']) and (s['macd'] < 0) and (s['adx'] > 20)
    if short_ok and regime != 'BULL':
        atr = max(s['atr'], 1e-6)
        sigs.append(Signal(symbol, regime, 'TREND', 'SHORT', s['Close'], s['sma50']+1.2*atr, s['Close']-3.2*atr, 1.5, 0.7,
                           'Below SMA50/SMA200, MACD-, ADX>20'))
    return sigs

def position(df, symbol, regime):
    s = df.iloc[-1]; sigs = []
    long_ok = (df['sma50'].iloc[-2] < df['sma200'].iloc[-2]) and (s['sma50'] > s['sma200']) \
              and (s['Close'] > s['donchian_h']) and (s['Volume'] >= 1.3 * df['vol_ma20'].iloc[-1])
    if long_ok and regime == 'BULL':
        atr = max(s['atr'], 1e-6)
        sigs.append(Signal(symbol, regime, 'POSITION', 'LONG', s['Close'], s['donchian_l'], s['Close']+4.2*atr, 2.0, 0.8,
                           'Golden cross + Donchian breakout + Volume'))
    short_ok = (df['sma50'].iloc[-2] > df['sma200'].iloc[-2]) and (s['sma50'] < s['sma200']) \
               and (s['Close'] < s['donchian_l']) and (s['Volume'] >= 1.3 * df['vol_ma20'].iloc[-1])
    if short_ok and regime == 'BEAR':
        atr = max(s['atr'], 1e-6)
        sigs.append(Signal(symbol, regime, 'POSITION', 'SHORT', s['Close'], s['donchian_h'], s['Close']-4.2*atr, 2.0, 0.8,
                           'Death cross + Donchian breakdown + Volume'))
    return sigs
