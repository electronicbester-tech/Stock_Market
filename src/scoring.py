from src.config import CONFIG

def illiquidity_penalty(df):
    value_traded = (df['Close'] * df['Volume']).iloc[-1]  # تقریبی
    if value_traded < CONFIG["liquidity"]["min_value_traded"]:
        return CONFIG["liquidity"]["penalty_factor"]
    return 0.0

def score_asset(df, regime: str, for_short: bool=False) -> float:
    s = df.iloc[-1]
    atr = max(s['atr'], 1e-6)
    momentum = s['roc10'] / atr
    trend = ((s['Close'] - s['sma200']) / s['sma200']) + s['trend_angle']
    breakout = (s['Close'] - s['bb_h']) / atr if not for_short else (s['bb_l'] - s['Close']) / atr
    risk = atr / s['Close']
    illiq = illiquidity_penalty(df)

    w = CONFIG["weights"][regime]
    return float(w["wm"]*momentum + w["wt"]*trend + w["wb"]*breakout - w["wr"]*risk - w["wl"]*illiq)
