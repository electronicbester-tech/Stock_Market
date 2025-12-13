#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Create review lists for included/excluded symbols so user can audit removals
"""
from src.data_loader import HybridDataManager
from src.bottom_seeker import BottomSeeker
from datetime import datetime
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_review(universe_n=800):
    hybrid = HybridDataManager()
    # load mapping CSV for names
    try:
        csv_df = pd.read_csv('data/indexes/symbols.csv', skiprows=2, encoding='utf-8')
        csv_df.columns = csv_df.columns.str.strip()
        name_map = {str(r['نماد']).strip(): str(r.get('نام','')).strip() for _, r in csv_df.iterrows()}
    except Exception:
        name_map = {}

    seeker = BottomSeeker(universe_n=universe_n, min_gain=0.20, prefer_db=False)
    data = seeker.hybrid.load_data(top_n=universe_n, prefer_db=False)

    included = []
    excluded = []

    for sym, df in data.items():
        reason = 'ok'
        # lookup name
        name = name_map.get(sym, '')
        # blacklist check
        if any(kw.upper() in str(name).upper() for kw in seeker.name_blacklist):
            reason = 'blacklist_name'
        elif df.empty or len(df) < 60:
            reason = 'insufficient_history'
        else:
            # compute indicators quickly
            close = df['Close']
            price = float(close.iloc[-1])
            low52 = float(close.tail(260).min()) if len(close) >= 50 else float(close.min())
            high50 = float(close.tail(50).max()) if len(close) >= 50 else float(close.max())
            potential_target = min(high50, price * 1.5)
            potential_gain_pct = (potential_target - price) / price if price > 0 else 0

            # quick RSI
            from src.bottom_seeker import rsi, macd
            rsi14 = float(rsi(close, 14).iloc[-1]) if len(close) >= 15 else None
            macd_line, signal_line, hist = macd(close)
            macd_trend = False
            if len(hist) >= 4:
                last_hist = hist.iloc[-4:]
                macd_trend = (last_hist.diff().dropna() > 0).all()

            near_low = price <= (1.10 * low52)
            oversold = (rsi14 is not None and rsi14 <= 40)
            momentum_building = macd_trend
            signals = sum([near_low, oversold, momentum_building])

            if (signals >= 2 and potential_gain_pct >= seeker.min_gain) or (oversold and potential_gain_pct >= seeker.min_gain):
                reason = 'selected'
            else:
                reason = 'no_signals_or_low_gain'

        if reason == 'selected':
            included.append({'symbol': sym, 'name': name, 'reason': reason})
        else:
            excluded.append({'symbol': sym, 'name': name, 'reason': reason})

    inc_df = pd.DataFrame(included)
    exc_df = pd.DataFrame(excluded)

    out_base = f'candidates_review_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    inc_fn = out_base + '_included.csv'
    exc_fn = out_base + '_excluded.csv'
    inc_df.to_csv(inc_fn, index=False, encoding='utf-8')
    exc_df.to_csv(exc_fn, index=False, encoding='utf-8')

    logger.info(f'✅ Review files saved: {inc_fn} ({len(inc_df)}) and {exc_fn} ({len(exc_df)})')


if __name__ == '__main__':
    run_review(800)
