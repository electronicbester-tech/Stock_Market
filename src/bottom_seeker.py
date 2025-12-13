#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bottom Seeker: find stocks near bottoms with growth potential
Criteria (configurable):
 - Load up to `universe_n` symbols (default 800) via HybridDataManager
 - Compute indicators: SMA20, SMA50, SMA200, RSI14, MACD (12,26,9)
 - 52-week low/high from available history
 - potential_gain = min(50-day high, price*1.5) - price (conservative target)
 - Selection rules (require 2 of 3):
    * price within 1.10 * 52w_low (near low)
    * RSI14 <= 40 (oversold)
    * MACD histogram increasing last 3 days (momentum building)
 - Also require estimated potential_gain_pct >= min_gain (default 0.20)

Returns ranked candidates sorted by potential_gain_pct desc

Exports: DataFrame with symbol,name,sector,price,volume,indicators,potential_gain_pct
"""

from typing import Dict, Any
from pathlib import Path
import pandas as pd
import numpy as np
from src.data_loader import HybridDataManager
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Indicator helpers

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).mean()


def rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/window, adjust=False).mean()
    ma_down = down.ewm(alpha=1/window, adjust=False).mean()
    rs = ma_up / ma_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


class BottomSeeker:
    def __init__(self, universe_n: int = 800, min_gain: float = 0.20, prefer_db: bool = True):
        self.universe_n = universe_n
        self.min_gain = min_gain
        self.hybrid = HybridDataManager(top_n=universe_n) if False else HybridDataManager()
        self.prefer_db = prefer_db
        # basic blacklist to exclude clearly non-equity instruments
        # Note: allow general 'صندوق' (funds) and 'ETF' except when they explicitly indicate fixed-income
        self.name_blacklist = [
            'مرابحه', 'صکو', 'اوراق', 'درآمد ثابت', 'قرضه',
            'تسه', 'تس', 'اختیار', 'حق', 'ص.س', 'صکوك'
        ]
        # list of removed symbols with reasons for later review
        self.removed = []
        # load CSV mapping symbol->name to help filtering when historical df lacks name
        try:
            import pandas as _pd
            csv_path = "data/indexes/symbols.csv"
            if _pd.io.common.file_exists(csv_path):
                csv_df = _pd.read_csv(csv_path, skiprows=2, encoding='utf-8')
                csv_df.columns = csv_df.columns.str.strip()
                self.symbol_name_map = {str(r['نماد']).strip(): str(r.get('نام', '')).strip() for _, r in csv_df.iterrows()}
            else:
                self.symbol_name_map = {}
        except Exception:
            self.symbol_name_map = {}

    def analyze_universe(self) -> pd.DataFrame:
        logger.info(f"Loading up to {self.universe_n} symbols (prefer_db={self.prefer_db})")
        data = self.hybrid.load_data(top_n=self.universe_n, prefer_db=self.prefer_db)
        logger.info(f"Computing indicators for {len(data)} symbols")

        rows = []
        for sym, df in data.items():
            try:
                # Determine human-readable name (prefer mapping from CSV)
                symbol_name = self.symbol_name_map.get(sym, None)
                if not symbol_name:
                    # try to find name column in df
                    if 'name' in df.columns:
                        symbol_name = df.iloc[0].get('name', sym)
                    else:
                        symbol_name = sym
                name_upper = str(symbol_name).upper()
                if any(kw.upper() in name_upper for kw in self.name_blacklist):
                    # record non-equity instruments and skip
                    self.removed.append({'symbol': sym, 'name': symbol_name, 'reason': 'blacklist'})
                    continue

                if df.empty or len(df) < 60:
                    self.removed.append({'symbol': sym, 'name': symbol_name, 'reason': 'insufficient_history'})
                    continue

                close = df['Close']
                vol = df['Volume']

                price = float(close.iloc[-1])
                avg_vol_30 = int(vol.tail(30).mean()) if len(vol) >= 30 else int(vol.mean())

                sma20 = float(sma(close, 20).iloc[-1]) if len(close) >= 20 else np.nan
                sma50 = float(sma(close, 50).iloc[-1]) if len(close) >= 50 else np.nan
                sma200 = float(sma(close, 200).iloc[-1]) if len(close) >= 200 else np.nan

                rsi14 = float(rsi(close, 14).iloc[-1]) if len(close) >= 15 else np.nan
                macd_line, signal_line, hist = macd(close)
                macd_hist = hist

                # 52-week (approx 260 trading days) low/high
                low52 = float(close.tail(260).min()) if len(close) >= 50 else float(close.min())
                high52 = float(close.tail(260).max()) if len(close) >= 50 else float(close.max())

                # Recent 50-day high (used for conservative target)
                high50 = float(close.tail(50).max()) if len(close) >= 50 else high52

                # potential target: min(high50, price * 1.5)
                potential_target = min(high50, price * 1.5)
                potential_gain_pct = (potential_target - price) / price if price > 0 else 0

                # MACD histogram trend (increasing last 3)
                macd_trend = False
                if len(macd_hist) >= 4:
                    last_hist = macd_hist.iloc[-4:]
                    macd_trend = (last_hist.diff().dropna() > 0).all()

                # Signals: near low, oversold, macd building
                near_low = price <= (1.10 * low52)
                oversold = not np.isnan(rsi14) and rsi14 <= 40
                momentum_building = macd_trend

                signals = sum([near_low, oversold, momentum_building])

                # select if at least 2 signals OR oversold+potential
                passed = (signals >= 2 and potential_gain_pct >= self.min_gain) or (
                    oversold and potential_gain_pct >= self.min_gain)

                # Additional filter: volume not zero
                if passed and avg_vol_30 <= 0:
                    passed = False
                    # record low liquidity
                    self.removed.append({'symbol': sym, 'name': symbol_name, 'reason': 'low_volume'})

                # compute helper metrics
                try:
                    recent = close[-260:]
                    idx_min = recent.idxmin()
                    days_since_low = (pd.to_datetime(close.index[-1]) - pd.to_datetime(idx_min)).days
                except Exception:
                    days_since_low = None

                try:
                    distance_from_low_pct = float((price - low52) / low52 * 100) if low52 > 0 else None
                except Exception:
                    distance_from_low_pct = None

                # Resistances: use recent 50-day high and 52-week high
                resistance_50 = high50
                resistance_52 = high52
                try:
                    nearest_res = min([x for x in [resistance_50, resistance_52] if x is not None])
                    resistance_distance_pct = float((nearest_res - price) / price * 100) if price > 0 else None
                except Exception:
                    resistance_distance_pct = None
                # Additional short-term metrics
                try:
                    returns = close.pct_change().dropna()
                    volatility_14d = float(returns.tail(14).std()) if len(returns) >= 7 else None
                    momentum_5d = float((close.iloc[-1] - close.iloc[-6]) / close.iloc[-6]) if len(close) >= 6 and close.iloc[-6] != 0 else None
                except Exception:
                    volatility_14d = None
                    momentum_5d = None

                rows.append({
                    'symbol': sym,
                    'price': price,
                    'volume': int(avg_vol_30) if not np.isnan(avg_vol_30) else None,
                    'sma20': sma20,
                    'sma50': sma50,
                    'sma200': sma200,
                    'rsi14': rsi14,
                    'low52': low52,
                    'high52': high52,
                    'high50': high50,
                    'potential_target': potential_target,
                    'potential_gain_pct': potential_gain_pct,
                    'near_low': near_low,
                    'oversold': oversold,
                    'macd_building': momentum_building,
                    'signals_count': signals,
                    'passed': passed,
                    'days_since_52w_low': days_since_low,
                    'distance_from_low_pct': distance_from_low_pct,
                    'resistance_50': resistance_50,
                    'resistance_52': resistance_52,
                    'resistance_distance_pct': resistance_distance_pct,
                    'volatility_14d': volatility_14d,
                    'momentum_5d': momentum_5d
                })

            except Exception as e:
                logger.warning(f"Error processing {sym}: {e}")
                self.removed.append({'symbol': sym, 'name': symbol_name if 'symbol_name' in locals() else sym, 'reason': f'error:{e}'})

        out_df = pd.DataFrame(rows)
        # keep only passed
        candidates = out_df[out_df['passed']].copy()
        if candidates.empty:
            logger.info("No candidates found with current rules")
            return candidates, out_df

        candidates['potential_gain_pct'] = candidates['potential_gain_pct'].astype(float)
        candidates = candidates.sort_values('potential_gain_pct', ascending=False)
        # return both the passed candidates and the full universe dataframe (for fallbacks and review)
        return candidates, out_df


if __name__ == '__main__':
    # Run two modes: conservative (stricter) and aggressive (looser). Ensure 20 symbols each by fallback.
    def ensure_n(selected_df: pd.DataFrame, full_df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
        # Ensure `n` rows by filling from full_df using a selection_score when needed.
        if selected_df is None:
            selected_df = pd.DataFrame()
        selected_df = selected_df.copy()
        if len(selected_df) >= n:
            return selected_df.head(n)
        need = n - len(selected_df)
        extras = full_df[~full_df['symbol'].isin(selected_df.get('symbol', []))].copy()
        # compute auxiliary scores if not present
        if 'momentum_5d' in extras.columns:
            # momentum_score: normalize 5-day momentum into 0-1 via tanh-like scaling
            extras['momentum_score'] = extras['momentum_5d'].fillna(0).astype(float).apply(lambda x: np.tanh(x * 5))
        else:
            extras['momentum_score'] = 0
        # liquidity_score: based on avg 30-day volume (log scale), capped
        extras['liquidity'] = extras['volume'].fillna(0).astype(float)
        # winsorize liquidity to reduce manipulation influence
        lower = extras['liquidity'].quantile(0.01)
        upper = extras['liquidity'].quantile(0.99)
        extras['liquidity_w'] = extras['liquidity'].clip(lower=lower, upper=upper)
        extras['liquidity_score'] = extras['liquidity_w'].apply(lambda v: min(1.0, np.log1p(v) / np.log1p(1e7)))
        # volatility penalty: lower is better
        extras['volatility_14d'] = extras.get('volatility_14d', np.nan)
        extras['vol_score'] = extras['volatility_14d'].fillna(0).apply(lambda v: 1.0 / (1.0 + v))

        # selection_score: combine potential_gain_pct (main), momentum, liquidity, and vol_score
        # Increase liquidity influence slightly (but robustified) because liquidity matters
        extras['selection_score'] = (
            extras['potential_gain_pct'].fillna(0).astype(float) * 0.55
            + extras['momentum_score'] * 0.15
            + extras['liquidity_score'] * 0.25
            + extras['vol_score'] * 0.05
        )

        extras = extras.sort_values('selection_score', ascending=False)
        extras = extras.head(need)
        result = pd.concat([selected_df, extras], ignore_index=True)
        return result.head(n)


    date_str = datetime.now().strftime('%Y-%m-%d')
    out_dir = Path('outputs') / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    # Conservative run (produce configurable sizes: 5 and 10 for professional lists)
    seeker_cons = BottomSeeker(universe_n=800, min_gain=0.25, prefer_db=True)
    cons_candidates, cons_full = seeker_cons.analyze_universe()
    cons_top5 = ensure_n(cons_candidates, cons_full, n=5)
    cons_top10 = ensure_n(cons_candidates, cons_full, n=10)

    # Aggressive run
    seeker_agg = BottomSeeker(universe_n=800, min_gain=0.15, prefer_db=True)
    agg_candidates, agg_full = seeker_agg.analyze_universe()
    agg_top5 = ensure_n(agg_candidates, agg_full, n=5)
    agg_top10 = ensure_n(agg_candidates, agg_full, n=10)

    # Also produce and save main 20-item lists (for testing phase)
    cons_top20 = ensure_n(cons_candidates, cons_full, n=20)
    agg_top20 = ensure_n(agg_candidates, agg_full, n=20)

    # Save conservative and aggressive outputs for 5 and 10 sizes
    cons5_csv = out_dir / f'test_cons_conservative_top5_{date_str}.csv'
    cons5_json = out_dir / f'test_cons_conservative_top5_{date_str}.json'
    cons10_csv = out_dir / f'test_cons_conservative_top10_{date_str}.csv'
    cons10_json = out_dir / f'test_cons_conservative_top10_{date_str}.json'

    agg5_csv = out_dir / f'test_cons_aggressive_top5_{date_str}.csv'
    agg5_json = out_dir / f'test_cons_aggressive_top5_{date_str}.json'
    agg10_csv = out_dir / f'test_cons_aggressive_top10_{date_str}.csv'
    agg10_json = out_dir / f'test_cons_aggressive_top10_{date_str}.json'

    cons_top5.to_csv(cons5_csv, index=False, encoding='utf-8')
    cons_top5.to_json(cons5_json, force_ascii=False, orient='records', indent=2)
    cons_top10.to_csv(cons10_csv, index=False, encoding='utf-8')
    cons_top10.to_json(cons10_json, force_ascii=False, orient='records', indent=2)

    agg_top5.to_csv(agg5_csv, index=False, encoding='utf-8')
    agg_top5.to_json(agg5_json, force_ascii=False, orient='records', indent=2)
    agg_top10.to_csv(agg10_csv, index=False, encoding='utf-8')
    agg_top10.to_json(agg10_json, force_ascii=False, orient='records', indent=2)

    # Save the 20-item primary files (test mode)
    cons20_csv = out_dir / f'test_01_conservative_{date_str}.csv'
    cons20_json = out_dir / f'test_01_conservative_{date_str}.json'
    agg20_csv = out_dir / f'test_01_aggressive_{date_str}.csv'
    agg20_json = out_dir / f'test_01_aggressive_{date_str}.json'
    cons_top20.to_csv(cons20_csv, index=False, encoding='utf-8')
    cons_top20.to_json(cons20_json, force_ascii=False, orient='records', indent=2)
    agg_top20.to_csv(agg20_csv, index=False, encoding='utf-8')
    agg_top20.to_json(agg20_json, force_ascii=False, orient='records', indent=2)

    # Persist removals (union of both seekers' removed lists and duplicates removed)
    removals = seeker_cons.removed + seeker_agg.removed
    try:
        rem_df = pd.DataFrame(removals).drop_duplicates(subset=['symbol'])
        rem_csv = out_dir / f'removed_{date_str}.csv'
        rem_json = out_dir / f'removed_{date_str}.json'
        rem_df.to_csv(rem_csv, index=False, encoding='utf-8')
        rem_df.to_json(rem_json, force_ascii=False, orient='records', indent=2)
    except Exception:
        logger.warning('Failed to save removals file')

    # Growth candidates: symbols (not necessarily at bottom) with potential to rise 20-30% from now
    try:
        universe = agg_full.copy()
        # take from the full universe those with potential 20-30%
        gc = universe[(universe['potential_gain_pct'] >= 0.20) & (universe['potential_gain_pct'] <= 0.30)].copy()
        # prefer ones with momentum or SMA50>SMA200
        cond_momentum = gc['macd_building'] == True
        cond_sma = (~gc['sma50'].isna()) & (~gc['sma200'].isna()) & (gc['sma50'] > gc['sma200'])
        # allow also funds/ETFs (we relaxed blacklist) but exclude fixed-income explicitly by name
        cond_not_fixed_income = ~gc['symbol'].astype(str).str.contains('درآمد ثابت')
        gc = gc[(cond_momentum | cond_sma) & cond_not_fixed_income]
        gc = gc.sort_values('potential_gain_pct', ascending=False).head(20)
        growth_csv = out_dir / f'growth_candidates_{date_str}.csv'
        growth_json = out_dir / f'growth_candidates_{date_str}.json'
        gc.to_csv(growth_csv, index=False, encoding='utf-8')
        gc.to_json(growth_json, force_ascii=False, orient='records', indent=2)
    except Exception as e:
        logger.warning(f'Failed to produce growth candidates: {e}')

    # Bottom-sharp candidates: symbols at/very-near 52w low with signs of imminent sharp rise
    try:
        universe = agg_full.copy()
        # stricter near-low threshold (5% above 52w low)
        near_low_strict = universe['price'] <= (1.05 * universe['low52'])
        cond_potential_high = universe['potential_gain_pct'] >= 0.30
        cond_momentum = universe['macd_building'] == True
        cond_oversold = universe['rsi14'] <= 40
        cond_sma = (~universe['sma50'].isna()) & (~universe['sma200'].isna()) & (universe['sma50'] > universe['sma200'])
        bs = universe[near_low_strict & cond_potential_high & (cond_momentum | cond_oversold | cond_sma)].copy()
        bs = bs.sort_values(['potential_gain_pct', 'distance_from_low_pct'], ascending=[False, True]).head(20)
        bs_csv = out_dir / f'bottom_sharp_{date_str}.csv'
        bs_json = out_dir / f'bottom_sharp_{date_str}.json'
        bs.to_csv(bs_csv, index=False, encoding='utf-8')
        bs.to_json(bs_json, force_ascii=False, orient='records', indent=2)
    except Exception as e:
        logger.warning(f'Failed to produce bottom_sharp candidates: {e}')

    logger.info(f"Saved outputs to {out_dir}")
