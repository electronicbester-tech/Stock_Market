#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Monte-Carlo backtester for option candidates.

Reads `options_candidates_{date}.csv` (must include at least: symbol, spot,
strike, T_years, sigma_annual, call_price, put_price, predicted_spot, dividend_yield)
and runs a simple MC to simulate terminal prices under GBM. For each option row
it computes the MC-estimated option price (discounted expected payoff) and the
expected return if buying at the market proxy price. Writes a detailed CSV and
an aggregate per-symbol summary.

This is intentionally simple and useful for quick sanity checks. For production
backtesting you'd want to add path-dependency, transaction costs, realistic
position sizing and slippage.
"""
import math
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def mc_terminal_price(S0, mu, sigma, T, n_sim):
    """Vectorized GBM terminal price sampler.

    S_T = S0 * exp((mu - 0.5*sigma**2)*T + sigma*sqrt(T)*Z)
    """
    Z = np.random.normal(size=n_sim)
    return S0 * np.exp((mu - 0.5 * sigma ** 2) * T + sigma * math.sqrt(T) * Z)


def run_backtest(options_csv: str, out_dir: Path, n_sim: int = 5000, r: float = 0.02):
    df = pd.read_csv(options_csv, encoding='utf-8')
    if df.empty:
        logger.warning('No options to backtest')
        return None

    rows = []
    summary_rows = []

    for _, rrow in df.iterrows():
        try:
            symbol = rrow.get('symbol')
            S0 = float(rrow.get('spot', np.nan))
            K = float(rrow.get('strike', np.nan))
            T = float(rrow.get('T_years', np.nan))
            sigma = float(rrow.get('sigma_annual', 0.0))
            market_call = float(rrow.get('call_price', np.nan))
            market_put = float(rrow.get('put_price', np.nan))
            pred_spot = float(rrow.get('predicted_spot', np.nan)) if not pd.isna(rrow.get('predicted_spot', np.nan)) else np.nan
            div_yield = float(rrow.get('dividend_yield', 0.0)) if not pd.isna(rrow.get('dividend_yield', 0.0)) else 0.0

            # infer drift mu from predicted_spot if available, otherwise use 0.0
            if not np.isnan(pred_spot) and pred_spot > 0 and S0 > 0 and T > 0:
                # continuous log-return implied drift
                mu = math.log(pred_spot / S0) / T
            else:
                mu = 0.0

            # adjust drift for dividend yield
            mu_adj = mu - div_yield

            # sample terminal prices
            S_T = mc_terminal_price(S0, mu_adj, sigma, T, n_sim)

            # call and put payoffs
            call_payoffs = np.maximum(S_T - K, 0.0)
            put_payoffs = np.maximum(K - S_T, 0.0)

            # discounted expected payoff
            mc_call_price = math.exp(-r * T) * np.mean(call_payoffs)
            mc_put_price = math.exp(-r * T) * np.mean(put_payoffs)

            mc_call_std = math.exp(-r * T) * np.std(call_payoffs)
            mc_put_std = math.exp(-r * T) * np.std(put_payoffs)

            # expected returns vs market prices (if market prices available)
            mc_call_return = None
            mc_put_return = None
            if not np.isnan(market_call) and market_call > 0:
                mc_call_return = (mc_call_price - market_call) / market_call
            if not np.isnan(market_put) and market_put > 0:
                mc_put_return = (mc_put_price - market_put) / market_put

            rows.append({
                'symbol': symbol,
                'days_to_expiry': rrow.get('days_to_expiry'),
                'strike': K,
                'spot': S0,
                'predicted_spot': pred_spot,
                'T_years': T,
                'sigma_annual': sigma,
                'dividend_yield': div_yield,
                'market_call': market_call,
                'market_put': market_put,
                'mc_call_price': mc_call_price,
                'mc_put_price': mc_put_price,
                'mc_call_std': mc_call_std,
                'mc_put_std': mc_put_std,
                'mc_call_return_pct': mc_call_return,
                'mc_put_return_pct': mc_put_return
            })

            summary_rows.append({
                'symbol': symbol,
                'strike': K,
                'days_to_expiry': rrow.get('days_to_expiry'),
                'mc_call_price': mc_call_price,
                'mc_put_price': mc_put_price,
                'mc_call_return_pct': mc_call_return,
                'mc_put_return_pct': mc_put_return
            })

        except Exception as e:
            logger.warning(f'Backtest row failed: {e}')

    detailed = pd.DataFrame(rows)
    summary = pd.DataFrame(summary_rows)

    date_str = datetime.now().strftime('%Y-%m-%d')
    detailed_file = out_dir / f'backtest_options_detailed_{date_str}.csv'
    summary_file = out_dir / f'backtest_options_summary_{date_str}.csv'

    detailed.to_csv(detailed_file, index=False, encoding='utf-8')
    summary.to_csv(summary_file, index=False, encoding='utf-8')

    logger.info(f'Backtest detailed saved to {detailed_file} ({len(detailed)} rows)')
    logger.info(f'Backtest summary saved to {summary_file} ({len(summary)} rows)')
    return detailed_file, summary_file


if __name__ == '__main__':
    out_dir = Path('outputs') / datetime.now().strftime('%Y-%m-%d')
    csv = out_dir / f'options_candidates_{datetime.now().strftime("%Y-%m-%d")}.csv'
    if csv.exists():
        run_backtest(str(csv), out_dir, n_sim=5000)
    else:
        logger.error('options_candidates CSV not found; run options_runner first')
