#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Options model scaffold (Black-Scholes baseline + simple forecast)
Provides:
 - forecast_volatility(close_series, window=30) -> annual sigma
 - predict_price_gbm(close_series, days) -> expected price (GBM expectation)
 - black_scholes_price(S, K, T, r, sigma, option='call') -> theoretical price
 - sample runner to generate option table for a given symbol and expiries

This is a conservative starting point: Black-Scholes for European-style options,
forecast volatility from historical returns (annualized), and predicted underlying
via geometric Brownian motion expectation. For higher accuracy we can add
Monte-Carlo, GARCH, or implied-volatility based methods later.
"""
from math import log, sqrt, exp
from math import erf
import numpy as np
import pandas as pd
from datetime import datetime


def _N(x):
    # Standard normal CDF using erf
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def forecast_volatility(close_series: pd.Series, window: int = 30) -> float:
    """Estimate annualized volatility (sigma) from historical log returns.
    Returns sigma (annualized, e.g., 0.25 for 25%).
    """
    prices = close_series.dropna()
    if len(prices) < 5:
        return float('nan')
    logrets = np.log(prices / prices.shift(1)).dropna()
    if len(logrets) < 2:
        return float('nan')
    w = int(min(window, len(logrets)))
    sigma_daily = logrets.tail(w).std()
    sigma_annual = sigma_daily * np.sqrt(252.0)
    return float(sigma_annual)


def predict_price_gbm(close_series: pd.Series, days: int) -> float:
    """Predict expected price at `days` ahead using geometric Brownian motion expectation:
    E[S_T] = S0 * exp(mu * T) where mu is historical mean log-return annualized.
    """
    prices = close_series.dropna()
    if len(prices) < 2:
        return float('nan')
    logrets = np.log(prices / prices.shift(1)).dropna()
    mu_daily = float(logrets.mean())
    mu_annual = mu_daily * 252.0
    T = days / 252.0
    S0 = float(prices.iloc[-1])
    expected = S0 * exp(mu_annual * T)
    return float(expected)


def black_scholes_price(S: float, K: float, T: float, r: float, sigma: float, option: str = 'call') -> float:
    """Black-Scholes price for European option.
    S: spot price
    K: strike price
    T: time to expiry in years
    r: risk-free rate (decimal)
    sigma: annual volatility (decimal)
    option: 'call' or 'put'
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    if option == 'call':
        price = S * _N(d1) - K * exp(-r * T) * _N(d2)
    else:
        price = K * exp(-r * T) * _N(-d2) - S * _N(-d1)
    return float(price)


def monte_carlo_option_price(S: float, K: float, T: float, r: float, sigma: float, option: str = 'call', n_sim: int = 2000, mu: float = None, dividend_yield: float = 0.0, random_seed: int = None) -> float:
    """Estimate option price via Monte-Carlo under GBM dynamics.
    S: spot
    K: strike
    T: years
    r: risk-free rate
    sigma: annual volatility
    option: 'call' or 'put'
    n_sim: number of simulated paths
    mu: expected annual drift; if None, assume r - dividend_yield (risk-neutral)
    dividend_yield: continuous dividend yield q
    Returns discounted expected payoff (float)
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    rng = np.random.default_rng(random_seed)
    dt = T
    if mu is None:
        mu = r - dividend_yield
    # simulate terminal price S_T under GBM: S0 * exp((mu - 0.5*sigma^2)*T + sigma*sqrt(T)*Z)
    z = rng.standard_normal(n_sim)
    S_T = S * np.exp((mu - 0.5 * sigma * sigma) * dt + sigma * np.sqrt(dt) * z)
    if option == 'call':
        payoffs = np.maximum(S_T - K, 0.0)
    else:
        payoffs = np.maximum(K - S_T, 0.0)
    discounted = np.exp(-r * T) * payoffs
    return float(np.mean(discounted))


def expected_option_return(mc_price: float, market_price: float) -> float:
    """Compute expected return percent of option based on MC price vs market price.
    If market_price is zero or None, returns nan."""
    try:
        if market_price is None or market_price <= 0:
            return float('nan')
        return (mc_price - market_price) / market_price
    except Exception:
        return float('nan')


def generate_option_table(symbol: str, df_close: pd.Series, strikes_pct: list, expiries_days: list, r: float = 0.02):
    """Generate a table of option theoretic prices and predicted underlying.
    strikes_pct: list of strike multipliers relative to spot (e.g., [0.9, 1.0, 1.1])
    expiries_days: list of days to expiry (e.g., [30, 60, 90])
    returns pandas DataFrame
    """
    S0 = float(df_close.dropna().iloc[-1])
    rows = []
    for days in expiries_days:
        T = max(days / 252.0, 1/252.0)
        sigma = forecast_volatility(df_close, window=30)
        predicted = predict_price_gbm(df_close, days)
        for sp in strikes_pct:
            K = S0 * sp
            call = black_scholes_price(S0, K, T, r, sigma, option='call')
            put = black_scholes_price(S0, K, T, r, sigma, option='put')
            expected_return = (predicted - K) / K if K > 0 else float('nan')
            rows.append({
                'symbol': symbol,
                'days_to_expiry': days,
                'T_years': T,
                'strike_pct': sp,
                'strike': K,
                'spot': S0,
                'predicted_spot': predicted,
                'sigma_annual': sigma,
                'call_price': call,
                'put_price': put,
                'expected_return_pct': expected_return
            })
    return pd.DataFrame(rows)


if __name__ == '__main__':
    # small demo: load sample from data index if available
    try:
        import pandas as pd
        csv_path = 'data/indexes/symbols.csv'
        if pd.io.common.file_exists(csv_path):
            df = pd.read_csv(csv_path, skiprows=2, encoding='utf-8')
            df.columns = df.columns.str.strip()
            # pick first symbol from snapshot if structure contains price columns
            # Otherwise this is only scaffold
    except Exception:
        pass

