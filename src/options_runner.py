#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Options runner: build option tables for top symbols using src.options_model
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
from src.options_model import generate_option_table
from src.data_loader import HybridDataManager, CSVDataLoader, SQLiteDataManager
import logging
import numpy as np
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    date_str = datetime.now().strftime('%Y-%m-%d')
    out_dir = Path('outputs') / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    # Prefer conservative top-20; fallback to aggressive
    cons_file = out_dir / f'test_01_conservative_{date_str}.csv'
    agg_file = out_dir / f'test_01_aggressive_{date_str}.csv'

    if cons_file.exists():
        df_top = pd.read_csv(cons_file, encoding='utf-8')
    elif agg_file.exists():
        df_top = pd.read_csv(agg_file, encoding='utf-8')
    else:
        logger.error('No top-20 source file found in outputs. Run bottom_seeker first.')
        return

    symbols = [str(s).strip() for s in df_top['symbol'].head(20).tolist()]
    hybrid = HybridDataManager()
    db = hybrid.db_manager

    all_rows = []
    strikes = [0.9, 1.0, 1.1]
    expiries = [30, 60, 90]

    for sym in symbols:
        df_sym = db.get_symbol_data(sym)
        if df_sym is None or df_sym.empty:
            # try to load from CSV snapshot and generate synthetic history
            csv_path = Path(hybrid.csv_path)
            if csv_path.exists():
                csv_df = pd.read_csv(csv_path, skiprows=2, encoding='utf-8')
                csv_df.columns = csv_df.columns.str.strip()
                row = csv_df[csv_df['نماد'].astype(str).str.strip() == str(sym)].head(1)
                if not row.empty:
                    try:
                        close_price = float(row.iloc[0].get('قیمت پایانی - مقدار') or row.iloc[0].get('آخرین معامله - مقدار'))
                        volume = float(row.iloc[0].get('حجم') or 0)
                        hist_df = CSVDataLoader.generate_historical_data(sym, close_price, volume)
                        # insert into DB for reuse
                        db.insert_symbol(sym, str(row.iloc[0].get('نام', '')), close_price)
                        db.insert_ohlcv(sym, hist_df)
                        df_sym = hist_df
                        logger.info(f'Generated synthetic history for {sym}')
                    except Exception as e:
                        logger.warning(f'Failed to generate history for {sym}: {e}')
                else:
                    logger.warning(f'No snapshot row for {sym} in CSV; skipping')
            else:
                logger.warning(f'CSV snapshot not found; cannot generate history for {sym}')

        if df_sym is None or df_sym.empty:
            logger.warning(f'No historical data for {sym}, skipping')
            continue
        close_series = df_sym['Close']
        tbl = generate_option_table(sym, close_series, strikes, expiries, r=0.02)
        if tbl.empty:
            continue

        # Enhance with Monte-Carlo expected price and recommendation
        from src.options_model import monte_carlo_option_price, expected_option_return

        mc_rows = []
        # baseline MC sims (fast)
        n_sim = 2000
        # try to read optional dividend yields mapping
        div_file = Path('data/dividends.csv')
        dividend_yield = 0.0
        if div_file.exists():
            try:
                div_df = pd.read_csv(div_file)
                div_df.columns = [c.strip() for c in div_df.columns]
                match = div_df[div_df.iloc[:,0].astype(str).str.strip() == str(sym)]
                if not match.empty:
                    dividend_yield = float(match.iloc[0].iloc[1])
            except Exception:
                dividend_yield = 0.0
        for _, row in tbl.iterrows():
            S0 = float(row['spot'])
            K = float(row['strike'])
            T = float(row['T_years'])
            r = 0.02
            sigma = float(row['sigma_annual']) if not pd.isna(row['sigma_annual']) else 0.2
            mc_call = monte_carlo_option_price(S0, K, T, r, sigma, option='call', n_sim=n_sim, mu=None, dividend_yield=dividend_yield)
            mc_put = monte_carlo_option_price(S0, K, T, r, sigma, option='put', n_sim=n_sim, mu=None, dividend_yield=dividend_yield)
            market_call = float(row.get('call_price', np.nan)) if not pd.isna(row.get('call_price', np.nan)) else np.nan
            market_put = float(row.get('put_price', np.nan)) if not pd.isna(row.get('put_price', np.nan)) else np.nan
            mc_call_ret = expected_option_return(mc_call, market_call)
            mc_put_ret = expected_option_return(mc_put, market_put)

            newr = row.to_dict()
            newr.update({
                'mc_call_price': mc_call,
                'mc_put_price': mc_put,
                'mc_call_expected_return_pct': mc_call_ret,
                'mc_put_expected_return_pct': mc_put_ret,
                'dividend_yield': dividend_yield
            })

            # simple recommendation logic
            rec = 'hold'
            if not pd.isna(mc_call_ret) and mc_call_ret > 0.20:
                rec = 'strong_buy_call'
            elif not pd.isna(mc_call_ret) and mc_call_ret > 0.05:
                rec = 'buy_call'
            elif not pd.isna(mc_put_ret) and mc_put_ret > 0.20:
                rec = 'strong_buy_put'
            elif not pd.isna(mc_put_ret) and mc_put_ret > 0.05:
                rec = 'buy_put'

            newr['recommendation'] = rec
            mc_rows.append(newr)

        if mc_rows:
            all_rows.append(pd.DataFrame(mc_rows))

    # concat baseline results
    if not all_rows:
        logger.warning('No option tables generated')
        return

    out_df = pd.concat(all_rows, ignore_index=True)

    # Run high-precision MC for a few selected symbols (top 3 from df_top)
    high_prec_symbols = [str(s).strip() for s in df_top['symbol'].head(3).tolist()]
    hp_rows = []
    try:
        from src.options_model import monte_carlo_option_price, expected_option_return
        for _, row in out_df.iterrows():
            if str(row['symbol']).strip() not in high_prec_symbols:
                continue
            S0 = float(row['spot'])
            K = float(row['strike'])
            T = float(row['T_years'])
            r = 0.02
            sigma = float(row['sigma_annual']) if not pd.isna(row['sigma_annual']) else 0.2
            # high precision sims (use 20000 sims as requested)
            row_div_yield = float(row.get('dividend_yield', 0.0)) if not pd.isna(row.get('dividend_yield', 0.0)) else 0.0
            mc_call_hp = monte_carlo_option_price(S0, K, T, r, sigma, option='call', n_sim=20000, mu=None, dividend_yield=row_div_yield)
            mc_put_hp = monte_carlo_option_price(S0, K, T, r, sigma, option='put', n_sim=20000, mu=None, dividend_yield=row_div_yield)
            out_df.loc[(out_df['symbol'] == row['symbol']) & (out_df['strike'] == row['strike']) & (out_df['days_to_expiry'] == row['days_to_expiry']), 'mc_call_price_hp'] = mc_call_hp
            out_df.loc[(out_df['symbol'] == row['symbol']) & (out_df['strike'] == row['strike']) & (out_df['days_to_expiry'] == row['days_to_expiry']), 'mc_put_price_hp'] = mc_put_hp
            # recompute expected returns vs market proxy
            market_call = float(row.get('call_price', np.nan)) if not pd.isna(row.get('call_price', np.nan)) else np.nan
            market_put = float(row.get('put_price', np.nan)) if not pd.isna(row.get('put_price', np.nan)) else np.nan
            out_df.loc[(out_df['symbol'] == row['symbol']) & (out_df['strike'] == row['strike']) & (out_df['days_to_expiry'] == row['days_to_expiry']), 'mc_call_expected_return_hp'] = expected_option_return(mc_call_hp, market_call)
            out_df.loc[(out_df['symbol'] == row['symbol']) & (out_df['strike'] == row['strike']) & (out_df['days_to_expiry'] == row['days_to_expiry']), 'mc_put_expected_return_hp'] = expected_option_return(mc_put_hp, market_put)
    except Exception as e:
        logger.warning(f'High-precision MC failed: {e}')
    # keep the modified out_df (it already contains baseline MC and HP columns)
    csv_out = out_dir / f'options_candidates_{date_str}.csv'
    json_out = out_dir / f'options_candidates_{date_str}.json'
    out_df.to_csv(csv_out, index=False, encoding='utf-8')
    out_df.to_json(json_out, force_ascii=False, orient='records', indent=2)

    logger.info(f'Wrote options candidates: {csv_out} ({len(out_df)} rows)')
    print(out_df.head(20).to_string())


if __name__ == '__main__':
    main()
