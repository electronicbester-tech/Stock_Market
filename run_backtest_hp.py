#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Quick script to run backtest with high precision."""
from pathlib import Path
from src.backtester import run_backtest

out_dir = Path('outputs') / '2025-12-12'
csv = out_dir / 'options_candidates_2025-12-12.csv'
print('Running backtest with n_sim=50000...')
run_backtest(str(csv), out_dir, n_sim=50000)
print('Backtest completed successfully.')
