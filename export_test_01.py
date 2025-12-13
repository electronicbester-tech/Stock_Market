#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export Test 01: generate test_01_{date}.csv and .json from final filter
Includes: symbol, name, sector, volume, last_price, floor, ceiling, approved_by
"""

import json
from datetime import datetime
from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import FinalSymbolFilter from final_filter
from final_filter import FinalSymbolFilter
from src.bottom_seeker import BottomSeeker

DATE = datetime.now().strftime('%Y-%m-%d')
CSV_OUT = f'test_01_{DATE}.csv'
JSON_OUT = f'test_01_{DATE}.json'

# Helper to detect sector from FinalSymbolFilter mapping
def detect_sector(name: str, mapping: dict) -> str:
    name_upper = name.upper()
    for sector, keywords in mapping.items():
        for kw in keywords:
            if kw.upper() in name_upper:
                return sector
    return 'UNKNOWN'


def build_export_df(df: pd.DataFrame, filter_obj: FinalSymbolFilter) -> pd.DataFrame:
    # Use BottomSeeker to find candidates (validated by formula)
    seeker = BottomSeeker(universe_n=800, min_gain=0.20, prefer_db=False)
    candidates = seeker.analyze_universe()

    if candidates.empty:
        # Fallback: use simple final_filter top symbols
        filtered = filter_obj.filter_dataframe(df)
        filtered.columns = [c.strip() for c in filtered.columns]
        filtered = filtered.head(20)
        out_rows = []
        for idx, row in filtered.iterrows():
            symbol = str(row.get('نماد', '')).strip()
            name = str(row.get('نام', '')).strip()
            volume = row.get('حجم') if 'حجم' in row else None
            last_price = row.get('آخرین معامله - مقدار') or row.get('قیمت پایانی - مقدار') or row.get('آخرین') or None
            floor = row.get('کمترین') if 'کمترین' in row else None
            ceiling = row.get('بیشترین') if 'بیشترین' in row else None
            sector = detect_sector(name, filter_obj.VALID_INDUSTRIES)
            out_rows.append({
                'symbol': symbol,
                'name': name,
                'sector': sector,
                'volume': int(volume) if pd.notna(volume) and str(volume) != '' else None,
                'last_price': int(last_price) if pd.notna(last_price) and str(last_price) != '' else None,
                'floor': int(floor) if pd.notna(floor) and str(floor) != '' else None,
                'ceiling': int(ceiling) if pd.notna(ceiling) and str(ceiling) != '' else None,
                'approved_by': 'final_filter_fallback'
            })
        return pd.DataFrame(out_rows).reset_index(drop=True)

    # Merge candidate metadata into export rows
    out_rows = []
    for _, row in candidates.head(20).iterrows():
        sym = row['symbol']
        # Try to get source info from CSV snapshot
        src_row = df[df['نماد'].astype(str).str.strip() == str(sym)].head(1)
        name = src_row.iloc[0]['نام'] if not src_row.empty else sym
        volume = src_row.iloc[0]['حجم'] if not src_row.empty and 'حجم' in src_row.columns else row.get('volume')
        last_price = row.get('price')
        floor = row.get('low52')
        ceiling = row.get('high50')
        sector = detect_sector(name, filter_obj.VALID_INDUSTRIES)

        out_rows.append({
            'symbol': sym,
            'name': name,
            'sector': sector,
            'volume': int(volume) if pd.notna(volume) and str(volume) != '' else None,
            'last_price': int(last_price) if last_price is not None else None,
            'floor': int(floor) if floor is not None else None,
            'ceiling': int(ceiling) if ceiling is not None else None,
            'potential_gain_pct': float(row.get('potential_gain_pct', 0)),
            'rsi14': float(row.get('rsi14', float('nan'))),
            'approved_by': 'bottom_seeker_v1'
        })

    return pd.DataFrame(out_rows).reset_index(drop=True)


def main():
    csv_path = Path('data/indexes/symbols.csv')
    if not csv_path.exists():
        logger.error('symbols.csv not found in data/indexes. Please provide the source file.')
        return

    # load
    df = pd.read_csv(csv_path, skiprows=2, encoding='utf-8')
    df.columns = df.columns.str.strip()

    # filter
    filter_obj = FinalSymbolFilter()
    export_df = build_export_df(df, filter_obj)

    # save CSV and JSON
    export_df.to_csv(CSV_OUT, index=False, encoding='utf-8')
    with open(JSON_OUT, 'w', encoding='utf-8') as f:
        json.dump(export_df.to_dict(orient='records'), f, ensure_ascii=False, indent=2)

    logger.info(f'✅ Exported {len(export_df)} symbols to {CSV_OUT} and {JSON_OUT}')
    print(export_df.to_string())


if __name__ == '__main__':
    main()
