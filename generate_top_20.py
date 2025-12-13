#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate Top 20 Symbols: تولید 20 نماد برتر بورس و فرابورس
============================================================
"""

import pandas as pd
import logging
from pathlib import Path
from src.symbol_filter import SymbolFilter

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def load_symbols_data():
    """بارگذاری داده‌های نمادهای بورس"""
    
    csv_path = Path('data/indexes/symbols.csv')
    
    if not csv_path.exists():
        logger.error(f"فایل یافت نشد: {csv_path}")
        return None
    
    try:
        # خواندن فایل CSV با تخطی از 2 سطر اول
        df = pd.read_csv(csv_path, skiprows=2, encoding='utf-8')
        
        # پاک‌سازی نام ستون‌ها
        df.columns = df.columns.str.strip()
        
        logger.info(f"✅ بارگذاری موفق: {len(df)} نماد")
        logger.info(f"ستون‌ها: {list(df.columns)}\n")
        
        return df
    
    except Exception as e:
        logger.error(f"❌ خطا در خواندن فایل: {e}")
        return None


def main():
    """تولید فایل خروجی برای 20 نماد برتر"""
    
    print("\n" + "="*80)
    print("📊 تولید 20 نماد برتر بورس و فرابورس")
    print("="*80 + "\n")
    
    # 1. بارگذاری داده‌ها
    logger.info("📥 مرحله 1: بارگذاری داده‌ها...")
    df = load_symbols_data()
    
    if df is None:
        return
    
    # 2. فیلتر کردن نمادهای نامعتبر
    logger.info("\n🔍 مرحله 2: فیلتر کردن نمادهای نامعتبر...")
    logger.info("-" * 80)
    
    filter_obj = SymbolFilter()
    
    # بررسی نمادهایی که باید حذف شوند
    top_20 = filter_obj.get_top_symbols(df, top_n=20)
    
    # 3. ذخیره‌ی نتایج
    logger.info("\n💾 مرحله 3: ذخیره‌ی فایل خروجی...")
    
    output_file = Path('top_symbols_20.csv')
    top_20.to_csv(output_file, index=False, encoding='utf-8')
    
    logger.info(f"✅ فایل ذخیره شد: {output_file.absolute()}")
    logger.info(f"📈 تعداد نمادهای برتر: {len(top_20)}\n")
    
    # 4. نمایش نتایج
    logger.info("="*80)
    logger.info("📋 خلاصۀ نتایج:")
    logger.info("="*80)
    logger.info(f"✅ نمادهای معتبر: {filter_obj.valid_count}")
    logger.info(f"❌ نمادهای حذف‌شده: {filter_obj.filtered_count}")
    logger.info(f"📊 درصد حذف: {(filter_obj.filtered_count / (filter_obj.valid_count + filter_obj.filtered_count) * 100):.1f}%\n")
    
    # 5. نمایش دقیق برترین 20 نماد
    print("📍 برترین 20 نماد برای تحلیل:\n")
    
    display_cols = ['نماد', 'نام', 'ارزش_معاملات'] if 'ارزش_معاملات' in top_20.columns else ['نماد', 'نام']
    
    # بررسی اینکه ستون‌ها وجود دارند
    available_cols = [col for col in display_cols if col in top_20.columns]
    
    if available_cols:
        display_df = top_20[available_cols].copy()
        display_df.index = range(1, len(display_df) + 1)
        
        # فرمت‌بندی
        for col in display_df.columns:
            if 'ارزش' in col or 'معاملات' in col or 'حجم' in col:
                display_df[col] = display_df[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
        
        print(display_df.to_string())
    
    print("\n" + "="*80)
    print("✅ انجام شد! فایل top_symbols_20.csv آماده است")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
