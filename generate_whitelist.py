#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Top 20 Stock Symbols: تولید 20 نماد برتر برای تحلیل
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrictSymbolFilter:
    """
    فیلتر دقیق برای انتخاب تنها سهام عادی صنعتی
    """
    
    # شرکت‌هایی که باید کل‌ی حذف شوند
    BLACKLIST = [
        'مخابرات ایران',  # Telecom (too dominant)
        'صندوق', 'ETF',  # Funds
        'سرمایه‌گذاری', 'سرمایه گذاری', 'ص.س',  # Investment firms
        'مسکن', 'تس', 'مسکن‌', 'مسکن‌گذاری',  # Housing
        'صکوك', 'اوراق قرضه',  # Bonds
        'مرابحه', 'رهن', 'مسکن‌',  # Islamic financing
        'ارز', 'تالار',  # Forex
        'حق تقدم', 'اختیار', 'واپشین',  # Options/Rights
        'وقف', 'خیریه', 'دولتی',  # Charitable/Government
    ]
    
    # شرکت‌های معتبر برای تحلیل (فقط صنعتی عادی)
    WHITELIST = {
        'مخابرات ایران': ['اخابر'],
        'پالایش نفت': ['شتران', 'شپنا', 'شبریز'],
        'فولاد': ['فولاد', 'فولاد مبارکه'],
        'پتروشیمی': ['ممسنی', 'نفت سپاهان'],
        'مس': ['فملی', 'ملی مس'],
        'سرب و روی': ['فسرب'],
        'سیمان': ['سیمان', 'سیمان‌'],
        'خودرو': ['خموتور'],
        'حمل و نقل': ['حفارس', 'حگهر', 'حتوکا'],
        'نفت و گاز': ['پارسان'],
    }
    
    def __init__(self):
        self.removed = {}
        self.valid_symbols = []
    
    def is_valid(self, symbol: str, name: str) -> bool:
        """بررسی معتبر بودن نماد"""
        
        # بررسی blacklist
        for blackword in self.BLACKLIST:
            if blackword in name or blackword.upper() in name.upper():
                key = f"حذف: {blackword}"
                self.removed[key] = self.removed.get(key, 0) + 1
                return False
        
        # بررسی whitelist (بهتر)
        is_whitelisted = False
        for company_type, symbols in self.WHITELIST.items():
            for sym in symbols:
                if sym in symbol or sym in name:
                    is_whitelisted = True
                    break
        
        if not is_whitelisted:
            self.removed['غیر whitelisted'] = self.removed.get('غیر whitelisted', 0) + 1
            return False
        
        return True
    
    def filter_and_sort(self, df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """فیلتر و مرتب سازی"""
        
        valid_rows = []
        
        for idx, row in df.iterrows():
            symbol = str(row.get('نماد', '')).strip()
            name = str(row.get('نام', '')).strip()
            
            if self.is_valid(symbol, name):
                valid_rows.append(idx)
        
        result = df.loc[valid_rows].reset_index(drop=True)
        
        # مرتب سازی بر اساس حجم
        if 'حجم' in result.columns:
            result = result.copy()
            result['حجم_num'] = pd.to_numeric(result['حجم'], errors='coerce')
            result = result.sort_values('حجم_num', ascending=False)
            result = result.drop('حجم_num', axis=1)
        
        return result.head(top_n).reset_index(drop=True)


def main():
    print("\n" + "="*80)
    print("📊 تولید 20 نماد برتر برای تحلیل تکنیکال")
    print("="*80 + "\n")
    
    # بارگذاری
    logger.info("📥 بارگذاری فایل...")
    try:
        df = pd.read_csv('data/indexes/symbols.csv', skiprows=2, encoding='utf-8')
        df.columns = df.columns.str.strip()
        logger.info(f"✅ بارگذاری: {len(df)} نماد")
    except Exception as e:
        logger.error(f"❌ خطا: {e}")
        return
    
    # فیلتر
    logger.info("\n🔍 اعمال فیلترها...")
    filter_obj = StrictSymbolFilter()
    top_20 = filter_obj.filter_and_sort(df, top_n=20)
    
    # گزارش
    logger.info("\n📊 خلاصۀ حذف:")
    for reason, count in sorted(filter_obj.removed.items(), 
                                key=lambda x: x[1], reverse=True):
        logger.info(f"  - {reason}: {count}")
    
    logger.info(f"\n✅ معتبر: {len(top_20)} نماد")
    
    # ذخیره
    top_20.to_csv('top_symbols_20.csv', index=False, encoding='utf-8')
    logger.info("💾 ذخیره شد: top_symbols_20.csv\n")
    
    # نمایش
    print("📋 برترین 20 نماد:\n")
    display = top_20[['نماد', 'نام', 'حجم']].copy()
    display.index = range(1, len(display) + 1)
    print(display.to_string())
    
    print("\n" + "="*80)
    print("✅ آماده برای تحلیل!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
