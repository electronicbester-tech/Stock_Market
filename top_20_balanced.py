#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Top 20 Balanced Symbols: تولید 20 نماد متوازن برای تحلیل
محدود کردن هر صنعت (حداکثر 3-4 نماد)
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BalancedSymbolFilter:
    """
    فیلتر متوازن برای انتخاب نمادهای متنوع
    """
    
    # صنایع معتبر با حداکثر تعداد نماد
    INDUSTRIES = {
        'مخابرات': {
            'keywords': ['مخابرات'],
            'max_symbols': 1,
            'symbols': []
        },
        'نفت_پالایش': {
            'keywords': ['پالایش نفت'],
            'max_symbols': 3,
            'symbols': []
        },
        'فولاد': {
            'keywords': ['فولاد'],
            'max_symbols': 4,
            'symbols': []
        },
        'پتروشیمی': {
            'keywords': ['پتروشیمی'],
            'max_symbols': 2,
            'symbols': []
        },
        'مس': {
            'keywords': ['مس'],
            'max_symbols': 1,
            'symbols': []
        },
        'سرب_روی': {
            'keywords': ['سرب', 'روی'],
            'max_symbols': 1,
            'symbols': []
        },
        'خودرو': {
            'keywords': ['خودرو', 'موتور'],
            'max_symbols': 2,
            'symbols': []
        },
        'حمل_نقل': {
            'keywords': ['حمل', 'نقل'],
            'max_symbols': 2,
            'symbols': []
        },
        'نفت_گاز': {
            'keywords': ['نفت', 'گاز'],
            'max_symbols': 2,
            'symbols': []
        },
        'سیمان': {
            'keywords': ['سیمان'],
            'max_symbols': 1,
            'symbols': []
        },
        'سرامیک': {
            'keywords': ['سرامیک', 'کاشی'],
            'max_symbols': 1,
            'symbols': []
        },
        'غذایی': {
            'keywords': ['غذایی', 'شیر'],
            'max_symbols': 1,
            'symbols': []
        },
    }
    
    # نمادهایی که باید حذف شوند
    BLACKLIST = [
        'صندوق', 'ETF', 'سرمایه‌گذاری', 'سرمایه گذاری',
        'مسکن', 'تس', 'صکوك', 'مرابحه', 'ارز', 'تالار',
        'ص.س', 'وقف', 'خیریه'
    ]
    
    def __init__(self):
        self.results = []
    
    def is_blacklisted(self, name: str) -> bool:
        """بررسی blacklist"""
        for word in self.BLACKLIST:
            if word in name or word.upper() in name.upper():
                return True
        return False
    
    def get_category(self, name: str) -> str:
        """تشخیص دسته‌ی صنعت"""
        name_upper = name.upper()
        
        for category, data in self.INDUSTRIES.items():
            for keyword in data['keywords']:
                if keyword.upper() in name_upper:
                    return category
        
        return None
    
    def filter_and_balance(self, df: pd.DataFrame, total: int = 20) -> pd.DataFrame:
        """فیلتر و متوازن کردن"""
        
        # فیلتر اولیه
        for idx, row in df.iterrows():
            name = str(row.get('نام', '')).strip()
            symbol = str(row.get('نماد', '')).strip()
            volume = pd.to_numeric(row.get('حجم', 0), errors='coerce')
            
            # بررسی blacklist
            if self.is_blacklisted(name):
                continue
            
            # تشخیص صنعت
            category = self.get_category(name)
            if not category:
                continue
            
            # اضافه کردن به نتیجه
            self.results.append({
                'category': category,
                'symbol': symbol,
                'name': name,
                'volume': volume if pd.notna(volume) else 0,
                'row': row
            })
        
        # فیلتر متوازن (حداکثر N نماد از هر صنعت)
        final_results = []
        category_counts = {cat: 0 for cat in self.INDUSTRIES.keys()}
        
        # مرتب سازی بر اساس حجم
        self.results.sort(key=lambda x: x['volume'], reverse=True)
        
        for item in self.results:
            cat = item['category']
            max_allowed = self.INDUSTRIES[cat]['max_symbols']
            
            if category_counts[cat] < max_allowed:
                final_results.append(item['row'])
                category_counts[cat] += 1
            
            if len(final_results) >= total:
                break
        
        # تبدیل به DataFrame
        result_df = pd.DataFrame(final_results).reset_index(drop=True)
        
        return result_df


def main():
    print("\n" + "="*80)
    print("🎯 تولید 20 نماد متوازن برای تحلیل تکنیکال")
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
    logger.info("\n🔍 اعمال فیلترهای متوازن...")
    filter_obj = BalancedSymbolFilter()
    top_20 = filter_obj.filter_and_balance(df, total=20)
    
    # ذخیره
    top_20.to_csv('top_symbols_20.csv', index=False, encoding='utf-8')
    logger.info(f"💾 ذخیره شد: {len(top_20)} نماد\n")
    
    # نمایش
    print("📊 20 نماد برتر (متوازن براساس صنعت):\n")
    display = top_20[['نماد', 'نام', 'حجم']].copy()
    display.index = range(1, len(display) + 1)
    print(display.to_string())
    
    print("\n" + "="*80)
    print("✅ آماده برای تحلیل تکنیکال!")
    print("="*80)
    print("\nنکات:")
    print("- فقط سهام عادی صنعتی")
    print("- بدون اختیار، حقوق، مرابحه، سرمایه‌گذاری")
    print("- بدون صندوق و اوراق دولتی")
    print("- متوازن براساس صنایع مختلف\n")


if __name__ == "__main__":
    main()
