#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Final Symbol Filter: فیلتر نهایی - فقط سهام عادی صنعتی
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FinalSymbolFilter:
    """
    فیلتر نهایی برای انتخاب سهام عادی صنعتی
    """
    
    # کلمات اسلامی که باید حذف شوند
    FORBIDDEN_KEYWORDS = [
        'حق', 'د', 'ـر', 'ـح',  # Warrant/Rights
        'صندوق', 'ETF', 'شاخص',  # Funds & ETFs
        'صکوك', 'اوراق', 'درآمد ثابت', 'بدهی', 'قرضه',  # Bonds
        'مرابحه',  # Murabaha/Islamic financing
        'سرمایه', 'سرمايه', 'سرمایه‌گذاری',  # Investment companies
        'مسکن', 'تس', 'مسکن‌',  # Housing
        'فقیهی', 'ص.س', 'شرکت سرمایه‌گذاری',  # Real estate/Investment firms
        'کیش', 'ارز', 'تالار',  # Forex/Currency
        'حذف', 'تعلیق', 'معلق', 'توقیف',  # Inactive
        'دولتی', 'خزانه', 'دولت',  # Government bonds
        'وقف', 'خیریه',  # Charitable/Waqf
        'رهن', 'انتقال', 'ملک', 'ملکی',  # Real estate transfer
    ]
    
    # صنایع معتبر برای سهام عادی
    VALID_INDUSTRIES = {
        'تلکام': ['مخابرات', 'ایرانسل', 'حمراه', 'رایتل'],
        'نفت_گاز': ['پالایش', 'نفت', 'گاز', 'پتروشیمی'],
        'فلزات': ['فولاد', 'مس', 'آلومینیوم', 'سرب', 'روی', 'ریخته', 'ملی'],
        'سیمان': ['سیمان'],
        'خودرو': ['خودرو', 'موتور', 'خودروسازی'],
        'برق': ['برق', 'توزیع', 'تولید'],
        'آب': ['آب'],
        'حمل_نقل': ['حمل', 'نقل', 'خطوط', 'شناور'],
        'غذایی': ['غذایی', 'شیر', 'خوراکی', 'دستی'],
        'دارو': ['دارو', 'داروسازی', 'بیوتک'],
        'شیمی': ['شیمی', 'رنگ', 'شیمیایی'],
        'نساجی': ['نساجی', 'پوشاک'],
        'سرامیک': ['سرامیک', 'کاشی', 'سفال'],
        'شیشه': ['شیشه'],
        'بیمه': ['بیمه'],
        'بانک': ['بانک'],
    }
    
    def __init__(self):
        self.stats = {'total': 0, 'removed': 0, 'valid': 0}
        self.removal_reasons = {}
    
    def should_remove(self, symbol: str, name: str) -> tuple[bool, str]:
        """بررسی اینکه نماد باید حذف شود"""
        
        full_text = f"{symbol} {name}".upper()
        
        # چک کلمات ممنوعه
        for keyword in self.FORBIDDEN_KEYWORDS:
            if keyword.upper() in full_text:
                return True, f"کلمه: {keyword}"
        
        # چک صنعت معتبر
        name_upper = name.upper()
        is_valid_industry = any(
            kw.upper() in name_upper 
            for keywords in self.VALID_INDUSTRIES.values() 
            for kw in keywords
        )
        
        if not is_valid_industry:
            return True, "صنعت نامعتبر"
        
        return False, ""
    
    def filter_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """فیلتر کردن داده‌ها"""
        
        valid_indices = []
        self.stats['total'] = len(df)
        
        for idx, row in df.iterrows():
            symbol = str(row.get('نماد', '')).strip()
            name = str(row.get('نام', '')).strip()
            
            should_remove, reason = self.should_remove(symbol, name)
            
            if should_remove:
                self.stats['removed'] += 1
                if reason not in self.removal_reasons:
                    self.removal_reasons[reason] = 0
                self.removal_reasons[reason] += 1
            else:
                valid_indices.append(idx)
                self.stats['valid'] += 1
        
        return df.loc[valid_indices].reset_index(drop=True)
    
    def get_top_symbols(self, df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """دریافت برترین نمادها"""
        
        filtered = self.filter_dataframe(df)
        
        # مرتب سازی بر اساس حجم
        if 'حجم' in filtered.columns:
            filtered = filtered.copy()
            filtered['حجم_numeric'] = pd.to_numeric(filtered['حجم'], errors='coerce')
            filtered = filtered.sort_values('حجم_numeric', ascending=False)
            filtered = filtered.drop('حجم_numeric', axis=1)
        
        return filtered.head(top_n).reset_index(drop=True)
    
    def print_report(self):
        """گزارش"""
        logger.info("\n" + "="*80)
        logger.info("📊 گزارش فیلتر کردن")
        logger.info("="*80)
        logger.info(f"📈 کل: {self.stats['total']}")
        logger.info(f"❌ حذف شده: {self.stats['removed']}")
        logger.info(f"✅ معتبر: {self.stats['valid']}")
        
        if self.removal_reasons:
            logger.info("\n📋 دلایل حذف:")
            for reason, count in sorted(self.removal_reasons.items(), 
                                        key=lambda x: x[1], reverse=True):
                logger.info(f"  - {reason}: {count}")


def main():
    """برنامه اصلی"""
    
    print("\n" + "="*80)
    print("🎯 فیلتر نهایی: انتخاب سهام عادی صنعتی")
    print("="*80 + "\n")
    
    # بارگذاری
    logger.info("📥 بارگذاری فایل CSV...")
    try:
        df = pd.read_csv('data/indexes/symbols.csv', skiprows=2, encoding='utf-8')
        df.columns = df.columns.str.strip()
        logger.info(f"✅ بارگذاری: {len(df)} نماد")
    except Exception as e:
        logger.error(f"❌ خطا: {e}")
        return
    
    # فیلتر
    logger.info("\n🔍 اعمال فیلترها...")
    filter_obj = FinalSymbolFilter()
    top_20 = filter_obj.get_top_symbols(df, top_n=20)
    
    # گزارش
    filter_obj.print_report()
    
    # ذخیره
    logger.info("\n💾 ذخیره‌ی فایل...")
    top_20.to_csv('top_symbols_20.csv', index=False, encoding='utf-8')
    logger.info(f"✅ ذخیره شد: top_symbols_20.csv\n")
    
    # نمایش
    print("📊 برترین 20 نماد صنعتی:\n")
    display_cols = ['نماد', 'نام']
    for col in ['حجم', 'آخرین معامله - مقدار']:
        if col in top_20.columns:
            display_cols.append(col)
    
    display_df = top_20[display_cols].copy()
    display_df.index = range(1, len(display_df) + 1)
    
    print(display_df.to_string())
    print("\n" + "="*80)
    print("✅ انجام شد!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
