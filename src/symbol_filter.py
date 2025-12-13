#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Symbol Filter: فیلتر کردن نمادهای نامعتبر برای تحلیل تکنیکال
===============================================================
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SymbolFilter:
    """
    فیلتر نمادهای بورس و فرابورس
    - حذف اختیار معامله (Option)
    - حذف اوراق دولتی و دولتی
    - حذف حقوق تقدم و اختیار
    - حذف مرابحه
    - حذف تس مسکن
    - حذف صندوق ها
    - حذف اوراق قرضه
    - حذف نمادهای غیرفعال
    """
    
    # نمادهای نامعتبر و الگوهایی که باید حذف شوند
    INVALID_PATTERNS = {
        'option': ['ـو', 'P_', 'C_'],  # Option symbols
        'warrant': ['د', 'ـر', 'ـح'],  # Warrant/Right symbols
        'government': ['دولتی', 'خزانه', 'اسلامی'],  # Government bonds
        'mortgage': ['مسکن', 'تس'],  # Mortgage/Housing
        'murabaha': ['مرابحه'],  # Murabaha
        'fund': ['صندوق', 'پ.ی', 'ETF'],  # Funds and ETFs
        'bond': ['اوراق قرضه', 'قرضه', 'بدهی'],  # Bonds
        'foreign': ['ارز', 'تالار'],  # Foreign/Forex
    }
    
    # نمادهای که معمولا صادق شامل می شود
    INACTIVE_MARKERS = [
        'حذف',
        'تعلیق',
        'کوچک',
        'معلق',
        'نهایت',
        'غیرفعال'
    ]
    
    # نمادهای اصلی معروف
    MAIN_SYMBOLS = {
        'خودروسازی': ['ایران', 'ایرانخودرو', 'سایپا', 'وایمو', 'پارس', 'فولاد', 'فناورید'],
        'بانکی': ['بانک', 'ملی', 'صادرات', 'تجارت', 'ملت', 'کارآفرین', 'توسعه'],
        'نفت‌و‌گاز': ['نفت', 'گاز', 'پتروشیمی', 'شیمی', 'نگین'],
        'فلزات': ['فولاد', 'آلومینیوم', 'مس', 'فلزات', 'معادن'],
        'سیمان': ['سیمان', 'سیمانی'],
        'تلکام': ['ایرانسل', 'حمراه', 'رایتل', 'هوچویی', 'افق'],
        'برق': ['برق', 'توانبخش', 'نیروگاه', 'توزیع'],
        'بیمه': ['بیمه', 'تازه', 'مهر', 'رازی', 'آرمان'],
        'غذایی': ['غذایی', 'خوراکی', 'دامپروری', 'شیر'],
        'دارویی': ['دارو', 'داروسازی', 'بیوتک', 'پزشکی'],
    }
    
    def __init__(self):
        self.filtered_count = 0
        self.valid_count = 0
        self.removal_reasons = {}
    
    def is_valid_symbol(self, symbol: str, name: str = "", trade_value: float = 0) -> tuple[bool, str]:
        """
        بررسی آیا نماد معتبر است یا نه
        
        Returns:
            (is_valid: bool, reason: str)
        """
        
        if not symbol or not isinstance(symbol, str):
            return False, "نماد خالی یا نامعتبر"
        
        symbol_upper = symbol.upper()
        name_upper = name.upper()
        
        # بررسی الگوهای نامعتبر
        for category, patterns in self.INVALID_PATTERNS.items():
            for pattern in patterns:
                if pattern in symbol_upper or pattern in symbol:
                    return False, f"نمادهای {category}: {pattern}"
        
        # بررسی نمادهایی که نام‌های خاصی دارند
        invalid_names = ['مرابحه', 'صندوق', 'صکوك', 'اوراق', 'درآمد ثابت', 'ETF']
        for invalid in invalid_names:
            if invalid in name:
                return False, f"نوع نامعتبر: {invalid}"
        
        # بررسی نشانگرهای غیرفعال
        for marker in self.INACTIVE_MARKERS:
            if marker in symbol or marker in name:
                return False, f"نماد غیرفعال: {marker}"
        
        # بررسی حداقل حجم معامله (برای فعال بودن)
        if trade_value > 0 and trade_value < 100_000_000:
            return False, "حجم معاملات پایین (< 100M)"
        
        return True, "معتبر"
    
    def filter_dataframe(self, df: pd.DataFrame, 
                        symbol_col: str = 'نماد', 
                        name_col: str = 'نام',
                        value_col: str = 'ارزش_معاملات') -> pd.DataFrame:
        """
        فیلتر کردن DataFrame شامل نمادهای معتبر
        """
        
        valid_symbols = []
        removal_log = []
        
        for idx, row in df.iterrows():
            symbol = str(row[symbol_col]).strip() if symbol_col in df.columns else ""
            name = str(row[name_col]).strip() if name_col in df.columns else ""
            trade_value = float(row[value_col]) if value_col in df.columns and pd.notna(row[value_col]) else 0
            
            is_valid, reason = self.is_valid_symbol(symbol, name, trade_value)
            
            if is_valid:
                valid_symbols.append(idx)
                self.valid_count += 1
            else:
                self.filtered_count += 1
                removal_log.append({
                    'symbol': symbol,
                    'name': name,
                    'reason': reason
                })
                
                if reason not in self.removal_reasons:
                    self.removal_reasons[reason] = 0
                self.removal_reasons[reason] += 1
        
        logger.info(f"✅ نمادهای معتبر: {self.valid_count}")
        logger.info(f"❌ نمادهای حذف‌شده: {self.filtered_count}")
        logger.info("\nدلایل حذف:")
        for reason, count in sorted(self.removal_reasons.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  - {reason}: {count}")
        
        return df.loc[valid_symbols].reset_index(drop=True)
    
    def get_top_symbols(self, df: pd.DataFrame, top_n: int = 20,
                       symbol_col: str = 'نماد',
                       name_col: str = 'نام',
                       value_col: str = 'ارزش_معاملات') -> pd.DataFrame:
        """
        دریافت برترین نمادهای معتبر بر اساس حجم معاملات
        """
        
        # ابتدا فیلتر کنید
        filtered_df = self.filter_dataframe(df, symbol_col, name_col, value_col)
        
        # مرتب کنید بر اساس ارزش معاملات
        if value_col in filtered_df.columns:
            filtered_df = filtered_df.sort_values(value_col, ascending=False)
        
        # برترین N نماد را برگردانید
        top_df = filtered_df.head(top_n).copy()
        
        logger.info(f"\n📊 برترین {top_n} نماد معتبر:")
        for idx, row in top_df.iterrows():
            symbol = row[symbol_col] if symbol_col in row else "N/A"
            name = row[name_col] if name_col in row else "N/A"
            value = row[value_col] if value_col in row else 0
            logger.info(f"  {idx+1}. {symbol:15} | {name:30} | ارزش: {value:,.0f}")
        
        return top_df


def create_main_symbols_file(symbols_df: pd.DataFrame, output_file: str = 'top_symbols_20.csv'):
    """
    تولید فایل خروجی برای 20 نماد اصلی
    """
    
    filter_obj = SymbolFilter()
    top_symbols = filter_obj.get_top_symbols(symbols_df, top_n=20)
    
    # ذخیره‌ی فایل
    top_symbols.to_csv(output_file, index=False, encoding='utf-8')
    logger.info(f"\n✅ فایل خروجی ذخیره شد: {output_file}")
    
    return top_symbols


if __name__ == "__main__":
    # نمونه استفاده
    print("=" * 80)
    print("Symbol Filter - فیلتر نمادهای بورس و فرابورس")
    print("=" * 80)
    
    # بارگذاری داده‌ها
    try:
        df = pd.read_csv('data/indexes/symbols.csv', skiprows=2, encoding='utf-8')
        
        # ستون‌های فارسی را پاک‌سازی کنید
        df.columns = df.columns.str.strip()
        
        logger.info(f"بارگذاری {len(df)} نماد از فایل CSV")
        
        # فیلتر کردن و ذخیره‌ی برترین 20 نماد
        top_20 = create_main_symbols_file(df, 'top_symbols_20.csv')
        
    except FileNotFoundError:
        logger.error("فایل CSV پیدا نشد. لطفا مسیر را بررسی کنید.")
    except Exception as e:
        logger.error(f"خطا: {e}")
