import streamlit as st
import pandas as pd
import logging
from src.analyze import analyze_universe

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="سیگنال‌دهی AI بورس", layout="wide")
st.title("سیگنال‌دهی AI برای بورس/فارکس")

uploaded = st.file_uploader(
    "فایل‌های CSV را آپلود کنید (OHLCV با ستون‌های: Date, Open, High, Low, Close, Volume)",
    accept_multiple_files=True
)

if uploaded:
    try:
        data_dict = {}
        index_dict = {}
        
        for file in uploaded:
            symbol = file.name.replace(".csv", "").strip()
            try:
                df = pd.read_csv(file, parse_dates=['Date']).set_index('Date').sort_index()
                
                # Validate required columns
                required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                if not all(col in df.columns for col in required_cols):
                    st.error(f"❌ فایل {symbol}: ستون‌های مورد نیاز وجود ندارند. مورد نیاز: {required_cols}")
                    continue
                
                # Separate indices from regular symbols
                if symbol in ["TEDPIX", "EQUAL"]:
                    index_dict[symbol] = df
                else:
                    data_dict[symbol] = df
                
                st.info(f"✅ بارگذاری موفق: {symbol} ({len(df)} سطر)")
            
            except Exception as e:
                st.error(f"❌ خطا در بارگذاری {symbol}: {str(e)}")
                logger.error(f"Error loading {symbol}: {e}")
        
        if not data_dict:
            st.warning("⚠️ هیچ داده معتبری بارگذاری نشد.")
        else:
            st.success(f"✅ {len(data_dict)} نماد بارگذاری شد.")
            
            try:
                st.info("⏳ در حال تجزیه و تحلیل...")
                signals, long_top, short_top = analyze_universe(data_dict, index_dict if index_dict else None)
                st.success("✅ تجزیه و تحلیل کامل شد")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📈 20 نماد برتر برای Long")
                    if long_top:
                        df_long = pd.DataFrame(long_top, columns=["نماد", "رژیم", "امتیاز"])
                        st.dataframe(df_long, use_container_width=True)
                    else:
                        st.info("سیگنال Long یافت نشد")
                
                with col2:
                    st.subheader("📉 20 نماد برتر برای Short")
                    if short_top:
                        df_short = pd.DataFrame(short_top, columns=["نماد", "رژیم", "امتیاز"])
                        st.dataframe(df_short, use_container_width=True)
                    else:
                        st.info("سیگنال Short یافت نشد")
                
                st.subheader(f"🎯 سیگنال‌ها ({len(signals)} سیگنال)")
                if signals:
                    # Display first 50 signals
                    df_signals = pd.DataFrame([s.__dict__ for s in signals[:50]])
                    st.dataframe(df_signals, use_container_width=True)
                    if len(signals) > 50:
                        st.info(f"نمایش 50 سیگنال اول از {len(signals)} سیگنال کل")
                else:
                    st.info("سیگنالی یافت نشد")
            
            except ValueError as e:
                st.error(f"❌ خطای اعتبارسنجی: {str(e)}")
                logger.error(f"Validation error: {e}")
            except Exception as e:
                st.error(f"❌ خطای غیرمنتظره: {str(e)}")
                logger.error(f"Unexpected error: {e}", exc_info=True)
    
    except Exception as e:
        st.error(f"❌ خطای کلی: {str(e)}")
        logger.error(f"General error: {e}", exc_info=True)
