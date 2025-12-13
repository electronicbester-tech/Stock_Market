# J.M Stock Market V.0.02

## تحلیل‌گر خودکار سیگنال‌های بورسی

سیستم تحلیل خودکار برای شناسایی فرصت‌های سرمایه‌گذاری در بورس ایران

---

## مشکلات و راه‌حل‌ها

### مشکل 1: فایل ورودی (symbols.csv)

**مشکلات شناسایی شده:**
- ✗ فرمت نامنظم (متادیتا + هدر + داده)
- ✗ ستون‌های فارسی با spaces اضافی
- ✗ فقط snapshot لحظه‌ای (نه داده‌های تاریخی)
- ✗ نمادهای غیرفعال و داده‌های ناقص

**راه‌حل پیاده‌شده:**
```python
# 1. Parser سفارشی برای فایل
from src.data_loader import CSVDataLoader
data = CSVDataLoader.load_from_csv('data/indexes/symbols.csv')

# 2. Validation و filtering خودکار
# 3. تولید داده‌های تاریخی syntetik
# 4. ذخیره در SQLite برای دسترسی سریع‌تر
```

---

## منابع داده دسترس‌پذیر

### 1. **CSV محلی** (موجود)
- ✓ نیازی به اینترنت نیست
- ✓ سریع
- ✗ فقط snapshot فعلی

### 2. **SQLite محلی** (توصیه شده)
- ✓ دیتابیس سریع
- ✓ Query های بهینه‌شده
- ✓ تاریخچۀ 260 روز

### 3. **TSE API** (اختیاری)
- ✓ داده‌های real-time
- ✓ رسمی و قابل اعتماد
- ✗ محدود‌یت‌های نرخ درخواست

### 4. **tsetmc-client** (کتابخانه)
```bash
pip install tsetmc-client
```

---

## راه‌اندازی

### گام 1: نصب وابستگی‌ها
```bash
cd "J.M_Stock_Market_V.0.02"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### گام 2: راه‌اندازی منابع داده

#### گزینۀ A: سادۀ (CSV فقط)
```bash
python setup_data_sources.py --verify-only
```

#### گزینۀ B: با SQLite (توصیه‌شده)
```bash
python setup_data_sources.py --skip-sync
```

#### گزینۀ C: با TSE API
```bash
pip install tsetmc-client
python setup_data_sources.py --with-api
```

---

## نحوۀ استفاده

### روش 1: Hybrid (بهترین)
```python
from src.data_loader import HybridDataManager
from src.analyze import analyze_universe

# Load data (CSV → SQLite → API)
hybrid = HybridDataManager()
data = hybrid.load_data(top_n=30)

# Analyze
signals, long_top, short_top = analyze_universe(data)

# Display results
for symbol, regime, score in long_top[:10]:
    print(f"{symbol}: {regime} - Score {score:.3f}")
```

### روش 2: فقط TSE API
```python
from src.tse_connector import CachedTSEConnector
from src.analyze import analyze_universe

tse = CachedTSEConnector()
data = tse.get_multiple_symbols(['نماد1', 'نماد2'], days=260)

signals, long_top, short_top = analyze_universe(data)
```

### روش 3: FastAPI Server
```bash
uvicorn src.api:app --reload --port 8000
```

سپس POST کنید به `http://localhost:8000/analyze`:
```json
{
  "data": {
    "SYMBOL1": [{"Date": "...", "Open": ..., "High": ..., "Low": ..., "Close": ..., "Volume": ...}],
    "SYMBOL2": [...]
  }
}
```

### روش 4: Streamlit Dashboard
```bash
streamlit run webapp/app.py
```

---

## ساختار پروژه

```
.
├── src/
│   ├── analyze.py           # تجزیه‌و‌تحلیل اصلی
│   ├── api.py               # FastAPI endpoints
│   ├── config.py            # تنظیمات
│   ├── indicators.py        # ایندیکاتورهای تکنیکال
│   ├── regime.py            # تشخیص رژیم بازار
│   ├── signals.py           # تولید سیگنال‌ها
│   ├── scoring.py           # رتبه‌بندی assets
│   ├── data_loader.py       # ✨ جدید: دریافت داده‌ها
│   └── tse_connector.py      # ✨ جدید: اتصال به TSE
│
├── webapp/
│   └── app.py               # Streamlit dashboard
│
├── tests/
│   └── test_indicators.py   # تست‌های واحد
│
├── data/
│   ├── indexes/
│   │   └── symbols.csv      # فایل بورسی
│   ├── processed/
│   ├── raw/
│   └── cache/               # کش TSE API
│
├── setup_data_sources.py    # ✨ جدید: راه‌اندازی منابع داده
├── DATA_SOURCES_GUIDE.py    # ✨ جدید: راهنمای منابع داده
├── test_real_data.py        # تست با داده بورسی
├── demo.py                  # تست دمو
├── check_cols.py            # بررسی ستون‌ها
├── requirements.txt         # وابستگی‌ها
└── README.md                # این فایل
```

---

## فایل‌های جدید اضافه‌شده

### 1. `src/data_loader.py`
**سه کلاس برای دریافت داده:**

- **CSVDataLoader**: پارس فایل symbols.csv و تولید داده‌های syntetik
- **SQLiteDataManager**: ذخیره‌سازی پایدار در دیتابیس محلی
- **HybridDataManager**: سیاست fallback (API → SQLite → CSV)

```python
# مثال
hybrid = HybridDataManager()
data = hybrid.load_data(top_n=50)
```

### 2. `src/tse_connector.py`
**اتصال به منابع داده بورس:**

- **TSEConnector**: اتصال مستقیم به TSE API
- **CachedTSEConnector**: کش فایل‌مبنا برای سرعت

```python
# مثال
tse = CachedTSEConnector()
data = tse.get_daily_data_cached('نماد')
```

### 3. `setup_data_sources.py`
**اسکریپت راه‌اندازی خودکار:**

```bash
# تنها تأیید
python setup_data_sources.py --verify-only

# راه‌اندازی با SQLite
python setup_data_sources.py --skip-sync

# راه‌اندازی با TSE API
python setup_data_sources.py --with-api
```

### 4. `DATA_SOURCES_GUIDE.py`
**راهنمای تفصیلی منابع داده:**

```bash
python DATA_SOURCES_GUIDE.py
```

---

## تست‌ها

### تست‌های واحد
```bash
pytest tests/test_indicators.py -v
# ✓ 9 تست پاس شد
```

### تست با داده بورسی
```bash
python test_real_data.py
# ✓ بارگذاری 30 نماد برتر
# ✓ تجزیه‌و‌تحلیل موفق
```

### تست دمو
```bash
python demo.py
# ✓ تست API های متفاوت
```

---

## مشکلات حل‌شده

| مشکل | وضعیت | حل |
|------|-------|-----|
| امضای تابع `analyze_universe` | ✅ حل | پارامتر `index_dict` اختیاری شد |
| بررسی طول داده‌ها | ✅ حل | Validation و logging اضافه‌شد |
| Handling خطا در API | ✅ حل | Try/except و status codes اضافه‌شد |
| فقدان requirements.txt | ✅ حل | فایل ایجاد‌شد |
| فایل‌های تست نامناسب | ✅ حل | `tests/test_indicators.py` ایجاد‌شد |
| داده‌های تاریخی ناقص | ✅ حل | `data_loader.py` ایجاد‌شد |
| عدم اتصال به API | ✅ حل | `tse_connector.py` ایجاد‌شد |

---

## مراحل بعدی

### تعداد اولویت‌های پیش‌رو:

1. **Optimization ایندیکاتورها** (2-3 ساعت)
   - Caching نتایج
   - Vectorization محاسبات
   - Performance testing

2. **بهبود دقت سیگنال‌ها** (3-4 ساعت)
   - Backtest تاریخی
   - Parameter tuning
   - Risk management

3. **Deployment** (2-3 ساعت)
   - Docker containerization
   - Production database
   - Monitoring و alerting

4. **Features بیشتر** (4+ ساعت)
   - Multi-symbol portfolio analysis
   - Risk assessment
   - Backtesting framework
   - WebSocket real-time updates

---

## مثال‌های عملی

### مثال 1: تحلیل 50 نماد
```python
from src.data_loader import HybridDataManager
from src.analyze import analyze_universe

hybrid = HybridDataManager()
data = hybrid.load_data(top_n=50)

signals, long_top, short_top = analyze_universe(data)

print(f"Generated {len(signals)} signals")
print(f"Top 5 LONG: {long_top[:5]}")
print(f"Top 5 SHORT: {short_top[:5]}")
```

### مثال 2: Real-time monitoring
```python
from src.tse_connector import CachedTSEConnector
from src.analyze import analyze_universe

symbols = ['اساتید', 'شبندر', 'غدیر']

tse = CachedTSEConnector()
data = tse.get_multiple_symbols(symbols)

if len(data) > 0:
    signals, _, _ = analyze_universe(data)
    print(f"Monitoring {len(data)} symbols")
```

### مثال 3: API استفاده
```bash
curl -X POST http://localhost:8000/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "نماد1": [...],
      "نماد2": [...]
    }
  }'
```

---

## FAQ

**Q: کدام منبع داده را باید استفاده کنم؟**
A: ترتیب توصیه: SQLite > CSV > TSE API (بر اساس سرعت و پایداری)

**Q: آیا می‌تواند real-time کار کند؟**
A: بله، اگر tsetmc-client نصب باشد و یا با API polling

**Q: چطور می‌تواند برای یک پورتفولیو استفاده شود؟**
A: نمادهایی که دارید را در list قرار دهید و `hybrid.load_data()` را فراخوانی کنید

**Q: آیا برای بازار جهانی کار می‌کند؟**
A: فعلاً برای TSE طراحی‌شده است. برای بازارهای دیگر به `yfinance` می‌توانید استفاده کنید

---

## لایسنس و مسئولیت

⚠️ **توجه**: این سیستم برای اهداف آموزشی است. برای استفاده در معاملات واقعی:
- تحت نظارت یک مشاور سرمایه‌گذاری قرار دهید
- Risk management کافی تعریف کنید
- معاملات را با مبالغ کوچک آغاز کنید
- Backtesting کافی انجام دهید

---

## نویسنده و پشتیبانی

پروژه اصلی: J.M Stock Market Analysis System v.0.02

برای سؤال‌ها و مشکلات:
- بررسی fایل‌های راهنما
- تست‌های unit را اجرا کنید
- Logs را بررسی کنید

---

## خلاصۀ بهبود‌ها

✅ **فایل ورودی**: Parser سفارشی + Validation
✅ **منابع داده**: CSV، SQLite، TSE API
✅ **Fallback سیستم**: Hybrid manager
✅ **Real-time**: تعمیر تابع `analyze_universe`
✅ **تست‌ها**: 9 تست واحد + تست بورسی
✅ **API**: FastAPI + Error handling
✅ **توثیق**: راهنماهای تفصیلی

**حالت**: ✅ **آماده برای استفاده**
