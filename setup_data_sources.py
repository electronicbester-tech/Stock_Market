"""
SETUP GUIDE - اتصال به منابع داده مرکزی
==========================================

راهنمای نصب و راه‌اندازی سیستم دریافت داده‌ها
"""

import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from src.data_loader import HybridDataManager, SQLiteDataManager, CSVDataLoader
from src.tse_connector import TSEConnector, CachedTSEConnector


def setup_database():
    """Setup 1: Initialize local database and sync from CSV"""
    print("\n" + "="*70)
    print("SETUP 1: Local Database Initialization")
    print("="*70)
    
    print("\n1. Creating SQLite database...")
    db = SQLiteDataManager("data/market_data.db")
    
    print("\n2. Syncing data from CSV (symbols.csv)...")
    count = db.sync_from_csv("data/indexes/symbols.csv", top_n=50)
    
    print(f"\n✅ Database Setup Complete!")
    print(f"   • Database: data/market_data.db")
    print(f"   • Symbols synced: {count}")
    print(f"   • Each symbol has 260 days of historical data")
    
    return db


def setup_tse_api(install_client: bool = False):
    """Setup 2: Configure TSE API connector"""
    print("\n" + "="*70)
    print("SETUP 2: TSE API Connector Configuration")
    print("="*70)
    
    if install_client:
        print("\n1. Installing tsetmc-client library...")
        import subprocess
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "tsetmc-client"])
            print("   ✓ tsetmc-client installed")
        except Exception as e:
            print(f"   ⚠️  Failed to install: {e}")
            print("   You can install manually: pip install tsetmc-client")
    
    print("\n2. Initializing TSE API connector...")
    tse = CachedTSEConnector()
    
    print(f"\n✅ TSE API Setup Complete!")
    print(f"   • Connector: CachedTSEConnector")
    print(f"   • Cache directory: {tse.cache_dir}")
    print(f"   • Cache validity: {tse.cache_age_days} day(s)")
    
    return tse


def setup_hybrid():
    """Setup 3: Configure hybrid data manager (RECOMMENDED)"""
    print("\n" + "="*70)
    print("SETUP 3: Hybrid Data Manager (Recommended)")
    print("="*70)
    
    print("\n1. Initializing hybrid manager...")
    hybrid = HybridDataManager()
    
    print("\n2. Loading data (CSV → SQLite → API fallback)...")
    data = hybrid.load_data(top_n=20)
    
    print(f"\n✅ Hybrid Setup Complete!")
    print(f"   • Loaded symbols: {len(data)}")
    print(f"   • Data sources: CSV, SQLite, API")
    print(f"   • Fallback strategy: Enabled")
    
    return hybrid, data


def verify_setup():
    """Verify all components are working"""
    print("\n" + "="*70)
    print("VERIFICATION: Testing All Components")
    print("="*70)
    
    checks = {
        "CSV File": False,
        "SQLite Database": False,
        "Hybrid Manager": False,
        "Data Loading": False,
    }
    
    try:
        # Check 1: CSV File
        csv_path = Path("data/indexes/symbols.csv")
        if csv_path.exists():
            checks["CSV File"] = True
            print("✓ CSV file found")
        else:
            print("✗ CSV file not found")
    except Exception as e:
        print(f"✗ CSV check failed: {e}")
    
    try:
        # Check 2: Database
        db = SQLiteDataManager()
        symbols = db.get_all_symbols()
        if symbols:
            checks["SQLite Database"] = True
            print(f"✓ SQLite database OK ({len(symbols)} symbols)")
        else:
            print("⚠ SQLite database empty - run setup first")
    except Exception as e:
        print(f"✗ Database check failed: {e}")
    
    try:
        # Check 3: Hybrid Manager
        hybrid = HybridDataManager()
        checks["Hybrid Manager"] = True
        print("✓ Hybrid manager initialized")
    except Exception as e:
        print(f"✗ Hybrid manager failed: {e}")
    
    try:
        # Check 4: Data Loading
        data = hybrid.load_data(top_n=5)
        if len(data) > 0:
            checks["Data Loading"] = True
            print(f"✓ Data loading works ({len(data)} symbols)")
        else:
            print("⚠ Data loading returned empty")
    except Exception as e:
        print(f"✗ Data loading failed: {e}")
    
    passed = sum(1 for v in checks.values() if v)
    total = len(checks)
    
    print(f"\n{'='*70}")
    print(f"VERIFICATION RESULTS: {passed}/{total} checks passed")
    print(f"{'='*70}")
    
    return checks


# ==============================================================================
# Complete Setup Function
# ==============================================================================

def complete_setup(use_api: bool = False, skip_csv_sync: bool = False):
    """
    Complete setup of data sources.
    
    Args:
        use_api: Try to setup TSE API connector
        skip_csv_sync: Skip CSV to database sync
    """
    print("\n" + "="*70)
    print("Stock Market Analysis System - Setup Guide")
    print("="*70)
    print("\nThis script configures data sources for the analysis system")
    print("Priority: API (Live) > Database (SQLite) > CSV (Cached)")
    print("="*70)
    
    # Step 1: Database
    if not skip_csv_sync:
        db = setup_database()
    else:
        db = SQLiteDataManager()
        print("⊘ Skipped database sync")
    
    # Step 2: TSE API (optional)
    tse = None
    if use_api:
        tse = setup_tse_api(install_client=True)
    else:
        print("\n" + "="*70)
        print("SETUP 2: TSE API (Skipped)")
        print("="*70)
        print("\nTo enable live data from TSE API:")
        print("  1. pip install tsetmc-client")
        print("  2. Use CachedTSEConnector or TSEConnector in your code")
    
    # Step 3: Hybrid Manager
    hybrid, data = setup_hybrid()
    
    # Step 4: Verification
    verify_setup()
    
    # Step 5: Instructions for use
    print("\n" + "="*70)
    print("NEXT STEPS: Using the System")
    print("="*70)
    print("""
    In your analysis code, use:
    
    Option A - Simple CSV/Database:
    ─────────────────────────────
    from src.data_loader import HybridDataManager
    
    hybrid = HybridDataManager()
    data = hybrid.load_data(top_n=30)  # {symbol: DataFrame}
    
    Option B - With TSE API (live data):
    ────────────────────────────────────
    from src.tse_connector import CachedTSEConnector
    
    tse = CachedTSEConnector()
    data = tse.get_multiple_symbols(['نماد1', 'نماد2'], days=260)
    
    Option C - Full Hybrid (recommended):
    ─────────────────────────────────────
    from src.data_loader import HybridDataManager
    from src.tse_connector import CachedTSEConnector
    
    hybrid = HybridDataManager()
    # Try API first, fallback to SQLite/CSV
    data = hybrid.load_data(prefer_db=True)
    
    Then use with analysis:
    ──────────────────────
    from src.analyze import analyze_universe
    
    signals, long_top, short_top = analyze_universe(data)
    """)
    
    print("\n" + "="*70)
    print("✅ SETUP COMPLETE!")
    print("="*70)
    
    return {
        'db': db,
        'tse': tse,
        'hybrid': hybrid,
        'data': data
    }


# ==============================================================================
# Run Setup
# ==============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Setup data sources")
    parser.add_argument('--with-api', action='store_true', 
                       help='Setup TSE API connector')
    parser.add_argument('--skip-sync', action='store_true',
                       help='Skip CSV sync to database')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify existing setup')
    
    args = parser.parse_args()
    
    if args.verify_only:
        verify_setup()
    else:
        components = complete_setup(use_api=args.with_api, 
                                   skip_csv_sync=args.skip_sync)
