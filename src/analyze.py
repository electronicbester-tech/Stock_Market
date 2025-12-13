import pandas as pd
import logging
from src.indicators import add_indicators
from src.regime import market_regime
from src.signals import swing, trend, position
from src.scoring import score_asset
from src.config import CONFIG

logger = logging.getLogger(__name__)

def analyze_universe(data_dict: dict, index_dict: dict = None):
    """
    Analyze a universe of assets and generate trading signals.
    
    Args:
        data_dict: Dictionary of {symbol: pd.DataFrame} with OHLCV data
        index_dict: Dictionary of {"TEDPIX": df, "EQUAL": df}. If None, tries to extract from data_dict.
    
    Returns:
        (signals, long_ranks, short_ranks)
    """
    results, long_ranks, short_ranks = [], [], []
    
    # Handle index_dict extraction if not provided
    if index_dict is None:
        index_dict = {}
        if "TEDPIX" in data_dict:
            index_dict["TEDPIX"] = data_dict["TEDPIX"]
        if "EQUAL" in data_dict:
            index_dict["EQUAL"] = data_dict["EQUAL"]
    
    # Validate that required indices exist
    if "TEDPIX" not in index_dict or "EQUAL" not in index_dict:
        logger.error("Missing required index data: TEDPIX and EQUAL")
        raise ValueError("Index data (TEDPIX and EQUAL) required in index_dict or data_dict")
    
    df_index = add_indicators(index_dict["TEDPIX"])  # شاخص کل
    df_equal = add_indicators(index_dict["EQUAL"])   # شاخص هم‌وزن

    for symbol, df in data_dict.items():
        # Skip index symbols if they're in data_dict (already processed above)
        if symbol in ["TEDPIX", "EQUAL"]:
            continue
        
        try:
            df = add_indicators(df)
        except Exception as e:
            logger.warning(f"Failed to add indicators for {symbol}: {e}")
            continue
        
        if len(df) < CONFIG["min_history_days"]:
            logger.debug(f"Skipping {symbol}: insufficient history ({len(df)} < {CONFIG['min_history_days']})")
            continue
        regime = market_regime(df, df_index, df_equal)
        sigs = []
        sigs += swing(df, symbol, regime, CONFIG)
        sigs += trend(df, symbol, regime)
        sigs += position(df, symbol, regime)
        results.extend(sigs)
        long_ranks.append((symbol, regime, score_asset(df, regime, for_short=False)))
        short_ranks.append((symbol, regime, score_asset(df, regime, for_short=True)))

    long_top = sorted(long_ranks, key=lambda x: x[2], reverse=True)[:20]
    short_top = sorted(short_ranks, key=lambda x: x[2], reverse=True)[:20]
    return results, long_top, short_top
