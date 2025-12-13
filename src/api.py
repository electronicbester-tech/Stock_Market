from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
import logging
from src.analyze import analyze_universe

logger = logging.getLogger(__name__)
app = FastAPI(title="Stock Market Signal API", version="0.0.2")

class OHLCV(BaseModel):
    Date: str
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float

class AnalyzeRequest(BaseModel):
    data: dict = Field(..., description="Dictionary of {symbol: [OHLCV...]}")
    indices: dict = Field(None, description="Optional index data {TEDPIX: [OHLCV...], EQUAL: [OHLCV...]}")

class SignalResponse(BaseModel):
    symbol: str
    regime: str
    horizon: str
    direction: str
    entry: float
    stop: float
    take: float
    trailing_mult: float
    confidence: float
    notes: str

class AnalyzeResponse(BaseModel):
    signals: list[SignalResponse]
    long_top: list
    short_top: list

@app.post("/analyze", response_model=AnalyzeResponse)
def analyze(payload: dict):
    """
    Analyze market data and generate signals.
    
    Expected payload format:
    {
        "data": {
            "AAPL": [{"Date": "...", "Open": ..., ...}, ...],
            "MSFT": [...]
        },
        "indices": {  # optional
            "TEDPIX": [...],
            "EQUAL": [...]
        }
    }
    """
    try:
        # Parse data
        data_dict = {}
        for symbol, rows in payload.get("data", {}).items():
            try:
                df = pd.DataFrame(rows)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date').sort_index()
                data_dict[symbol] = df
            except Exception as e:
                logger.warning(f"Failed to parse data for {symbol}: {e}")
                raise HTTPException(status_code=400, detail=f"Invalid data for symbol {symbol}: {str(e)}")
        
        # Parse indices if provided
        index_dict = None
        if "indices" in payload and payload["indices"]:
            index_dict = {}
            for idx_name, rows in payload["indices"].items():
                try:
                    df = pd.DataFrame(rows)
                    df['Date'] = pd.to_datetime(df['Date'])
                    df = df.set_index('Date').sort_index()
                    index_dict[idx_name] = df
                except Exception as e:
                    logger.warning(f"Failed to parse index {idx_name}: {e}")
        
        # Analyze
        if not data_dict:
            raise HTTPException(status_code=400, detail="No data provided")
        
        signals, long_top, short_top = analyze_universe(data_dict, index_dict)
        
        return {
            "signals": [s.__dict__ for s in signals],
            "long_top": [{"symbol": s, "regime": r, "score": float(sc)} for s, r, sc in long_top],
            "short_top": [{"symbol": s, "regime": r, "score": float(sc)} for s, r, sc in short_top]
        }
    
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in analyze: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
