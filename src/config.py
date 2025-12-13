CONFIG = {
    "weights": {
        "BULL": {"wm":0.30, "wt":0.35, "wb":0.25, "wr":0.07, "wl":0.03},
        "BEAR": {"wm":0.20, "wt":0.25, "wb":0.15, "wr":0.30, "wl":0.10},
        "NEUTRAL": {"wm":0.25, "wt":0.30, "wb":0.20, "wr":0.20, "wl":0.05},
    },
    "min_history_days": 260,
    "liquidity": {
        "min_value_traded": 1.5e9,  # حداقل ارزش معاملات روزانه (ریال) - قابل تنظیم
        "penalty_factor": 0.5
    },
    "filters": {
        "avoid_open_gap": True,
        "avoid_limit_up_down": True,
        "limit_buffer_pct": 0.5  # فاصله از سقف/کف دامنه برای ورود
    }
}
