
import pandas as pd
from quant_collector_enhanced import _fetch_investor_trading_naver

def verify_fix(tickers):
    results = []
    for ticker in tickers:
        print(f"Testing {ticker}...", end=" ", flush=True)
        rows = _fetch_investor_trading_naver(ticker, 10)
        if rows:
            print(f"SUCCESS ({len(rows)} rows)")
            results.append((ticker, "Success", len(rows)))
        else:
            print("FAILED")
            results.append((ticker, "Failed", 0))
    return results

test_tickers = ["005930", "000660", "035420", "035720", "005380", "005490", "003550", "034220", "066570", "000270"]
verify_fix(test_tickers)
