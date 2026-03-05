
import pandas as pd
import requests
from io import StringIO
import time

def test_investor_detailed(tickers):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
    }
    
    results = []
    for ticker in tickers:
        url = f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page=1"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                print(f"[{ticker}] HTTP Error: {r.status_code}")
                results.append((ticker, "HTTP Error", r.status_code))
                continue
                
            html = r.content.decode("euc-kr", errors="replace")
            if "b_panel" in html or "spam" in html.lower() or "captcha" in html.lower():
                print(f"[{ticker}] Block detected!")
                results.append((ticker, "Blocked", None))
                continue
                
            tables = pd.read_html(StringIO(html), displayed_only=False)
            if len(tables) < 4:
                print(f"[{ticker}] Too few tables: {len(tables)}")
                results.append((ticker, "Few Tables", len(tables)))
                continue
                
            t = tables[3]
            # row 0 is header, row 1 is first data
            if t.shape[0] < 2:
                print(f"[{ticker}] No data rows")
                results.append((ticker, "No Data Rows", None))
                continue
                
            first_data = t.iloc[1]
            cols_count = t.shape[1]
            
            print(f"[{ticker}] Cols: {cols_count}, Row1: {list(first_data.values)[:8]}")
            results.append((ticker, "Success", cols_count))
            
        except Exception as e:
            print(f"[{ticker}] Exception: {type(e).__name__}")
            results.append((ticker, "Exception", type(e).__name__))
        
        time.sleep(0.1)
        
    return results

# 테스트 종목 확대
test_tickers = ["005930", "000660", "035420", "035720", "005380", "005490", "003550", "034220", "066570", "000270"]
res = test_investor_detailed(test_tickers)

success_count = sum(1 for r in res if r[1] == "Success")
print(f"\nSummary: Success {success_count}/{len(test_tickers)}")
