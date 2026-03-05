
import pandas as pd
import requests
from io import StringIO

def debug_ticker(ticker):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
    }
    url = f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page=1"
    r = requests.get(url, headers=headers, timeout=10)
    html = r.content.decode("euc-kr", errors="replace")
    tables = pd.read_html(StringIO(html), displayed_only=False)
    
    print(f"--- Debugging {ticker} ---")
    print(f"Total tables: {len(tables)}")
    for i, t in enumerate(tables):
        print(f"Table {i} shape: {t.shape}")
        if t.shape[0] > 0:
            print(f"Table {i} first row: {list(t.iloc[0].values)}")
            
debug_ticker("034220")
