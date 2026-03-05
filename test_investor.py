
import pandas as pd
import requests
from io import StringIO
from datetime import date, timedelta

def test_naver_frgn(ticker):
    url = f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page=1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        html = r.content.decode("euc-kr", errors="replace")
        tables = pd.read_html(StringIO(html), displayed_only=False)
        
        print(f"\n--- {ticker} Table Info ---")
        print(f"Total tables found: {len(tables)}")
        
        for i, t in enumerate(tables):
            print(f"Table {i} shape: {t.shape}")
            if i == 3:
                print("Table 3 head:")
                print(t.head(3))
                
        if len(tables) < 4:
            print(f"FAILED: Tables count ({len(tables)}) is less than 4")
            return False
            
        t = tables[3]
        # Check if expected columns exist by position
        if t.shape[1] < 7:
            print(f"FAILED: Table 3 columns count ({t.shape[1]}) is less than 7")
            return False
            
        print("Success: Found potential data table")
        return True
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        return False

# 삼성전자(005930), 카카오(035720) 테스트
test_naver_frgn("005930")
test_naver_frgn("035720")
