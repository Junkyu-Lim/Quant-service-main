
import pandas as pd
import sqlite3
import config
from quant_collector_enhanced import _fetch_investor_trading_naver, get_biz_day

def test_failed_tickers(limit=100):
    conn = sqlite3.connect(config.DB_PATH)
    biz_day = get_biz_day()
    
    # 1. 마스터 종목 중 수집 대상(보통주, KOSPI/KOSDAQ) 가져오기
    master_query = """
    SELECT 종목코드, 종목명 FROM master 
    WHERE 종목구분 = '보통주' AND 시장구분 IN ('KOSPI', 'KOSDAQ')
    """
    master_df = pd.read_sql(master_query, conn)
    master_df['종목코드'] = master_df['종목코드'].astype(str).str.zfill(6)
    
    # 2. 이미 수집된 종목 가져오기
    # (주의: investor_trading 테이블은 날짜별로 데이터가 쌓이므로, 최신 데이터 존재 여부 확인)
    collected_query = "SELECT DISTINCT 종목코드 FROM investor_trading"
    try:
        collected_df = pd.read_sql(collected_query, conn)
        collected_codes = set(collected_df['종목코드'].astype(str).str.zfill(6))
    except:
        collected_codes = set()
    
    # 3. 누락된 종목 추출
    failed_targets = master_df[~master_df['종목코드'].isin(collected_codes)]
    print(f"Total missing tickers: {len(failed_targets)}")
    
    if len(failed_targets) == 0:
        print("No missing tickers found in DB. Testing 100 random tickers instead.")
        test_samples = master_df.sample(min(limit, len(master_df)))
    else:
        test_samples = failed_targets.head(limit)
    
    results = []
    success_count = 0
    
    print(f"\n--- Testing {len(test_samples)} tickers ---")
    for _, row in test_samples.iterrows():
        code = row['종목코드']
        name = row['종목명']
        rows = _fetch_investor_trading_naver(code, 5) # 속도를 위해 5일치만
        if rows:
            success_count += 1
            results.append({"code": code, "name": name, "status": "Success", "rows": len(rows)})
            if success_count % 10 == 0:
                print(f"Progress: {success_count} succeeded...")
        else:
            results.append({"code": code, "name": name, "status": "Failed", "rows": 0})
            print(f"FAILED: {code} {name}")
            
    conn.close()
    
    res_df = pd.DataFrame(results)
    print(f"\nFinal Result: {success_count}/{len(test_samples)} Succeeded")
    if not res_df.empty:
        print("\nSample of results:")
        print(res_df.head(10))

test_failed_tickers(100)
