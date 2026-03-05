
import pandas as pd
import duckdb
import config
from quant_collector_enhanced import _fetch_investor_trading_naver, get_biz_day

def test_failed_tickers_duckdb(limit=100):
    # DuckDB 연결
    conn = duckdb.connect(str(config.DB_PATH))
    
    # 1. 마스터 종목 중 수집 대상(보통주, KOSPI/KOSDAQ) 가져오기
    master_query = """
    SELECT 종목코드, 종목명 FROM master 
    WHERE 종목구분 = '보통주' AND 시장구분 IN ('KOSPI', 'KOSDAQ')
    """
    master_df = conn.execute(master_query).df()
    master_df['종목코드'] = master_df['종목코드'].astype(str).str.zfill(6)
    
    # 2. 이미 수집된 종목 가져오기
    try:
        collected_query = "SELECT DISTINCT 종목코드 FROM investor_trading"
        collected_df = conn.execute(collected_query).df()
        collected_codes = set(collected_df['종목코드'].astype(str).str.zfill(6))
    except Exception as e:
        print(f"investor_trading table error (likely empty): {e}")
        collected_codes = set()
    
    # 3. 누락된 종목 추출
    failed_targets = master_df[~master_df['종목코드'].isin(collected_codes)]
    print(f"Total missing tickers: {len(failed_targets)}")
    
    if len(failed_targets) == 0:
        print("No missing tickers found in DB. Testing 100 random tickers instead.")
        test_samples = master_df.sample(min(limit, len(master_df)))
    else:
        # 무작위로 100개 섞어서 테스트 (앞부분만 하면 특정 시장에 쏠릴 수 있음)
        test_samples = failed_targets.sample(min(limit, len(failed_targets)))
    
    results = []
    success_count = 0
    
    print(f"\n--- Testing {len(test_samples)} tickers using DuckDB ---")
    for _, row in test_samples.iterrows():
        code = row['종목코드']
        name = row['종목명']
        try:
            rows = _fetch_investor_trading_naver(code, 5) # 속도를 위해 5일치만
            if rows:
                success_count += 1
                results.append({"code": code, "name": name, "status": "Success", "rows": len(rows)})
                if success_count % 10 == 0:
                    print(f"Progress: {success_count} succeeded...")
            else:
                results.append({"code": code, "name": name, "status": "Failed", "rows": 0})
                print(f"FAILED: {code} {name}")
        except Exception as e:
            print(f"ERROR: {code} {name} -> {e}")
            results.append({"code": code, "name": name, "status": "Error", "rows": 0})
            
    conn.close()
    
    res_df = pd.DataFrame(results)
    print(f"\nFinal Result: {success_count}/{len(test_samples)} Succeeded")
    if not res_df.empty:
        print("\nSuccess rate: {:.1f}%".format(success_count / len(test_samples) * 100))
        failed_list = res_df[res_df['status'] != 'Success']
        if not failed_list.empty:
            print("\nFailed/Error sample:")
            print(failed_list.head(10))

test_failed_tickers_duckdb(100)
