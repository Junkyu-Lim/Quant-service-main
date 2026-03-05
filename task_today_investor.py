
import pandas as pd
import duckdb
import config
import logging
from datetime import datetime
from quant_collector_enhanced import collect_investor_trading, get_biz_day

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("INVESTOR_TASK")

def run_today_investor_collection():
    biz_day = get_biz_day()
    log.info(f"🚀 오늘 날짜({biz_day}) 투자자 매매동향 전체 수집 시작")
    
    # 1. 대상 종목 추출 (보통주 + KOSPI/KOSDAQ)
    conn = duckdb.connect(str(config.DB_PATH))
    try:
        master_df = conn.execute("""
            SELECT 종목코드 FROM master 
            WHERE 종목구분 = '보통주' AND 시장구분 IN ('KOSPI', 'KOSDAQ')
            AND collected_date = (SELECT MAX(collected_date) FROM master)
        """).df()
    except Exception as e:
        log.error(f"마스터 데이터 로드 실패: {e}")
        conn.close()
        return

    targets = [str(x).zfill(6) for x in master_df['종목코드'].tolist()]
    log.info(f"🎯 수집 대상: {len(targets)}개 종목")

    # 2. 투자자 매매동향 수집 (최근 60일치 데이터 확보하여 누락 방지)
    # 개선된 collect_investor_trading 함수 사용
    inv_df = collect_investor_trading(targets, days=60)
    
    if inv_df.empty:
        log.warning("⚠️ 수집된 데이터가 없습니다.")
        conn.close()
        return

    # 3. 데이터 정제 및 collected_date 추가
    inv_df["collected_date"] = biz_day
    
    # 4. DB 저장 (기존 오늘 날짜 데이터 삭제 후 삽입 - db.save_df 로직과 동일)
    log.info(f"💾 DB 저장 중... (총 {len(inv_df)}행)")
    try:
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DELETE FROM investor_trading WHERE collected_date = ?", [biz_day])
        # DataFrame을 DuckDB에 직접 삽입
        conn.execute("INSERT INTO investor_trading SELECT * FROM inv_df")
        conn.execute("COMMIT")
        log.info(f"✅ 저장 완료: investor_trading (date={biz_day})")
    except Exception as e:
        conn.execute("ROLLBACK")
        log.error(f"❌ DB 저장 실패: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_today_investor_collection()
