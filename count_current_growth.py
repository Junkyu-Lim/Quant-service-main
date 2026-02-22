import pandas as pd
import db
import config
import numpy as np

def count_growth_stocks_relaxed():
    try:
        # DB에서 데이터 로드
        df = db.load_dashboard()
        if df.empty:
            print("데이터 없음")
            return

        # quant_screener.py의 완화된 apply_growth_mom_screen 로직 적용
        mask = (
            # 1. 외형과 내실의 동반 성장 (10% 이상)
            (df["매출_CAGR"].fillna(0) >= 10)
            & (df["영업이익_CAGR"].fillna(0) >= 10)
            
            # 2. 최근 분기 실적 성장 (역성장 제외)
            & (df["Q_영업이익_YoY(%)"].fillna(0) > 0)
            
            # 3. 시장 주도 모멘텀 (RS 50 이상)
            & (df["RS_등급"].fillna(0) >= 50)
            
            # 4. 리스크 방어 (흑자도산 방지)
            & (df["TTM_영업CF"].fillna(-1) > 0)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )
        
        filtered_df = df[mask]
        
        print(f"Total Stocks: {len(df)}")
        print(f"Relaxed Growth Momentum Stocks: {len(filtered_df)}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    count_growth_stocks_relaxed()
