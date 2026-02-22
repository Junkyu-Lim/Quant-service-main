import pandas as pd
import db
import config

def count_growth_stocks():
    try:
        # DB에서 데이터 로드
        df = db.load_dashboard()
        if df.empty:
            print("No data in DB.")
            return

        # 고성장 모멘텀 (GrowthMom) 필터링 로직 적용
        mask = (
            ((df["매출_CAGR"].fillna(0) >= 15) | (df["영업이익_CAGR"].fillna(0) >= 15))
            & (df["Q_영업이익_YoY(%)"].fillna(0) > 0)
            & (df["MA20_이격도(%)"].fillna(-999) >= -5)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )
        
        filtered_df = df[mask]
        
        print(f"Total Stocks: {len(df)}")
        print(f"Growth Momentum Stocks: {len(filtered_df)}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    count_growth_stocks()
