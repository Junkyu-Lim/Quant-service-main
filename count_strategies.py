import pandas as pd
import db
import config

def count_strategies():
    try:
        # Load data
        df = db.load_dashboard()
        if df.empty:
            print("No data in DB.")
            return

        # 1. Growth Momentum (For reference)
        mask_growth = (
            ((df["매출_CAGR"].fillna(0) >= 15) | (df["영업이익_CAGR"].fillna(0) >= 15))
            & (df["Q_영업이익_YoY(%)"].fillna(0) > 0)
            & (df["MA20_이격도(%)"].fillna(-999) >= -5)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )

        # 2. Quality Value
        mask_quality = (
            (df["ROE(%)"].fillna(0) >= 10)
            & (df["PEG"].fillna(99) < 1.5)
            & (df["PER"].fillna(0).between(1, 40))
            & (df["F스코어"].fillna(0) >= 4)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )
        
        print(f"Total Stocks: {len(df)}")
        print(f"Growth Momentum: {len(df[mask_growth])}")
        print(f"Quality Value: {len(df[mask_quality])}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    count_strategies()
