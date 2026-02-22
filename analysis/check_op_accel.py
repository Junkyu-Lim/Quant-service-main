
import sys
import os
import pandas as pd
import numpy as np

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

def check_op_acceleration():
    print("Loading dashboard data...")
    df = db.load_dashboard()
    
    if df.empty:
        print("Dashboard table is empty.")
        return

    if "영업이익_가속도" not in df.columns:
        print("Column '영업이익_가속도' does not exist in dashboard_result.")
        return

    total = len(df)
    non_null = df["영업이익_가속도"].count()
    null_count = total - non_null
    
    print(f"Total rows: {total}")
    print(f"Non-null '영업이익_가속도': {non_null} ({non_null/total*100:.1f}%)")
    print(f"Null '영업이익_가속도': {null_count}")

    if non_null > 0:
        print("\n--- Sample Values (Top 10 by Market Cap) ---")
        sample = df.sort_values("시가총액", ascending=False).head(10)[["종목명", "종목코드", "영업이익_가속도", "매출_가속도", "Q_영업이익_YoY(%)"]]
        print(sample)
        
        print("\n--- Sample with Values (Top 5 Positive Accel) ---")
        positive = df[df["영업이익_가속도"] > 0].sort_values("영업이익_가속도", ascending=False).head(5)[["종목명", "종목코드", "영업이익_가속도"]]
        print(positive)
    else:
        print("\nALL VALUES ARE NULL. Something is wrong with the calculation.")

if __name__ == "__main__":
    check_op_acceleration()
