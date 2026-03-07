#!/usr/bin/env python3
"""
487240 항목의 매입금액을 예수금에 반영합니다.
"""
import duckdb

DB_PATH = "data/quant.duckdb"

# DuckDB 연결
conn = duckdb.connect(DB_PATH, read_only=False)

# 487240 정보 조회
print("=== 487240 정보 조회 ===")
result = conn.execute(
    "SELECT 종목명, 수량, 평균매입가 FROM portfolio WHERE 종목코드 = '487240'"
).fetchone()

if result:
    name, qty, price = result
    cost = qty * price
    print(f"종목명: {name}")
    print(f"수량: {qty}주")
    print(f"단가: {price:.0f}원")
    print(f"매입금액: {cost:,.0f}원")

    # 예수금 조회
    cash_result = conn.execute(
        "SELECT amount FROM portfolio_cash WHERE id = 1"
    ).fetchone()
    current_cash = cash_result[0] if cash_result else 0
    print(f"\n현재 예수금: {current_cash:,.0f}원")

    # 새 예수금 = 현재 - 487240 매입금액
    new_cash = current_cash - cost
    print(f"반영 후 예수금: {new_cash:,.0f}원")

    # 예수금 업데이트
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_cash (id, amount, updated_at) VALUES (1, ?, CURRENT_TIMESTAMP)",
        [new_cash]
    )
    print("\n✓ 예수금이 업데이트되었습니다.")
else:
    print("487240를 포트폴리오에서 찾을 수 없습니다.")

conn.close()
