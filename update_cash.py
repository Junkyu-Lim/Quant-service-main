#!/usr/bin/env python3
import sys
sys.path.insert(0, '/c/Users/limjk/Quant-service-main')

import db

# 487240 매입금액: 100주 × 30,280원 = 3,028,000원
cost = 100 * 30280
print(f"487240 매입금액: {cost:,}원")

# 현재 예수금
current_cash = db.load_cash()
print(f"현재 예수금: {current_cash:,.0f}원")

# 새 예수금 = 현재 - 매입금액
new_cash = current_cash - cost
print(f"반영 후 예수금: {new_cash:,.0f}원")

# 예수금 저장
db.save_cash(new_cash)
print("✓ 예수금이 업데이트되었습니다.")
