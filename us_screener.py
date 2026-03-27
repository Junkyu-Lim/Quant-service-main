# =========================================================
# us_screener.py  —  미국 주식 퀀트 스크리너
# ---------------------------------------------------------
# quant_screener.py(한국) 로직을 US yfinance 데이터 구조에 맞게 포팅.
# 주요 차이:
#   - 계정명: 한국어 → US_ACCOUNTS 영문 별칭 매핑
#   - multiplier = 1 (yfinance는 USD raw 값)
#   - Ke = US_RISK_FREE_RATE + US_EQUITY_RISK_PREMIUM (config)
#   - ROIC 세율 21% (US 법인세)
#   - 수급강도/스마트머니/외인·기관 데이터 없음 → 가중치 재배분
#   - VCP: 가격+거래량 압축만 (스마트머니 조건 제거)
#   - Forward 컨센서스: 모두 NaN (yfinance 미지원)
#   - RS 기준: NYSE→SP500, NASDAQ→NASDAQ
# =========================================================

import logging
import numpy as np
import pandas as pd

import config

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, desc="", **kwargs):
        items = list(iterable)
        total = len(items)
        for i, item in enumerate(items):
            if i % max(1, total // 10) == 0:
                print(f"  {desc}: {i}/{total} ({i*100//total}%)")
            yield item
        print(f"  {desc}: {total}/{total} (100%)")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("US_SCREENER")

DATA_DIR = config.DATA_DIR

# ─────────────────────────────────────────────
# US 계정명 매핑 (yfinance 영문 → 내부 키)
# ─────────────────────────────────────────────
US_ACCOUNTS = {
    "매출액":           ["Total Revenue", "Revenue", "Operating Revenue"],
    "영업이익":         ["Operating Income", "EBIT", "Operating Income Loss"],
    "순이익":           ["Net Income", "Net Income Common Stockholders",
                        "Net Income Including Noncontrolling Interests",
                        "Net Income From Continuing Operations",
                        "Net Income Continuous Operations"],
    "자본":             ["Stockholders Equity", "Total Stockholder Equity",
                        "Common Stock Equity", "Total Equity Gross Minority Interest"],
    "부채":             ["Total Liabilities Net Minority Interest", "Total Liabilities"],
    "영업CF":           ["Operating Cash Flow",
                        "Cash Flow From Continuing Operating Activities",
                        "Net Cash Provided By Operating Activities"],
    "CAPEX":            ["Capital Expenditure", "Purchase Of Fixed Assets",
                        "Capital Expenditures"],
    "자산총계":         ["Total Assets"],
    "유동자산":         ["Current Assets", "Total Current Assets"],
    "유동부채":         ["Current Liabilities", "Total Current Liabilities"],
    "매출총이익":       ["Gross Profit"],
    "이자비용":         ["Interest Expense", "Interest Expense Non Operating"],
    "현금및현금성자산": ["Cash And Cash Equivalents",
                        "Cash Cash Equivalents And Short Term Investments"],
    "총차입금":         ["Total Debt", "Long Term Debt And Capital Lease Obligation",
                        "Long Term Debt"],
    "이익잉여금":       ["Retained Earnings"],
    "자사주매입":       ["Repurchase Of Capital Stock", "Common Stock Payments",
                        "Repurchase Of Common Stock"],
    "발행주식수":       ["Ordinary Shares Number", "Share Issued", "Basic Average Shares"],
}

# RS 계산용 지수 매핑 (exchange → index_code)
US_INDEX_MAP = {
    "NYSE":   "SP500",
    "NASDAQ": "NASDAQ",
}
US_DEFAULT_INDEX = "SP500"

# CAGR/YoY 극단값 캡
_CAGR_CAP   = 150
_CAGR_FLOOR = -80
_YOY_CAP    = 300
_YOY_FLOOR  = -90


def normalize_us_stock_type(value) -> str:
    """US stock_type을 한국 UI용 종목구분으로 변환."""
    if value is None:
        return "보통주"
    text = str(value).strip()
    if not text:
        return "보통주"
    lower = text.lower()
    if "preferred" in lower or "pref" in lower:
        return "우선주"
    if "reit" in lower:
        return "리츠"
    if "spac" in lower or "blank check" in lower:
        return "스팩"
    if "etf" in lower:
        return "ETF"
    if "common" in lower:
        return "보통주"
    return text


# ═════════════════════════════════════════════
# DB 로드 유틸리티
# ═════════════════════════════════════════════

def load_us_table(table: str) -> pd.DataFrame:
    """us_* 테이블 최신 수집 배치 로드"""
    import db as _db
    df = _db.load_latest(table)
    if df.empty:
        return df
    df.columns = df.columns.str.strip()
    # 날짜 컬럼 정규화
    for date_col in ["base_date", "date"]:
        if date_col in df.columns:
            df[date_col] = df[date_col].astype(str).str[:10]
    return df


def _load_us_table_per_ticker(table: str) -> pd.DataFrame:
    """ticker별 최신 행 로드 (rate limit 누락 방어용)"""
    import db as _db
    df = _db.load_latest_per_ticker(table, ticker_col="ticker")
    if df.empty:
        return df
    df.columns = df.columns.str.strip()
    for date_col in ["base_date", "date"]:
        if date_col in df.columns:
            df[date_col] = df[date_col].astype(str).str[:10]
    return df


# ═════════════════════════════════════════════
# 계정 조회 (US)
# ═════════════════════════════════════════════

def find_us_account_value(fs_df: pd.DataFrame, target_key: str) -> dict:
    """us_financial_statements long-format에서 계정값 추출.

    fs_df 컬럼: ticker, base_date, account, period, value, is_estimate
    반환: {base_date: float}
    """
    if fs_df.empty or "account" not in fs_df.columns:
        return {}
    aliases = US_ACCOUNTS.get(target_key, [target_key])
    matched = fs_df[fs_df["account"].isin(aliases)]
    if matched.empty:
        return {}
    # 우선순위: aliases 순서가 빠를수록 우선
    alias_priority = {a: i for i, a in enumerate(aliases)}
    matched = matched.copy()
    matched["_prio"] = matched["account"].map(alias_priority).fillna(999)
    # 같은 base_date에 여러 계정 매칭 시 우선순위 낮은(index 작은) 것 선택
    matched = matched.sort_values(["base_date", "_prio"]).drop_duplicates("base_date", keep="first")
    return {row["base_date"]: float(row["value"]) if pd.notna(row["value"]) else None
            for _, row in matched.iterrows()}


# ═════════════════════════════════════════════
# 분석 유틸리티 (한국 버전과 동일 로직)
# ═════════════════════════════════════════════

def calc_cagr(series_dict: dict, min_years: int = 2) -> float:
    if not series_dict or len(series_dict) < min_years:
        return np.nan
    dates = sorted(series_dict.keys())
    v0, v1 = series_dict[dates[0]], series_dict[dates[-1]]
    if v0 is None or v1 is None or v0 <= 0 or v1 <= 0:
        return np.nan
    try:
        years = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days / 365.25
        return ((v1 / v0) ** (1 / years) - 1) * 100 if years > 0.5 else np.nan
    except Exception:
        return np.nan


def count_consecutive_growth(series_dict: dict) -> int:
    if not series_dict or len(series_dict) < 2:
        return 0
    vals = [series_dict[d] for d in sorted(series_dict.keys())]
    while vals and vals[-1] is None:
        vals.pop()
    if len(vals) < 2:
        return 0
    count = 0
    for i in range(len(vals) - 1, 0, -1):
        if vals[i] is None or vals[i - 1] is None:
            break
        if vals[i] > vals[i - 1]:
            count += 1
        else:
            break
    return count


def calc_ttm_yoy(q_df: pd.DataFrame, target_key: str) -> dict:
    """분기 재무제표 DataFrame에서 TTM YoY 계산"""
    res = {"ttm_current": np.nan, "ttm_prev": np.nan, "ttm_yoy": np.nan}
    vals = find_us_account_value(q_df, target_key)
    if len(vals) < 4:
        return res
    dates = sorted(vals.keys())
    last4 = dates[-4:]
    prev4 = dates[-8:-4] if len(dates) >= 8 else []
    last4_valid = [d for d in last4 if d in vals and vals[d] is not None]
    prev4_valid = [d for d in prev4 if d in vals and vals[d] is not None]
    ttm_curr = sum(vals[d] for d in last4_valid)
    ttm_prev = sum(vals[d] for d in prev4_valid)
    if len(last4_valid) == 4:
        res["ttm_current"] = ttm_curr
    if len(prev4_valid) == 4:
        res["ttm_prev"] = ttm_prev
    if pd.notna(res["ttm_current"]) and pd.notna(res["ttm_prev"]) and res["ttm_prev"] > 0:
        res["ttm_yoy"] = ((res["ttm_current"] / res["ttm_prev"]) - 1) * 100
    return res


def fill_ttm_yoy_from_annual(ttm_res: dict, annual_series: dict, latest_quarter: str) -> dict:
    """yfinance 5분기 제한으로 비는 TTM YoY를 연간 결산 시점에서 보완."""
    if pd.notna(ttm_res.get("ttm_yoy")):
        return ttm_res
    if not annual_series or len(annual_series) < 2 or not latest_quarter:
        return ttm_res

    annual_dates = sorted(annual_series.keys())
    if annual_dates[-1] != latest_quarter:
        return ttm_res

    cur = annual_series.get(annual_dates[-1])
    prev = annual_series.get(annual_dates[-2])
    if cur is not None:
        ttm_res["ttm_current"] = cur
    if prev is not None:
        ttm_res["ttm_prev"] = prev
    if cur is not None and prev is not None and prev > 0:
        ttm_res["ttm_yoy"] = ((cur / prev) - 1) * 100
    return ttm_res


def calc_quarterly_yoy_us(q_df: pd.DataFrame, target_key: str) -> dict:
    """분기별 YoY% 시리즈 계산"""
    res = {"latest_yoy": np.nan, "consecutive_yoy_growth": 0, "latest_quarter": "", "yoy_series": {}}
    vals = find_us_account_value(q_df, target_key)
    yoy_s = {}
    if len(vals) >= 5:
        for d in sorted(vals.keys()):
            try:
                prev_d = str(int(d[:4]) - 1) + d[4:]
            except (ValueError, IndexError):
                continue
            if (prev_d in vals and vals[prev_d] is not None
                    and vals[d] is not None and vals[prev_d] > 0):
                yoy_s[d] = ((vals[d] / vals[prev_d]) - 1) * 100
    if not yoy_s:
        return res
    res["yoy_series"] = yoy_s
    res["latest_quarter"] = max(yoy_s.keys())
    res["latest_yoy"] = yoy_s[res["latest_quarter"]]
    for d in sorted(yoy_s.keys(), reverse=True):
        if yoy_s[d] > 0:
            res["consecutive_yoy_growth"] += 1
        else:
            break
    return res


# ═════════════════════════════════════════════
# 종목 분석 (한국 analyze_one_stock US 버전)
# ═════════════════════════════════════════════

def analyze_one_us_stock(ticker: str, ind_grp: pd.DataFrame, fs_grp: pd.DataFrame) -> dict:
    res = {"종목코드": ticker}

    has_fs = not fs_grp.empty

    if not has_fs:
        return res

    fs_y = fs_grp[fs_grp["period"] == "y"] if "period" in fs_grp.columns else pd.DataFrame()
    fs_q = fs_grp[fs_grp["period"] == "q"] if "period" in fs_grp.columns else pd.DataFrame()

    def _yr(key):
        v = find_us_account_value(fs_y, key)
        return v

    def _qr(key):
        return find_us_account_value(fs_q, key)

    # ── 분기 YoY & TTM ──
    def _calc_acceleration(qyoy_result):
        yoy_s = qyoy_result.get("yoy_series", {})
        if len(yoy_s) < 3:
            return np.nan, False
        dates = sorted(yoy_s.keys())
        d0, d1, d2 = dates[-3], dates[-2], dates[-1]
        delta_prev = yoy_s[d1] - yoy_s[d0]
        delta_latest = yoy_s[d2] - yoy_s[d1]
        consecutive = (delta_prev > 0 and delta_latest > 0)
        return delta_latest, consecutive

    def _calc_deceleration(qyoy_result):
        yoy_s = qyoy_result.get("yoy_series", {})
        if len(yoy_s) < 3:
            return 0, np.nan
        dates = sorted(yoy_s.keys())
        recent_3 = [yoy_s[d] for d in dates[-3:]]
        if all(v > 0 for v in recent_3) and recent_3[0] > recent_3[1] > recent_3[2]:
            return 1, recent_3[0] - recent_3[2]
        if len(recent_3) >= 2 and recent_3[-2] < 0 and recent_3[-1] < 0 and recent_3[-1] < recent_3[-2]:
            return 1, recent_3[-2] - recent_3[-1]
        if len(recent_3) >= 2 and recent_3[-2] > 0 and recent_3[-1] < 0:
            return 1, recent_3[-2] - recent_3[-1]
        return 0, np.nan

    latest_quarter = sorted(fs_q["base_date"].unique())[-1] if (not fs_q.empty and "base_date" in fs_q.columns) else ""

    for label, key in [("매출", "매출액"), ("영업이익", "영업이익"), ("순이익", "순이익")]:
        annual_vals = _yr(key)
        qyoy = calc_quarterly_yoy_us(fs_q, key)
        res[f"Q_{label}_YoY(%)"] = qyoy["latest_yoy"]
        res[f"Q_{label}_연속YoY성장"] = qyoy["consecutive_yoy_growth"]
        ttmy = calc_ttm_yoy(fs_q, key)
        ttmy = fill_ttm_yoy_from_annual(ttmy, annual_vals, latest_quarter)
        res[f"TTM_{label}_YoY(%)"] = ttmy["ttm_yoy"]
        val = ttmy["ttm_current"]
        if pd.isna(val):
            if annual_vals:
                val = annual_vals[max(annual_vals.keys())]
        res[f"TTM_{label}"] = val

    res["최근분기"] = latest_quarter

    op_qyoy = calc_quarterly_yoy_us(fs_q, "영업이익")
    rev_qyoy = calc_quarterly_yoy_us(fs_q, "매출액")
    op_accel, op_consec = _calc_acceleration(op_qyoy)
    rev_accel, rev_consec = _calc_acceleration(rev_qyoy)
    res["영업이익_가속도"] = op_accel
    res["매출_가속도"] = rev_accel

    op_decel, op_decel_mag = _calc_deceleration(op_qyoy)
    rev_decel, _ = _calc_deceleration(rev_qyoy)
    res["영업이익_감속경고"] = op_decel
    res["영업이익_감속폭(pp)"] = op_decel_mag
    res["매출_감속경고"] = rev_decel
    res["실적감속_경고"] = 1 if op_decel == 1 else 0

    latest_q = res.get("최근분기", "")
    is_q4 = (latest_q.endswith("12") or latest_q.endswith("Q4") or
             (len(latest_q) >= 6 and latest_q[4:6] == "12"))
    res["_op_consec_accel"] = op_consec
    res["_rev_consec_accel"] = rev_consec
    res["_is_q4"] = is_q4

    # ── 연간 기반 지표 (연간 없으면 분기 BS로 fallback) ──
    def _yr_or_qr(key):
        v = _yr(key)
        if not v:
            v = _qr(key)
        return v

    total_assets_s = _yr_or_qr("자산총계")
    equity_s       = _yr_or_qr("자본")
    debt_s         = _yr_or_qr("부채")
    current_assets_s = _yr_or_qr("유동자산")
    current_liab_s   = _yr_or_qr("유동부채")
    gross_profit_s   = _yr("매출총이익")  # GPM은 연간 기준 유지
    cash_s           = _yr_or_qr("현금및현금성자산")
    interest_s       = _yr("이자비용")

    res["자산총계"] = total_assets_s[max(total_assets_s.keys())] if total_assets_s else np.nan
    res["자본"] = equity_s[max(equity_s.keys())] if equity_s else np.nan
    res["부채"] = debt_s[max(debt_s.keys())] if debt_s else np.nan

    # 발행주식수 (market_cap 보완용)
    shares_fs_s = _yr("발행주식수")
    if not shares_fs_s:
        shares_fs_s = find_us_account_value(fs_q, "발행주식수")
    res["발행주식수_fs"] = shares_fs_s[max(shares_fs_s.keys())] if shares_fs_s else np.nan

    rev_s = _yr("매출액")
    op_s  = _yr("영업이익")
    ni_s  = _yr("순이익")

    res.update({
        "매출_CAGR": calc_cagr(rev_s),
        "영업이익_CAGR": calc_cagr(op_s),
        "순이익_CAGR": calc_cagr(ni_s),
        "매출_연속성장": count_consecutive_growth(rev_s),
        "영업이익_연속성장": count_consecutive_growth(op_s),
        "순이익_연속성장": count_consecutive_growth(ni_s),
    })
    res["데이터_연수"] = len(rev_s)

    if len(rev_s) >= 2 and len(op_s) >= 2:
        l, p = sorted(rev_s.keys())[-1], sorted(rev_s.keys())[-2]
        opm_l = (op_s[l] / rev_s[l] * 100) if (rev_s.get(l) and op_s.get(l) is not None and rev_s[l] > 0) else np.nan
        opm_p = (op_s[p] / rev_s[p] * 100) if (rev_s.get(p) and op_s.get(p) is not None and rev_s[p] > 0) else np.nan
        res["영업이익률_최근"] = opm_l
        res["영업이익률_전년"] = opm_p
        res["이익률_개선"] = 1 if pd.notna(opm_l) and pd.notna(opm_p) and opm_l > opm_p else 0
        delta = opm_l - opm_p if pd.notna(opm_l) and pd.notna(opm_p) else np.nan
        res["이익률_변동폭"] = delta
        res["이익률_급개선"] = 1 if (delta or 0) >= 5 else 0
    else:
        res["영업이익률_최근"] = np.nan
        res["영업이익률_전년"] = np.nan
        res["이익률_개선"] = 0
        res["이익률_변동폭"] = np.nan
        res["이익률_급개선"] = 0

    _rev_growing = res.get("매출_연속성장", 0) >= 1
    _opm_improving = res.get("이익률_개선", 0) == 1
    if _rev_growing and _opm_improving:
        res["매출이익_동행성"] = 2
    elif _rev_growing and not _opm_improving:
        res["매출이익_동행성"] = 0
    elif not _rev_growing and _opm_improving:
        res["매출이익_동행성"] = 1
    else:
        res["매출이익_동행성"] = -1

    if len(ni_s) >= 2:
        ni_vals = [ni_s[d] for d in sorted(ni_s.keys())]
        res["흑자전환"] = 1 if (ni_vals[-2] is not None and ni_vals[-1] is not None
                                and ni_vals[-2] < 0 and ni_vals[-1] > 0) else 0
        res["순이익_당기양수"] = 1 if (ni_vals[-1] is not None and ni_vals[-1] > 0) else 0
        res["순이익_전년음수"] = 1 if (ni_vals[-2] is not None and ni_vals[-2] < 0) else 0
    else:
        res["흑자전환"] = 0
        res["순이익_당기양수"] = 0
        res["순이익_전년음수"] = 0

    # ── 현금흐름 ──
    ocf_s = _yr("영업CF")
    capex_s = _yr("CAPEX")
    capex_s = {d: abs(v) for d, v in capex_s.items() if v is not None}
    fcf_s = {d: (ocf_s[d] - capex_s[d])
             for d in (set(ocf_s.keys()) & set(capex_s.keys()))
             if ocf_s[d] is not None and capex_s[d] is not None}
    ttm_ocf = ocf_s[max(ocf_s.keys())] if ocf_s else np.nan
    ttm_capex = capex_s[max(capex_s.keys())] if capex_s else np.nan
    res.update({
        "TTM_영업CF": ttm_ocf,
        "TTM_CAPEX": ttm_capex,
        "TTM_FCF": (ttm_ocf - ttm_capex if pd.notna(ttm_ocf) and pd.notna(ttm_capex) else np.nan),
        "영업CF_CAGR": calc_cagr(ocf_s),
        "FCF_CAGR": calc_cagr(fcf_s),
        "영업CF_연속성장": count_consecutive_growth(ocf_s),
    })

    # ── 자사주 매입 (Buyback) ──
    buyback_s = _yr("자사주매입")
    # yfinance cashflow에서 자사주 매입은 음수로 표시됨 → abs
    buyback_s = {d: abs(v) for d, v in buyback_s.items() if v is not None and v != 0}
    if buyback_s:
        res["순자사주매입"] = buyback_s[max(buyback_s.keys())]
    else:
        res["순자사주매입"] = np.nan

    # ── F-Score ──
    f1 = 1 if (res.get("TTM_순이익") or 0) > 0 else 0
    f2 = 1 if (ttm_ocf or 0) > 0 else 0
    f4 = 1 if (ttm_ocf or 0) > (res.get("TTM_순이익") or 0) and f1 else 0

    f3 = 0
    if len(ni_s) >= 2 and len(total_assets_s) >= 2:
        ni_dates = sorted(ni_s.keys())
        ta_dates = sorted(total_assets_s.keys())
        for offset in [(ni_dates[-1], ni_dates[-2])]:
            d1, d2 = offset
            ta1 = total_assets_s.get(d1) or (total_assets_s[ta_dates[-1]] if ta_dates else None)
            ta2 = total_assets_s.get(d2) or (total_assets_s[ta_dates[-2]] if len(ta_dates) >= 2 else None)
            if ta1 and ta2 and ni_s[d1] is not None and ni_s[d2] is not None and ta1 > 0 and ta2 > 0:
                f3 = 1 if (ni_s[d1] / ta1) > (ni_s[d2] / ta2) else 0

    f5 = 0
    if len(debt_s) >= 2 and len(equity_s) >= 2:
        d_dates = sorted(debt_s.keys())
        e_dates = sorted(equity_s.keys())
        _eq_cur = equity_s[e_dates[-1]]
        _eq_prev = equity_s[e_dates[-2]] if len(e_dates) >= 2 else None
        dr_cur = debt_s[d_dates[-1]] / _eq_cur if (_eq_cur is not None and abs(_eq_cur) > 1e-6 and debt_s[d_dates[-1]] is not None) else 999
        dr_prev = debt_s[d_dates[-2]] / _eq_prev if (len(d_dates) >= 2 and _eq_prev is not None and abs(_eq_prev) > 1e-6 and debt_s[d_dates[-2]] is not None) else 999
        f5 = 1 if dr_cur < dr_prev else 0

    f6 = 0
    _cr_cur_ratio = np.nan
    if len(current_assets_s) >= 2 and len(current_liab_s) >= 2:
        ca_dates = sorted(current_assets_s.keys())
        cl_dates = sorted(current_liab_s.keys())
        _cl_cur = current_liab_s[cl_dates[-1]]
        _cl_prev = current_liab_s[cl_dates[-2]] if len(cl_dates) >= 2 else None
        cr_cur = current_assets_s[ca_dates[-1]] / _cl_cur if (_cl_cur is not None and abs(_cl_cur) > 1e-6 and current_assets_s[ca_dates[-1]] is not None) else 0
        cr_prev = (current_assets_s[ca_dates[-2]] / _cl_prev
                   if (len(ca_dates) >= 2 and _cl_prev is not None and abs(_cl_prev) > 1e-6 and
                       current_assets_s[ca_dates[-2]] is not None) else 0)
        f6 = 1 if cr_cur > cr_prev else 0
        if cr_cur > 0:
            _cr_cur_ratio = cr_cur * 100

    # F7: 자본 증가분이 순이익 1.5배 초과 시 희석 의심
    f7 = 1
    if len(equity_s) >= 2 and len(ni_s) >= 2:
        eq_dates = sorted(equity_s.keys())
        eq_prev = equity_s[eq_dates[-2]]
        eq_cur = equity_s[eq_dates[-1]]
        ni_cur = ni_s.get(sorted(ni_s.keys())[-1])
        if (eq_prev is not None and eq_cur is not None and
                ni_cur is not None and eq_prev > 0):
            eq_growth = eq_cur - eq_prev
            if eq_growth > abs(ni_cur) * 1.5 + abs(eq_prev) * 0.05:
                f7 = 0

    f8 = 0
    if len(gross_profit_s) >= 2 and len(rev_s) >= 2:
        gp_dates = sorted(gross_profit_s.keys())
        rv_dates = sorted(rev_s.keys())
        if len(gp_dates) >= 2 and len(rv_dates) >= 2:
            _rv_c = rev_s[rv_dates[-1]]; _rv_p = rev_s[rv_dates[-2]]
            gpm_c = gross_profit_s[gp_dates[-1]] / _rv_c if (_rv_c is not None and abs(_rv_c) > 1e-6 and gross_profit_s[gp_dates[-1]] is not None) else 0
            gpm_p = gross_profit_s[gp_dates[-2]] / _rv_p if (_rv_p is not None and abs(_rv_p) > 1e-6 and gross_profit_s[gp_dates[-2]] is not None) else 0
            f8 = 1 if gpm_c > gpm_p else 0

    f9 = 0
    if len(rev_s) >= 2 and len(total_assets_s) >= 2:
        rv_dates = sorted(rev_s.keys())
        ta_dates = sorted(total_assets_s.keys())
        if len(rv_dates) >= 2 and len(ta_dates) >= 2:
            _ta_c = total_assets_s[ta_dates[-1]]; _ta_p = total_assets_s[ta_dates[-2]]
            at_c = rev_s[rv_dates[-1]] / _ta_c if (_ta_c is not None and abs(_ta_c) > 1e-6 and rev_s[rv_dates[-1]] is not None) else 0
            at_p = rev_s[rv_dates[-2]] / _ta_p if (_ta_p is not None and abs(_ta_p) > 1e-6 and rev_s[rv_dates[-2]] is not None) else 0
            f9 = 1 if at_c > at_p else 0

    res["F스코어"] = f1 + f2 + f3 + f4 + f5 + f6 + f7 + f8 + f9
    res["F1_수익성"] = f1; res["F2_영업CF"] = f2; res["F3_ROA개선"] = f3; res["F4_이익품질"] = f4
    res["F5_레버리지"] = f5; res["F6_유동성"] = f6; res["F7_희석없음"] = f7
    res["F8_매출총이익률"] = f8; res["F9_자산회전율"] = f9
    res["유동비율(%)"] = _cr_cur_ratio

    # ── GPM 연속값 & ROIC ──
    gpm_cur_val = np.nan
    gpm_prev_val = np.nan
    if len(gross_profit_s) >= 2 and len(rev_s) >= 2:
        gp_dates = sorted(gross_profit_s.keys())
        rv_dates_y = sorted(rev_s.keys())
        if len(gp_dates) >= 2 and len(rv_dates_y) >= 2:
            gpm_cur_val = (gross_profit_s[gp_dates[-1]] / rev_s[rv_dates_y[-1]] * 100
                           if rev_s[rv_dates_y[-1]] and gross_profit_s[gp_dates[-1]] is not None else np.nan)
            gpm_prev_val = (gross_profit_s[gp_dates[-2]] / rev_s[rv_dates_y[-2]] * 100
                            if rev_s[rv_dates_y[-2]] and gross_profit_s[gp_dates[-2]] is not None else np.nan)
    gpm_delta = (gpm_cur_val - gpm_prev_val) if pd.notna(gpm_cur_val) and pd.notna(gpm_prev_val) else np.nan
    res["GPM_최근(%)"] = gpm_cur_val
    res["GPM_전년(%)"] = gpm_prev_val
    res["GPM_변화(pp)"] = gpm_delta

    # 4Q 빅배스 Fallback 최종 결정
    op_consec_ = res.pop("_op_consec_accel", False)
    rev_consec_ = res.pop("_rev_consec_accel", False)
    is_q4_ = res.pop("_is_q4", False)
    gpm_ok = pd.notna(gpm_delta) and gpm_delta >= 0
    has_opm_data = pd.notna(res.get("영업이익률_최근")) and pd.notna(res.get("영업이익률_전년"))
    opm_ok = (not has_opm_data) or (res.get("이익률_개선", 0) == 1)
    if op_consec_ and opm_ok:
        res["실적가속_연속"] = 1
    elif is_q4_ and rev_consec_ and gpm_ok:
        res["실적가속_연속"] = 1
    else:
        res["실적가속_연속"] = 0

    # ROIC (US 세율 21%)
    roic_cur = np.nan
    roic_prev = np.nan
    if op_s and total_assets_s:
        op_dates_y = sorted(op_s.keys())
        ta_dates_y = sorted(total_assets_s.keys())
        cl_dates_r = sorted(current_liab_s.keys()) if current_liab_s else []
        rv_dates_y2 = sorted(rev_s.keys()) if rev_s else []
        cash_dates_r = sorted(cash_s.keys()) if cash_s else []
        for i in range(2):
            if i >= len(op_dates_y) or i >= len(ta_dates_y):
                continue
            op_key = op_dates_y[-(i + 1)]
            ta_key = ta_dates_y[-(i + 1)]
            op_val = op_s.get(op_key)
            ta_val = total_assets_s.get(ta_key)
            if op_val is None or ta_val is None or ta_val == 0:
                continue
            cl_val = current_liab_s.get(cl_dates_r[-(i + 1)]) if cl_dates_r and len(cl_dates_r) > i else None
            ic = (ta_val - cl_val) if cl_val is not None else ta_val
            cash_val = cash_s.get(cash_dates_r[-(i + 1)]) if cash_dates_r and len(cash_dates_r) > i else None
            rev_val = rev_s.get(rv_dates_y2[-(i + 1)]) if rv_dates_y2 and len(rv_dates_y2) > i else None
            if cash_val is not None:
                op_cash_need = (rev_val * 0.05) if rev_val is not None and rev_val > 0 else 0
                excess_cash = max(0, cash_val - op_cash_need)
                ic = ic - excess_cash
            if ic <= 0:
                ic = ta_val
            nopat = op_val * (1 - 0.21)  # US tax rate 21%
            roic_val = nopat / ic * 100
            if i == 0:
                roic_cur = roic_val
            else:
                roic_prev = roic_val

    res["ROIC(%)"] = roic_cur
    res["ROIC_전년(%)"] = roic_prev
    res["ROIC_개선"] = 1 if pd.notna(roic_cur) and pd.notna(roic_prev) and roic_cur > roic_prev else 0

    # 퀄리티 턴어라운드
    ttm_ocf_val = res.get("TTM_영업CF")
    ocf_positive = pd.notna(ttm_ocf_val) and ttm_ocf_val > 0
    gpm_surge = pd.notna(gpm_delta) and gpm_delta >= 2.0
    res["퀄리티_턴어라운드"] = 1 if (gpm_surge and ocf_positive and res["ROIC_개선"] == 1) else 0

    # 지속가치 품질 (0-6점)
    _sq = 0
    _comove = res.get("매출이익_동행성", -1)
    if _comove == 2:
        _sq += 2
    elif _comove == 1:
        _sq += 1
    _sq += f4
    if pd.notna(roic_cur) and roic_cur >= 10:
        _sq += 1
    if res.get("ROIC_개선", 0) == 1:
        _sq += 1
    _fcf_val = res.get("TTM_FCF")
    if pd.notna(_fcf_val) and _fcf_val > 0:
        _sq += 1
    res["지속가치_품질"] = _sq

    # 이자보상배율 / 무차입 기업
    ttm_interest = interest_s[max(interest_s.keys())] if interest_s else np.nan
    ttm_op_for_icr = res.get("TTM_영업이익")
    if pd.isna(ttm_op_for_icr):
        ttm_op_for_icr = op_s[max(op_s.keys())] if op_s else np.nan
    if pd.notna(ttm_op_for_icr) and pd.notna(ttm_interest) and ttm_interest != 0:
        res["이자보상배율"] = ttm_op_for_icr / abs(ttm_interest)
    else:
        res["이자보상배율"] = 20.0 if (pd.notna(ttm_interest) and ttm_interest == 0) else np.nan
    res["무차입_기업"] = 1 if (pd.notna(ttm_interest) and ttm_interest == 0) else 0

    # DPS: us_daily에서 가져올 것이므로 여기서 NaN
    res["DPS_최근"] = np.nan
    res["DPS_CAGR"] = np.nan
    res["배당_연속증가"] = 0
    res["배당_수익동반증가"] = 0

    # Forward 컨센서스: US에서 yfinance 미지원 → 모두 NaN
    res["컨센서스_커버리지"] = np.nan
    res["Fwd_PER"] = np.nan
    res["Fwd_EPS"] = np.nan
    res["Fwd_ROE(%)"] = np.nan
    res["Fwd_OPM(%)"] = np.nan

    # 수급 컬럼 (US에서 불가 → NaN)
    res["수급강도"] = np.nan
    res["외인순매수_20d"] = np.nan
    res["기관순매수_20d"] = np.nan
    res["스마트머니_승률"] = np.nan
    res["양매수_비율"] = np.nan
    res["VCP_신호"] = 0  # VCP는 기술적으로 calc_us_investor_vcp에서 재계산

    return res


def analyze_all_us(fs: pd.DataFrame, ind: pd.DataFrame) -> pd.DataFrame:
    """모든 US 종목 분석"""
    if fs.empty:
        log.warning("us_financial_statements 데이터 없음")
        return pd.DataFrame()

    results = []
    # ticker 별로 그룹핑
    fs_grouped = {k: v for k, v in fs.groupby("ticker")} if "ticker" in fs.columns else {}
    ind_grouped = {k: v for k, v in ind.groupby("ticker")} if (not ind.empty and "ticker" in ind.columns) else {}

    tickers = list(fs_grouped.keys())
    for ticker in tqdm(tickers, desc="US 종목 분석"):
        try:
            res = analyze_one_us_stock(
                ticker,
                ind_grouped.get(ticker, pd.DataFrame()),
                fs_grouped.get(ticker, pd.DataFrame()),
            )
            results.append(res)
        except Exception as e:
            log.debug("분석 실패 [%s]: %s", ticker, e)
            results.append({"종목코드": ticker})

    return pd.DataFrame(results) if results else pd.DataFrame()


def calc_us_dividend_metrics(ind: pd.DataFrame) -> pd.DataFrame:
    """us_indicators의 DPS 이력으로 연 배당 성장 지표를 계산."""
    if ind is None or ind.empty:
        return pd.DataFrame(columns=["종목코드", "DPS_CAGR", "배당_연속증가"])
    required = {"ticker", "base_date", "indicator_type", "account", "value"}
    if not required.issubset(ind.columns):
        return pd.DataFrame(columns=["종목코드", "DPS_CAGR", "배당_연속증가"])

    dps_hist = ind[
        (ind["indicator_type"].astype(str) == "DPS")
        & (ind["account"].astype(str) == "DPS")
    ].copy()
    if dps_hist.empty:
        return pd.DataFrame(columns=["종목코드", "DPS_CAGR", "배당_연속증가"])

    dps_hist["base_date"] = pd.to_datetime(dps_hist["base_date"], errors="coerce")
    dps_hist["value"] = pd.to_numeric(dps_hist["value"], errors="coerce")
    dps_hist = dps_hist.dropna(subset=["base_date", "value"])
    dps_hist = dps_hist[dps_hist["value"] > 0]
    if dps_hist.empty:
        return pd.DataFrame(columns=["종목코드", "DPS_CAGR", "배당_연속증가"])

    # 현재 연도는 배당이 진행 중인 경우가 많아서 CAGR/연속증가 계산에서는 제외한다.
    current_year = pd.Timestamp.today().year
    dps_hist["year"] = dps_hist["base_date"].dt.year
    annual = (
        dps_hist[dps_hist["year"] < current_year]
        .groupby(["ticker", "year"], as_index=False)["value"]
        .sum()
    )
    if annual.empty:
        return pd.DataFrame(columns=["종목코드", "DPS_CAGR", "배당_연속증가"])

    rows = []
    for ticker, grp in annual.groupby("ticker"):
        grp = grp.sort_values("year")
        yearly = grp.set_index("year")["value"]
        yearly = yearly[yearly > 0]
        if yearly.empty:
            continue

        cagr_input = {
            f"{int(year)}-12-31": float(value)
            for year, value in yearly.tail(5).items()
        }
        dps_cagr = calc_cagr(cagr_input, min_years=2)

        consec = 0
        vals = yearly.tolist()
        for i in range(len(vals) - 1, 0, -1):
            if vals[i] > vals[i - 1]:
                consec += 1
            else:
                break

        rows.append({
            "종목코드": ticker,
            "DPS_CAGR": dps_cagr,
            "배당_연속증가": int(consec),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["종목코드", "DPS_CAGR", "배당_연속증가"]
    )


# ═════════════════════════════════════════════
# 밸류에이션
# ═════════════════════════════════════════════

def calc_us_valuation(daily_us: pd.DataFrame, anal_df: pd.DataFrame, shares_df: pd.DataFrame,
                      ind_df: pd.DataFrame = None) -> pd.DataFrame:
    """US 밸류에이션 지표 계산.

    US S-RIM: Ke = US_RISK_FREE_RATE + US_EQUITY_RISK_PREMIUM (config)
    multiplier = 1 (yfinance raw USD)
    """
    df = daily_us[["종목코드", "종목명", "종가", "시가총액", "상장주식수"]].drop_duplicates("종목코드").merge(
        anal_df, on="종목코드", how="left"
    )

    # us_indicators로 종가/시가총액/상장주식수 보완 (bulk download 실패 케이스 대응)
    if ind_df is not None and not ind_df.empty and "ticker" in ind_df.columns:
        ind_ticker = ind_df.rename(columns={"ticker": "종목코드"})
        # 최신 날짜 기준으로 pivot
        ind_latest = (
            ind_ticker[ind_ticker["indicator_type"] == "INFO"]
            .sort_values("base_date")
            .drop_duplicates(subset=["종목코드", "account"], keep="last")
        )
        for ind_account, target_col in [("Market_Cap", "시가총액"), ("Shares_Outstanding", "상장주식수")]:
            sub = ind_latest[ind_latest["account"] == ind_account].set_index("종목코드")["value"]
            if not sub.empty:
                mask = df[target_col].isna() | (df[target_col] == 0)
                df.loc[mask, target_col] = df.loc[mask, "종목코드"].map(sub)

        # 종가 보완: us_indicators에는 직접 종가가 없으므로 market_cap / shares로 역산하지 않음
        # 대신 us_price_history 최신값을 활용하는 것이 정확하나 여기서는 접근 불가
        # → bulk download 재시도는 collector 수준에서 해결

    # shares_df로 상장주식수 보완
    if shares_df is not None and not shares_df.empty:
        s_map = shares_df.drop_duplicates("종목코드").set_index("종목코드")
        if "shares_outstanding" in s_map.columns:
            s_map = s_map["shares_outstanding"]
            mask = df["상장주식수"].isna() | (df["상장주식수"] == 0)
            df.loc[mask, "상장주식수"] = df.loc[mask, "종목코드"].map(s_map)

    # 재무제표 발행주식수로 상장주식수 추가 보완 (shares_df에도 없는 경우)
    if "발행주식수_fs" in df.columns:
        mask2 = df["상장주식수"].isna() | (df["상장주식수"] == 0)
        df.loc[mask2, "상장주식수"] = df.loc[mask2, "발행주식수_fs"]

    # market_cap 보완: NaN이고 close + 상장주식수가 있으면 계산
    mc_mask = df["시가총액"].isna() | (df["시가총액"] == 0)
    if mc_mask.any():
        df.loc[mc_mask, "시가총액"] = np.where(
            (df.loc[mc_mask, "종가"] > 0) & (df.loc[mc_mask, "상장주식수"] > 0),
            df.loc[mc_mask, "종가"] * df.loc[mc_mask, "상장주식수"],
            np.nan,
        )

    # DPS 보완: us_daily에서 가져온 값 활용
    dps_col = next((c for c in ["dps", "DPS", "주당배당금"] if c in daily_us.columns), None)
    if dps_col:
        dps_map = daily_us.drop_duplicates("종목코드").set_index("종목코드")[dps_col]
        df["DPS_최근"] = df["종목코드"].map(dps_map).fillna(np.nan)

    M = 1  # multiplier = 1 (USD raw)

    df["PER"] = np.where((df["TTM_순이익"] > 0) & (df["시가총액"] > 0),
                         df["시가총액"] / (df["TTM_순이익"] * M), np.nan)
    df["PBR"] = np.where((df["자본"] > 0) & (df["시가총액"] > 0),
                         df["시가총액"] / (df["자본"] * M), np.nan)
    df["PSR"] = np.where((df["TTM_매출"] > 0) & (df["시가총액"] > 0),
                         df["시가총액"] / (df["TTM_매출"] * M), np.nan)
    df["ROE(%)"] = np.where(df["자본"] > 0, (df["TTM_순이익"] / df["자본"]) * 100, np.nan)
    df["ROE(%)"] = df["ROE(%)"].clip(-100, 100)

    # us_indicators에서 직접 가져온 값으로 NaN 보완 (rate limit 누락 방어)
    if ind_df is not None and not ind_df.empty and "ticker" in ind_df.columns:
        _ind_latest = (
            ind_df[ind_df["indicator_type"] == "INFO"]
            .sort_values("base_date")
            .drop_duplicates(subset=["ticker", "account"], keep="last")
        )
        _ind_pivot = _ind_latest.pivot_table(index="ticker", columns="account", values="value", aggfunc="last")
        _ind_pivot.index.name = "종목코드"
        for _ind_col, _target_col in [
            ("PER", "PER"), ("PBR", "PBR"), ("PSR", "PSR"),
            ("ROE(%)", "ROE(%)"), ("ROA(%)", "ROA(%)"),
            ("Operating_Margin(%)", "영업이익률(%)"),
        ]:
            if _ind_col in _ind_pivot.columns:
                _sub = _ind_pivot[_ind_col].dropna()
                _mask = df["PER"].isna() if _target_col == "PER" else df[_target_col].isna() if _target_col in df.columns else pd.Series(False, index=df.index)
                df.loc[_mask & df["종목코드"].isin(_sub.index), _target_col] = \
                    df.loc[_mask & df["종목코드"].isin(_sub.index), "종목코드"].map(_sub)
    df["ROIC(%)"] = df["ROIC(%)"].clip(-50, 100) if "ROIC(%)" in df.columns else np.nan
    df["부채비율(%)"] = np.where(df["자본"] > 0, (df["부채"] / df["자본"]) * 100, np.nan)
    df["부채비율(%)"] = df["부채비율(%)"].clip(0, 500)
    df["영업이익률(%)"] = df["영업이익률_최근"] if "영업이익률_최근" in df.columns else np.nan
    df["배당수익률(%)"] = np.where(
        (df["종가"] > 0) & (df["DPS_최근"].fillna(0) > 0),
        (df["DPS_최근"] / df["종가"]) * 100, np.nan
    )
    # 자사주수익률 & 주주환원수익률 (US 핵심 지표)
    df["자사주수익률(%)"] = np.where(
        (df["시가총액"] > 0) & pd.notna(df.get("순자사주매입")) & (df.get("순자사주매입", np.nan) > 0),
        (df["순자사주매입"] / df["시가총액"]) * 100, np.nan
    ) if "순자사주매입" in df.columns else np.nan
    df["주주환원수익률(%)"] = df["배당수익률(%)"].fillna(0) + df["자사주수익률(%)"].fillna(0)
    df["주주환원수익률(%)"] = df["주주환원수익률(%)"].where(
        df["배당수익률(%)"].notna() | df["자사주수익률(%)"].notna(), np.nan
    )
    cagr_capped = np.minimum(df["순이익_CAGR"].fillna(np.nan), 200)
    df["PEG"] = np.where((df["PER"] > 0) & (cagr_capped > 0), df["PER"] / cagr_capped, np.nan)
    df["FCF수익률(%)"] = np.where(
        (df["시가총액"] > 0) & pd.notna(df["TTM_FCF"]) & (df["TTM_FCF"] != 0),
        (df["TTM_FCF"] * M / df["시가총액"]) * 100, np.nan
    )
    df["이익수익률(%)"] = np.where(
        (df["시가총액"] > 0) & (df["TTM_순이익"] > 0),
        (df["TTM_순이익"] * M / df["시가총액"]) * 100, np.nan
    )
    df["현금전환율(%)"] = np.where(
        pd.notna(df["TTM_영업CF"]) & (df["TTM_순이익"] > 0),
        (df["TTM_영업CF"] / df["TTM_순이익"]) * 100, np.nan
    )
    df["현금전환율(%)"] = df["현금전환율(%)"].clip(-500, 500)
    df["CAPEX비율(%)"] = np.where(
        pd.notna(df["TTM_CAPEX"]) & (df["TTM_영업CF"] > 0),
        (df["TTM_CAPEX"] / df["TTM_영업CF"]) * 100, np.nan
    )
    df["CAPEX비율(%)"] = df["CAPEX비율(%)"].clip(0, 300)
    df["부채상환능력"] = np.where(
        (df["TTM_영업CF"] > 0) & (df["부채"] > 0),
        df["TTM_영업CF"] / df["부채"], np.nan
    )
    df["부채상환능력"] = df["부채상환능력"].clip(0, 5)
    if "이자보상배율" in df.columns:
        df["이자보상배율"] = df["이자보상배율"].clip(0, 50)
    if "유동비율(%)" in df.columns:
        df["유동비율(%)"] = df["유동비율(%)"].clip(0, 500)
    df["이익품질_양호"] = np.where(
        (df["TTM_영업CF"] > df["TTM_순이익"]) & (df["TTM_순이익"] > 0), 1, 0
    )

    shares = df["상장주식수"].replace(0, np.nan)
    df["BPS"] = (df["자본"] * M) / shares
    df["EPS"] = (df["TTM_순이익"] * M) / shares

    df["배당성향(%)"] = np.where(
        (df["EPS"] > 0) & (df["DPS_최근"].fillna(0) > 0),
        (df["DPS_최근"] / df["EPS"]) * 100, np.nan
    )
    df["배당성향(%)"] = df["배당성향(%)"].clip(0, 150)
    if "DPS_CAGR" in df.columns:
        df["DPS_CAGR"] = df["DPS_CAGR"].clip(-50, 100)

    # 배당 경고신호
    _rs_weak = (df["RS_등급"].fillna(50) < 30) if "RS_등급" in df.columns else False
    df["배당_경고신호"] = np.where(
        (df["배당성향(%)"].fillna(0) > 80)
        | ((df["배당수익률(%)"].fillna(0) > 8) & _rs_weak)
        | (df["현금전환율(%)"].fillna(100) < 70),
        1, 0
    ).astype(int)

    # 가치함정 경고
    _low_per = (df["PER"].fillna(99) < 8) & (df["PER"].fillna(99) > 0)
    _low_pbr = df["PBR"].fillna(99) < 0.7
    _cheap_look = _low_per | _low_pbr
    _sq_col = df.get("지속가치_품질", pd.Series(0, index=df.index)).fillna(0)
    _comove_col = df.get("매출이익_동행성", pd.Series(0, index=df.index)).fillna(0)
    _opm_imp_col = df.get("이익률_개선", pd.Series(1, index=df.index)).fillna(1)
    _quality_poor = (_sq_col <= 1) | (df["F스코어"].fillna(0) <= 3)
    _margin_declining = (_opm_imp_col == 0) & (_comove_col <= 0)
    _ocf_weak = df["현금전환율(%)"].fillna(100) < 50
    df["가치함정_경고"] = np.where(
        _cheap_look & (_quality_poor | _margin_declining | _ocf_weak), 1, 0
    ).astype(int)

    # ── US S-RIM: Ke = US_RISK_FREE_RATE + US_EQUITY_RISK_PREMIUM ──
    Ke = config.US_RISK_FREE_RATE + config.US_EQUITY_RISK_PREMIUM  # default: 4.3 + 5.0 = 9.3%
    Ke_dec = Ke / 100.0

    # 동적 지속계수 ω
    _q = pd.Series(0.0, index=df.index)
    _q += (df["F스코어"].fillna(0).clip(0, 9) / 9) * 0.30
    _q += (df["ROIC(%)"].fillna(0).clip(0, 20) / 20) * 0.25
    _consec_op = df["영업이익_연속성장"].fillna(0) if "영업이익_연속성장" in df.columns else pd.Series(0, index=df.index)
    _q += np.where(_consec_op >= 3, 1.0, np.where(_consec_op >= 1, 0.5, 0.0)) * 0.20
    _icr = df["이자보상배율"].fillna(0).clip(0, 10) if "이자보상배율" in df.columns else pd.Series(0, index=df.index)
    _q += (_icr / 10) * 0.15
    _dr = df["부채비율(%)"].fillna(100)
    _q += np.where(_dr < 100, 1.0, np.where(_dr < 200, 0.5, 0.0)) * 0.10
    omega_series = (0.5 + _q * 0.45).clip(0.50, 0.95)
    df["SRIM_오메가"] = omega_series.round(3)

    _roe = df["ROE(%)"]
    _bps = df["BPS"]
    _srim_premium = _bps + _bps * (_roe / 100.0 - Ke_dec) * omega_series / (
        (1 + Ke_dec - omega_series).clip(lower=1e-6)
    )
    df["적정주가_SRIM"] = np.where(
        (_roe > Ke) & (_bps > 0), _srim_premium,
        np.where(
            (_roe > 0) & (_roe <= Ke) & (_bps > 0), _bps * (_roe / Ke),
            np.where(_bps > 0, _bps * 0.5, np.nan),
        ),
    )
    df["괴리율(%)"] = ((df["적정주가_SRIM"] - df["종가"]) / df["종가"]) * 100

    # EPV
    _fcf = df["TTM_FCF"] if "TTM_FCF" in df.columns else pd.Series(np.nan, index=df.index)
    _ocf = df["TTM_영업CF"] if "TTM_영업CF" in df.columns else pd.Series(np.nan, index=df.index)
    _norm_fcf = np.where(_ocf > 0, np.minimum(_fcf, _ocf * 0.9), _fcf)
    _epv_total = np.where(_norm_fcf > 0, _norm_fcf * M / Ke_dec, np.nan)
    _epv_ps = np.where(shares > 0, _epv_total / shares, np.nan)
    _epv_cap = np.where(_bps > 0, _bps * 8, np.nan)
    df["적정주가_EPV"] = np.where(
        pd.notna(_epv_ps) & (_bps > 0), np.minimum(_epv_ps, _epv_cap), np.nan
    )

    # DDM (배당 종목만)
    _dps = df["DPS_최근"].fillna(0) if "DPS_최근" in df.columns else pd.Series(0, index=df.index)
    _dps_cagr = df["DPS_CAGR"].fillna(0) if "DPS_CAGR" in df.columns else pd.Series(0, index=df.index)
    _payout = df["배당성향(%)"].fillna(np.nan) if "배당성향(%)" in df.columns else pd.Series(np.nan, index=df.index)
    _eps = df["EPS"] if "EPS" in df.columns else pd.Series(np.nan, index=df.index)
    _warn = df["배당_경고신호"].fillna(0) if "배당_경고신호" in df.columns else pd.Series(0, index=df.index)
    _ddm_vals = []
    for _i in df.index:
        _d = _dps.at[_i]
        if _d <= 0 or pd.isna(_d):
            _ddm_vals.append(np.nan)
            continue
        _py = _payout.at[_i]
        _ep = _eps.at[_i] if pd.notna(_eps.at[_i]) else np.nan
        if pd.notna(_py) and 0 < _py < 100 and pd.notna(_ep) and _ep > 0:
            _d_used = min(_d, (_ep * (_py / 100)) * 1.1)
        else:
            _d_used = _d
        _g_raw = max(_dps_cagr.at[_i], 0)
        _g = min(_g_raw, max(Ke - 1.0, 2.0))
        _g_dec = _g / 100.0
        if _g_dec > 0:
            _s1 = sum(_d_used * (1 + _g_dec)**t / (1 + Ke_dec)**t for t in range(1, 6))
            _g_term = _g_dec / 2
            _term_dps = _d_used * (1 + _g_dec)**5 * (1 + _g_term)
            _denom_term = max(Ke_dec - _g_term, 1e-6)
            _tv = _term_dps / _denom_term / (1 + Ke_dec)**5
            _ddm_raw = _s1 + _tv
        else:
            _ddm_raw = _d_used / max(Ke_dec, 1e-6)
        if _warn.at[_i] == 1:
            _ddm_raw *= 0.7
        _ddm_vals.append(_ddm_raw)
    df["적정주가_DDM"] = _ddm_vals

    # 복합 적정주가
    _qual_양호 = df["이익품질_양호"].fillna(0)
    _consec_div = df["배당_연속증가"].fillna(0) if "배당_연속증가" in df.columns else pd.Series(0, index=df.index)
    _srim_v = df["적정주가_SRIM"]
    _epv_v = df["적정주가_EPV"]
    _ddm_v = pd.Series(df["적정주가_DDM"], index=df.index)
    _composite_vals = []
    _model_counts = []
    for _i in df.index:
        _models = {}
        _s = _srim_v.at[_i]
        if pd.notna(_s) and _s > 0:
            _models["SRIM"] = (_s, 1.0)
        _e = _epv_v.at[_i]
        if pd.notna(_e) and _e > 0:
            _conf_epv = 1.0 if _qual_양호.at[_i] == 1 else 0.5
            _models["EPV"] = (_e, _conf_epv)
        _d_val = _ddm_v.at[_i]
        if pd.notna(_d_val) and _d_val > 0:
            _cd = _consec_div.at[_i]
            _wn = _warn.at[_i] if hasattr(_warn, 'at') else 0
            _conf_ddm = 0.8 if (_cd >= 3 and _wn == 0) else 0.5
            _models["DDM"] = (_d_val, _conf_ddm)
        if len(_models) >= 2:
            _vals_list = [v for v, _ in _models.values()]
            _med = float(np.median(_vals_list))
            _models = {k: (v, c) for k, (v, c) in _models.items()
                       if _med / 3 <= v <= _med * 3}
        if not _models:
            _composite_vals.append(np.nan)
            _model_counts.append(0)
            continue
        _weighted = sum(v * c for v, c in _models.values())
        _total_c = sum(c for _, c in _models.values())
        _composite_vals.append(_weighted / _total_c if _total_c > 0 else np.nan)
        _model_counts.append(len(_models))
    df["적정주가_종합"] = _composite_vals
    df["밸류_모델수"] = _model_counts
    df["종합괴리율(%)"] = ((pd.Series(_composite_vals, index=df.index) - df["종가"]) / df["종가"]) * 100

    df["PER_이상"] = np.where(
        pd.notna(df["PER"]) & ((df["PER"] < 0.5) | (df["PER"] > 500)), 1, 0
    ).astype(int)

    if "이자보상배율" not in df.columns:
        df["이자보상배율"] = np.nan

    return df


# ═════════════════════════════════════════════
# 기술적 지표 (US)
# ═════════════════════════════════════════════

def calc_us_technical(df: pd.DataFrame, price_hist: pd.DataFrame,
                      index_hist: pd.DataFrame = None, master: pd.DataFrame = None) -> pd.DataFrame:
    """US 기술적 지표 계산.

    price_hist: us_price_history (종목코드, 날짜, 종가, 거래량 등)
    index_hist: us_index_history (지수코드, 날짜, 종가) — SP500/NASDAQ
    master: us_master (종목코드, exchange)
    """
    if price_hist.empty:
        return df

    # 지수 데이터 준비
    idx_map = {}
    if index_hist is not None and not index_hist.empty:
        for idx_code in ["SP500", "NASDAQ"]:
            date_col = "날짜" if "날짜" in index_hist.columns else "date"
            close_col = "종가" if "종가" in index_hist.columns else "close"
            code_col = "지수코드" if "지수코드" in index_hist.columns else "index_code"
            sub = index_hist[index_hist[code_col] == idx_code].copy()
            if sub.empty:
                continue
            sub[date_col] = pd.to_datetime(sub[date_col])
            sub = sub.sort_values(date_col).set_index(date_col)[close_col]
            idx_map[idx_code] = sub

    # exchange → index 매핑
    exchange_map = {}
    if master is not None and not master.empty and "exchange" in master.columns:
        exchange_map = master.drop_duplicates("종목코드").set_index("종목코드")["exchange"].to_dict()

    def _rs_ret(price_series, n):
        if len(price_series) < n + 1:
            return np.nan
        p_now = price_series.iloc[-1]
        p_prev = price_series.iloc[-(n + 1)]
        if p_prev <= 0:
            return np.nan
        return (p_now / p_prev - 1) * 100

    def _index_ret(idx_series, stock_dates, n):
        if idx_series is None or len(idx_series) == 0:
            return np.nan
        stock_dates_dt = pd.to_datetime(stock_dates)
        latest_date = stock_dates_dt.max()
        idx_sorted = idx_series.sort_index()
        available = idx_sorted.index[idx_sorted.index <= latest_date]
        if len(available) == 0:
            return np.nan
        p_now = idx_sorted[available[-1]]
        if len(stock_dates_dt) < n + 1:
            return np.nan
        target_date = sorted(stock_dates_dt)[-(n + 1)]
        available_prev = idx_sorted.index[idx_sorted.index <= target_date]
        if len(available_prev) == 0:
            return np.nan
        p_prev = idx_sorted[available_prev[-1]]
        if p_prev <= 0:
            return np.nan
        return (p_now / p_prev - 1) * 100

    # 컬럼명 정규화 (us_price_history는 영문 컬럼)
    date_col = "날짜" if "날짜" in price_hist.columns else "date"
    close_col = "종가" if "종가" in price_hist.columns else "close"
    vol_col_ph = "거래량" if "거래량" in price_hist.columns else "volume"
    amt_col = "거래대금" if "거래대금" in price_hist.columns else "amount"

    ph_grouped = {k: v for k, v in price_hist.groupby("종목코드")} if not price_hist.empty else {}
    techs = []

    for code in df["종목코드"].unique():
        if code not in ph_grouped:
            continue
        ph = ph_grouped[code].sort_values(date_col)
        if len(ph) < 5:
            continue
        close = ph[close_col].iloc[-1]
        ma20 = ph[close_col].rolling(20).mean().iloc[-1] if len(ph) >= 20 else np.nan
        ma60 = ph[close_col].rolling(60).mean().iloc[-1] if len(ph) >= 60 else np.nan
        h52, l52 = ph[close_col].max(), ph[close_col].min()

        rsi = np.nan
        if len(ph) >= 15:
            delta = ph[close_col].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean().iloc[-1]
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean().iloc[-1]
            rsi = 100 - (100 / (1 + (gain / loss))) if loss > 0 else 100

        # 거래대금: amount 컬럼 우선, 없으면 종가×거래량
        has_amt = (amt_col in ph.columns and ph[amt_col].notna().any())
        def _vol(rows):
            if has_amt:
                return rows[amt_col].mean()
            return (rows[close_col] * rows[vol_col_ph]).mean() if vol_col_ph in rows.columns else np.nan

        v20 = _vol(ph.tail(20)) if len(ph) >= 20 else np.nan
        v5  = _vol(ph.tail(5))
        v20_prior = _vol(ph.iloc[-25:-5]) if len(ph) >= 25 else np.nan
        vol60 = ph[close_col].pct_change().tail(60).std() * np.sqrt(252) * 100 if len(ph) >= 60 else np.nan

        # 거래량-가격 괴리
        vp_divergence = np.nan
        if len(ph) >= 40 and has_amt:
            price_ret_20 = ((ph[close_col].iloc[-1] / ph[close_col].iloc[-20] - 1) * 100
                            if ph[close_col].iloc[-20] > 0 else np.nan)
            vol_first = ph[amt_col].iloc[-40:-20].mean()
            vol_second = ph[amt_col].iloc[-20:].mean()
            if pd.notna(price_ret_20) and vol_first > 0:
                vol_chg = (vol_second / vol_first - 1) * 100
                if price_ret_20 > 0 and vol_chg < 0:
                    vp_divergence = min(abs(price_ret_20) + abs(vol_chg), 100)
                else:
                    vp_divergence = 0

        # RS 계산 (exchange 기반 지수 선택)
        rs_60d = rs_120d = rs_250d = np.nan
        if idx_map:
            exchange = exchange_map.get(str(code), "NYSE")
            idx_key = US_INDEX_MAP.get(exchange, US_DEFAULT_INDEX)
            if idx_key not in idx_map:
                idx_key = US_DEFAULT_INDEX
            idx_s = idx_map.get(idx_key)
            if idx_s is not None:
                stock_dates = ph[date_col]
                st60  = _rs_ret(ph[close_col], 60)
                st120 = _rs_ret(ph[close_col], 120)
                st250 = _rs_ret(ph[close_col], 250)
                ix60  = _index_ret(idx_s, stock_dates, 60)
                ix120 = _index_ret(idx_s, stock_dates, 120)
                ix250 = _index_ret(idx_s, stock_dates, 250)
                if pd.notna(st60) and pd.notna(ix60):
                    rs_60d = st60 - ix60
                if pd.notna(st120) and pd.notna(ix120):
                    rs_120d = st120 - ix120
                if pd.notna(st250) and pd.notna(ix250):
                    rs_250d = st250 - ix250

        techs.append({
            "종목코드": code,
            "RSI_14": rsi,
            "MA20_이격도(%)": (close / ma20 - 1) * 100 if pd.notna(ma20) and ma20 > 0 else np.nan,
            "MA60_이격도(%)": (close / ma60 - 1) * 100 if pd.notna(ma60) and ma60 > 0 else np.nan,
            "52주_최고대비(%)": (close / h52 - 1) * 100 if h52 > 0 else np.nan,
            "52주_최저대비(%)": (close / l52 - 1) * 100 if l52 > 0 else np.nan,
            "거래대금_20일평균": v20,
            "거래대금_증감(%)": (v5 / v20_prior - 1) * 100 if pd.notna(v20_prior) and v20_prior > 0 else np.nan,
            "변동성_60일(%)": vol60,
            "거래량_가격_괴리": vp_divergence,
            "RS_60d": rs_60d,
            "RS_120d": rs_120d,
            "RS_250d": rs_250d,
        })

    tech_df = pd.DataFrame(techs)
    if tech_df.empty:
        return df

    # Composite RS
    _rs_weights = [("RS_60d", 0.4), ("RS_120d", 0.3), ("RS_250d", 0.3)]
    for rs_col, _ in _rs_weights:
        if rs_col in tech_df.columns and tech_df[rs_col].notna().any():
            tech_df[f"_rank_{rs_col}"] = tech_df[rs_col].rank(pct=True, na_option="keep") * 100
        else:
            tech_df[f"_rank_{rs_col}"] = np.nan

    def _weighted_composite(row):
        total_val, total_w = 0.0, 0.0
        for rs_col, w in _rs_weights:
            v = row.get(f"_rank_{rs_col}", np.nan)
            if pd.notna(v):
                total_val += v * w
                total_w += w
        return total_val / total_w if total_w > 0 else np.nan

    tech_df["Composite_RS"] = tech_df.apply(_weighted_composite, axis=1)
    tech_df = tech_df.drop(columns=[f"_rank_{c}" for c, _ in _rs_weights if f"_rank_{c}" in tech_df.columns])

    result = df.merge(tech_df, on="종목코드", how="left")

    if "Composite_RS" in result.columns and result["Composite_RS"].notna().any():
        result["RS_등급"] = result["Composite_RS"].rank(pct=True, na_option="keep") * 100
    else:
        result["RS_등급"] = np.nan

    return result


def calc_us_vcp(df: pd.DataFrame, price_hist: pd.DataFrame) -> pd.DataFrame:
    """US VCP 신호: 가격+거래량 압축만 (스마트머니 조건 없음)"""
    if price_hist.empty:
        df["VCP_신호"] = 0
        return df

    close_col = "종가" if "종가" in price_hist.columns else "close"
    vol_col_ph = "거래량" if "거래량" in price_hist.columns else "volume"
    date_col = "날짜" if "날짜" in price_hist.columns else "date"

    ph_grouped = {k: v for k, v in price_hist.groupby("종목코드")}
    vcp_map = {}
    for code, ph in ph_grouped.items():
        ph = ph.sort_values(date_col)
        if len(ph) < 60:
            vcp_map[code] = 0
            continue
        close_s = ph[close_col]
        _mean20 = close_s.tail(20).mean()
        _mean60 = close_s.tail(60).mean()
        cv20 = close_s.tail(20).std() / _mean20 if (pd.notna(_mean20) and _mean20 > 0.01) else np.nan
        cv60 = close_s.tail(60).std() / _mean60 if (pd.notna(_mean60) and _mean60 > 0.01) else np.nan
        price_compress = pd.notna(cv20) and pd.notna(cv60) and cv20 < cv60
        vol_compress = False
        if vol_col_ph in ph.columns:
            vol20 = ph[vol_col_ph].tail(20).mean()
            vol60_mean = ph[vol_col_ph].tail(60).mean()
            vol_compress = (pd.notna(vol20) and pd.notna(vol60_mean)
                            and vol60_mean > 0 and vol20 < vol60_mean)
        vcp_map[code] = 1 if (price_compress and vol_compress) else 0

    df["VCP_신호"] = df["종목코드"].map(vcp_map).fillna(0).astype(int)
    return df


# ═════════════════════════════════════════════
# 과열도 & 상승조짐 (US — 수급축 재배분)
# ═════════════════════════════════════════════

def calc_us_overheat_score(df: pd.DataFrame) -> pd.DataFrame:
    """과열도 (한국과 동일 4축, 기술적 지표만 사용)"""
    h52 = df["52주_최고대비(%)"].fillna(-20) if "52주_최고대비(%)" in df.columns else pd.Series(-20, index=df.index)
    s_h52 = np.clip((h52 + 20) / 20 * 100, 0, 100)
    s_ma = np.clip(df["MA20_이격도(%)"].fillna(0) / 15 * 100, 0, 100) if "MA20_이격도(%)" in df.columns else pd.Series(0, index=df.index)
    s_rsi = np.clip((df["RSI_14"].fillna(50) - 50) / 30 * 100, 0, 100) if "RSI_14" in df.columns else pd.Series(0, index=df.index)
    s_vpd = np.clip(df["거래량_가격_괴리"].fillna(0), 0, 100) if "거래량_가격_괴리" in df.columns else pd.Series(0, index=df.index)
    df["과열도"] = (s_h52 * 0.30 + s_ma * 0.25 + s_rsi * 0.25 + s_vpd * 0.20).round(1)
    if "실적감속_경고" in df.columns:
        mask = df["실적감속_경고"].fillna(0) == 1
        df.loc[mask, "과열도"] = np.clip(df.loc[mask, "과열도"] + 10, 0, 100)
    return df


def calc_us_breakout_signal(df: pd.DataFrame) -> pd.DataFrame:
    """상승조짐 (3축 — 수급 축 제거, 펀더멘털40%+비과열20%+거래대금/패턴40%)

    구성:
    1. 펀더멘털 품질 (40%): 실적가속 + ROIC개선 + GPM개선 + 흑자전환 + F스코어
    2. 비과열 확인 (20%): 과열도 낮을수록
    3. 가격/거래대금 패턴 (40%): VCP + MA20 + RSI + 거래대금 증감
    """
    # ── 축 1: 펀더멘털 (40%) ──
    fund = pd.Series(0.0, index=df.index)
    fund += df.get("실적가속_연속", pd.Series(0, index=df.index)).fillna(0) * 30
    fund += df.get("ROIC_개선", pd.Series(0, index=df.index)).fillna(0) * 20
    gpm = df.get("GPM_변화(pp)", pd.Series(0, index=df.index)).fillna(0)
    fund += np.where(gpm > 0, np.clip(gpm / 3 * 15, 0, 15), 0)
    fund += df.get("흑자전환", pd.Series(0, index=df.index)).fillna(0) * 15
    fscore = df.get("F스코어", pd.Series(0, index=df.index)).fillna(0)
    fund += np.where(fscore >= 7, 20, np.where(fscore >= 5, 10, 0))
    s_fund = np.clip(fund, 0, 100)

    # ── 축 2: 비과열 (20%) ──
    OVERHEAT_THRESHOLD = 70
    overheat = df.get("과열도", pd.Series(50, index=df.index)).fillna(50)
    s_cool = np.clip((OVERHEAT_THRESHOLD - overheat) / OVERHEAT_THRESHOLD * 100, 0, 100)

    # ── 축 3: 가격/거래대금 패턴 (40%) ──
    pattern = pd.Series(0.0, index=df.index)
    # VCP 신호 (+25)
    pattern += df.get("VCP_신호", pd.Series(0, index=df.index)).fillna(0) * 25
    # MA20 근접 상향 (+20)
    ma20 = df.get("MA20_이격도(%)", pd.Series(0, index=df.index)).fillna(0)
    pattern += np.where(
        (ma20 >= 0) & (ma20 <= 10),
        np.clip(20 - np.abs(ma20 - 3) * 2, 0, 20), 0
    )
    # RSI 50~60 전환 (+25)
    rsi = df.get("RSI_14", pd.Series(50, index=df.index)).fillna(50)
    rsi_score = np.where(
        (rsi >= 50) & (rsi <= 60), 25,
        np.where((rsi >= 40) & (rsi < 50), 12,
                 np.where((rsi > 60) & (rsi <= 70), 12, 0))
    )
    pattern += rsi_score
    # 거래대금 증감 양수 (+30)
    vol_chg = df.get("거래대금_증감(%)", pd.Series(0, index=df.index)).fillna(0)
    pattern += np.where(vol_chg > 0, np.clip(vol_chg / 50 * 30, 0, 30), 0)
    s_pattern = np.clip(pattern, 0, 100)

    df["상승조짐"] = (s_fund * 0.40 + s_cool * 0.20 + s_pattern * 0.40).round(1)
    return df


# ═════════════════════════════════════════════
# US 전략 스코어링
# ═════════════════════════════════════════════

def calc_us_strategy_scores(df: pd.DataFrame) -> pd.DataFrame:
    """US 전략별 점수 계산 (수급강도/스마트머니 제거 → 거래대금/RS로 재배분)"""

    def get_rank(col, asc=True, zero_if_nan=False):
        if col not in df.columns:
            return pd.Series(0.0 if zero_if_nan else 50.0, index=df.index)
        series = df[col].copy()
        if zero_if_nan:
            nan_mask = series.isna()
            fill_val = (series.min() - 1 if (asc and series.notna().any())
                        else series.max() + 1 if series.notna().any() else 0)
            series = series.fillna(fill_val)
            ranked = series.rank(pct=True) * 100 if asc else (1 - series.rank(pct=True)) * 100
            ranked[nan_mask] = 0.0
            return ranked
        else:
            series = series.fillna(series.median() if not series.isna().all() else 0)
            return series.rank(pct=True) * 100 if asc else (1 - series.rank(pct=True)) * 100

    # 턴어라운드 프리미엄: 흑자전환 종목의 NaN CAGR을 75th percentile로 대체
    if "순이익_CAGR" in df.columns and "흑자전환" in df.columns:
        _cagr_p75 = df["순이익_CAGR"].quantile(0.75) if df["순이익_CAGR"].notna().any() else 0
        _turnaround_mask = (df["흑자전환"] == 1) & df["순이익_CAGR"].isna()
        df["_순이익_CAGR_adj"] = df["순이익_CAGR"].copy()
        df.loc[_turnaround_mask, "_순이익_CAGR_adj"] = _cagr_p75
    else:
        df["_순이익_CAGR_adj"] = df.get("순이익_CAGR", pd.Series(np.nan, index=df.index))

    # CAGR/YoY Winsorization
    for _gcol in ["매출_CAGR", "영업이익_CAGR", "순이익_CAGR"]:
        if _gcol in df.columns:
            df[_gcol] = df[_gcol].clip(lower=_CAGR_FLOOR, upper=_CAGR_CAP)
    for _gcol in ["Q_매출_YoY(%)", "Q_영업이익_YoY(%)", "Q_순이익_YoY(%)"]:
        if _gcol in df.columns:
            df[_gcol] = df[_gcol].clip(lower=_YOY_FLOOR, upper=_YOY_CAP)

    S_PER_inv   = get_rank("PER", False, zero_if_nan=True)
    S_PBR_inv   = get_rank("PBR", False, zero_if_nan=True)
    S_PEG_inv   = get_rank("PEG", False, zero_if_nan=True)
    S_ROE       = get_rank("ROE(%)", asc=True, zero_if_nan=True)
    S_FCF       = get_rank("FCF수익률(%)", asc=True, zero_if_nan=True)
    S_OpCAGR    = get_rank("영업이익_CAGR")
    S_QOpYoY    = get_rank("Q_영업이익_YoY(%)")
    S_Div       = get_rank("배당수익률(%)")
    S_Vol       = get_rank("거래대금_20일평균")
    S_ROIC      = get_rank("ROIC(%)", asc=True, zero_if_nan=True)
    S_RS        = get_rank("RS_등급", asc=True, zero_if_nan=True)
    S_Accel     = get_rank("실적가속_연속", asc=True, zero_if_nan=True)
    S_GPM_delta = get_rank("GPM_변화(pp)", asc=True, zero_if_nan=True)
    S_SustainedQ = get_rank("지속가치_품질", asc=True, zero_if_nan=True)
    S_CoMove    = get_rank("매출이익_동행성", asc=True, zero_if_nan=True)
    S_VolChg    = get_rank("거래대금_증감(%)", asc=True, zero_if_nan=False)
    S_RSI       = get_rank("RSI_14", asc=True, zero_if_nan=False)
    # US 핵심: 주주환원수익률 (배당+자사주매입)
    S_ShareholderYield = get_rank("주주환원수익률(%)", asc=True, zero_if_nan=False)

    # ── 주도주: RS(30%)+거래대금증감(20%)+영업이익CAGR(15%)+Q_YoY(15%)+실적가속(10%)+거래대금(5%)+RSI(5%) ──
    # (수급강도 20% → RS +5%, 거래대금_증감 신규 20%)
    df["주도주_점수"] = (
        S_RS * 0.30
        + S_VolChg * 0.20
        + S_OpCAGR * 0.15
        + S_QOpYoY * 0.15
        + S_Accel * 0.10
        + S_Vol * 0.05
        + S_RSI * 0.05
    )

    # ── 우량가치: 주주환원수익률(10%) 신규 추가 (US 핵심 환원 지표) ──
    df["우량가치_점수"] = (
        S_FCF * 0.18
        + S_ROIC * 0.18
        + get_rank("F스코어") * 0.14
        + get_rank("종합괴리율(%)") * 0.18
        + S_SustainedQ * 0.12
        + S_CoMove * 0.10
        + S_ShareholderYield * 0.10   # 배당+자사주매입 통합 환원
    )

    # ── 고성장: 변경 없음 ──
    df["고성장_점수"] = (
        S_QOpYoY * 0.20
        + S_Accel * 0.20
        + S_OpCAGR * 0.15
        + S_RS * 0.25
        + S_PEG_inv * 0.20
    )

    # ── 현금배당: 주주환원수익률(15%) 신규 추가 (배당+자사주 통합 환원) ──
    S_PayoutInv = get_rank("배당성향(%)", asc=False, zero_if_nan=False)
    _raw_div = (
        S_FCF * 0.20
        + S_Div * 0.15
        + get_rank("DPS_CAGR") * 0.15
        + S_ROIC * 0.10
        + S_PayoutInv * 0.10
        + get_rank("F스코어") * 0.10
        + get_rank("부채비율(%)", False) * 0.05
        + S_ShareholderYield * 0.15   # 배당+자사주 통합 환원율 (US 핵심)
    )
    _consec = df.get("배당_연속증가", pd.Series(0, index=df.index)).fillna(0).clip(lower=0)
    _div_bonus = np.minimum(np.log2(_consec.where(_consec > 0, np.nan) + 1) * 3, 10).fillna(0)
    _div_bonus += df.get("배당_수익동반증가", pd.Series(0, index=df.index)).fillna(0) * 2
    _div_penalty = np.where(df["배당_경고신호"] == 1, 0.7, 1.0)
    df["현금배당_점수"] = (_raw_div + _div_bonus) * _div_penalty

    # ── 턴어라운드: 스마트머니(15%) → 이익률변동(+5%), GPM(+5%), 거래대금증감(10%) 신규 ──
    S_Sales_YoY    = get_rank("Q_매출_YoY(%)", asc=True, zero_if_nan=True)
    S_Interest_Cov = get_rank("이자보상배율", asc=True, zero_if_nan=True)
    S_Qual_Turn    = get_rank("퀄리티_턴어라운드", asc=True, zero_if_nan=True)
    df["턴어라운드_점수"] = (
        get_rank("이익률_변동폭") * 0.15      # 이익률 개선 폭 (10→15%)
        + get_rank("흑자전환") * 0.15          # 흑자전환
        + S_GPM_delta * 0.15                   # 원가 경쟁력 (10→15%)
        + S_Sales_YoY * 0.15                   # 탑라인 회복
        + S_Interest_Cov * 0.10                # 파산 리스크
        + S_Qual_Turn * 0.15                   # GPM+OCF+ROIC
        + get_rank("종합괴리율(%)") * 0.05     # 안전마진 (10→5%)
        + S_VolChg * 0.10                      # 거래대금 증감 신규 (스마트머니 대체)
    )  # 합계: 15+15+15+15+10+15+5+10 = 100%

    # ── 과열도 소프트 페널티 ──
    if "과열도" in df.columns and df["과열도"].notna().any():
        _oh = df["과열도"].fillna(0)
        _effective = np.clip((_oh - 40) / 60, 0, 1)
        OVERHEAT_SENSITIVITY = {
            "주도주_점수": 0.15,
            "고성장_점수": 0.20,
            "턴어라운드_점수": 0.25,
            "현금배당_점수": 0.30,
            "우량가치_점수": 0.35,
        }
        for _col, _s in OVERHEAT_SENSITIVITY.items():
            if _col in df.columns:
                df[_col] = df[_col] * (1 - _effective * _s)

    # ── 종합점수 (성장25% + 안정35% + 가격25% + 타이밍15%) ──
    _CAGR_CAP_check = _CAGR_CAP
    _YOY_CAP_check = _YOY_CAP
    _cagr_capped_flag = df.get("영업이익_CAGR", pd.Series(0, index=df.index)).fillna(0) >= _CAGR_CAP_check
    _yoy_capped_flag  = df.get("Q_영업이익_YoY(%)", pd.Series(0, index=df.index)).fillna(0) >= _YOY_CAP_check
    _opm_prev_low = df.get("영업이익률_전년", pd.Series(np.nan, index=df.index)).fillna(np.nan) < 3
    _base_effect = (_cagr_capped_flag & _yoy_capped_flag & _opm_prev_low).fillna(False)

    S_OpStreak = get_rank("영업이익_연속성장", asc=True, zero_if_nan=True)
    성장성_점수 = (
        S_OpCAGR * 0.30
        + get_rank("매출_CAGR") * 0.25
        + S_QOpYoY * 0.25
        + S_Accel * 0.10
        + S_OpStreak * 0.10
    )
    성장성_점수 = 성장성_점수 * np.where(_base_effect, 0.75, 1.0)

    S_IntCov = get_rank("이자보상배율", asc=True, zero_if_nan=True)
    안정성_점수 = (
        get_rank("F스코어") * 0.30
        + S_SustainedQ * 0.25
        + S_FCF * 0.20
        + S_ROE * 0.15
        + S_IntCov * 0.10
    )
    _debt_ratio = df.get("부채비율(%)", pd.Series(0, index=df.index)).fillna(0)
    안정성_점수 = 안정성_점수 * np.where(_debt_ratio > 200, 0.80, 1.0)

    S_composite_div = get_rank("종합괴리율(%)")
    S_model_count = get_rank("밸류_모델수", asc=True, zero_if_nan=False)
    _div_adj = np.where(_base_effect, S_composite_div * 0.5, S_composite_div)
    가격_점수 = (
        S_PER_inv * 0.35
        + _div_adj * 0.25
        + S_PBR_inv * 0.20
        + S_FCF * 0.10
        + S_model_count * 0.10
    )
    _vt_flag = df.get("가치함정_경고", pd.Series(0, index=df.index)).fillna(0)
    가격_점수 = 가격_점수 * np.where(_vt_flag == 1, 0.60, 1.0)

    # 타이밍: 수급강도 축 제거 → RS(30%) + 상승조짐(40%) + 과열도역순(30%)
    _anti_oh  = 100 - df.get("과열도", pd.Series(50, index=df.index)).fillna(50)
    _breakout = df.get("상승조짐", pd.Series(0, index=df.index)).fillna(0)
    _base = _anti_oh * 0.30 + _breakout * 0.40 + S_RS * 0.30
    _decel = np.where(df.get("실적감속_경고", pd.Series(0, index=df.index)).fillna(0) == 1, -15, 0)
    타이밍_점수 = np.clip(_base + _decel, 0, 100)

    df["성장성_점수"] = 성장성_점수
    df["안정성_점수"] = 안정성_점수
    df["가격_점수"]   = 가격_점수
    df["타이밍_점수"] = pd.Series(타이밍_점수, index=df.index).round(1)
    df["종합점수"]    = (성장성_점수 * 0.25 + 안정성_점수 * 0.35
                        + 가격_점수 * 0.25 + df["타이밍_점수"] * 0.15)

    if "_순이익_CAGR_adj" in df.columns:
        df = df.drop(columns=["_순이익_CAGR_adj"])
    return df


# ═════════════════════════════════════════════
# US 스크린 필터 (USD 기준)
# ═════════════════════════════════════════════

def apply_us_leaders_screen(df: pd.DataFrame) -> pd.DataFrame:
    """주도주: 시총 $10B+ + 흑자 + RS 상위 20% + 거래대금 $5M+"""
    mask = (
        (df["시가총액"] >= 10_000_000_000)
        & (df["TTM_순이익"] > 0)
        & (df["주도주_점수"] > 0)
    )
    if "RS_등급" in df.columns and df["RS_등급"].notna().any():
        mask = mask & (df["RS_등급"].fillna(0) >= 80)
    if "거래대금_20일평균" in df.columns:
        mask = mask & ((df["거래대금_20일평균"] > 5_000_000) | df["거래대금_20일평균"].isna())
    return df[mask].sort_values("주도주_점수", ascending=False)


def apply_us_quality_value_screen(df: pd.DataFrame) -> pd.DataFrame:
    """우량가치: 시총 $2B+ + ROIC 10%+ + F스코어 5+ + PEG<1.5 + 흑자연속"""
    mask = (
        (df["ROIC(%)"].fillna(0) >= 10)
        & (df["F스코어"].fillna(0) >= 5)
        & (df["PEG"].fillna(99) < 1.5)
        & (df["부채비율(%)"].fillna(999) < 150)
        & (df["유동비율(%)"].fillna(0) > 100)
        & (df["순이익_당기양수"].fillna(0) == 1)
        & (df["순이익_전년음수"].fillna(0) == 0)
        & (df["시가총액"].fillna(0) >= 2_000_000_000)
        & (df.get("가치함정_경고", pd.Series(0, index=df.index)).fillna(0) == 0)
    )
    return df[mask].sort_values("우량가치_점수", ascending=False)


def apply_us_growth_mom_screen(df: pd.DataFrame) -> pd.DataFrame:
    """고성장: 시총 $1B+ + 매출/영업이익 CAGR 10%+ + 최근 분기 성장 + RS 50+"""
    mask = (
        (df["매출_CAGR"].fillna(0) >= 10)
        & (df["영업이익_CAGR"].fillna(0) >= 10)
        & (df["Q_영업이익_YoY(%)"].fillna(0) > 0)
        & (df["RS_등급"].fillna(0) >= 50)
        & (df["TTM_영업CF"].fillna(-1) > 0)
        & (df["시가총액"].fillna(0) >= 1_000_000_000)
    )
    return df[mask].sort_values("고성장_점수", ascending=False)


def apply_us_cash_div_screen(df: pd.DataFrame) -> pd.DataFrame:
    """현금배당: 시총 $1B+ + FCF 3%+ + 배당수익률 1%+ + 부채비율 150% 미만"""
    mask = (
        (df.get("FCF수익률(%)", pd.Series(0, index=df.index)).fillna(0) >= 3)
        & (df["배당수익률(%)"].fillna(0) >= 1)
        & (df["부채비율(%)"].fillna(999) < 150)
        & (df["시가총액"].fillna(0) >= 1_000_000_000)
        & (df["배당성향(%)"].fillna(999) < 80)
        & (df["현금전환율(%)"].fillna(0) >= 70)
        & (df["배당_연속증가"].fillna(0) >= 2)
    )
    return df[mask].sort_values("현금배당_점수", ascending=False)


def apply_us_turnaround_screen(df: pd.DataFrame) -> pd.DataFrame:
    """턴어라운드: 시총 $500M+ + 흑자전환 OR 이익률 급개선 + 현금 창출 + 거래대금 증감 20%+"""
    base_mask = (
        ((df.get("흑자전환") == 1) | (df.get("이익률_급개선") == 1))
        & (df["TTM_순이익"] > 0)
        & (df["TTM_영업CF"].fillna(-1) > 0)
        & (df["Q_매출_YoY(%)"].fillna(0) > -15)
        & (df["시가총액"] >= 500_000_000)
        & (df["이자보상배율"].fillna(0) > 1.5)
    )
    # 거래대금 증감 20%+ 또는 VCP 신호 (스마트머니 대체)
    if "거래대금_증감(%)" in df.columns:
        vol_mask = (df["거래대금_증감(%)"].fillna(0) >= 20) | (df.get("VCP_신호", 0).fillna(0) == 1)
        mask = base_mask & vol_mask
        no_data_mask = base_mask & df["거래대금_증감(%)"].isna()
        mask = mask | no_data_mask
    else:
        mask = base_mask
    return df[mask].sort_values("턴어라운드_점수", ascending=False)


# ═════════════════════════════════════════════
# 오케스트레이터
# ═════════════════════════════════════════════

def run(progress_callback=None):
    """US 스크리너 메인 실행"""
    import db as _db

    def _progress(msg):
        log.info(msg)
        if progress_callback:
            progress_callback(msg)

    _progress("US 스크리너 시작")

    # ── 1. 데이터 로드 ──
    _progress("데이터 로드 중...")
    # us_daily: 종가는 전체 MAX 날짜 기준, market_cap/eps/bps는 ticker별 최신으로 보완
    daily_raw  = load_us_table("us_daily")
    if not daily_raw.empty:
        _daily_per_ticker = _load_us_table_per_ticker("us_daily")
        if not _daily_per_ticker.empty:
            # 현재 daily_raw에서 NaN인 market_cap/eps/bps를 이전 날짜 데이터로 채움
            _patch_cols = [c for c in ["market_cap", "eps", "bps", "dps", "shares_outstanding", "name"]
                           if c in _daily_per_ticker.columns and c in daily_raw.columns]
            if _patch_cols:
                _per = _daily_per_ticker[["ticker"] + _patch_cols].set_index("ticker")
                for col in _patch_cols:
                    _mask = daily_raw[col].isna()
                    if _mask.any():
                        daily_raw.loc[_mask, col] = daily_raw.loc[_mask, "ticker"].map(_per[col])
    fs_raw     = load_us_table("us_financial_statements")
    # indicators/shares는 ticker별 최신 데이터 로드 (rate limit으로 특정 날에
    # 일부 종목이 누락돼도 이전 수집 데이터를 활용)
    ind_raw    = _load_us_table_per_ticker("us_indicators")
    shares_raw = _load_us_table_per_ticker("us_shares")
    hist_raw   = load_us_table("us_price_history")
    idx_raw    = load_us_table("us_index_history")
    master_raw = load_us_table("us_master")

    if daily_raw.empty:
        log.error("us_daily 데이터 없음 — us-collect 먼저 실행하세요")
        return

    # ── 2. daily 컬럼 정규화 (영문 → 한국어, 종목코드 = ticker) ──
    daily_raw = daily_raw.rename(columns={
        "ticker": "종목코드",
        "name": "종목명",
        "close": "종가",
        "market_cap": "시가총액",
        "shares_outstanding": "상장주식수",
        "eps": "EPS",
        "bps": "BPS",
        "dps": "DPS",
    })

    if shares_raw is not None and not shares_raw.empty:
        shares_raw = shares_raw.rename(columns={
            "ticker": "종목코드",
            "shares_outstanding": "shares_outstanding",
        })

    if not hist_raw.empty:
        hist_raw = hist_raw.rename(columns={
            "ticker": "종목코드",
            "date": "날짜",
            "close": "종가",
            "volume": "거래량",
            "amount": "거래대금",
        })

    if not idx_raw.empty:
        idx_raw = idx_raw.rename(columns={
            "index_code": "지수코드",
            "date": "날짜",
            "close": "종가",
        })

    if not master_raw.empty:
        master_raw = master_raw.rename(columns={"ticker": "종목코드"})

    # ── 3. 재무 분석 ──
    _progress("재무제표 분석 중...")
    anal_df = analyze_all_us(fs_raw, ind_raw)
    if anal_df.empty:
        raise RuntimeError(
            "재무제표 분석 결과 없음 — us_financial_statements 데이터가 없습니다. "
            "먼저 전체 파이프라인(US 파이프라인)을 실행하세요."
        )

    dividend_df = calc_us_dividend_metrics(ind_raw)
    if not dividend_df.empty:
        div_idx = dividend_df.set_index("종목코드")
        anal_df["DPS_CAGR"] = anal_df["종목코드"].map(div_idx["DPS_CAGR"])
        anal_df["배당_연속증가"] = (
            anal_df["종목코드"].map(div_idx["배당_연속증가"]).fillna(0).astype(int)
        )
    else:
        anal_df["DPS_CAGR"] = np.nan
        anal_df["배당_연속증가"] = 0

    _earnings_cagr = anal_df.get("순이익_CAGR", pd.Series(np.nan, index=anal_df.index)).fillna(0)
    _div_consec = anal_df.get("배당_연속증가", pd.Series(0, index=anal_df.index)).fillna(0)
    anal_df["배당_수익동반증가"] = np.where(
        (_div_consec >= 1) & (_earnings_cagr > 0),
        1,
        0,
    ).astype(int)

    # ── 3b. 종가 NaN 보완: us_price_history 최신 종가로 채움 ──
    _close_nan_mask = daily_raw["종가"].isna() | (daily_raw["종가"] == 0)
    if _close_nan_mask.any() and not hist_raw.empty:
        _close_col = "종가" if "종가" in hist_raw.columns else ("close" if "close" in hist_raw.columns else None)
        _tc_col = "종목코드" if "종목코드" in hist_raw.columns else ("ticker" if "ticker" in hist_raw.columns else None)
        _date_col = "날짜" if "날짜" in hist_raw.columns else ("date" if "date" in hist_raw.columns else None)
        if _close_col and _tc_col and _date_col:
            _latest_ph = (
                hist_raw.sort_values(_date_col)
                .drop_duplicates(subset=[_tc_col], keep="last")
                .set_index(_tc_col)[_close_col]
            )
            daily_raw.loc[_close_nan_mask, "종가"] = daily_raw.loc[_close_nan_mask, "종목코드"].map(_latest_ph)
            _filled = _close_nan_mask.sum() - (daily_raw["종가"].isna() | (daily_raw["종가"] == 0)).sum()
            if _filled > 0:
                log.info("종가 NaN → us_price_history로 %d건 보완", _filled)

    # ── 4. 밸류에이션 ──
    _progress("밸류에이션 계산 중...")
    full = calc_us_valuation(daily_raw, anal_df, shares_raw, ind_df=ind_raw)
    if full.empty:
        log.warning("밸류에이션 결과 없음")
        return

    # ── 4b. 애널리스트 컨센서스 / 기관·내부자 보유 merge ──
    if not ind_raw.empty and "ticker" in ind_raw.columns:
        _info_accounts = {
            "Forward_PER", "Forward_EPS",
            "Target_Mean_Price", "Target_High_Price", "Target_Low_Price",
            "Analyst_Count", "Recommendation_Mean",
            "Insider_Holdings_Pct", "Institution_Holdings_Pct",
            "Short_Ratio", "Short_Float_Pct",
            "EV_Revenue", "EV_EBITDA",
        }
        _fwd_df = ind_raw[
            (ind_raw["indicator_type"] == "INFO") &
            (ind_raw["account"].isin(_info_accounts))
        ].copy()
        if not _fwd_df.empty:
            _fwd_pivot = (
                _fwd_df.sort_values("base_date")
                .groupby(["ticker", "account"])["value"]
                .last()
                .unstack("account")
                .reset_index()
                .rename(columns={"ticker": "종목코드"})
            )
            # 목표가 괴리율 계산
            if "Target_Mean_Price" in _fwd_pivot.columns:
                _fwd_pivot = _fwd_pivot.merge(
                    full[["종목코드", "종가"]], on="종목코드", how="left"
                )
                _fwd_pivot["목표가_괴리율(%)"] = np.where(
                    (_fwd_pivot["종가"] > 0) & _fwd_pivot["Target_Mean_Price"].notna(),
                    (_fwd_pivot["Target_Mean_Price"] - _fwd_pivot["종가"]) / _fwd_pivot["종가"] * 100,
                    np.nan
                )
                _fwd_pivot = _fwd_pivot.drop(columns=["종가"], errors="ignore")
            full = full.merge(_fwd_pivot, on="종목코드", how="left")
            log.info("애널리스트 컨센서스/보유 데이터 merge: %d종목", _fwd_pivot["종목코드"].nunique())

    # ── 5. 기술적 지표 ──
    _progress("기술적 지표 계산 중...")
    full = calc_us_technical(
        full, hist_raw,
        index_hist=idx_raw if not idx_raw.empty else None,
        master=master_raw if not master_raw.empty else None,
    )

    # ── 6. VCP ──
    full = calc_us_vcp(full, hist_raw)

    # ── 7. 이상치 클리핑 ──
    if "거래대금_증감(%)" in full.columns:
        full["거래대금_증감(%)"] = full["거래대금_증감(%)"].clip(-90, 500)
    if "변동성_60일(%)" in full.columns:
        full["변동성_60일(%)"] = full["변동성_60일(%)"].clip(0, 200)

    # ── 8. 과열도 / 상승조짐 ──
    full = calc_us_overheat_score(full)
    full = calc_us_breakout_signal(full)

    # ── 9. 전략 점수 ──
    _progress("전략 점수 계산 중...")
    full = calc_us_strategy_scores(full)

    # ── 10. 시장구분/섹터 보완 ──
    if not master_raw.empty:
        meta_cols = ["종목코드"]
        if "exchange" in master_raw.columns:
            meta_cols.append("exchange")
        if "stock_type" in master_raw.columns:
            meta_cols.append("stock_type")
        if "sector" in master_raw.columns:
            meta_cols.append("sector")
        if "industry" in master_raw.columns:
            meta_cols.append("industry")
        if "source" in master_raw.columns:
            meta_cols.append("source")
        meta = master_raw[meta_cols].drop_duplicates("종목코드")
        full = full.merge(meta, on="종목코드", how="left")
        if "exchange" in full.columns:
            full["시장구분"] = full["exchange"].fillna("NYSE")
            full["exchange"] = full["exchange"].fillna("NYSE")
        if "stock_type" in full.columns:
            full["종목구분"] = full["stock_type"].apply(normalize_us_stock_type)
        if "sector" in full.columns:
            full["섹터"] = full["sector"].fillna("")
        if "industry" in full.columns:
            full["industry"] = full["industry"].fillna("")
        if "source" in full.columns:
            full["index_membership"] = full["source"].fillna("")
    else:
        if "시장구분" not in full.columns:
            full["시장구분"] = "NYSE"
        if "exchange" not in full.columns:
            full["exchange"] = full["시장구분"]
        if "종목구분" not in full.columns:
            full["종목구분"] = "보통주"
        if "섹터" not in full.columns:
            full["섹터"] = ""
        if "industry" not in full.columns:
            full["industry"] = ""
        if "index_membership" not in full.columns:
            full["index_membership"] = ""

    # ── 11. 시가총액 필터 (마이크로캡 제외) ──
    if "시가총액" in full.columns:
        _before_mcap = len(full)
        full = full[
            full["시가총액"].isna() | (full["시가총액"] >= config.US_MIN_MARKET_CAP)
        ]
        _removed_mcap = _before_mcap - len(full)
        if _removed_mcap:
            log.info("시가총액 필터: %d종목 제거 (< $%.0fM)",
                     _removed_mcap, config.US_MIN_MARKET_CAP / 1_000_000)

    # ── 12. 저장 ──
    _progress(f"DB 저장 중... ({len(full)}종목)")
    _db.save_us_dashboard(full)
    log.info("US 스크리너 완료: %d종목", len(full))
    _progress(f"US 스크리너 완료: {len(full)}종목")


if __name__ == "__main__":
    run()
