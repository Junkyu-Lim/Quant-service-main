# =========================================================
# quant_screener.py  (v14 - COMPLETE RESTORE)
# ---------------------------------------------------------
# [FIXED] Restored missing 'preprocess_indicators' and all exports.
# [RESTORED] 100% of original v8 logic for indicators.
# [INTEGRATED] 5-Strategy scoring & Market Leaders.
# =========================================================

import sys
import logging
import re
from pathlib import Path

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
log = logging.getLogger("SCREENER")

DATA_DIR = config.DATA_DIR

# ─────────────────────────────────────────────
# 계정 매핑 (v8 원본 완벽 복구)
# ─────────────────────────────────────────────
EXACT_ACCOUNTS = {
    "매출액": ["매출액", "영업수익", "이자수익", "보험료수익", "순영업수익"],
    "영업이익": ["영업이익"],
    "순이익": ["지배주주순이익", "당기순이익"],
    "자본": ["자본", "자본총계", "지배주주지분", "지배기업주주지분"],
    "부채": ["부채", "부채총계"],
    "배당금": ["주당배당금"],
    "영업CF": [
        "영업활동현금흐름", "영업활동으로인한현금흐름",
        "영업활동 현금흐름", "영업활동으로 인한 현금흐름",
        "영업활동에서창출된현금", "영업에서창출된현금흐름",
    ],
    "투자CF": ["투자활동현금흐름", "투자활동으로인한현금흐름", "투자활동 현금흐름"],
    "CAPEX": [
        "유형자산의취득", "유형자산취득",
        "(-)유형자산의취득", "(-)유형자산취득",
        "유형자산 취득", "유형자산의 취득",
        "유형자산매입",
        "유형자산의증가", "(-)유형자산의증가",
    ],
    "자산총계": ["자산총계", "자산"],
    "유동자산": ["유동자산"],
    "유동부채": ["유동부채"],
    "매출총이익": ["매출총이익"],
}

EXCLUDE_KEYWORDS = ["증가율", "(-1Y)", "(평균)", "률(", "비율", "배율", "(-1A", "(-1Q", "/ 수정평균"]

# ═════════════════════════════════════════════
# 유틸리티 (v8 원본 로직)
# ═════════════════════════════════════════════

def normalize_code(x):
    try:
        if pd.isna(x) or str(x).strip() == "": return np.nan
        s = str(x).strip()
        if '.' in s: s = s.split('.')[0]
        return s.zfill(6)
    except: return np.nan

def load_table(prefix: str) -> pd.DataFrame:
    import db as _db
    df = _db.load_latest(prefix)
    if df.empty: return df
    df.columns = df.columns.str.strip()
    if "종목코드" in df.columns:
        df["종목코드"] = df["종목코드"].apply(normalize_code)
        df = df.dropna(subset=["종목코드"])
    if "기준일" in df.columns: df["기준일"] = df["기준일"].astype(str).str[:10]
    for col in ["값", "종가", "시가총액", "상장주식수"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def _normalize_account(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r'^\([+\-±]\)\s*', '', name)
    name = name.replace(' ', '')
    return name

def find_account_value(df, target_key, date_filter=None):
    if df.empty or "계정" not in df.columns: return {}
    targets = EXACT_ACCOUNTS.get(target_key, [target_key])
    norm_targets = {_normalize_account(t) for t in targets}
    work = df.copy()
    if date_filter is not None: work = work[work["기준일"].isin(date_filter)]
    
    mask = work["계정"].isin(targets)
    matched = work[mask]
    if matched.empty:
        matched = work[work["계정"].apply(lambda n: _normalize_account(n) in norm_targets)]
    if matched.empty:
        def _startswith_any(name):
            norm = _normalize_account(str(name))
            return any(norm.startswith(t) for t in norm_targets)
        matched = work[work["계정"].apply(_startswith_any)]

    if matched.empty: return {}
    matched = matched.drop_duplicates(["종목코드", "기준일"], keep="first")
    return {str(r["기준일"]): (float(r["값"]) if pd.notna(r["값"]) else None) for _, r in matched.iterrows()}

def preprocess_indicators(ind_df):
    if ind_df.empty: return ind_df
    return ind_df.drop_duplicates(["종목코드", "기준일", "계정", "지표구분"], keep="first")

def detect_unit_multiplier(ind_df):
    sam = ind_df[ind_df["종목코드"] == "005930"]
    if sam.empty: return 100_000_000
    rev_dict = find_account_value(sam[sam["지표구분"] == "RATIO_Y"], "매출액")
    if not rev_dict: return 100_000_000
    valid_revs = [v for v in rev_dict.values() if v is not None]
    if not valid_revs: return 100_000_000
    latest_rev = max(valid_revs)
    if latest_rev > 1e14: return 1
    elif latest_rev > 1e8: return 1_000_000
    else: return 100_000_000

# ═════════════════════════════════════════════
# 분석 유틸리티 (v8 원본)
# ═════════════════════════════════════════════

def calc_cagr(series_dict, min_years=2):
    if not series_dict or len(series_dict) < min_years: return np.nan
    dates = sorted(series_dict.keys())
    v0, v1 = series_dict[dates[0]], series_dict[dates[-1]]
    if v0 is None or v1 is None or v0 <= 0 or v1 <= 0: return np.nan
    try:
        years = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days / 365.25
        return ((v1 / v0) ** (1 / years) - 1) * 100 if years > 0.5 else np.nan
    except: return np.nan

def count_consecutive_growth(series_dict):
    if not series_dict or len(series_dict) < 2: return 0
    vals = [series_dict[d] for d in sorted(series_dict.keys())]
    count = 0
    for i in range(len(vals) - 1, 0, -1):
        if vals[i] is None or vals[i - 1] is None: break
        if vals[i] > vals[i - 1]: count += 1
        else: break
    return count

def calc_quarterly_yoy(q_data, key):
    res = {"latest_yoy": np.nan, "consecutive_yoy_growth": 0, "latest_quarter": "", "yoy_series": {}}
    vals = find_account_value(q_data, key)
    if len(vals) < 5: return res
    yoy_s = {}
    for d in sorted(vals.keys()):
        prev_d = str(int(d[:4])-1) + d[4:]
        if prev_d in vals and vals[prev_d] is not None and vals[d] is not None and vals[prev_d] > 0: yoy_s[d] = ((vals[d]/vals[prev_d])-1)*100
    if not yoy_s: return res
    res["yoy_series"], res["latest_quarter"] = yoy_s, max(yoy_s.keys())
    res["latest_yoy"] = yoy_s[res["latest_quarter"]]
    for d in sorted(yoy_s.keys(), reverse=True):
        if yoy_s[d] > 0: res["consecutive_yoy_growth"] += 1
        else: break
    return res

def calc_ttm_yoy(q_data, key):
    res = {"ttm_current": np.nan, "ttm_prev": np.nan, "ttm_yoy": np.nan}
    vals = find_account_value(q_data, key)
    if len(vals) < 8: return res
    dates = sorted(vals.keys())
    last4, prev4 = dates[-4:], dates[-8:-4]
    last4_valid = [d for d in last4 if d in vals and vals[d] is not None]
    prev4_valid = [d for d in prev4 if d in vals and vals[d] is not None]
    ttm_curr = sum(vals[d] for d in last4_valid)
    ttm_prev = sum(vals[d] for d in prev4_valid)
    if len(last4_valid) == 4: res["ttm_current"] = ttm_curr
    if len(prev4_valid) == 4: res["ttm_prev"] = ttm_prev
    if pd.notna(res["ttm_current"]) and pd.notna(res["ttm_prev"]) and res["ttm_prev"] > 0:
        res["ttm_yoy"] = ((res["ttm_current"] / res["ttm_prev"]) - 1) * 100
    return res

# ═════════════════════════════════════════════
# 메인 분석 엔진 (v8 원본 완벽 복구)
# ═════════════════════════════════════════════

def analyze_one_stock(ticker, ind_grp, fs_grp):
    res = {"종목코드": ticker}
    has_ind, has_fs = not ind_grp.empty, not fs_grp.empty
    
    if has_ind:
        q_data = ind_grp[ind_grp["지표구분"] == "RATIO_Q"]
        y_data = ind_grp[ind_grp["지표구분"] == "RATIO_Y"]
        
        for label, key in [("매출", "매출액"), ("영업이익", "영업이익"), ("순이익", "순이익")]:
            qyoy = calc_quarterly_yoy(q_data, key)
            res[f"Q_{label}_YoY(%)"] = qyoy["latest_yoy"]
            res[f"Q_{label}_연속YoY성장"] = qyoy["consecutive_yoy_growth"]
            ttmy = calc_ttm_yoy(q_data, key)
            res[f"TTM_{label}_YoY(%)"] = ttmy["ttm_yoy"]
            val = ttmy["ttm_current"]
            if pd.isna(val):
                y_vals = find_account_value(y_data, key)
                if y_vals: val = y_vals[max(y_vals.keys())]
            res[f"TTM_{label}"] = val
        res["최근분기"] = sorted(q_data["기준일"].unique())[-1] if not q_data.empty else ""

        total_assets_s = find_account_value(y_data, "자산총계")
        equity_s = find_account_value(y_data, "자본")
        debt_s = find_account_value(y_data, "부채")
        current_assets_s = find_account_value(y_data, "유동자산")
        current_liab_s = find_account_value(y_data, "유동부채")
        gross_profit_s = find_account_value(y_data, "매출총이익")
        res["자산총계"] = total_assets_s[max(total_assets_s.keys())] if total_assets_s else np.nan
        res["자본"] = equity_s[max(equity_s.keys())] if equity_s else np.nan
        res["부채"] = debt_s[max(debt_s.keys())] if debt_s else np.nan

        rev_s = find_account_value(y_data, "매출액")
        op_s = find_account_value(y_data, "영업이익")
        ni_s = find_account_value(y_data, "순이익")
        res.update({
            "매출_CAGR": calc_cagr(rev_s), "영업이익_CAGR": calc_cagr(op_s), "순이익_CAGR": calc_cagr(ni_s),
            "매출_연속성장": count_consecutive_growth(rev_s), "영업이익_연속성장": count_consecutive_growth(op_s), "순이익_연속성장": count_consecutive_growth(ni_s),
        })
        res["데이터_연수"] = len(rev_s)

        if len(rev_s) >= 2 and len(op_s) >= 2:
            l, p = sorted(rev_s.keys())[-1], sorted(rev_s.keys())[-2]
            opm_l = (op_s[l]/rev_s[l]*100) if (rev_s[l] is not None and op_s.get(l) is not None and rev_s[l] > 0) else np.nan
            opm_p = (op_s[p]/rev_s[p]*100) if (rev_s[p] is not None and op_s.get(p) is not None and rev_s[p] > 0) else np.nan
            res["영업이익률_최근"], res["영업이익률_전년"] = opm_l, opm_p
            res["이익률_개선"] = 1 if pd.notna(opm_l) and pd.notna(opm_p) and opm_l > opm_p else 0
            delta = opm_l - opm_p if pd.notna(opm_l) and pd.notna(opm_p) else np.nan
            res["이익률_변동폭"], res["이익률_급개선"] = delta, (1 if (delta or 0) >= 5 else 0)
        else:
            res["영업이익률_최근"] = np.nan
            res["영업이익률_전년"] = np.nan
            res["이익률_개선"] = 0
            res["이익률_변동폭"] = np.nan
            res["이익률_급개선"] = 0

        if len(ni_s) >= 2:
            ni_vals = [ni_s[d] for d in sorted(ni_s.keys())]
            res["흑자전환"] = 1 if (ni_vals[-2] is not None and ni_vals[-1] is not None and ni_vals[-2] < 0 and ni_vals[-1] > 0) else 0
            res["순이익_당기양수"] = 1 if (ni_vals[-1] is not None and ni_vals[-1] > 0) else 0
            res["순이익_전년음수"] = 1 if (ni_vals[-2] is not None and ni_vals[-2] < 0) else 0
        else:
            res["흑자전환"] = 0
            res["순이익_당기양수"] = 0
            res["순이익_전년음수"] = 0

        ocf_s = find_account_value(y_data, "영업CF")
        if not ocf_s: ocf_s = find_account_value(ind_grp[ind_grp["지표구분"].isin(["HIGHLIGHT", "HIGHLIGHT_E"])], "영업CF")
        if not ocf_s and has_fs: ocf_s = find_account_value(fs_grp[fs_grp["주기"]=="y"], "영업CF")
        capex_s = find_account_value(y_data, "CAPEX")
        if not capex_s and has_fs: capex_s = find_account_value(fs_grp[fs_grp["주기"]=="y"], "CAPEX")
        capex_s = {d: abs(v) for d, v in capex_s.items() if v is not None}
        
        fcf_s = {d: (ocf_s[d] - capex_s[d]) for d in (set(ocf_s.keys()) & set(capex_s.keys())) if ocf_s[d] is not None and capex_s[d] is not None}
        ttm_ocf = ocf_s[max(ocf_s.keys())] if ocf_s else np.nan
        ttm_capex = capex_s[max(capex_s.keys())] if capex_s else np.nan
        res.update({
            "TTM_영업CF": ttm_ocf, "TTM_CAPEX": ttm_capex, "TTM_FCF": (ttm_ocf - ttm_capex if pd.notna(ttm_ocf) and pd.notna(ttm_capex) else np.nan),
            "영업CF_CAGR": calc_cagr(ocf_s), "FCF_CAGR": calc_cagr(fcf_s), "영업CF_연속성장": count_consecutive_growth(ocf_s)
        })

        f1 = 1 if (res.get("TTM_순이익") or 0) > 0 else 0
        f2 = 1 if (ttm_ocf or 0) > 0 else 0
        f4 = 1 if (ttm_ocf or 0) > (res.get("TTM_순이익") or 0) and f1 else 0

        # F3: ROA 개선 (ni/total_assets 최근 > 전년)
        f3 = 0
        if len(ni_s) >= 2 and len(total_assets_s) >= 2:
            ni_dates = sorted(ni_s.keys())
            ta_dates = sorted(total_assets_s.keys())
            if ni_dates[-1] in total_assets_s and ni_dates[-2] in total_assets_s:
                roa_cur = ni_s[ni_dates[-1]] / total_assets_s[ni_dates[-1]] if (total_assets_s[ni_dates[-1]] and ni_s[ni_dates[-1]] is not None) else 0
                roa_prev = ni_s[ni_dates[-2]] / total_assets_s[ni_dates[-2]] if (total_assets_s[ni_dates[-2]] and ni_s[ni_dates[-2]] is not None) else 0
                f3 = 1 if roa_cur > roa_prev else 0
            elif len(ta_dates) >= 2:
                roa_cur = ni_s[ni_dates[-1]] / total_assets_s[ta_dates[-1]] if (total_assets_s[ta_dates[-1]] and ni_s[ni_dates[-1]] is not None) else 0
                roa_prev = ni_s[ni_dates[-2]] / total_assets_s[ta_dates[-2]] if (total_assets_s[ta_dates[-2]] and ni_s[ni_dates[-2]] is not None) else 0
                f3 = 1 if roa_cur > roa_prev else 0

        # F5: 레버리지 감소 (부채비율 최근 < 전년)
        f5 = 0
        if len(debt_s) >= 2 and len(equity_s) >= 2:
            d_dates = sorted(debt_s.keys())
            e_dates = sorted(equity_s.keys())
            if len(d_dates) >= 2 and len(e_dates) >= 2:
                dr_cur = debt_s[d_dates[-1]] / equity_s[e_dates[-1]] if (equity_s[e_dates[-1]] and debt_s[d_dates[-1]] is not None) else 999
                dr_prev = debt_s[d_dates[-2]] / equity_s[e_dates[-2]] if (equity_s[e_dates[-2]] and debt_s[d_dates[-2]] is not None) else 999
                f5 = 1 if dr_cur < dr_prev else 0

        # F6: 유동성 개선 (유동비율 최근 > 전년)
        f6 = 0
        if len(current_assets_s) >= 2 and len(current_liab_s) >= 2:
            ca_dates = sorted(current_assets_s.keys())
            cl_dates = sorted(current_liab_s.keys())
            if len(ca_dates) >= 2 and len(cl_dates) >= 2:
                cr_cur = current_assets_s[ca_dates[-1]] / current_liab_s[cl_dates[-1]] if (current_liab_s[cl_dates[-1]] and current_assets_s[ca_dates[-1]] is not None) else 0
                cr_prev = current_assets_s[ca_dates[-2]] / current_liab_s[cl_dates[-2]] if (current_liab_s[cl_dates[-2]] and current_assets_s[ca_dates[-2]] is not None) else 0
                f6 = 1 if cr_cur > cr_prev else 0

        # F7: 희석 없음 (placeholder — calc_valuation에서 shares 기반 업데이트)
        f7 = 0

        # F8: 매출총이익률 개선
        f8 = 0
        if len(gross_profit_s) >= 2 and len(rev_s) >= 2:
            gp_dates = sorted(gross_profit_s.keys())
            rv_dates = sorted(rev_s.keys())
            if len(gp_dates) >= 2 and len(rv_dates) >= 2:
                gpm_cur = gross_profit_s[gp_dates[-1]] / rev_s[rv_dates[-1]] if (rev_s[rv_dates[-1]] and gross_profit_s[gp_dates[-1]] is not None) else 0
                gpm_prev = gross_profit_s[gp_dates[-2]] / rev_s[rv_dates[-2]] if (rev_s[rv_dates[-2]] and gross_profit_s[gp_dates[-2]] is not None) else 0
                f8 = 1 if gpm_cur > gpm_prev else 0

        # F9: 자산회전율 개선 (매출/자산총계 최근 > 전년)
        f9 = 0
        if len(rev_s) >= 2 and len(total_assets_s) >= 2:
            rv_dates = sorted(rev_s.keys())
            ta_dates = sorted(total_assets_s.keys())
            if len(rv_dates) >= 2 and len(ta_dates) >= 2:
                at_cur = rev_s[rv_dates[-1]] / total_assets_s[ta_dates[-1]] if (total_assets_s[ta_dates[-1]] and rev_s[rv_dates[-1]] is not None) else 0
                at_prev = rev_s[rv_dates[-2]] / total_assets_s[ta_dates[-2]] if (total_assets_s[ta_dates[-2]] and rev_s[rv_dates[-2]] is not None) else 0
                f9 = 1 if at_cur > at_prev else 0

        res["F스코어"] = f1 + f2 + f3 + f4 + f5 + f6 + f7 + f8 + f9
        res["F1_수익성"], res["F2_영업CF"], res["F3_ROA개선"], res["F4_이익품질"] = f1, f2, f3, f4
        res["F5_레버리지"], res["F6_유동성"], res["F7_희석없음"] = f5, f6, f7
        res["F8_매출총이익률"], res["F9_자산회전율"] = f8, f9

        dps_s = find_account_value(ind_grp[ind_grp["지표구분"]=="DPS"], "배당금")
        res["DPS_최근"] = dps_s[max(dps_s.keys())] if dps_s else np.nan
        res["DPS_CAGR"], res["배당_연속증가"] = calc_cagr(dps_s), count_consecutive_growth(dps_s)
        res["배당_수익동반증가"] = 1 if res["순이익_연속성장"] >= 2 and res["배당_연속증가"] >= 1 else 0

    return res

def analyze_all(fs_df, ind_df):
    results = []
    tickers = list(set(fs_df["종목코드"].unique()) | set(ind_df["종목코드"].unique()))
    for ticker in tqdm(tickers, desc="펀더멘털 분석"):
        results.append(analyze_one_stock(ticker, ind_grp=ind_df[ind_df["종목코드"]==ticker], fs_grp=fs_df[fs_df["종목코드"]==ticker]))
    return pd.DataFrame(results)

# ═════════════════════════════════════════════
# 밸류에이션 & 기술적 지표 (v8 원본 + 수급)
# ═════════════════════════════════════════════

def calc_valuation(daily, anal_df, multiplier, shares_df):
    df = daily[["종목코드", "종목명", "종가", "시가총액", "상장주식수"]].drop_duplicates("종목코드").merge(anal_df, on="종목코드", how="inner")
    if shares_df is not None and not shares_df.empty:
        s_map = shares_df.drop_duplicates("종목코드").set_index("종목코드")["발행주식수"]
        mask = df["상장주식수"].isna() | (df["상장주식수"] == 0)
        df.loc[mask, "상장주식수"] = df.loc[mask, "종목코드"].map(s_map)

    M = multiplier
    df["PER"] = np.where((df["TTM_순이익"] > 0) & (df["시가총액"] > 0), df["시가총액"] / (df["TTM_순이익"] * M), np.nan)
    df["PBR"] = np.where((df["자본"] > 0) & (df["시가총액"] > 0), df["시가총액"] / (df["자본"] * M), np.nan)
    df["PSR"] = np.where((df["TTM_매출"] > 0) & (df["시가총액"] > 0), df["시가총액"] / (df["TTM_매출"] * M), np.nan)
    df["ROE(%)"] = np.where((df["자본"] > 0), (df["TTM_순이익"] / df["자본"]) * 100, np.nan)
    df["부채비율(%)"] = np.where((df["자본"] > 0), (df["부채"] / df["자본"]) * 100, np.nan)
    df["영업이익률(%)"] = df["영업이익률_최근"]
    df["배당수익률(%)"] = np.where((df["종가"] > 0) & (df["DPS_최근"] > 0), (df["DPS_최근"] / df["종가"]) * 100, 0)
    df["PEG"] = np.where((df["PER"] > 0) & (df["순이익_CAGR"] > 0), df["PER"] / df["순이익_CAGR"], np.nan)
    df["FCF수익률(%)"] = np.where((df["시가총액"] > 0) & (df["TTM_FCF"] != 0), (df["TTM_FCF"] * M / df["시가총액"]) * 100, np.nan)
    df["이익수익률(%)"] = np.where((df["시가총액"] > 0) & (df["TTM_순이익"] > 0), (df["TTM_순이익"] * M / df["시가총액"]) * 100, np.nan)
    df["현금전환율(%)"] = np.where(pd.notna(df["TTM_영업CF"]) & (df["TTM_순이익"] > 0), (df["TTM_영업CF"] / df["TTM_순이익"]) * 100, np.nan)
    df["CAPEX비율(%)"] = np.where(pd.notna(df["TTM_CAPEX"]) & (df["TTM_영업CF"] > 0), (df["TTM_CAPEX"] / df["TTM_영업CF"]) * 100, np.nan)
    df["부채상환능력"] = np.where((df["TTM_영업CF"] > 0) & (df["부채"] > 0), df["TTM_영업CF"] / df["부채"], np.nan)
    df["이익품질_양호"] = np.where((df["TTM_영업CF"] > df["TTM_순이익"]) & (df["TTM_순이익"] > 0), 1, 0)

    shares = df["상장주식수"].replace(0, np.nan)
    df["BPS"], df["EPS"] = (df["자본"] * M) / shares, (df["TTM_순이익"] * M) / shares
    Ke = 8.0
    df["적정주가_SRIM"] = np.where((df["ROE(%)"] > Ke) & (df["BPS"] > 0), df["BPS"] + df["BPS"] * (df["ROE(%)"] - Ke) / Ke, df["BPS"] * 0.9)
    df["괴리율(%)"] = ((df["적정주가_SRIM"] - df["종가"]) / df["종가"]) * 100

    # PER 이상치 플래그
    df["PER_이상"] = np.where(
        pd.notna(df["PER"]) & ((df["PER"] < 0.5) | (df["PER"] > 500)), 1, 0
    ).astype(int)

    # F7 업데이트: 발행주식수 미증가 (shares_df 기반)
    if shares_df is not None and not shares_df.empty:
        shares_by_code = shares_df.sort_values("기준일").groupby("종목코드")["발행주식수"]
        for idx, row in df.iterrows():
            code = row["종목코드"]
            if code in shares_by_code.groups:
                s_vals = shares_by_code.get_group(code).dropna().values
                if len(s_vals) >= 2:
                    f7_val = 1 if s_vals[-1] <= s_vals[-2] else 0
                    df.at[idx, "F7_희석없음"] = f7_val
                    df.at[idx, "F스코어"] = (
                        row.get("F1_수익성", 0) + row.get("F2_영업CF", 0)
                        + row.get("F3_ROA개선", 0) + row.get("F4_이익품질", 0)
                        + row.get("F5_레버리지", 0) + row.get("F6_유동성", 0)
                        + f7_val
                        + row.get("F8_매출총이익률", 0) + row.get("F9_자산회전율", 0)
                    )

    return df

def calc_technical_indicators(df, price_hist):
    if price_hist.empty: return df
    techs = []
    for code in df["종목코드"].unique():
        ph = price_hist[price_hist["종목코드"]==code].sort_values("날짜")
        if len(ph) < 5: continue
        close = ph["종가"].iloc[-1]
        ma20 = ph["종가"].rolling(20).mean().iloc[-1] if len(ph)>=20 else np.nan
        ma60 = ph["종가"].rolling(60).mean().iloc[-1] if len(ph)>=60 else np.nan
        h52, l52 = ph["종가"].max(), ph["종가"].min()
        
        rsi = np.nan
        if len(ph) >= 15:
            delta = ph["종가"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean().iloc[-1]
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean().iloc[-1]
            rsi = 100 - (100 / (1 + (gain / loss))) if loss > 0 else 100
        
        v_col = "거래대금" if "거래대금" in ph.columns else None
        v20 = (ph[v_col].tail(20).mean() if v_col else (ph["종가"]*ph["거래량"]).tail(20).mean()) if len(ph)>=20 else np.nan
        v5 = (ph[v_col].tail(5).mean() if v_col else (ph["종가"]*ph["거래량"]).tail(5).mean())
        vol60 = ph["종가"].pct_change().tail(60).std() * np.sqrt(252) * 100 if len(ph)>=60 else np.nan

        techs.append({
            "종목코드": code, "RSI_14": rsi, "MA20_이격도(%)": (close/ma20-1)*100 if pd.notna(ma20) else np.nan,
            "MA60_이격도(%)": (close/ma60-1)*100 if pd.notna(ma60) else np.nan,
            "52주_최고대비(%)": (close/h52-1)*100, "52주_최저대비(%)": (close/l52-1)*100,
            "거래대금_20일평균": v20, "거래대금_증감(%)": (v5/v20-1)*100 if pd.notna(v20) and v20 > 0 else np.nan,
            "변동성_60일(%)": vol60
        })
    return df.merge(pd.DataFrame(techs), on="종목코드", how="left")

def calc_investor_strength(inv_df, daily):
    if inv_df.empty: return pd.DataFrame(columns=["종목코드", "수급강도", "외인순매수_20d", "기관순매수_20d"])
    res = []
    for code in inv_df["종목코드"].unique():
        df_code = inv_df[inv_df["종목코드"]==code].sort_values("날짜", ascending=False).head(20)
        f_sum, i_sum = df_code["외국인순매수"].sum(), df_code["기관순매수"].sum()
        mcap = daily.loc[daily["종목코드"]==code, "시가총액"].values
        mcap = mcap[0] if len(mcap) > 0 else np.nan
        strength = ((f_sum + i_sum) / mcap) * 100 if pd.notna(mcap) and mcap > 0 else np.nan
        res.append({"종목코드": code, "수급강도": strength, "외인순매수_20d": f_sum, "기관순매수_20d": i_sum})
    return pd.DataFrame(res)

# ═════════════════════════════════════════════
# 스코어링 & 저장 (v8 스타일 유지)
# ═════════════════════════════════════════════

def calc_strategy_scores(df):
    def get_rank(col, asc=True):
        if col not in df.columns: return pd.Series(50.0, index=df.index)
        series = df[col].fillna(df[col].median() if not df[col].isna().all() else 0)
        return series.rank(pct=True) * 100 if asc else (1 - series.rank(pct=True)) * 100
    
    S_PER_inv, S_PBR_inv, S_PEG_inv = get_rank("PER", False), get_rank("PBR", False), get_rank("PEG", False)
    S_ROE, S_OpCAGR, S_QOpYoY = get_rank("ROE(%)"), get_rank("영업이익_CAGR"), get_rank("Q_영업이익_YoY(%)")
    S_FCF, S_Div, S_Supply, S_Vol = get_rank("FCF수익률(%)"), get_rank("배당수익률(%)"), get_rank("수급강도"), get_rank("거래대금_20일평균")
    
    df["주도주_점수"] = (S_Supply*0.3 + S_Vol*0.2 + S_OpCAGR*0.2 + S_QOpYoY*0.2 + get_rank("RSI_14")*0.1)
    df["우량가치_점수"] = (S_PEG_inv*0.3 + S_PER_inv*0.1 + S_ROE*0.3 + get_rank("F스코어")*0.2 + get_rank("부채비율(%)", False)*0.1)
    df["고성장_점수"] = (get_rank("매출_CAGR")*0.2 + S_OpCAGR*0.3 + S_QOpYoY*0.3 + get_rank("MA20_이격도(%)")*0.1 + get_rank("52주_최고대비(%)")*0.1)
    df["현금배당_점수"] = (S_FCF*0.3 + S_Div*0.3 + get_rank("DPS_CAGR")*0.2 + get_rank("F스코어")*0.1 + get_rank("부채비율(%)", False)*0.1)
    df["턴어라운드_점수"] = (
        get_rank("이익률_변동폭") * 0.3
        + get_rank("흑자전환") * 0.3
        + get_rank("순이익_당기양수") * 0.1
        + get_rank("F스코어") * 0.15
        + get_rank("괴리율(%)") * 0.15
    )
    
    # Original v8 Comprehensive Score Weights
    df["종합점수"] = (S_PER_inv*1.5 + S_PBR_inv*1.0 + S_ROE*2.0 + get_rank("매출_CAGR")*2.0 + S_OpCAGR*2.0 + get_rank("F스코어")*2.0 + S_FCF*1.5 + get_rank("괴리율(%)")*1.0) / 13.0 * 100
    return df

def apply_leaders_screen(df):
    mask = (df["시가총액"]>=200_000_000_000) & (df.get("거래대금_20일평균",0)>=1_000_000_000) & (df["TTM_순이익"]>0)
    return df[mask].sort_values("주도주_점수", ascending=False)

def apply_quality_value_screen(df):
    mask = (df["ROE(%)"]>=10) & (df.get("PEG",99)<1.5) & (df["PER"].between(1, 40)) & (df["F스코어"]>=4) & (df["시가총액"]>=50_000_000_000)
    return df[mask].sort_values("우량가치_점수", ascending=False)

def apply_growth_mom_screen(df):
    mask = ((df.get("매출_CAGR",0)>=15) | (df.get("영업이익_CAGR",0)>=15)) & (df.get("Q_영업이익_YoY(%)",0)>0) & (df["MA20_이격도(%)"]>= -5) & (df["시가총액"]>=50_000_000_000)
    return df[mask].sort_values("고성장_점수", ascending=False)

def apply_cash_div_screen(df):
    mask = (df.get("FCF수익률(%)",0)>=3) & (df["배당수익률(%)"]>=1) & (df["부채비율(%)"] < 150) & (df["시가총액"]>=50_000_000_000)
    return df[mask].sort_values("현금배당_점수", ascending=False)

def apply_turnaround_screen(df):
    mask = ((df.get("흑자전환")==1) | (df.get("이익률_급개선")==1)) & (df["TTM_순이익"]>0) & (df["시가총액"]>=30_000_000_000)
    return df[mask].sort_values("턴어라운드_점수", ascending=False)

def save_to_excel(df, filepath, sheet_name="Result"):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    wb = Workbook(); ws = wb.active; ws.title = sheet_name
    col_groups = {
        "기본정보": ["종목코드", "종목명", "종가", "시가총액", "상장주식수", "데이터_연수"],
        "주요지표": ["PER", "PBR", "PSR", "PEG", "PER_이상", "ROE(%)", "EPS", "BPS", "부채비율(%)", "영업이익률(%)", "이익수익률(%)", "FCF수익률(%)", "배당수익률(%)", "이익품질_양호"],
        "F스코어": ["F스코어", "F1_수익성", "F2_영업CF", "F3_ROA개선", "F4_이익품질", "F5_레버리지", "F6_유동성", "F7_희석없음", "F8_매출총이익률", "F9_자산회전율"],
        "수급/거래": ["수급강도", "외인순매수_20d", "기관순매수_20d", "거래대금_20일평균", "거래대금_증감(%)"],
        "점수": ["종합점수", "주도주_점수", "우량가치_점수", "고성장_점수", "현금배당_점수", "턴어라운드_점수"],
        "성장추세": ["매출_CAGR", "영업이익_CAGR", "순이익_CAGR", "매출_연속성장", "영업이익_연속성장", "이익률_변동폭", "흑자전환", "순이익_전년음수", "순이익_당기양수"],
        "밸류에이션": ["적정주가_SRIM", "괴리율(%)"],
    }
    ordered_cols = []
    for g in col_groups.values():
        for c in g:
            if c in df.columns: ordered_cols.append(c)
    for c in df.columns:
        if c not in ordered_cols and not c.startswith("S_"): ordered_cols.append(c)
    export_df = df[ordered_cols].copy()
    fills = {
        "기본정보": PatternFill("solid", fgColor="D6E4F0"), "주요지표": PatternFill("solid", fgColor="E2EFDA"),
        "F스코어": PatternFill("solid", fgColor="FCE4D6"), "수급/거래": PatternFill("solid", fgColor="FDE9D9"),
        "점수": PatternFill("solid", fgColor="C6EFCE"), "성장추세": PatternFill("solid", fgColor="FFF2CC"),
        "밸류에이션": PatternFill("solid", fgColor="DAEEF3")
    }
    for col_idx, col_name in enumerate(ordered_cols, 1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal='center')
        for grp, cols in col_groups.items():
            if col_name in cols: cell.fill = fills[grp]; break
    for row_idx, (_, row_data) in enumerate(export_df.iterrows(), 2):
        for col_idx, col_name in enumerate(ordered_cols, 1):
            val = row_data[col_name]
            if pd.isna(val): val = None
            elif isinstance(val, (float, np.floating)): val = round(float(val), 2)
            ws.cell(row=row_idx, column=col_idx, value=val)
    ws.freeze_panes = "C2"
    wb.save(filepath); log.info(f"Saved: {filepath}")

def run():
    daily, fs, ind = load_table("daily"), load_table("financial_statements"), load_table("indicators")
    shares, hist, inv = load_table("shares"), load_table("price_history"), load_table("investor_trading")
    if daily.empty: return
    ind = preprocess_indicators(ind); mult = detect_unit_multiplier(ind); anal = analyze_all(fs, ind)
    full = calc_valuation(daily, anal, mult, shares)
    full = calc_technical_indicators(full, hist)
    full = full.merge(calc_investor_strength(inv, daily), on="종목코드", how="left")
    full = calc_strategy_scores(full)
    save_to_excel(full.sort_values("종합점수", ascending=False), DATA_DIR / "quant_all_stocks.xlsx", "All")
    save_to_excel(apply_leaders_screen(full), DATA_DIR / "quant_leaders.xlsx", "Leaders")
    save_to_excel(apply_quality_value_screen(full), DATA_DIR / "quant_quality_value.xlsx", "QualityValue")
    save_to_excel(apply_growth_mom_screen(full), DATA_DIR / "quant_growth_mom.xlsx", "GrowthMom")
    save_to_excel(apply_cash_div_screen(full), DATA_DIR / "quant_cash_div.xlsx", "CashDiv")
    save_to_excel(apply_turnaround_screen(full), DATA_DIR / "quant_turnaround.xlsx", "Turnaround")

if __name__ == "__main__": run()
