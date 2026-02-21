"""
Flask web application – SQLite-based API for the Quant dashboard.
Reads dashboard_result from SQLite DB produced by the pipeline and serves it
with server-side filtering, sorting, and pagination.
"""

import json
import logging
import os
import threading

import numpy as np
import pandas as pd
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

import config
import db as _db
from analysis.claude_analyzer import generate_report

log = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)
CORS(app)

# ── In-memory data cache ──
_cache: dict = {"df": pd.DataFrame(), "mtime": 0}
_prev_cache: dict = {"df": pd.DataFrame(), "mtime": 0}

# ── Pipeline state ──
_pipeline: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "stage": "",
    "progress": 0,
}

# Columns exposed to the frontend
DISPLAY_COLS = [
    "종목코드", "종목명", "시장구분", "종가", "시가총액",
    "PER", "PBR", "PSR", "PEG", "ROE(%)", "EPS", "BPS",
    "부채비율(%)", "영업이익률(%)", "이익수익률(%)", "FCF수익률(%)",
    "배당수익률(%)", "이익품질_양호", "이자보상배율", "현금전환율(%)", "CAPEX비율(%)",
    "부채상환능력", "F스코어",
    "F1_수익성", "F2_영업CF", "F3_ROA개선", "F4_이익품질",
    "F5_레버리지", "F6_유동성", "F7_희석없음", "F8_매출총이익률", "F9_자산회전율",
    "PER_이상", "데이터_연수", "순이익_전년음수", "순이익_당기양수",
    "52주_최고대비(%)", "52주_최저대비(%)", "MA20_이격도(%)", "MA60_이격도(%)",
    "RSI_14", "거래대금_20일평균", "거래대금_증감(%)", "변동성_60일(%)",
    "수급강도", "외인순매수_20d", "기관순매수_20d",
    "스마트머니_승률", "양매수_비율", "VCP_신호",
    "RS_60d", "RS_120d", "RS_250d", "Composite_RS", "RS_등급",
    "영업이익_가속도", "매출_가속도", "실적가속_연속",
    "GPM_최근(%)", "GPM_전년(%)", "GPM_변화(pp)",
    "ROIC(%)", "ROIC_전년(%)", "ROIC_개선", "퀄리티_턴어라운드",
    "매출_CAGR", "영업이익_CAGR", "순이익_CAGR", "영업CF_CAGR", "FCF_CAGR",
    "DPS_최근", "DPS_CAGR", "배당_연속증가", "배당_수익동반증가",
    "매출_연속성장", "영업이익_연속성장", "순이익_연속성장", "영업CF_연속성장",
    "이익률_개선", "이익률_급개선", "이익률_변동폭",
    "흑자전환", "영업이익률_최근", "영업이익률_전년",
    "최근분기",
    "Q_매출_YoY(%)", "Q_영업이익_YoY(%)", "Q_순이익_YoY(%)",
    "Q_매출_연속YoY성장", "Q_영업이익_연속YoY성장", "Q_순이익_연속YoY성장",
    "TTM_매출_YoY(%)", "TTM_영업이익_YoY(%)", "TTM_순이익_YoY(%)",
    "적정주가_SRIM", "괴리율(%)",
    "종합점수", "성장성_점수", "안정성_점수", "가격_점수",
    "주도주_점수", "우량가치_점수", "고성장_점수", "현금배당_점수", "턴어라운드_점수",
    "TTM_매출", "TTM_영업이익", "TTM_순이익", "TTM_영업CF", "TTM_CAPEX", "TTM_FCF",
    "자본", "부채", "자산총계",
    "전략수",
]

def _load_data() -> pd.DataFrame:
    db_path = str(config.DB_PATH)
    if not os.path.exists(db_path):
        _cache["df"] = pd.DataFrame()
        _cache["mtime"] = 0
        return _cache["df"]
    mtime = os.path.getmtime(db_path)
    if mtime != _cache["mtime"]:
        df = _db.load_dashboard()
        if not df.empty:
            if "종목코드" in df.columns:
                df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
            df = df.replace({np.nan: None})
        _cache["df"] = df
        _cache["mtime"] = mtime
    return _cache["df"]

def _load_prev_data() -> pd.DataFrame:
    db_path = str(config.DB_PATH)
    if not os.path.exists(db_path):
        _prev_cache["df"] = pd.DataFrame()
        _prev_cache["mtime"] = 0
        return _prev_cache["df"]
    mtime = os.path.getmtime(db_path)
    if mtime != _prev_cache["mtime"]:
        df = _db.load_dashboard_prev()
        if not df.empty:
            if "종목코드" in df.columns:
                df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)
            df = df.replace({np.nan: None})
        _prev_cache["df"] = df
        _prev_cache["mtime"] = mtime
    return _prev_cache["df"]

def _safe_val(v):
    if v is None: return None
    if isinstance(v, (np.integer,)): return int(v)
    if isinstance(v, (np.floating,)): return round(float(v), 4) if not np.isnan(v) else None
    return v

def _row_to_dict(row, cols):
    return {c: _safe_val(row.get(c)) for c in cols if c in row.index}

@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/api/stocks")
def api_stocks():
    df = _load_data()
    if df.empty: return jsonify({"total": 0, "page": 1, "size": 50, "items": []})
    screen = request.args.get("screen", "all")
    market = request.args.get("market", "")
    q = request.args.get("q", "").strip()
    order = request.args.get("order", "desc")
    page = max(int(request.args.get("page", 1)), 1)
    size = min(int(request.args.get("size", 50)), 200)
    _default_sort = {
        "leaders": "주도주_점수", "quality_value": "우량가치_점수", "growth_mom": "고성장_점수",
        "cash_div": "현금배당_점수", "turnaround": "턴어라운드_점수", "multi_strategy": "전략수",
    }
    sort_col = request.args.get("sort", _default_sort.get(screen, "종합점수"))
    filtered = df.copy()
    if screen in ["leaders", "quality_value", "growth_mom", "cash_div", "turnaround", "multi_strategy"]:
        filtered = _apply_screen_filter(filtered, screen)
    codes_param = request.args.get("codes", "")
    if codes_param:
        codes = [c.strip().zfill(6) for c in codes_param.split(",") if c.strip()]
        if codes: filtered = filtered[filtered["종목코드"].isin(codes)]
    if market and "시장구분" in filtered.columns:
        filtered = filtered[filtered["시장구분"] == market.upper()]
    if q:
        mask = (filtered["종목명"].str.contains(q, case=False, na=False) | filtered["종목코드"].str.contains(q, case=False, na=False))
        filtered = filtered[mask]
    for key, val in request.args.items():
        if key.startswith("min_") or key.startswith("max_"):
            col = key[4:]
            if col in filtered.columns:
                try:
                    v = float(val)
                    filtered = filtered[filtered[col] >= v] if key.startswith("min_") else filtered[filtered[col] <= v]
                except: pass
    total = len(filtered)
    if sort_col in filtered.columns:
        filtered = filtered.sort_values(sort_col, ascending=(order != "desc"), na_position="last")
    start = (page - 1) * size
    page_df = filtered.iloc[start : start + size]
    available = [c for c in DISPLAY_COLS if c in page_df.columns]
    items = [_row_to_dict(row, available) for _, row in page_df.iterrows()]
    return jsonify({"total": total, "page": page, "size": size, "items": items})

@app.route("/api/stocks/<code>")
def api_stock_detail(code: str):
    df = _load_data()
    if df.empty: return jsonify({"error": "No data"}), 404
    row = df[df["종목코드"] == code.zfill(6)]
    if row.empty: return jsonify({"error": "Stock not found"}), 404
    return jsonify({c: _safe_val(row.iloc[0].get(c)) for c in df.columns})

@app.route("/api/markets/summary")
def api_market_summary():
    df = _load_data()
    if df.empty: return jsonify([])
    results = []
    for mkt in ("KOSPI", "KOSDAQ"):
        sub = df[df["시장구분"] == mkt]
        results.append({
            "market": mkt, "stock_count": len(sub),
            "avg_per": _safe_val(sub["PER"].median()) if "PER" in sub.columns else None,
            "avg_pbr": _safe_val(sub["PBR"].median()) if "PBR" in sub.columns else None,
            "avg_roe": _safe_val(sub["ROE(%)"].median()) if "ROE(%)" in sub.columns else None,
        })
    return jsonify(results)

def _set_progress(stage: str, pct: int):
    _pipeline["stage"], _pipeline["progress"] = stage, pct

def _run_pipeline_tracked(**opts):
    from pipeline import run_pipeline
    from datetime import datetime
    _pipeline["running"], _pipeline["started_at"], _pipeline["error"] = True, datetime.now().isoformat(), None
    try: run_pipeline(progress_callback=_set_progress, **opts)
    except Exception as e: _pipeline["error"] = str(e); log.exception("Pipeline failed")
    finally: _pipeline["running"], _pipeline["finished_at"] = False, datetime.now().isoformat()

@app.route("/api/batch/trigger", methods=["POST"])
def api_batch_trigger():
    if _pipeline["running"]: return jsonify({"status": "already_running"}), 409
    opts = request.get_json(silent=True) or {}
    threading.Thread(target=_run_pipeline_tracked, kwargs=opts, daemon=True).start()
    return jsonify({"status": "triggered"})

@app.route("/api/batch/status")
def api_batch_status():
    return jsonify(_pipeline)

@app.route("/api/batch/changes")
def api_batch_changes():
    curr_df, prev_df = _load_data(), _load_prev_data()
    if curr_df.empty or prev_df.empty: return jsonify({"has_changes": False, "strategies": {}})
    screens = ["all", "leaders", "quality_value", "growth_mom", "cash_div", "turnaround"]
    result = {}
    for s in screens:
        c_f = curr_df if s == "all" else _apply_screen_filter(curr_df.copy(), s)
        p_f = prev_df if s == "all" else _apply_screen_filter(prev_df.copy(), s)
        c_c, p_c = set(c_f["종목코드"]), set(p_f["종목코드"])
        added, removed = c_c - p_c, p_c - c_c
        result[s] = {
            "added": [{"code": c, "name": curr_df[curr_df["종목코드"]==c].iloc[0]["종목명"]} for c in sorted(added)],
            "removed": [{"code": c, "name": prev_df[prev_df["종목코드"]==c].iloc[0]["종목명"]} for c in sorted(removed)],
            "added_count": len(added), "removed_count": len(removed),
        }
    return jsonify({"has_changes": True, "strategies": result})

@app.route("/api/stocks/<code>/financials")
def api_stock_financials(code: str):
    df = _db.load_stock_financials(code)
    if df.empty: return jsonify({"years": [], "series": []})
    df["year"] = df["기준일"].astype(str).str[:4]
    years = sorted(df["year"].unique())
    series = []
    for acc in ["매출액", "영업이익", "당기순이익"]:
        data = []
        for y in years:
            v = df[(df["year"]==y) & (df["계정"]==acc)]["값"]
            data.append(_safe_val(v.iloc[0]) if not v.empty else None)
        series.append({"name": acc, "data": data})
    return jsonify({"years": years, "series": series})

COMPARE_METRICS_META = {
    "PER": {"best": "low"}, "PBR": {"best": "low"}, "PEG": {"best": "low"},
    "ROE(%)": {"best": "high"}, "영업이익률(%)": {"best": "high"}, "FCF수익률(%)": {"best": "high"},
    "매출_CAGR": {"best": "high"}, "영업이익_CAGR": {"best": "high"}, "순이익_CAGR": {"best": "high"},
    "수급강도": {"best": "high"}, "거래대금_20일평균": {"best": "high"}, "F스코어": {"best": "high"},
    "종합점수": {"best": "high"}, "괴리율(%)": {"best": "high"},
}

@app.route("/api/stocks/tab_counts")
def api_stocks_tab_counts():
    df = _load_data()
    screens = ["all", "leaders", "quality_value", "growth_mom", "cash_div", "turnaround", "multi_strategy"]
    if df.empty:
        return jsonify({s: 0 for s in screens})
    result = {"all": len(df)}
    for s in screens[1:]:
        result[s] = len(_apply_screen_filter(df.copy(), s))
    return jsonify(result)

@app.route("/api/stocks/compare")
def api_stocks_compare():
    codes = [c.strip().zfill(6) for c in request.args.get("codes", "").split(",") if c.strip()]
    if not codes: return jsonify({"error": "No codes"}), 400
    df = _load_data()
    matched = df[df["종목코드"].isin(codes)]
    available = [c for c in DISPLAY_COLS if c in matched.columns]
    stocks = [_row_to_dict(row, available) for _, row in matched.iterrows()]
    return jsonify({"stocks": stocks, "metrics_meta": COMPARE_METRICS_META})

@app.route("/api/stocks/<code>/analysis", methods=["GET", "POST"])
def api_stock_analysis(code: str):
    code = code.zfill(6)
    if request.method == "GET":
        row = _db.load_report(code)
        if row is None:
            return jsonify({"error": "No report"}), 404
        mode_hint = "gemini" if "Gemini" in (row.get("model_used") or "") else "claude"
        return jsonify({
            "report_html": row.get("report_html", ""),
            "scores": json.loads(row.get("scores_json") or "{}"),
            "model": row.get("model_used", ""),
            "generated_date": row.get("generated_date", ""),
            "mode": mode_hint,
        })
    # POST – generate new report
    df = _load_data()
    if df.empty:
        return jsonify({"error": "No data"}), 404
    rows = df[df["종목코드"] == code]
    if rows.empty:
        return jsonify({"error": "Stock not found"}), 404
    body = request.get_json(silent=True) or {}
    mode = body.get("mode", "gemini")
    stock = {c: _safe_val(rows.iloc[0].get(c)) for c in df.columns}
    try:
        result = generate_report(stock, mode=mode)
        if "error" not in result:
            _db.save_report(
                code=code,
                name=stock.get("종목명", ""),
                html=result.get("report_html", ""),
                scores_json=json.dumps(result.get("scores", {}), ensure_ascii=False),
                model=result.get("model", ""),
                date=result.get("generated_date", ""),
            )
        return jsonify(result)
    except Exception as e:
        log.exception("Analysis failed for %s", code)
        return jsonify({"error": str(e)}), 500


def _apply_screen_filter(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if name == "leaders":
        mask = (
            (df["시가총액"].fillna(0) >= 100_000_000_000)
            & (df["TTM_순이익"].fillna(0) > 0)
        )
        # 거래대금이 있으면 추가 필터 (없으면 무시)
        if "거래대금_20일평균" in df.columns and df["거래대금_20일평균"].notna().any():
            mask = mask & ((df["거래대금_20일평균"].fillna(0) > 100_000_000) | (df["거래대금_20일평균"].isna()))
        # RS_등급 상위 30% 필터 (데이터 있을 때만 적용, NaN 종목은 통과)
        if "RS_등급" in df.columns and df["RS_등급"].notna().any():
            mask = mask & ((df["RS_등급"].fillna(0) >= 70) | (df["RS_등급"].isna()))
        return df[mask]
    elif name == "quality_value":
        mask = (
            (df["ROE(%)"].fillna(0) >= 10)
            & (df["PEG"].fillna(99) < 1.5)
            & (df["PER"].fillna(0).between(1, 40))
            & (df["F스코어"].fillna(0) >= 4)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )
        return df[mask]
    elif name == "growth_mom":
        mask = (
            ((df["매출_CAGR"].fillna(0) >= 15) | (df["영업이익_CAGR"].fillna(0) >= 15))
            & (df["Q_영업이익_YoY(%)"].fillna(0) > 0)
            & (df["MA20_이격도(%)"].fillna(-999) >= -5)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )
        return df[mask]
    elif name == "cash_div":
        mask = (
            (df["FCF수익률(%)"].fillna(0) >= 3)
            & (df["배당수익률(%)"].fillna(0) >= 1)
            & (df["부채비율(%)"].fillna(999) < 150)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )
        return df[mask]
    elif name == "turnaround":
        base_mask = (
            ((df["흑자전환"].fillna(0) == 1) | (df["이익률_급개선"].fillna(0) == 1))
            & (df["TTM_순이익"].fillna(0) > 0)
            & (df["시가총액"].fillna(0) >= 30_000_000_000)
        )
        # 스마트머니 승률 50%+ OR VCP 신호 보조 (데이터 있을 때만, NaN 종목은 통과)
        if "스마트머니_승률" in df.columns:
            smart_mask = (
                (df["스마트머니_승률"].fillna(0) >= 0.5)
                | (df.get("VCP_신호", pd.Series(0, index=df.index)).fillna(0) == 1)
            )
            no_data_mask = df["스마트머니_승률"].isna()
            mask = base_mask & (smart_mask | no_data_mask)
        else:
            mask = base_mask
        return df[mask]
    elif name == "multi_strategy":
        strats = ["leaders", "quality_value", "growth_mom", "cash_div", "turnaround"]
        counts = pd.Series(0, index=df.index)
        for s in strats:
            idx = _apply_screen_filter(df, s).index
            counts[idx] += 1
        res = df[counts >= 3].copy()
        res["전략수"] = counts[counts >= 3]
        return res
    return df

if __name__ == "__main__":
    app.run(debug=True)
