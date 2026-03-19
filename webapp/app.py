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
from analysis.claude_analyzer import (
    generate_report,
    generate_portfolio_report,
    generate_macro_assessment,
    generate_diff_summary,
    compute_correlation_matrix,
    build_stock_analysis_input_hash,
)

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

# ── Analysis task state (per stock code) ──
# { code: {"status": "running"|"done"|"error", "result": {...}, "name": str} }
_analysis_tasks: dict = {}
_analysis_tasks_lock = threading.Lock()

# Columns exposed to the frontend
DISPLAY_COLS = [
    "종목코드", "종목명", "시장구분", "종가", "시가총액",
    "PER", "PBR", "PSR", "PEG", "ROE(%)", "EPS", "BPS",
    "부채비율(%)", "유동비율(%)", "영업이익률(%)", "이익수익률(%)", "FCF수익률(%)",
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
    "영업이익_감속경고", "영업이익_감속폭(pp)", "매출_감속경고", "실적감속_경고",
    "거래량_가격_괴리",
    "GPM_최근(%)", "GPM_전년(%)", "GPM_변화(pp)",
    "ROIC(%)", "ROIC_전년(%)", "ROIC_개선", "퀄리티_턴어라운드",
    "매출_CAGR", "영업이익_CAGR", "순이익_CAGR", "영업CF_CAGR", "FCF_CAGR",
    "DPS_최근", "DPS_CAGR", "배당_연속증가", "배당_수익동반증가",
    "배당성향(%)", "배당_경고신호",
    "매출_연속성장", "영업이익_연속성장", "순이익_연속성장", "영업CF_연속성장",
    "이익률_개선", "이익률_급개선", "이익률_변동폭",
    "흑자전환", "영업이익률_최근", "영업이익률_전년",
    "최근분기",
    "Q_매출_YoY(%)", "Q_영업이익_YoY(%)", "Q_순이익_YoY(%)",
    "Q_매출_연속YoY성장", "Q_영업이익_연속YoY성장", "Q_순이익_연속YoY성장",
    "TTM_매출_YoY(%)", "TTM_영업이익_YoY(%)", "TTM_순이익_YoY(%)",
    "적정주가_SRIM", "괴리율(%)",
    "적정주가_EPV", "적정주가_DDM", "적정주가_FWD", "적정주가_종합",
    "종합괴리율(%)", "밸류_모델수", "SRIM_오메가",
    "과열도", "상승조짐", "타이밍_점수",
    "종합점수", "성장성_점수", "안정성_점수", "가격_점수",
    "주도주_점수", "우량가치_점수", "고성장_점수", "현금배당_점수", "턴어라운드_점수",
    "TTM_매출", "TTM_영업이익", "TTM_순이익", "TTM_영업CF", "TTM_CAPEX", "TTM_FCF",
    "자본", "부채", "자산총계",
    "전략수",
    # Forward 컨센서스 추정치 (애널리스트 커버 종목만 유효)
    "컨센서스_커버리지",
    "Fwd_PER", "Fwd_PBR", "Fwd_EPS", "Fwd_ROE(%)", "Fwd_OPM(%)",
    "Fwd_영업이익_성장률(%)", "Fwd_매출_성장률(%)", "Fwd_순이익_성장률(%)", "Fwd_2yr_영업이익_성장(%)",
    "Fwd_모멘텀_점수",  # ephemeral: forward_covered 탭에서만 동적 계산됨
    "섹터",
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

def _normalize_code(code: str) -> str:
    """종목코드 정규화: 숫자만 있는 코드는 6자리 zero-pad, 알파벳 포함 코드(0072R0 등)는 그대로 반환."""
    code = code.strip()
    if code.isdigit():
        return code.zfill(6)
    return code

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
    try:
        page = max(int(request.args.get("page", 1)), 1)
        size = min(int(request.args.get("size", 50)), 200)
    except (ValueError, TypeError):
        page, size = 1, 50
    _default_sort = {
        "leaders": "주도주_점수", "quality_value": "우량가치_점수", "growth_mom": "고성장_점수",
        "cash_div": "현금배당_점수", "turnaround": "턴어라운드_점수", "multi_strategy": "전략수",
        "forward_covered": "Fwd_모멘텀_점수",
    }
    sort_col = request.args.get("sort", _default_sort.get(screen, "종합점수"))
    filtered = df.copy()
    if screen in ["leaders", "quality_value", "growth_mom", "cash_div", "turnaround", "multi_strategy", "forward_covered"]:
        filtered = _apply_screen_filter(filtered, screen)
    codes_param = request.args.get("codes", "")
    if codes_param:
        codes = [c.strip().zfill(6) for c in codes_param.split(",") if c.strip()]
        if codes: filtered = filtered[filtered["종목코드"].isin(codes)]
    if market and "시장구분" in filtered.columns:
        filtered = filtered[filtered["시장구분"] == market.upper()]
    sectors_raw = request.args.get("sectors", request.args.get("sector", "")).strip()
    if sectors_raw and "섹터" in filtered.columns:
        sector_list = [s.strip() for s in sectors_raw.split(",") if s.strip()]
        if sector_list:
            filtered = filtered[filtered["섹터"].isin(sector_list)]
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
        elif key.startswith("flag_"):
            col = key[5:]
            if col in filtered.columns:
                try:
                    flag_val = int(float(val))
                except (TypeError, ValueError):
                    continue
                if flag_val not in (0, 1):
                    continue
                col_vals = pd.to_numeric(filtered[col], errors="coerce")
                if flag_val == 1:
                    filtered = filtered[col_vals == 1]
                else:
                    filtered = filtered[col_vals != 1]
    # ── 뱃지 필터 (절대 기준) ──────────────────────────────────────────────
    _BADGE_TH = {"관심": ("상승조짐", 40), "조짐": ("상승조짐", 55),
                 "경계": ("과열도", 45), "주의": ("과열도", 60), "과열": ("과열도", 75)}
    badge_filter = request.args.get("badge", "").strip()
    if badge_filter and not filtered.empty:
        badges = [b.strip() for b in badge_filter.split(",") if b.strip()]
        badge_mask = pd.Series(False, index=filtered.index)
        for badge in badges:
            if badge in _BADGE_TH:
                col, th = _BADGE_TH[badge]
                if col in filtered.columns:
                    badge_mask |= filtered[col].notna() & (filtered[col] >= th)
        filtered = filtered[badge_mask]

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
    code = code.zfill(6)
    row = df[df["종목코드"] == code]
    if row.empty and code[-1] != "0":
        # 우선주 → 보통주 프록시: 마지막 자리를 0으로 변환하여 재조회
        common_code = code[:-1] + "0"
        row = df[df["종목코드"] == common_code]
        if not row.empty:
            result = {c: _safe_val(row.iloc[0].get(c)) for c in df.columns}
            result["_proxy"] = True
            result["_proxy_from"] = code
            result["_proxy_name"] = _get_stock_name(code)
            return jsonify(result)
    if row.empty: return jsonify({"error": "Stock not found"}), 404
    return jsonify({c: _safe_val(row.iloc[0].get(c)) for c in df.columns})

@app.route("/api/sectors")
def api_sectors():
    """섹터 목록 + 종목 수 반환"""
    df = _load_data()
    if df.empty or "섹터" not in df.columns:
        return jsonify([])
    counts = df["섹터"].dropna().value_counts().reset_index()
    counts.columns = ["섹터", "count"]
    return jsonify(counts.to_dict(orient="records"))


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
    period = request.args.get("period", "annual")  # "annual" | "quarter"
    code = code.zfill(6)
    df = _db.load_stock_financials(code, period=period)
    # 우선주 → 보통주 재무 데이터 프록시
    if df.empty and code[-1] != "0":
        common_code = code[:-1] + "0"
        df = _db.load_stock_financials(common_code, period=period)
    if df.empty: return jsonify({"years": [], "series": []})
    if period == "quarter":
        # 기준일 YYYYMM → "YYYYQN" 레이블
        df["label"] = df["기준일"].astype(str).apply(lambda x: _month_to_quarter(x))
        labels = sorted(df["label"].unique())
        series = []
        for acc in ["매출액", "영업이익", "당기순이익"]:
            data = []
            for lbl in labels:
                v = df[(df["label"] == lbl) & (df["계정"] == acc)]["값"]
                data.append(_safe_val(v.iloc[0]) if not v.empty else None)
            series.append({"name": acc, "data": data})
        return jsonify({"years": labels, "series": series})
    else:
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

def _month_to_quarter(date_str: str) -> str:
    """'YYYYMM' or 'YYYY-MM-DD' → 'YYYYQN'"""
    try:
        s = date_str.replace("-", "")[:6]
        y, m = int(s[:4]), int(s[4:6])
        q = (m - 1) // 3 + 1
        return f"{y}Q{q}"
    except Exception:
        return date_str[:7]

COMPARE_METRICS_META = {
    "PER": {"best": "low"}, "PBR": {"best": "low"}, "PEG": {"best": "low"},
    "ROE(%)": {"best": "high"}, "영업이익률(%)": {"best": "high"}, "FCF수익률(%)": {"best": "high"},
    "매출_CAGR": {"best": "high"}, "영업이익_CAGR": {"best": "high"}, "순이익_CAGR": {"best": "high"},
    "수급강도": {"best": "high"}, "거래대금_20일평균": {"best": "high"}, "F스코어": {"best": "high"},
    "종합점수": {"best": "high"}, "괴리율(%)": {"best": "high"}, "종합괴리율(%)": {"best": "high"},
    "밸류_모델수": {"best": "high"}, "SRIM_오메가": {"best": "high"},
}

@app.route("/api/stocks/tab_counts")
def api_stocks_tab_counts():
    df = _load_data()
    screens = ["all", "leaders", "quality_value", "growth_mom", "cash_div", "turnaround", "multi_strategy", "forward_covered"]
    if df.empty:
        return jsonify({s: 0 for s in screens})
    result = {"all": len(df)}
    for s in screens[1:]:
        result[s] = len(_apply_screen_filter(df.copy(), s))
    return jsonify(result)

@app.route("/api/info")
def api_info():
    """DB 수집일, 종목 수, 최근분기, 주가 기준일 정보 반환 (데이터 품질 표시용)"""
    import os, datetime
    db_path = str(config.DB_PATH)
    result = {"db_mtime": None, "stock_count": 0, "latest_quarter": None, "days_old": None, "price_date": None, "price_days_old": None}
    if os.path.exists(db_path):
        mtime = os.path.getmtime(db_path)
        dt = datetime.datetime.fromtimestamp(mtime)
        result["db_mtime"] = dt.strftime("%Y-%m-%d %H:%M")
        days_old = (datetime.datetime.now() - dt).days
        result["days_old"] = days_old
    df = _load_data()
    if not df.empty:
        result["stock_count"] = len(df)
        if "최근분기" in df.columns:
            qvals = df["최근분기"].dropna()
            if not qvals.empty:
                result["latest_quarter"] = qvals.mode().iloc[0]
    # daily 테이블에서 최신 기준일 조회
    try:
        import db as _db
        con = _db.get_con()
        row = con.execute("SELECT MAX(기준일) FROM daily").fetchone()
        if row and row[0]:
            price_date_raw = str(row[0])  # e.g. "20260311"
            if len(price_date_raw) == 8:
                price_date_fmt = f"{price_date_raw[:4]}-{price_date_raw[4:6]}-{price_date_raw[6:]}"
            else:
                price_date_fmt = price_date_raw
            result["price_date"] = price_date_fmt
            try:
                pd_dt = datetime.datetime.strptime(price_date_fmt, "%Y-%m-%d")
                result["price_days_old"] = (datetime.datetime.now() - pd_dt).days
            except Exception:
                pass
    except Exception:
        pass
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

@app.route("/api/reports", methods=["GET"])
def api_list_reports():
    reports = _db.list_reports()
    result = {r["종목코드"]: {"model": r["model_used"], "date": r["generated_date"]} for r in reports}
    return jsonify(result)

def _run_analysis_task(code: str, stock: dict, prev_scores_json, input_hash: str):
    """백그라운드 스레드에서 AI 분석 실행."""
    try:
        result = generate_report(stock)
        if "error" not in result:
            diff_html = None
            if prev_scores_json:
                diff_html = generate_diff_summary(
                    prev_scores_json, result.get("scores", {})
                )
                result["diff_html"] = diff_html
            _db.save_report(
                code=code,
                name=stock.get("종목명", ""),
                html=result.get("report_html", ""),
                scores_json=json.dumps(result.get("scores", {}), ensure_ascii=False),
                model=result.get("model", ""),
                date=result.get("generated_date", ""),
                input_hash=input_hash,
                diff_html=diff_html,
            )
        with _analysis_tasks_lock:
            _analysis_tasks[code]["status"] = "done"
            _analysis_tasks[code]["result"] = result
    except Exception as e:
        log.exception("Analysis failed for %s", code)
        err_name = type(e).__name__
        err_str = str(e)
        if "Timeout" in err_name or "timeout" in err_str.lower():
            err_str = "분석 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
        with _analysis_tasks_lock:
            _analysis_tasks[code]["status"] = "error"
            _analysis_tasks[code]["result"] = {"error": err_str}


@app.route("/api/stocks/<code>/analysis", methods=["GET", "POST"])
def api_stock_analysis(code: str):
    code = code.zfill(6)
    df = _load_data()
    rows = df[df["종목코드"] == code] if not df.empty else pd.DataFrame()
    stock = {c: _safe_val(rows.iloc[0].get(c)) for c in df.columns} if not rows.empty else None
    current_input_hash = None
    if stock is not None:
        current_input_hash = build_stock_analysis_input_hash(
            stock, _db.get_analysis_data_version()
        )

    if request.method == "GET":
        row = _db.load_report(code)
        if row is None:
            return jsonify({"error": "No report"}), 404
        stale = bool(stock and row.get("input_hash") != current_input_hash)
        return jsonify({
            "report_html": row.get("report_html", ""),
            "scores": json.loads(row.get("scores_json") or "{}"),
            "model": row.get("model_used", ""),
            "generated_date": row.get("generated_date", ""),
            "diff_html": row.get("diff_html") or None,
            "mode": "claude",
            "stale": stale,
        })
    # POST – 백그라운드에서 보고서 생성 시작
    with _analysis_tasks_lock:
        task = _analysis_tasks.get(code)
        if task and task["status"] == "running":
            return jsonify({"status": "running", "name": task.get("name", "")}), 202

    if df.empty:
        return jsonify({"error": "No data"}), 404
    if rows.empty:
        return jsonify({"error": "Stock not found"}), 404
    name = stock.get("종목명", code)

    prev_row = _db.load_report(code)
    if prev_row and prev_row.get("input_hash") == current_input_hash:
        return jsonify({
            "status": "done",
            "report_html": prev_row.get("report_html", ""),
            "scores": json.loads(prev_row.get("scores_json") or "{}"),
            "model": prev_row.get("model_used", ""),
            "generated_date": prev_row.get("generated_date", ""),
            "diff_html": prev_row.get("diff_html") or None,
            "mode": "claude",
            "cached": True,
        }), 200
    prev_scores_json = prev_row.get("scores_json") if prev_row else None

    with _analysis_tasks_lock:
        _analysis_tasks[code] = {"status": "running", "name": name, "result": None}

    t = threading.Thread(
        target=_run_analysis_task,
        args=(code, stock, prev_scores_json, current_input_hash),
        daemon=True,
    )
    t.start()
    return jsonify({"status": "running", "name": name}), 202


@app.route("/api/stocks/<code>/analysis/status", methods=["GET"])
def api_stock_analysis_status(code: str):
    """백그라운드 분석 태스크 상태 조회."""
    code = code.zfill(6)
    with _analysis_tasks_lock:
        task = _analysis_tasks.get(code)
    if task is None:
        return jsonify({"status": "idle"}), 200
    status = task["status"]
    if status == "running":
        return jsonify({"status": "running", "name": task.get("name", "")}), 200
    # done or error – 결과 반환 후 태스크 정리
    result = task.get("result") or {}
    with _analysis_tasks_lock:
        _analysis_tasks.pop(code, None)
    if status == "error":
        return jsonify({"status": "error", **result}), 200
    return jsonify({"status": "done", **result}), 200


@app.route("/api/stocks/<code>/analysis/history")
def api_stock_analysis_history(code: str):
    """특정 종목의 이전 분석 보고서 목록."""
    code = code.zfill(6)
    return jsonify(_db.list_report_history(code))


@app.route("/api/stocks/analysis/history/<int:history_id>")
def api_stock_analysis_history_detail(history_id: int):
    """히스토리 ID로 이전 보고서 상세 조회."""
    row = _db.load_report_history(history_id)
    if row is None:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "id": row["id"],
        "report_html": row.get("report_html", ""),
        "scores": json.loads(row.get("scores_json") or "{}"),
        "model": row.get("model_used", ""),
        "generated_date": row.get("generated_date", ""),
    })


# ── Portfolio API ──────────────────────────────────────────────────────

def _build_portfolio_response(entries, df, supp):
    """포트폴리오 항목 리스트를 현재가/수익률/비중 계산 결과로 변환."""
    items = []
    for e in entries:
        code = e["종목코드"]
        qty = e.get("수량", 0) or 0
        avg_price = e.get("평균매입가", 0) or 0
        buy_amount = qty * avg_price

        # portfolio 테이블에서 저장된 종목명 우선 사용
        stock_name = e.get("종목명") or code

        # dashboard_result 에서 현재가/섹터 등 조회 (보통주)
        cur_price = None
        sector = None
        per = None
        score = None
        stock_type = None
        if not df.empty:
            row = df[df["종목코드"] == code]
            if not row.empty:
                r = row.iloc[0]
                cur_price = _safe_val(r.get("종가"))
                if stock_name == code:
                    stock_name = r.get("종목명", code)
                sector = r.get("섹터")
                per = _safe_val(r.get("PER"))
                score = _safe_val(r.get("종합점수"))
                stock_type = r.get("종목구분")

        # dashboard_result에 없는 경우 price_supplement(ETF/우선주/리츠) 조회
        if code in supp:
            s = supp[code]
            if cur_price is None:
                cur_price = s.get("현재가")
            if stock_name == code:
                stock_name = s.get("종목명") or code
            if not stock_type:
                stock_type = s.get("종목구분")

        # ETF 메타데이터에서 이름 보충
        if stock_name == code and code in config.ETF_METADATA:
            stock_name = config.ETF_METADATA[code].get("name") or code

        # ETF 메타데이터에서 섹터 보충
        if sector is None and code in config.ETF_METADATA:
            sector = config.ETF_METADATA[code].get("sector")

        # 우선주 섹터 보완: 코드 끝자리를 '0'으로 바꿔 보통주 섹터 참조
        if (not sector) and not df.empty:
            common_code = code[:-1] + "0"
            common_row = df[df["종목코드"] == common_code]
            if not common_row.empty:
                sector = common_row.iloc[0].get("섹터")

        eval_amount = (qty * cur_price) if cur_price else None
        profit = (eval_amount - buy_amount) if eval_amount else None
        profit_pct = ((cur_price / avg_price - 1) * 100) if (cur_price and avg_price) else None

        items.append({
            "종목코드": code,
            "종목명": stock_name,
            "종목구분": stock_type,
            "수량": qty,
            "평균매입가": avg_price,
            "현재가": cur_price,
            "매입금액": buy_amount,
            "평가금액": eval_amount,
            "수익금액": round(profit, 0) if profit is not None else None,
            "수익률": round(profit_pct, 2) if profit_pct is not None else None,
            "비중": 0,  # 아래에서 계산
            "섹터": sector,
            "PER": per,
            "종합점수": score,
            "매입일": e.get("매입일"),
            "메모": e.get("메모", ""),
        })

    # 비중 계산
    total_eval = sum(i["평가금액"] for i in items if i["평가금액"])
    if total_eval > 0:
        for i in items:
            if i["평가금액"]:
                i["비중"] = round(i["평가금액"] / total_eval * 100, 1)
            else:
                i["비중"] = None  # 현재가 없는 종목은 None으로 명시

    total_buy = sum(i["매입금액"] for i in items if i["매입금액"])
    total_profit = (total_eval - total_buy) if total_eval else 0
    total_pct = ((total_eval / total_buy - 1) * 100) if (total_eval and total_buy) else 0

    # 섹터별 분포
    sector_map = {}
    sector_stocks = {}
    for i in items:
        s = i["섹터"] or "기타"
        sector_map.setdefault(s, 0)
        sector_map[s] += (i["평가금액"] or 0)
        sector_stocks.setdefault(s, [])
        sector_stocks[s].append(i.get("종목명") or i.get("종목코드", ""))
    sector_list = sorted(
        [{"섹터": k, "평가금액": v, "비중": round(v / total_eval * 100, 1) if total_eval else 0,
          "종목": sector_stocks.get(k, [])}
         for k, v in sector_map.items()],
        key=lambda x: x["비중"], reverse=True,
    )

    return {
        "items": items,
        "summary": {
            "총매입금액": round(total_buy),
            "총평가금액": round(total_eval) if total_eval else 0,
            "총수익금액": round(total_profit),
            "총수익률": round(total_pct, 2),
            "종목수": len(items),
        },
        "섹터별": sector_list,
    }


@app.route("/api/portfolio", methods=["GET"])
def api_portfolio():
    """포트폴리오 목록 + 현재가/수익률/비중 계산"""
    cash_balance = _db.load_cash()
    entries = _db.load_portfolio()
    if not entries:
        return jsonify({
            "items": [],
            "summary": {"총매입금액": 0, "총평가금액": 0, "총수익금액": 0,
                         "총수익률": 0, "종목수": 0, "예수금": cash_balance},
            "섹터별": [],
            "예수금": cash_balance,
        })

    df = _load_data()
    supp = _db.load_price_supplement()
    res = _build_portfolio_response(entries, df, supp)
    res["예수금"] = cash_balance
    res["summary"]["예수금"] = cash_balance
    res["summary"]["총자산"] = res["summary"]["총평가금액"] + cash_balance
    return jsonify(res)


@app.route("/api/portfolio", methods=["POST"])
def api_portfolio_add():
    """포트폴리오에 종목 추가"""
    body = request.get_json(silent=True) or {}
    code = (body.get("종목코드") or body.get("code", "")).strip()
    if not code:
        return jsonify({"error": "종목코드가 필요합니다."}), 400
    code = _normalize_code(code)
    try:
        qty = float(body.get("수량", body.get("qty", 0)))
        price = float(body.get("평균매입가", body.get("price", 0)))
    except (ValueError, TypeError):
        return jsonify({"error": "수량과 매입가는 숫자여야 합니다."}), 400
    buy_date = body.get("매입일", body.get("date", ""))
    memo = (body.get("메모", body.get("memo", "")) or "")[:200]
    if qty <= 0 or price <= 0:
        return jsonify({"error": "수량과 매입가는 0보다 커야 합니다."}), 400
    # 종목코드 존재 확인
    df = _db.load_dashboard()
    supp = _db.load_price_supplement()
    code_known = (
        (not df.empty and code in df["종목코드"].values)
        or code in supp
        or code in config.ETF_METADATA
    )
    if not code_known:
        return jsonify({"error": f"종목코드 {code}를 DB에서 찾을 수 없습니다. 데이터 수집 후 다시 시도하세요."}), 404
    name = body.get("종목명", body.get("name", ""))
    if not name and not df.empty:
        rows = df[df["종목코드"] == code]
        if not rows.empty:
            name = rows.iloc[0].get("종목명", "")
    if not name and code in supp:
        name = supp[code].get("종목명", "")
    if not name and code in config.ETF_METADATA:
        name = config.ETF_METADATA[code].get("name", "")
    _db.upsert_portfolio_item(code, qty, price, buy_date, memo, name=name, adjust_cash=True)
    return jsonify({"status": "ok", "종목코드": code})


@app.route("/api/portfolio/cash", methods=["GET"])
def api_portfolio_cash_get():
    """예수금 조회"""
    return jsonify({"amount": _db.load_cash()})


@app.route("/api/portfolio/cash", methods=["POST"])
def api_portfolio_cash_post():
    """예수금 저장"""
    body = request.get_json(silent=True) or {}
    amount = body.get("amount", 0)
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({"error": "amount must be a number"}), 400
    _db.save_cash(amount)
    return jsonify({"status": "ok", "amount": amount})


@app.route("/api/portfolio/trade", methods=["POST"])
def api_portfolio_trade():
    """매수/매도 직접 실행: portfolio 갱신 + 예수금 자동 반영"""
    body = request.get_json(silent=True) or {}
    code = (body.get("종목코드") or body.get("code", "")).strip()
    if not code:
        return jsonify({"error": "종목코드가 필요합니다."}), 400
    code = code.zfill(6)
    tx_type = (body.get("거래유형") or body.get("type", "")).upper()
    if tx_type not in ("BUY", "SELL"):
        return jsonify({"error": "거래유형은 BUY 또는 SELL이어야 합니다."}), 400
    try:
        qty = int(float(body.get("수량", body.get("qty", 0))))
        price = float(body.get("단가", body.get("price", 0)))
    except (ValueError, TypeError):
        return jsonify({"error": "수량과 단가는 숫자여야 합니다."}), 400
    if qty <= 0 or price <= 0:
        return jsonify({"error": "수량과 단가는 0보다 커야 합니다."}), 400
    tx_date = body.get("거래일", body.get("date", ""))
    memo = (body.get("메모", body.get("memo", "")) or "")[:200]
    name = body.get("종목명", body.get("name", ""))
    if not name:
        entries = _db.load_portfolio()
        name = next((e.get("종목명", "") or "" for e in entries if e["종목코드"] == code), "")
    result = _db.execute_trade(code, name, tx_type, qty, price, tx_date, memo)
    if "error" in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/api/portfolio/<code>", methods=["PUT"])
def api_portfolio_update(code: str):
    """포트폴리오 항목 수정"""
    code = code.zfill(6)
    body = request.get_json(silent=True) or {}
    try:
        qty = float(body.get("수량", body.get("qty", 0)))
        price = float(body.get("평균매입가", body.get("price", 0)))
    except (ValueError, TypeError):
        return jsonify({"error": "수량과 매입가는 숫자여야 합니다."}), 400
    buy_date = body.get("매입일", body.get("date", ""))
    memo = (body.get("메모", body.get("memo", "")) or "")[:200]
    if qty <= 0 or price <= 0:
        return jsonify({"error": "수량과 매입가는 0보다 커야 합니다."}), 400
    name = body.get("종목명", body.get("name", ""))
    if not name:
        # 기존 저장된 이름 유지
        existing = _db.load_portfolio()
        name = next((e.get("종목명", "") or "" for e in existing if e["종목코드"] == code), "")
    if not name:
        supp = _db.load_price_supplement()
        name = supp.get(code, {}).get("종목명", "")
    if not name and code in config.ETF_METADATA:
        name = config.ETF_METADATA[code].get("name", "")
    _db.upsert_portfolio_item(code, qty, price, buy_date, memo, name=name)
    return jsonify({"status": "ok"})


@app.route("/api/portfolio/<code>", methods=["DELETE"])
def api_portfolio_delete(code: str):
    """포트폴리오에서 종목 삭제"""
    # 종목명 조회 (거래 기록용)
    entries = _db.load_portfolio()
    name = next((e.get("종목명", "") or "" for e in entries if e["종목코드"] == code.zfill(6)), "")
    _db.delete_portfolio_item(code, name=name, adjust_cash=True)
    cash_balance = _db.load_cash()
    return jsonify({"status": "ok", "cash": cash_balance})


# ── Portfolio Management: Performance / Health / Transactions / Rebalance ─

@app.route("/api/portfolio/performance", methods=["GET"])
def api_portfolio_performance():
    """포트폴리오 수익률 추이 (에퀴티 커브)"""
    range_param = request.args.get("range", "3M")
    n_days_map = {"1M": 22, "3M": 66, "6M": 132, "1Y": 250}
    n_days = n_days_map.get(range_param, 66)

    entries = _db.load_portfolio()
    if not entries:
        return jsonify({"dates": [], "portfolio": [], "benchmark": [], "stocks": {}})

    codes = [e["종목코드"] for e in entries]
    qty_map = {e["종목코드"]: int(e.get("수량", 0)) for e in entries}
    name_map = {}
    df_dash = _db.load_dashboard()
    if not df_dash.empty:
        for _, row in df_dash[df_dash["종목코드"].isin(codes)].iterrows():
            name_map[row["종목코드"]] = row.get("종목명", row["종목코드"])

    price_df = _db.load_price_history_multi(codes, n_days)
    kospi_df = _db.load_index_history("KOSPI", n_days)

    if price_df.empty:
        return jsonify({"dates": [], "portfolio": [], "benchmark": [], "stocks": {}})

    # 공통 날짜 기준 (price_df 기준)
    dates = price_df.index.tolist()

    # 포트폴리오 에퀴티 커브 (보유 수량 기반)
    portfolio_values = []
    for date in dates:
        total = 0.0
        for code in codes:
            if code not in price_df.columns:
                continue
            p = price_df.loc[date, code]
            if p and not (p != p):  # NaN 체크
                total += qty_map.get(code, 0) * float(p)
        portfolio_values.append(total)

    base = portfolio_values[0] if portfolio_values and portfolio_values[0] > 0 else 1
    portfolio_returns = [round((v / base - 1) * 100, 2) if base > 0 else 0 for v in portfolio_values]

    # KOSPI 벤치마크
    benchmark_returns = []
    if not kospi_df.empty:
        # price_df 날짜와 매칭
        kospi_series = kospi_df["종가"].reindex(dates, method="ffill")
        k_base = kospi_series.iloc[0] if len(kospi_series) > 0 else 1
        benchmark_returns = [
            round((float(v) / float(k_base) - 1) * 100, 2) if k_base and not (v != v) else 0
            for v in kospi_series
        ]

    # 개별 종목 수익률
    stocks_data = {}
    for code in codes:
        if code not in price_df.columns:
            continue
        series = price_df[code].fillna(method="ffill")
        s_base = series.iloc[0] if len(series) > 0 and series.iloc[0] else 1
        rets = [round((float(v) / float(s_base) - 1) * 100, 2) if s_base else 0 for v in series]
        stocks_data[code] = {"name": name_map.get(code, code), "returns": rets}

    return jsonify({
        "dates": dates,
        "portfolio": portfolio_returns,
        "benchmark": benchmark_returns,
        "stocks": stocks_data,
    })


@app.route("/api/portfolio/health", methods=["GET"])
def api_portfolio_health():
    """포트폴리오 종목 건강 상태 모니터링"""
    entries = _db.load_portfolio()
    if not entries:
        return jsonify({"stocks": [], "alerts": [], "ai_summary": None})

    df_dash = _db.load_dashboard()
    supp = _db.load_price_supplement()
    pf_res = _build_portfolio_response(entries, df_dash, supp)
    pf_items = {item["종목코드"]: item for item in pf_res.get("items", [])}

    # 포트폴리오 AI 분석 요약
    pf_analysis = _db.load_portfolio_analysis()
    ai_summary = None
    if pf_analysis:
        from datetime import datetime
        gen_date = pf_analysis.get("generated_date", "")
        stale = False
        try:
            delta = datetime.now() - datetime.strptime(gen_date[:10], "%Y-%m-%d")
            stale = delta.days > 7
        except Exception:
            pass
        # 포트폴리오 변경 여부 확인
        current_hash = _portfolio_hash(entries)
        hash_mismatch = pf_analysis.get("portfolio_hash") != current_hash
        ai_summary = {
            "generated_date": gen_date,
            "model": pf_analysis.get("model_used", ""),
            "stale": stale,
            "hash_mismatch": hash_mismatch,
        }

    def _status(val, green, yellow):
        if val is None:
            return "na"
        if val >= green:
            return "green"
        if val >= yellow:
            return "yellow"
        return "red"

    stocks = []
    alerts = []
    for entry in entries:
        code = entry["종목코드"]
        pf_item = pf_items.get(code, {})
        ret_pct = pf_item.get("수익률")

        # dashboard_result 메트릭
        rs = None
        score = None
        high52 = None
        fscore = None
        if not df_dash.empty:
            rows = df_dash[df_dash["종목코드"] == code]
            if not rows.empty:
                r = rows.iloc[0]
                rs = _safe_val(r.get("RS_등급"))
                score = _safe_val(r.get("종합점수"))
                high52 = _safe_val(r.get("52주_최고대비(%)"))
                fscore = _safe_val(r.get("F스코어"))

        # 개별 종목 AI 분석
        ai_master_avg = None
        ai_moat = None
        report_row = _db.load_report(code)
        if report_row and report_row.get("scores_json"):
            try:
                sc = json.loads(report_row["scores_json"])
                masters = sc.get("stage7_masters", {})
                if masters:
                    scores_list = [v.get("score", 0) for v in masters.values() if isinstance(v, dict)]
                    if scores_list:
                        ai_master_avg = round(sum(scores_list) / len(scores_list), 1)
                moat_info = sc.get("stage3_moat", {})
                ai_moat = moat_info.get("moat_rating") if isinstance(moat_info, dict) else None
            except Exception:
                pass

        indicators = {
            "수익률": {"value": ret_pct, "status": _status(ret_pct, 0, -10) if ret_pct is not None else "na"},
            "RS등급": {"value": rs, "status": _status(rs, 70, 40) if rs is not None else "na"},
            "종합점수": {"value": score, "status": _status(score, 70, 40) if score is not None else "na"},
            "고가대비": {"value": high52, "status": _status(high52, -10, -25) if high52 is not None else "na"},
            "F스코어": {"value": fscore, "status": _status(fscore, 6, 4) if fscore is not None else "na"},
            "AI거장평균": {"value": ai_master_avg, "status": _status(ai_master_avg, 7, 5) if ai_master_avg is not None else "na"},
            "해자등급": {
                "value": ai_moat,
                "status": "green" if ai_moat in ("Wide", "Narrow") else ("red" if ai_moat == "None" else "na"),
            },
        }

        red_count = sum(1 for v in indicators.values() if v["status"] == "red")
        yellow_count = sum(1 for v in indicators.values() if v["status"] == "yellow")

        stock_info = {
            "종목코드": code,
            "종목명": pf_item.get("종목명", entry.get("종목명", code)),
            "수익률": ret_pct,
            "indicators": indicators,
            "alert_count": red_count,
        }
        stocks.append(stock_info)

        if red_count >= 2 or (red_count >= 1 and yellow_count >= 2):
            issues = [k for k, v in indicators.items() if v["status"] == "red"]
            alerts.append({
                "종목코드": code,
                "종목명": stock_info["종목명"],
                "message": f"주의 지표: {', '.join(issues)}",
                "severity": "danger" if red_count >= 3 else "warning",
            })

    stocks.sort(key=lambda x: x["alert_count"], reverse=True)
    return jsonify({"stocks": stocks, "alerts": alerts, "ai_summary": ai_summary})


@app.route("/api/portfolio/transactions", methods=["GET"])
def api_portfolio_transactions():
    """포트폴리오 거래 이력 조회"""
    code = request.args.get("code")
    limit = int(request.args.get("limit", 100))
    txs = _db.load_transactions(code=code, limit=limit)
    return jsonify({"transactions": txs})


@app.route("/api/portfolio/rebalance", methods=["GET"])
def api_portfolio_rebalance():
    """리밸런싱 가이드 조회"""
    entries = _db.load_portfolio()
    if not entries:
        return jsonify({"mode": "equal", "total_eval": 0, "items": []})

    df_dash = _db.load_dashboard()
    supp = _db.load_price_supplement()
    pf_res = _build_portfolio_response(entries, df_dash, supp)
    pf_items = pf_res.get("items", [])
    total_eval = pf_res.get("summary", {}).get("총평가금액", 0)

    targets_list = _db.load_targets()
    targets_map = {t["종목코드"]: t["목표비중"] for t in targets_list}
    mode = "custom" if targets_map else "equal"

    n = len(pf_items)
    equal_weight = round(100 / n, 2) if n > 0 else 0

    result_items = []
    for item in pf_items:
        code = item["종목코드"]
        cur_weight = item.get("비중", 0) or 0
        target_weight = targets_map.get(code, equal_weight)
        deviation = round(cur_weight - target_weight, 2)
        cur_price = item.get("현재가") or 0
        adjust_shares = 0
        adjust_amount = 0
        direction = "-"
        if abs(deviation) >= 2 and total_eval > 0 and cur_price > 0:
            adjust_amount = round(abs(deviation) / 100 * total_eval)
            adjust_shares = max(1, round(adjust_amount / cur_price))
            direction = "매도" if deviation > 0 else "매수"

        result_items.append({
            "종목코드": code,
            "종목명": item.get("종목명", code),
            "현재가": cur_price,
            "현재비중": round(cur_weight, 2),
            "목표비중": round(target_weight, 2),
            "편차": deviation,
            "조정방향": direction,
            "조정수량": adjust_shares,
            "조정금액": adjust_amount,
        })

    return jsonify({"mode": mode, "total_eval": total_eval, "items": result_items})


@app.route("/api/portfolio/rebalance/targets", methods=["POST"])
def api_portfolio_rebalance_targets():
    """리밸런싱 목표 비중 저장"""
    body = request.get_json(silent=True) or {}
    targets = body.get("targets", [])
    if not isinstance(targets, list):
        return jsonify({"error": "targets must be a list"}), 400
    valid = [t for t in targets if "종목코드" in t and "목표비중" in t]
    _db.save_targets(valid)
    return jsonify({"status": "ok", "saved": len(valid)})


# ── Portfolio AI Analysis ─────────────────────────────────────────────

def _portfolio_hash(entries: list[dict], watchlist_codes: list[str] | None = None,
                    cash: float = 0) -> str:
    """포트폴리오 구성의 해시 생성 (캐시 무효화 판단용)"""
    import hashlib
    parts = sorted(
        f"{e['종목코드']}:{e.get('수량', 0)}:{e.get('평균매입가', 0)}"
        for e in entries
    )
    if watchlist_codes:
        parts.append("wl:" + ",".join(sorted(watchlist_codes)))
    if cash:
        parts.append(f"cash:{int(cash)}")
    return hashlib.md5("|".join(parts).encode()).hexdigest()


@app.route("/api/portfolio/analysis/history", methods=["GET"])
def api_portfolio_analysis_history():
    """포트폴리오 분석 이력 목록 조회"""
    history = _db.load_portfolio_analysis_history()
    return jsonify({"history": history})


@app.route("/api/portfolio/analysis/<int:report_id>", methods=["GET"])
def api_portfolio_analysis_by_id(report_id: int):
    """특정 포트폴리오 분석 보고서 조회"""
    row = _db.load_portfolio_analysis_by_id(report_id)
    if row is None:
        return jsonify({"error": "보고서를 찾을 수 없습니다."}), 404
    return jsonify({
        "report_html": row.get("report_html", ""),
        "scores": json.loads(row.get("scores_json") or "{}"),
        "model": row.get("model_used", ""),
        "generated_date": row.get("generated_date", ""),
        "saved_at": row.get("saved_at", ""),
    })


@app.route("/api/macro/analysis", methods=["GET"])
def api_macro_analysis_get():
    """최신 AI 매크로 분석 결과 조회"""
    row = _db.load_macro_analysis()
    if row is None:
        return jsonify({"error": "No macro analysis"}), 404
    try:
        scores = json.loads(row.get("scores_json") or "{}")
    except (json.JSONDecodeError, TypeError):
        scores = {}
    return jsonify({
        "scores": scores,
        "model": row.get("model_used", ""),
        "generated_date": row.get("generated_date", ""),
    })


@app.route("/api/macro/analysis", methods=["POST"])
def api_macro_analysis_post():
    """AI 매크로 분석 실행 및 저장"""
    try:
        result = generate_macro_assessment()
        scores = result.get("scores", {})
        _db.save_macro_analysis(
            scores_json=json.dumps(scores, ensure_ascii=False),
            model=result.get("model", ""),
            date=result.get("generated_date", ""),
        )
        return jsonify({
            "scores": scores,
            "model": result.get("model", ""),
            "generated_date": result.get("generated_date", ""),
        })
    except Exception as e:
        log.exception("Macro analysis failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/portfolio/analysis", methods=["GET"])
def api_portfolio_analysis_get():
    """포트폴리오 분석 보고서 조회 (캐시)"""
    row = _db.load_portfolio_analysis()
    if row is None:
        return jsonify({"error": "No report"}), 404

    # 캐시 유효성 확인
    entries = _db.load_portfolio()
    current_hash = _portfolio_hash(entries) if entries else ""
    if row.get("portfolio_hash") != current_hash:
        return jsonify({
            "stale": True,
            "report_html": row.get("report_html", ""),
            "scores": json.loads(row.get("scores_json") or "{}"),
            "model": row.get("model_used", ""),
            "generated_date": row.get("generated_date", ""),
            "message": "포트폴리오가 변경되어 재분석이 필요합니다.",
        })

    return jsonify({
        "report_html": row.get("report_html", ""),
        "scores": json.loads(row.get("scores_json") or "{}"),
        "model": row.get("model_used", ""),
        "generated_date": row.get("generated_date", ""),
    })


@app.route("/api/portfolio/analysis", methods=["POST"])
def api_portfolio_analysis_post():
    """포트폴리오 분석 보고서 생성"""
    entries = _db.load_portfolio()
    if not entries:
        return jsonify({"error": "포트폴리오가 비어 있습니다."}), 400

    # 요청 body에서 워치리스트 코드 파싱
    body = request.get_json(silent=True) or {}
    watchlist_codes = [str(c).strip().zfill(6) for c in body.get("watchlist_codes", []) if c]

    # 매크로 컨텍스트: 요청에 포함된 경우 우선 사용, 없으면 DB에서 최신 AI 분석 결과 로드
    macro_context = body.get("macro_context") or None
    if not macro_context:
        macro_row = _db.load_macro_analysis()
        if macro_row and macro_row.get("scores_json"):
            try:
                macro_context = json.loads(macro_row["scores_json"])
            except (json.JSONDecodeError, TypeError):
                macro_context = None

    # 예수금 조회
    cash_balance = _db.load_cash()

    try:
        df = _load_data()
        supp = _db.load_price_supplement()
        portfolio_res = _build_portfolio_response(entries, df, supp)
        portfolio_items = portfolio_res["items"]

        # 종목별 정량 데이터 수집
        stock_data = {}
        for item in portfolio_items:
            code = item["종목코드"]
            if not df.empty:
                rows = df[df["종목코드"] == code]
                if not rows.empty:
                    stock_data[code] = {c: _safe_val(rows.iloc[0].get(c)) for c in df.columns}

        # 기존 AI 분석 보고서 수집
        ai_reports = {}
        for item in portfolio_items:
            code = item["종목코드"]
            report_row = _db.load_report(code)
            if report_row and report_row.get("scores_json"):
                try:
                    ai_reports[code] = json.loads(report_row["scores_json"])
                except (json.JSONDecodeError, TypeError) as e:
                    log.warning("AI report JSON parse failed for %s: %s", code, e)

        # 관심종목 정량 데이터 수집 (포트폴리오에 없는 종목만)
        portfolio_codes = {item["종목코드"] for item in portfolio_items}
        watchlist_data = {}
        if watchlist_codes and not df.empty:
            for code in watchlist_codes:
                if code in portfolio_codes:
                    continue  # 이미 포트폴리오에 있는 종목 제외
                rows = df[df["종목코드"] == code]
                if not rows.empty:
                    watchlist_data[code] = {c: _safe_val(rows.iloc[0].get(c)) for c in df.columns}

        # 종목 간 상관관계 계산
        pf_codes = [item["종목코드"] for item in portfolio_items]
        pf_names = {item["종목코드"]: item.get("종목명", item["종목코드"]) for item in portfolio_items}
        correlation_data = None
        if len(pf_codes) >= 2:
            try:
                correlation_data = compute_correlation_matrix(pf_codes, pf_names)
            except Exception as e:
                log.warning("상관관계 계산 실패: %s", e)

        result = generate_portfolio_report(
            portfolio_items, stock_data, ai_reports,
            watchlist_data=watchlist_data or None,
            correlation_data=correlation_data,
            cash_balance=cash_balance,
            macro_context=macro_context,
        )
        if "error" not in result:
            _db.save_portfolio_analysis(
                html=result.get("report_html", ""),
                scores_json=json.dumps(result.get("scores", {}), ensure_ascii=False),
                portfolio_hash=_portfolio_hash(entries, cash=cash_balance),
                model=result.get("model", ""),
                date=result.get("generated_date", ""),
            )
        return jsonify(result)
    except Exception as e:
        log.exception("Portfolio analysis failed")
        err_name = type(e).__name__
        err_str = str(e)
        if "Timeout" in err_name or "timeout" in err_str.lower():
            return jsonify({"error": "분석 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."}), 504
        return jsonify({"error": err_str}), 500


@app.route("/api/stock-info/<code>", methods=["GET"])
def api_stock_info(code: str):
    """종목코드로 종목명/종목구분/시장구분 조회 (포트폴리오 추가 모달에서 사용)"""
    code = _normalize_code(code)
    # 1) dashboard_result에서 먼저 조회 (가장 최신 정보)
    df = _load_data()
    if not df.empty:
        row = df[df["종목코드"] == code]
        if not row.empty:
            r = row.iloc[0]
            return jsonify({
                "종목코드": code,
                "종목명": r.get("종목명", code),
                "종목구분": r.get("종목구분", "보통주"),
                "시장구분": r.get("시장구분", ""),
                "섹터": r.get("섹터"),
            })
    # 2) price_supplement에서 조회 (ETF/우선주)
    supp = _db.load_price_supplement()
    if code in supp:
        s = supp[code]
        return jsonify({
            "종목코드": code,
            "종목명": s.get("종목명", code),
            "종목구분": s.get("종목구분", "기타"),
            "시장구분": s.get("시장구분", ""),
            "섹터": None,
        })
    # 3) ETF_METADATA에서 조회 (하드코딩된 ETF)
    if code in config.ETF_METADATA:
        meta = config.ETF_METADATA[code]
        return jsonify({
            "종목코드": code,
            "종목명": meta.get("name", code),
            "종목구분": "ETF",
            "시장구분": "",
            "섹터": meta.get("sector"),
        })
    # 4) master 테이블에서 조회
    info = _db.get_stock_info_from_master(code)
    if info:
        return jsonify({
            "종목코드": code,
            "종목명": info["종목명"],
            "종목구분": info["종목구분"],
            "시장구분": info["시장구분"],
            "섹터": None,
        })
    # 5) FDR DataReader로 가격 조회 가능 여부 확인 (ETF/우선주 등 master에 없는 종목 fallback)
    try:
        import FinanceDataReader as fdr
        from datetime import datetime, timedelta
        yesterday = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d")
        df_price = fdr.DataReader(code, yesterday, today)
        if not df_price.empty:
            # 가격 조회 성공 → 종목명은 코드로 대체, 종목구분은 미상
            return jsonify({
                "종목코드": code,
                "종목명": code,  # 종목명 불명 시 코드로 표시
                "종목구분": "기타",
                "시장구분": "",
                "섹터": None,
            })
    except Exception as e:
        log.warning(f"FDR 가격 조회 실패 ({code}): {e}")
    return jsonify({"error": "종목을 찾을 수 없습니다."}), 404


def _get_stock_name(code: str) -> str:
    """종목코드로 종목명 조회 (price_supplement → master 순 fallback)."""
    supp = _db.load_price_supplement()
    if code in supp:
        return supp[code].get("종목명") or code
    info = _db.get_stock_info_from_master(code)
    if info:
        return info.get("종목명") or code
    return code


def _apply_screen_filter(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if name == "leaders":
        mask = (
            (df["시가총액"].fillna(0) >= 100_000_000_000)
            & (df["TTM_순이익"].fillna(0) > 0)
        )
        # RS_등급 상위 20% (기존 70 → 80)
        if "RS_등급" in df.columns and df["RS_등급"].notna().any():
            mask = mask & ((df["RS_등급"].fillna(0) >= 80) | (df["RS_등급"].isna()))
        # 거래대금 5억 이상 (기존 1억 → 5억)
        if "거래대금_20일평균" in df.columns and df["거래대금_20일평균"].notna().any():
            mask = mask & ((df["거래대금_20일평균"].fillna(0) > 500_000_000) | (df["거래대금_20일평균"].isna()))
        # 수급강도 양수 (외국인+기관 순매수)
        if "수급강도" in df.columns:
            mask = mask & (df["수급강도"].fillna(0) > 0)
        return df[mask].sort_values("주도주_점수", ascending=False)
    elif name == "quality_value":
        is_finance = (
            df["종목명"].str.contains("지주|금융|은행|증권|생명|화재", na=False)
            | df["유동비율(%)"].isna()
            | (df["유동비율(%)"].fillna(0) == 0)
        )
        mask_general = (
            (~is_finance)
            & (df["ROIC(%)"].fillna(0) >= 10)
            & (df["F스코어"].fillna(0) >= 5)
            & (df["PEG"].fillna(99) < 1.2)
            & (df["부채비율(%)"].fillna(999) < 120)
            & (df["유동비율(%)"].fillna(0) > 120)
            & (df["순이익_당기양수"].fillna(0) == 1)
            & (df["순이익_전년음수"].fillna(0) == 0)
            & (df["시가총액"].fillna(0) >= 100_000_000_000)
            & (df.get("가치함정_경고", pd.Series(0, index=df.index)).fillna(0) == 0)
        )
        mask_finance = (
            is_finance
            & (df["ROE(%)"].fillna(0) >= 8)
            & (df["PBR"].fillna(99) < 1.5)
            & (df["배당수익률(%)"].fillna(0) >= 2.0)
            & (df["F스코어"].fillna(0) >= 4)
            & (df["순이익_당기양수"].fillna(0) == 1)
            & (df["순이익_전년음수"].fillna(0) == 0)
            & (df["시가총액"].fillna(0) >= 300_000_000_000)
            & (df.get("가치함정_경고", pd.Series(0, index=df.index)).fillna(0) == 0)
        )
        return df[mask_general | mask_finance]
    elif name == "growth_mom":
        mask = (
            (df["매출_CAGR"].fillna(0) >= 10)
            & (df["영업이익_CAGR"].fillna(0) >= 10)
            & (df["Q_영업이익_YoY(%)"].fillna(0) > 0)
            & (df["RS_등급"].fillna(0) >= 50)
            & (df["TTM_영업CF"].fillna(-1) > 0)
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
        )
        return df[mask]
    elif name == "cash_div":
        mask = (
            (df["FCF수익률(%)"].fillna(0) >= 3)
            & (df["배당수익률(%)"].fillna(0) >= 1)
            & (df["부채비율(%)"].fillna(999) < 120)        # 150 → 120
            & (df["시가총액"].fillna(0) >= 50_000_000_000)
            & (df["배당성향(%)"].fillna(999) < 80)          # 신규: Payout Trap 차단
            & (df["현금전환율(%)"].fillna(0) >= 70)         # 신규: 이익→현금 품질
            & (df["배당_연속증가"].fillna(0) >= 2)          # 2년 이상 연속 배당 증가 (이력 축적에 따라 상향 가능)
        )
        return df[mask]
    elif name == "turnaround":
        base_mask = (
            ((df["흑자전환"].fillna(0) == 1) | (df["이익률_급개선"].fillna(0) == 1))
            & (df["TTM_순이익"].fillna(0) > 0)
            & (df["TTM_영업CF"].fillna(-1) > 0)        # [신규] 실제 현금 창출 검증
            & (df["Q_매출_YoY(%)"].fillna(0) > -15)    # [신규] 매출 급감(-15%↓) 방지
            & (df["시가총액"].fillna(0) >= 30_000_000_000)
            & (df["이자보상배율"].fillna(0) > 1.5)      # [신규] 이자 상환 능력
        )
        # 스마트머니 승률 50%+ OR VCP 신호 보조 (데이터 있을 때만, NaN 종목은 통과)
        if "스마트머니_승률" in df.columns:
            smart_mask = (
                (df["스마트머니_승률"].fillna(0) >= 50)
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
    elif name == "forward_covered":
        # PER 또는 ROE 값이 실제로 존재하는 종목만 (행만 있고 모두 NaN인 종목 제외)
        fwd_key = "Fwd_PER" if "Fwd_PER" in df.columns else None
        fwd_key2 = "Fwd_ROE(%)" if "Fwd_ROE(%)" in df.columns else None
        if fwd_key is None and fwd_key2 is None:
            return df.iloc[0:0]
        mask = pd.Series(False, index=df.index)
        if fwd_key:
            mask |= df[fwd_key].notna()
        if fwd_key2:
            mask |= df[fwd_key2].notna()
        covered = df[mask].copy()
        if covered.empty:
            return covered
        def _rank_w(col, asc=True):
            if col not in covered.columns or covered[col].isna().all():
                return pd.Series(50.0, index=covered.index)
            s = covered[col]
            filled = s.fillna(s.min() - 1 if asc else s.max() + 1)
            return filled.rank(pct=True) * 100 if asc else (1 - filled.rank(pct=True)) * 100
        covered["Fwd_모멘텀_점수"] = (
            # Forward 성장성/수익성/밸류에이션 (60%)
            _rank_w("Fwd_영업이익_성장률(%)", True) * 0.25
            + _rank_w("Fwd_ROE(%)",              True) * 0.15
            + _rank_w("Fwd_PER",                False) * 0.10
            + _rank_w("Fwd_OPM(%)",              True) * 0.05
            + _rank_w("Fwd_2yr_영업이익_성장(%)", True) * 0.05
            # 안정성 (25%)
            + _rank_w("이자보상배율",             True) * 0.10
            + _rank_w("부채비율(%)",              False) * 0.10
            + _rank_w("F스코어",                 True) * 0.05
            # 배당성 (15%)
            + _rank_w("배당수익률(%)",            True) * 0.10
            + _rank_w("DPS_CAGR",               True) * 0.05
        )
        return covered
    return df

if __name__ == "__main__":
    app.run(debug=True)
