"""
Pipeline orchestrator: runs quant_collector_enhanced → quant_screener
and saves dashboard results to SQLite alongside the Excel outputs.
"""

import logging
from datetime import datetime

import db as _db
from quant_collector_enhanced import run_full as collector_run, test_crawling, collect_daily, get_biz_day
from quant_screener import (
    load_table,
    preprocess_indicators,
    detect_unit_multiplier,
    analyze_all,
    calc_valuation,
    calc_technical_indicators,
    calc_investor_strength,
    calc_overheat_score,
    calc_breakout_signal,
    calc_strategy_scores,
    apply_leaders_screen,
    apply_quality_value_screen,
    apply_growth_mom_screen,
    apply_cash_div_screen,
    apply_turnaround_screen,
    save_to_excel,
    DATA_DIR,
)

log = logging.getLogger("PIPELINE")


def run_update_prices():
    """주가(종가/시총)만 빠르게 업데이트하고 스크리너를 재실행한다.
    FnGuide 크롤링 없이 fdr로 시세만 갱신하므로 빠르다."""
    start = datetime.now()
    log.info("=== 주가 업데이트 시작 ===")

    _db.init_db()
    biz_day = get_biz_day()
    log.info("기준 영업일: %s", biz_day)

    # 1) 최신 종가 수집
    daily = collect_daily(biz_day)
    _db.save_df(daily, "daily", biz_day)
    log.info("daily 저장 완료 (%d rows)", len(daily))

    # 2) 기존 데이터 로드 + 스크리너 재실행
    log.info("스크리너 재실행 중...")
    master = load_table("master")
    fs = load_table("financial_statements")
    ind = load_table("indicators")
    shares = load_table("shares")
    price_hist = load_table("price_history")
    inv = load_table("investor_trading")
    index_hist = load_table("index_history")

    ind = preprocess_indicators(ind)
    multiplier = detect_unit_multiplier(ind)

    # DPS 이력 누적 보존: indicators는 배치마다 덮어써서 과거 데이터가 유실될 수 있으므로
    # DPS 행만 추출하여 dividend_history에 UPSERT (종목코드+기준일 기준)
    dps_rows = ind[ind["지표구분"] == "DPS"][["종목코드", "기준일", "값"]].copy()
    dps_rows = dps_rows.rename(columns={"값": "DPS"})
    _db.save_dividend_history(dps_rows, biz_day)

    div_hist = _db.load_dividend_history()
    anal_df = analyze_all(fs, ind, div_hist_df=div_hist)

    full_df = calc_valuation(daily, anal_df, multiplier, shares)
    full_df = calc_technical_indicators(
        full_df, price_hist,
        index_hist=index_hist if not index_hist.empty else None,
        master=master if not master.empty else None,
    )
    full_df = full_df.merge(
        calc_investor_strength(inv, daily, price_hist=price_hist if not price_hist.empty else None),
        on="종목코드", how="left",
    )

    if not master.empty and "시장구분" in master.columns:
        master_info = master[["종목코드", "시장구분", "종목구분"]].drop_duplicates("종목코드")
        full_df = full_df.merge(master_info, on="종목코드", how="left")

    if not shares.empty and "섹터" in shares.columns:
        sector_map = shares[["종목코드", "섹터"]].drop_duplicates("종목코드")
        full_df = full_df.merge(sector_map, on="종목코드", how="left")
        sector_map_dict = sector_map.dropna(subset=["섹터"]).set_index("종목코드")["섹터"].to_dict()
        mask = full_df["섹터"].isna() | (full_df["섹터"] == "")
        if mask.any():
            full_df.loc[mask, "섹터"] = full_df.loc[mask, "종목코드"].apply(
                lambda c: sector_map_dict.get(c[:-1] + "0")
            )

    full_df = calc_overheat_score(full_df)
    full_df = calc_breakout_signal(full_df)
    full_df = calc_strategy_scores(full_df)

    _db.save_dashboard(full_df)
    log.info("Dashboard 저장 완료 (%d rows)", len(full_df))

    elapsed = datetime.now() - start
    log.info("=== 주가 업데이트 완료 (%s) ===", elapsed)


def run_pipeline(skip_collect: bool = False, test_mode: bool = False, skip_price_history: bool = False, skip_investor: bool = False, daily_only: bool = False, progress_callback=None):
    """Run full pipeline: collect data then screen.

    Args:
        skip_collect: If True, skip collection and only run screener
            (useful when data already exists in DB).
        test_mode: If True, only collect 3 sample stocks.
        skip_price_history: If True, skip price history collection (faster, but no technical indicators).
        skip_investor: If True, skip investor trading collection (외국인/기관 매매동향).
        daily_only: If True, collect only daily-changing data (daily, price_history, index_history,
            investor_trading) then run screener. Skips FnGuide crawling (fs/indicators/shares).
        progress_callback: Optional callable(stage: str, pct: int) to track progress.
    """
    def _progress(stage: str, pct: int):
        """Call progress callback if provided."""
        if progress_callback:
            progress_callback(stage, pct)

    start = datetime.now()
    log.info("Pipeline started at %s", start.strftime("%Y-%m-%d %H:%M:%S"))

    _progress("DB 초기화 중", 2)
    _db.init_db()

    # ── Step 1: Collect ──
    if not skip_collect:
        _progress("데이터 수집 준비 중", 5)
        if test_mode:
            log.info("Running collector in TEST mode (3 stocks)...")
            collector_run(test_mode=True, skip_price_history=skip_price_history, skip_investor=skip_investor, daily_only=daily_only, progress_callback=_progress)
        else:
            log.info("Running %s collector...", "daily-only" if daily_only else "full")
            collector_run(skip_price_history=skip_price_history, skip_investor=skip_investor, daily_only=daily_only, progress_callback=_progress)
    else:
        log.info("Skipping collection (--skip-collect)")
        _progress("수집 단계 건너뜀", 48)

    # ── Step 2: Screen & Analyse ──
    log.info("Running screener...")
    _progress("데이터 로드 중", 52)
    master = load_table("master")
    daily = load_table("daily")
    fs = load_table("financial_statements")
    ind = load_table("indicators")
    shares = load_table("shares")
    price_hist = load_table("price_history")
    inv = load_table("investor_trading")
    index_hist = load_table("index_history")

    if daily.empty:
        log.error("daily data not found in DB – cannot run screener")
        return

    _progress("지표 전처리 중", 62)
    ind = preprocess_indicators(ind)
    multiplier = detect_unit_multiplier(ind)

    # DPS 이력 누적 보존
    dps_rows = ind[ind["지표구분"] == "DPS"][["종목코드", "기준일", "값"]].copy()
    dps_rows = dps_rows.rename(columns={"값": "DPS"})
    _db.save_dividend_history(dps_rows, get_biz_day())

    div_hist = _db.load_dividend_history()
    anal_df = analyze_all(fs, ind, progress_callback=_progress, div_hist_df=div_hist)

    _progress("밸류에이션 계산 중", 70)
    full_df = calc_valuation(daily, anal_df, multiplier, shares)

    # 기술적 지표 (주가 히스토리 기반)
    _progress("기술적 지표 계산 중", 78)
    full_df = calc_technical_indicators(
        full_df, price_hist,
        index_hist=index_hist if not index_hist.empty else None,
        master=master if not master.empty else None,
    )

    # 수급 강도 (투자자별 매매동향 기반)
    full_df = full_df.merge(
        calc_investor_strength(inv, daily, price_hist=price_hist if not price_hist.empty else None),
        on="종목코드", how="left",
    )

    # Merge market/sector info from master
    if not master.empty and "시장구분" in master.columns:
        master_info = master[["종목코드", "시장구분", "종목구분"]].drop_duplicates("종목코드")
        full_df = full_df.merge(master_info, on="종목코드", how="left")

    # Merge FICS 섹터 from shares
    if not shares.empty and "섹터" in shares.columns:
        sector_map = shares[["종목코드", "섹터"]].drop_duplicates("종목코드")
        full_df = full_df.merge(sector_map, on="종목코드", how="left")

        # 우선주 섹터 보완: 섹터가 없는 종목(우선주 등)은 코드 끝자리를 '0'으로 바꿔 보통주 섹터 참조
        sector_map_dict = sector_map.dropna(subset=["섹터"]).set_index("종목코드")["섹터"].to_dict()
        mask = full_df["섹터"].isna() | (full_df["섹터"] == "")
        if mask.any():
            full_df.loc[mask, "섹터"] = full_df.loc[mask, "종목코드"].apply(
                lambda c: sector_map_dict.get(c[:-1] + "0")
            )

    # 전략별 종합점수 사전 계산 (기술적 지표 이후, DB 저장 전)
    _progress("전략 점수 계산 중", 84)
    full_df = calc_overheat_score(full_df)
    full_df = calc_breakout_signal(full_df)
    full_df = calc_strategy_scores(full_df)

    # ── Save dashboard to DB ──
    _progress("데이터베이스 저장 중", 88)
    _db.save_dashboard(full_df)
    log.info("Dashboard saved to DB (%d rows)", len(full_df))

    # ── Save Excel outputs (same as original screener) ──
    _progress("엑셀 파일 저장 중", 90)
    save_to_excel(
        full_df.sort_values("종합점수", ascending=False),
        DATA_DIR / "quant_all_stocks.xlsx", "전체종목",
    )

    leaders_df = apply_leaders_screen(full_df)
    save_to_excel(leaders_df, DATA_DIR / "quant_leaders.xlsx", "주도주")

    _progress("엑셀 파일 저장 중", 93)
    quality_df = apply_quality_value_screen(full_df)
    save_to_excel(quality_df, DATA_DIR / "quant_quality_value.xlsx", "우량가치")

    growth_df = apply_growth_mom_screen(full_df)
    save_to_excel(growth_df, DATA_DIR / "quant_growth_mom.xlsx", "고성장모멘텀")

    _progress("엑셀 파일 저장 중", 96)
    cashdiv_df = apply_cash_div_screen(full_df)
    save_to_excel(cashdiv_df, DATA_DIR / "quant_cash_div.xlsx", "현금배당")

    turnaround_df = apply_turnaround_screen(full_df)
    save_to_excel(turnaround_df, DATA_DIR / "quant_turnaround.xlsx", "턴어라운드")

    _progress("완료", 100)

    elapsed = datetime.now() - start
    log.info(
        "Pipeline finished in %s — %d total, %d leaders, %d quality_value, "
        "%d growth_mom, %d cash_div, %d turnaround",
        elapsed, len(full_df), len(leaders_df), len(quality_df),
        len(growth_df), len(cashdiv_df), len(turnaround_df),
    )
