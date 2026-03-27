"""
US Pipeline orchestrator: runs us_collector → us_screener
"""

import logging
from datetime import datetime

import db as _db

log = logging.getLogger("US_PIPELINE")


def _emit_progress(progress_callback, stage: str, pct: int):
    log.info(stage)
    if not progress_callback:
        return
    try:
        progress_callback(stage, pct)
    except TypeError:
        progress_callback(stage)


def run_pipeline(
    skip_collect: bool = False,
    test_mode: bool = False,
    skip_price_history: bool = False,
    daily_only: bool = False,
    progress_callback=None,
):
    """US 전체 파이프라인: collect → screen

    Args:
        skip_collect: True이면 수집 건너뛰고 스크린만 실행
        test_mode: True이면 AAPL/MSFT/GOOGL 3종목만
        skip_price_history: True이면 주가 히스토리 수집 건너뜀
        daily_only: True이면 일별 데이터만 수집 (재무제표 등 생략)
        progress_callback: 진행상황 콜백 (msg: str)
    """
    start = datetime.now()
    log.info("=== US 파이프라인 시작 (%s) ===", start.strftime("%Y-%m-%d %H:%M:%S"))

    _emit_progress(progress_callback, "US DB 초기화 중", 2)
    _db.init_db()

    # ── Step 1: Collect ──
    if not skip_collect:
        from us_collector import run_full as us_run_full
        _emit_progress(progress_callback, "US 데이터 수집 시작", 8)
        # daily_only 모드에서는 주가 히스토리 수집도 건너뜀
        _skip_ph = skip_price_history or daily_only
        us_run_full(
            test_mode=test_mode,
            skip_price_history=_skip_ph,
            daily_only=daily_only,
        )
    else:
        log.info("수집 단계 건너뜀 (--skip-collect)")
        _emit_progress(progress_callback, "US 수집 단계 건너뜀", 48)

    # ── Step 2: Screen ──
    _emit_progress(progress_callback, "US 스크리닝 시작", 52)
    from us_screener import run as us_screener_run

    def _screen_progress(msg: str):
        pct = 60
        if "데이터 로드" in msg:
            pct = 58
        elif "재무제표 분석" in msg:
            pct = 64
        elif "밸류에이션" in msg:
            pct = 72
        elif "기술적 지표" in msg:
            pct = 80
        elif "전략 점수" in msg:
            pct = 88
        elif "DB 저장" in msg:
            pct = 94
        elif "완료" in msg:
            pct = 100
        _emit_progress(progress_callback, msg, pct)

    us_screener_run(progress_callback=_screen_progress)

    elapsed = datetime.now() - start
    log.info("=== US 파이프라인 완료 (%s) ===", elapsed)
    _emit_progress(progress_callback, f"US 파이프라인 완료 ({elapsed})", 100)


def run_us_update_prices():
    """US 주가만 빠르게 업데이트 → 스크리너 재실행"""
    start = datetime.now()
    log.info("=== US 주가 업데이트 시작 ===")

    _db.init_db()

    from us_collector import collect_us_daily, collect_us_price_history, collect_us_master
    import db as _db2

    master = collect_us_master()
    if master.empty:
        log.error("마스터 데이터 없음")
        return

    tickers = master["ticker"].tolist()
    from datetime import datetime as dt
    today = dt.now().strftime("%Y-%m-%d")

    daily_df = collect_us_daily(tickers)
    if not daily_df.empty:
        _db2.save_df(daily_df, "us_daily", today)
        log.info("us_daily 저장 완료 (%d rows)", len(daily_df))

    ph_df = collect_us_price_history(tickers)
    if not ph_df.empty:
        _db2.save_df(ph_df, "us_price_history", today)
        log.info("us_price_history 저장 완료 (%d rows)", len(ph_df))

    from us_screener import run as us_screener_run
    us_screener_run()

    elapsed = datetime.now() - start
    log.info("=== US 주가 업데이트 완료 (%s) ===", elapsed)


def run_update_prices():
    """호환용 별칭."""
    run_us_update_prices()
