# =========================================================
# quant_collector.py  —  한국 주식 퀀트 데이터 수집기
# ---------------------------------------------------------
# 수집 항목:
#   1) 종목 마스터 (KRX 전종목)
#   2) 일별 시세 + 펀더멘털 (종가, 시가총액, EPS, BPS, DPS)
#   3) 재무제표 (IS / BS / CF — 연간 + 분기)
#   4) 핵심 지표 (Financial Highlight + 재무비율)
#   5) 주식수 (발행주식수, 자사주, 유통주식수)
#
# 출력: ./data/ 폴더에 CSV 파일로 저장 (DB 불필요)
# 
# 실행:
#   테스트 모드: python quant_collector.py --test
#   전체 실행:   python quant_collector.py
# =========================================================

import os
import re
import sys
import logging
import warnings
import argparse
from datetime import datetime, date, timedelta  # timedelta 추가
from io import StringIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

import numpy as np
import pandas as pd
import requests
import FinanceDataReader as fdr
from pykrx import stock
from tqdm import tqdm

import config

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("COLLECTOR")

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
DATA_DIR = config.DATA_DIR

MAX_WORKERS = 15          # FnGuide 동시 요청 수 (너무 높으면 차단됨)
REQUEST_TIMEOUT = 12      # 초

# 테스트용 샘플 종목 (대표 종목 선정)
TEST_TICKERS = [
    "005930",  # 삼성전자
    "035720",  # 카카오
    "000660",  # SK하이닉스
]

# 전역 세션 (TCP 커넥션 재사용)
_session = requests.Session()
_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
})


# ═════════════════════════════════════════════
# 공통 유틸리티
# ═════════════════════════════════════════════

def get_biz_day() -> str:
    """최근 영업일 (YYYYMMDD) - 서버 통신 없이 로컬 계산"""
    d = datetime.now()
    # 월=0 ... 금=4, 토=5, 일=6
    if d.weekday() == 5:    # 토요일이면
        d = d - timedelta(days=1)  # 금요일로
    elif d.weekday() == 6:  # 일요일이면
        d = d - timedelta(days=2)  # 금요일로
    
    # 평일 오전 9시 이전이면(장 시작 전), 전날 데이터를 보기 위해 하루 뺌 (선택사항, 일단은 당일 기준)
    return d.strftime("%Y%m%d")


def load_tables(url: str) -> list:
    """FnGuide HTML 테이블 파싱 (인코딩 자동 감지)"""
    try:
        r = _session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception:
        return []

    for enc in ("cp949", "euc-kr", "utf-8"):
        try:
            html = r.content.decode(enc, errors="strict")
            return pd.read_html(StringIO(html), displayed_only=False)
        except Exception:
            continue
    # fallback
    try:
        html = r.content.decode("cp949", errors="replace")
        return pd.read_html(StringIO(html), displayed_only=False)
    except Exception:
        return []


def safe_float(x):
    """안전한 float 변환"""
    if x is None:
        return None
    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
        return None
    try:
        s = str(x).replace(",", "").strip()
        if s in ("", "-", "N/A", "nan", "None"):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def safe_int(x):
    v = safe_float(x)
    return int(v) if v is not None else None


def parse_period(col_name: str):
    """컬럼명에서 기준일 파싱 (2023/12, 2024.03 등)"""
    s = str(col_name)
    is_estimate = "(E)" in s
    m = re.search(r"(\d{4})[\./](\d{2})", s)
    if not m:
        return None, is_estimate
    d = pd.to_datetime(f"{m.group(1)}-{m.group(2)}") + pd.offsets.MonthEnd()
    return d, is_estimate


def normalize_market(m: str) -> str:
    if not m:
        return "ETC"
    m = m.upper()
    if "KOSPI" in m:
        return "KOSPI"
    if "KOSDAQ" in m:
        return "KOSDAQ"
    if "KONEX" in m:
        return "KONEX"
    return "ETC"


# ═════════════════════════════════════════════
# 1. 종목 마스터
# ═════════════════════════════════════════════

def collect_master() -> pd.DataFrame:
    """KRX 전종목 마스터 수집 (실패 시 DB 캐시 사용)"""
    log.info("📘 종목 마스터 수집 중...")
    df = None
    try:
        df = fdr.StockListing("KRX")[["Code", "Name", "Market"]]
        df.columns = ["종목코드", "종목명", "시장구분"]
        df["시장구분"] = df["시장구분"].apply(normalize_market)
    except Exception as e:
        log.warning(f"  ⚠️ fdr.StockListing 실패: {e} — DB 캐시에서 로드합니다.")
        import db as _db
        df = _db.load_latest("master")
        if df.empty:
            raise RuntimeError("KRX 마스터 수집 실패 및 DB 캐시 없음") from e
        log.info(f"  → DB 캐시 사용: {len(df)}개 종목")
        return df

    name = df["종목명"].fillna("")
    code = df["종목코드"].fillna("")
    # ETF 판별: 종목명에 ETF 운용사 브랜드명 포함 여부로 구분
    _etf_pattern = (
        r"KODEX|TIGER|KBSTAR|ARIRANG|KOSEF|HANARO|SMART|FOCUS|TREX|SOL|ACE|PLUS|"
        r"파워|마이티|히어로즈|네비게이터|킨덱스|퀀트"
    )
    is_etf = name.str.contains(_etf_pattern, case=False, regex=True, na=False)
    df["종목구분"] = np.select(
        [
            name.str.contains("스팩", na=False),
            is_etf,
            name.str.endswith("리츠", na=False),
            code.str[-1] != "0",
        ],
        ["스팩", "ETF", "리츠", "우선주"],
        default="보통주",
    )
    log.info(f"  → 전체 {len(df)}개 종목 ({df['종목구분'].value_counts().to_dict()})")
    return df


# ═════════════════════════════════════════════
# 2. 일별 시세 + 펀더멘털
# ═════════════════════════════════════════════

def _fetch_naver_all_prices() -> pd.DataFrame:
    """네이버 금융 시가총액 페이지에서 전종목 종가/시총을 벌크로 수집한다.
    fdr.StockListing('KRX') 실패 시 fallback으로 사용. (~10초)"""
    import requests
    from bs4 import BeautifulSoup

    headers = {"User-Agent": "Mozilla/5.0"}
    all_items = []

    for sosok in [0, 1]:  # 0=KOSPI, 1=KOSDAQ
        # 첫 페이지에서 총 페이지 수 확인
        url = f"https://finance.naver.com/sise/sise_market_sum.nhn?sosok={sosok}&page=1"
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "euc-kr"
        soup = BeautifulSoup(resp.text, "html.parser")
        paging = soup.select("td.pgRR a")
        last_page = int(paging[0]["href"].split("page=")[-1]) if paging else 1

        for page in range(1, last_page + 1):
            url = f"https://finance.naver.com/sise/sise_market_sum.nhn?sosok={sosok}&page={page}"
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "euc-kr"
            soup = BeautifulSoup(resp.text, "html.parser")

            for row in soup.select("table.type_2 tbody tr"):
                tds = row.select("td")
                if len(tds) < 7:
                    continue
                name_tag = tds[1].select_one("a")
                if not name_tag:
                    continue
                code = name_tag["href"].split("=")[-1]
                price_text = tds[2].text.strip().replace(",", "")
                try:
                    price = float(price_text)
                except (ValueError, TypeError):
                    continue
                all_items.append({
                    "종목코드": code,
                    "종가": price,
                })

    log.info(f"  → 네이버 금융에서 {len(all_items)}개 종목 시세 수집 완료")
    return pd.DataFrame(all_items)


def _update_prices_via_naver(df: pd.DataFrame) -> pd.DataFrame:
    """fdr.StockListing 실패 시, 네이버 금융 벌크 크롤링으로
    DB 캐시 DataFrame의 종가/시가총액을 최신화한다."""
    naver_df = _fetch_naver_all_prices()
    if naver_df.empty:
        log.warning("  ⚠️ 네이버 금융 크롤링도 실패 — DB 캐시 그대로 사용")
        return df

    price_map = naver_df.set_index("종목코드")["종가"].to_dict()

    updated = 0
    for idx, row in df.iterrows():
        code = row["종목코드"]
        if code in price_map:
            df.at[idx, "종가"] = price_map[code]
            # 시총은 종가 × 상장주식수로 재계산 (네이버 시총 단위가 불안정하므로)
            if "시가총액" in df.columns and "상장주식수" in df.columns:
                shares = safe_float(row.get("상장주식수"))
                if shares:
                    df.at[idx, "시가총액"] = price_map[code] * shares
            updated += 1

    log.info(f"  → 네이버 종가 업데이트: {updated}/{len(df)}개 종목")
    return df


def collect_daily(biz_day: str) -> pd.DataFrame:
    """FinanceDataReader를 이용한 시세 + 펀더멘털 수집"""
    # biz_day 포맷 변경 (YYYYMMDD -> YYYY-MM-DD) 필요 시 변환, 
    # 하지만 fdr.StockListing('KRX')는 '현재' 기준 가장 최신 데이터를 가져옵니다.
    # 과거 특정일 데이터를 가져오려면 복잡해지므로, 스크리너 목적상 '최신' 데이터로 진행합니다.
    
    log.info(f"📊 일별 시세 수집 (최신 기준)...")

    # 1. KRX 전종목 리스팅 (가격, 시가총액, 거래량 등 포함됨)
    # fdr.StockListing('KRX')는 종가, 시가총액, 거래량 등을 기본 포함합니다.
    try:
        df_krx = fdr.StockListing('KRX')
    except Exception as e:
        log.warning(f"  ⚠️ fdr.StockListing 실패: {e} — 네이버 금융에서 최신 종가 수집 시도...")
        import db as _db
        df_krx = _db.load_latest("daily")
        if df_krx.empty:
            raise RuntimeError("KRX 시세 수집 실패 및 DB 캐시 없음") from e
        # 네이버 금융 벌크 크롤링으로 종가/시총 최신화
        df_krx = _update_prices_via_naver(df_krx)
        log.info(f"  → DB 캐시 + 네이버 최신 종가 업데이트: {len(df_krx)}개 종목")
        df_krx["기준일"] = biz_day
        return df_krx
    
    # 컬럼 이름이 한글/영문 혼용될 수 있어 정리
    # (최신 fdr 버전에 따라 컬럼명이 다를 수 있으니 확인 후 매핑)
    # 일반적인 fdr KRX 컬럼: Code, Name, Close, Marcap, Stocks, Market ...
    
    # 필요한 컬럼만 선택 및 리네임
    rename_map = {
        'Code': '종목코드',
        'Name': '종목명',
        'Close': '종가',
        'Marcap': '시가총액',
        'Stocks': '상장주식수' # 필요하다면
    }
    # 실제 존재하는 컬럼만 변경
    avail_cols = [c for c in rename_map if c in df_krx.columns]
    df_krx = df_krx[avail_cols].rename(columns=rename_map)
    
    # 2. 펀더멘털(PER, PBR 등)은 fdr.StockListing('KRX-DESC') 등에 일부 있으나, 
    #    정확한 EPS/BPS/DPS는 KRX 정보시스템에서 별도로 긁어야 하는데 fdr로는 한계가 있을 수 있음.
    #    하지만 스크리너 로직상 PER/PBR은 '종가 / EPS' 등으로 재계산하므로 
    #    EPS, BPS 데이터가 필수입니다.
    #    
    #    대안: pykrx가 안되므로, fdr의 'KRX' 데이터에 있는 PER, PBR을 쓰거나
    #    FnGuide 크롤링 단계(fetch_indicators)에서 가져온 데이터를 믿고 가야 합니다.
    #    
    #    여기서는 일단 '시세(종가, 시총)'는 fdr로 확실히 챙기고,
    #    EPS/BPS 컬럼은 비워둔 뒤 나중에 채우거나 0으로 처리합니다.
    #    (quant_screener.py에서 EPS/BPS가 없으면 PER/PBR 계산을 못하지만, 
    #     FnGuide 데이터가 있으면 거기서 보완 가능할 수도 있음. 
    #     하지만 현재 구조는 daily.csv에 EPS/BPS가 있어야 함.)

    # **중요**: pykrx가 계속 터지므로, 일단 안정적인 fdr 데이터로 '종가/시가총액'만이라도 확보하여 저장합니다.
    # EPS, BPS, 배당금은 0 또는 None으로 채워서 에러를 방지합니다.
    
    for c in ["EPS", "BPS", "주당배당금"]:
        df_krx[c] = None
        
    # 기준일 추가
    df_krx["기준일"] = biz_day
    
    log.info(f"  → {len(df_krx)}개 종목 시세 수집 완료 (fdr 사용)")
    return df_krx


def collect_price_history(tickers: list[str], days: int = 400) -> pd.DataFrame:
    """FinanceDataReader + pykrx 병용으로 최근 N거래일 주가/거래량 히스토리 수집.

    1차: fdr (동시 8개, 네이버 금융)
    2차: 1차 실패 종목을 pykrx (KRX API)로 재시도

    Args:
        tickers: 종목코드 리스트
        days: 수집 기간 (캘린더 일 기준, 기본 400일 ≈ 270+ 거래일, RS_250d 계산에 필요)

    Returns:
        DataFrame with 종목코드, 날짜, 시가, 고가, 저가, 종가, 거래량, 거래대금
    """
    import socket
    import requests

    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    log.info(f"📈 주가 히스토리 수집 ({start_str} ~ {end_str}, {len(tickers)}개 종목)...")

    # ── 네트워크 타임아웃 강제 적용 (fdr 내부 요청이 무한 대기하는 것 방지) ──
    socket.setdefaulttimeout(30)

    _original_session_request = requests.Session.request

    def _patched_request(self, method, url, **kwargs):
        kwargs.setdefault("timeout", 30)
        return _original_session_request(self, method, url, **kwargs)

    requests.Session.request = _patched_request

    # ── 1차: fdr 수집 ──
    all_rows = []
    failed_tickers = []

    def _fetch_one_fdr(ticker: str) -> list[dict]:
        rows = []
        try:
            df = fdr.DataReader(ticker, start_str, end_str)
            if df is None or df.empty:
                log.debug(f"주가 히스토리(fdr): {ticker} → 데이터 없음")
                return rows
            for dt, r in df.iterrows():
                dt_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
                rows.append({
                    "종목코드": ticker,
                    "날짜": dt_str,
                    "시가": safe_float(r.get("Open")),
                    "고가": safe_float(r.get("High")),
                    "저가": safe_float(r.get("Low")),
                    "종가": safe_float(r.get("Close")),
                    "거래량": safe_float(r.get("Volume")),
                    "거래대금": safe_float(r.get("Amount") if "Amount" in r.index else None),
                })
        except Exception as e:
            log.debug(f"주가 히스토리(fdr): {ticker} → {type(e).__name__}")
            return None  # None = 실패 (빈 리스트와 구분)
        return rows

    TIMEOUT_PER_TICKER = 30
    TOTAL_TIMEOUT = max(600, len(tickers) // 8 * TIMEOUT_PER_TICKER)

    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_fetch_one_fdr, t): t for t in tickers}
            done_count = 0
            try:
                for f in tqdm(as_completed(futures, timeout=TOTAL_TIMEOUT), total=len(futures), desc="주가 히스토리(1차 fdr)", ncols=100):
                    ticker = futures[f]
                    done_count += 1
                    try:
                        result = f.result(timeout=TIMEOUT_PER_TICKER)
                        if result is None:
                            failed_tickers.append(ticker)
                        else:
                            all_rows.extend(result)
                    except (TimeoutError, FuturesTimeoutError):
                        failed_tickers.append(ticker)
                    except Exception:
                        failed_tickers.append(ticker)
            except (TimeoutError, FuturesTimeoutError):
                pending_tickers = [futures[f] for f in futures if not f.done()]
                failed_tickers.extend(pending_tickers)
                for f in futures:
                    f.cancel()
    finally:
        requests.Session.request = _original_session_request

    fdr_success = len(tickers) - len(failed_tickers)
    log.info(f"  → 1차(fdr): {fdr_success}/{len(tickers)}건 성공, {len(failed_tickers)}건 실패")

    # ── 2차: pykrx로 실패 종목 재시도 ──
    if failed_tickers:
        import time
        log.info(f"📈 2차 수집(pykrx): 실패 {len(failed_tickers)}개 종목 재시도...")
        # pykrx 날짜 형식: YYYYMMDD
        pykrx_start = start_str.replace("-", "")
        pykrx_end = end_str.replace("-", "")
        retry_success = 0

        for ticker in tqdm(failed_tickers, desc="주가 히스토리(2차 pykrx)", ncols=100):
            try:
                df = stock.get_market_ohlcv_by_date(pykrx_start, pykrx_end, ticker)
                if df is not None and not df.empty:
                    for dt, r in df.iterrows():
                        all_rows.append({
                            "종목코드": ticker,
                            "날짜": dt.strftime("%Y-%m-%d"),
                            "시가": safe_float(r.get("시가")),
                            "고가": safe_float(r.get("고가")),
                            "저가": safe_float(r.get("저가")),
                            "종가": safe_float(r.get("종가")),
                            "거래량": safe_float(r.get("거래량")),
                            "거래대금": safe_float(r.get("거래대금")),
                        })
                    retry_success += 1
                time.sleep(0.5)
            except Exception as e:
                log.debug(f"주가 히스토리(pykrx): {ticker} → {type(e).__name__}")

        final_fail = len(failed_tickers) - retry_success
        log.info(f"  → 2차(pykrx): {retry_success}/{len(failed_tickers)}건 추가 성공, 최종 누락 {final_fail}건")

    log.info(f"  → 주가 히스토리 총 {len(all_rows)}건 수집 완료")
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ═════════════════════════════════════════════
# 2-a2. KOSPI / KOSDAQ 지수 히스토리 (RS Rating용)
# ═════════════════════════════════════════════

def collect_index_history(days: int = 400) -> pd.DataFrame:
    """KOSPI/KOSDAQ 지수 히스토리 수집 (1차 FDR, 2차 yfinance fallback).

    RS_250d 계산에 250거래일이 필요하므로 400 캘린더일(≈270 거래일) 기준으로 수집.

    Returns:
        DataFrame with 지수코드, 날짜, 종가
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    log.info(f"📊 지수 히스토리 수집 ({start_str} ~ {end_str}, KOSPI/KOSDAQ)...")

    indices = {"KOSPI": "KS11", "KOSDAQ": "KQ11"}
    all_rows = []
    failed_indices = []

    # ── 1차: FinanceDataReader ──
    for idx_name, fdr_code in indices.items():
        try:
            df = fdr.DataReader(fdr_code, start_str, end_str)
            if df is None or df.empty:
                log.warning(f"  → {idx_name} FDR 데이터 없음")
                failed_indices.append(idx_name)
                continue
            for dt, r in df.iterrows():
                dt_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
                close_val = safe_float(r.get("Close") if "Close" in r.index else r.iloc[3])
                if close_val is not None:
                    all_rows.append({"지수코드": idx_name, "날짜": dt_str, "종가": close_val})
            log.info(f"  → {idx_name}(FDR): {sum(1 for r in all_rows if r['지수코드'] == idx_name)}건")
        except Exception as e:
            log.warning(f"  → {idx_name} FDR 수집 실패: {type(e).__name__}: {e}")
            failed_indices.append(idx_name)

    # ── 2차: yfinance fallback (FDR 실패 시) ──
    if failed_indices:
        import yfinance as yf
        yf_indices = {"KOSPI": "^KS11", "KOSDAQ": "^KQ11"}
        log.info(f"📈 지수 히스토리 yfinance fallback: {failed_indices}")
        for idx_name in failed_indices:
            yf_ticker = yf_indices.get(idx_name)
            if not yf_ticker:
                continue
            try:
                df = yf.download(yf_ticker, start=start_str, end=end_str, progress=False)
                if df is None or df.empty:
                    log.warning(f"  → {idx_name} yfinance 데이터 없음")
                    continue
                # yfinance MultiIndex columns: ('Close', '^KS11')
                close_col = df["Close"]
                if isinstance(close_col, pd.DataFrame):
                    close_col = close_col.iloc[:, 0]
                for dt, close_val in close_col.items():
                    dt_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
                    val = safe_float(close_val)
                    if val is not None:
                        all_rows.append({"지수코드": idx_name, "날짜": dt_str, "종가": val})
                log.info(f"  → {idx_name}(yfinance): {sum(1 for r in all_rows if r['지수코드'] == idx_name)}건")
            except Exception as e:
                log.warning(f"  → {idx_name} yfinance 수집 실패: {type(e).__name__}: {e}")

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ═════════════════════════════════════════════
# 2-b. 외국인/기관 투자자 매매 동향
# ═════════════════════════════════════════════

def _fetch_investor_trading_naver(ticker: str, days: int) -> list[dict]:
    """Naver Finance frgn.nhn에서 외국인/기관 순매수 데이터 수집.

    종목별로 데이터 테이블 인덱스가 다를 수 있어(2 또는 3) 동적으로 탐색합니다.
    """
    from io import StringIO as _StringIO
    import time as _time

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.naver.com/",
    })

    cutoff = date.today() - timedelta(days=days)
    rows = []
    seen_dates = set()
    page = 1

    while True:
        try:
            url = f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page={page}"
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()

            # 차단 여부 확인
            if "b_panel" in r.text or "spam" in r.text.lower() or "captcha" in r.text.lower():
                log.warning(f"투자자 매매동향: {ticker} -> 네이버 접속 차단 감지")
                break

            html = r.content.decode("euc-kr", errors="replace")
            tables = pd.read_html(_StringIO(html), displayed_only=False)
            if len(tables) < 3:
                break

            # 데이터가 있는 테이블(보통 2번 또는 3번) 찾기
            target_table = None
            start_row = 0  # 매 페이지마다 초기화
            for idx in [2, 3]:
                if idx >= len(tables): continue
                t = tables[idx]
                if t.shape[1] >= 7 and t.shape[0] > 1:
                    # 첫 번째 데이터 행에서 날짜 형식 확인
                    for row_idx in range(min(5, len(t))):
                        val = str(t.iloc[row_idx, 0])
                        if re.search(r"\d{4}[\./]\d{2}[\./]\d{2}", val):
                            target_table = t
                            start_row = row_idx
                            break
                if target_table is not None: break

            if target_table is None:
                break

            for _, row in target_table.iloc[start_row:].iterrows():
                raw_date = row.iloc[0]
                if pd.isna(raw_date):
                    continue
                try:
                    dt_str = str(raw_date).replace(".", "-")
                    dt = date.fromisoformat(dt_str)
                except ValueError:
                    continue
                if dt < cutoff:
                    return rows

                dt_str_key = dt.strftime("%Y-%m-%d")
                if dt_str_key in seen_dates:
                    continue
                seen_dates.add(dt_str_key)

                rows.append({
                    "종목코드": ticker,
                    "날짜": dt_str_key,
                    "외국인순매수": safe_float(row.iloc[6]),
                    "기관순매수": safe_float(row.iloc[5]),
                    "개인순매수": None,
                })
        except Exception as e:
            log.debug(f"투자자 매매동향 naver: {ticker} p{page} → {type(e).__name__}: {e}")
            break

        _time.sleep(0.1) # 지연 시간 증가
        page += 1
        if page > 15: # 최대 페이지 수 약간 축소
            break

    return rows


def collect_investor_trading(tickers: list[str], days: int = 60) -> pd.DataFrame:
    """Naver Finance에서 외국인/기관 순매수 데이터 수집 (최근 N일).

    Args:
        tickers: 종목코드 리스트
        days: 수집 기간 (캘린더 일 기준, 기본 60일)

    Returns:
        DataFrame with 종목코드, 날짜, 외국인순매수, 기관순매수, 개인순매수
    """
    INVESTOR_WORKERS = 4  # 동시 요청 수 감소 (차단 방지)

    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    log.info(f"투자자 매매동향 수집 ({start_date} ~ {end_date}, {len(tickers)}개 종목, Naver)...")

    all_rows = []
    failed_tickers = []

    with ThreadPoolExecutor(max_workers=INVESTOR_WORKERS) as pool:
        futures = {pool.submit(_fetch_investor_trading_naver, ticker, days): ticker for ticker in tickers}
        for future in tqdm(as_completed(futures), total=len(futures), desc="투자자 매매동향", ncols=100):
            ticker = futures[future]
            try:
                rows = future.result(timeout=120) # 타임아웃 넉넉히
                if not rows:
                    failed_tickers.append(ticker)
                else:
                    all_rows.extend(rows)
            except Exception as e:
                log.debug(f"투자자 매매동향: {ticker} -> {type(e).__name__}: {e}")
                failed_tickers.append(ticker)

    success = len(tickers) - len(failed_tickers)
    log.info(f"  -> 투자자 매매동향: {success}/{len(tickers)}건 성공, {len(failed_tickers)}건 실패")
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ═════════════════════════════════════════════
# 3. 재무제표 (FnGuide)
# ═════════════════════════════════════════════

def _classify_fs_table(t: pd.DataFrame) -> str | None:
    """테이블 1열 키워드로 IS/BS/CF 판별"""
    if t.shape[0] < 2 or t.shape[1] < 2:
        return None
    text = " ".join(t.iloc[:, 0].astype(str).tolist())
    # CF를 먼저 체크 (CF 테이블에도 "자산"/"부채" 등 하위계정이 포함되므로)
    if "영업활동" in text and "투자활동" in text:
        return "CF"
    if "자산총계" in text or ("자산" in text and "부채" in text and "자본" in text
                             and "영업이익" not in text):
        return "BS"
    if "매출액" in text or "영업수익" in text or "영업이익" in text:
        return "IS"
    return None


def _melt_fs(df: pd.DataFrame, ticker: str, freq: str) -> list[dict]:
    """재무제표 테이블 → 세로형 dict 리스트"""
    if df is None or df.empty:
        return []
    df = df.loc[:, ~df.columns.str.contains("전년동기")]
    df = df.rename(columns={df.columns[0]: "계정"})
    df["계정"] = df["계정"].astype(str).str.replace(
        "계산에 참여한 계정 펼치기", "", regex=False
    ).str.strip()
    df = df.drop_duplicates("계정", keep="first")

    try:
        melted = pd.melt(df, id_vars="계정", var_name="기간", value_name="값")
    except Exception:
        return []

    rows = []
    for _, r in melted.iterrows():
        biz_date, is_est = parse_period(r["기간"])
        if biz_date is None:
            continue
        val = safe_float(r["값"])
        if val is None:
            continue
        rows.append({
            "종목코드": ticker,
            "기준일": biz_date,
            "계정": r["계정"],
            "주기": freq,
            "값": val,
            "추정치": is_est,
        })
    return rows


def fetch_fs(ticker: str) -> list[dict]:
    """종목 1개의 재무제표 수집"""
    url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}"
    tables = load_tables(url)
    if len(tables) < 2:
        return []

    # 테이블 분류: IS/BS/CF 각각 연간(y) → 분기(q) 순서로 채움
    slots = {k: {"y": None, "q": None} for k in ("IS", "BS", "CF")}
    for t in tables:
        label = _classify_fs_table(t)
        if label is None:
            continue
        if slots[label]["y"] is None:
            slots[label]["y"] = t
        elif slots[label]["q"] is None:
            slots[label]["q"] = t

    rows = []
    for fs_type in ("IS", "BS", "CF"):
        for freq_key, freq_label in (("y", "y"), ("q", "q")):
            rows += _melt_fs(slots[fs_type][freq_key], ticker, freq_label)
    return rows


# ═════════════════════════════════════════════
# 4. 핵심 지표 (Financial Highlight + 재무비율)
# ═════════════════════════════════════════════

def _extract_indicator_rows(
    df: pd.DataFrame, ticker: str, source: str, freq: str = "y"
) -> list[dict]:
    """지표 테이블 → dict 리스트"""
    if df is None or df.empty or df.shape[1] < 2:
        return []

    # MultiIndex 컬럼 평탄화 (충돌 방지: 동일 inner 컬럼명은 outer level로 구분)
    if isinstance(df.columns, pd.MultiIndex):
        new_cols, seen = [], {}
        for col_tuple in df.columns:
            inner = str(col_tuple[-1])
            if inner not in seen:
                seen[inner] = 0
                new_cols.append(inner)
            else:
                outer = str(col_tuple[0]) if len(col_tuple) > 1 else ""
                new_cols.append(f"{outer}_{inner}" if outer else f"{inner}_{seen[inner]}")
                seen[inner] += 1
        df.columns = new_cols

    # 첫 번째 컬럼을 '계정'으로 이름 변경
    df = df.rename(columns={df.columns[0]: "계정"})

    # '계정' 컬럼이 문자열인지 확인 및 공백 제거
    df["계정"] = df["계정"].astype(str).str.strip()

    try:
        melted = pd.melt(df, id_vars="계정", var_name="기간", value_name="값")
    except Exception:
        return []

    rows = []
    for _, r in melted.iterrows():
        biz_date, is_est = parse_period(r["기간"])
        if biz_date is None:
            continue
        account = str(r["계정"]).strip()
        if not account or account.lower() in ("nan", "none"):
            continue
        val = safe_float(r["값"])
        # 추정치 행: HIGHLIGHT → FORWARD_Y / FORWARD_Q, 그 외 → {source}_E
        if is_est and "HIGHLIGHT" in source:
            ind_type = f"FORWARD_{freq.upper()}"
        elif is_est:
            ind_type = f"{source}_E"
        else:
            ind_type = source
        rows.append({
            "종목코드": ticker,
            "기준일": biz_date,
            "지표구분": ind_type,
            "계정": account,
            "값": val,
        })
    return rows


def fetch_indicators(ticker: str) -> list[dict]:
    """Financial Highlight + 재무비율 + 배당금 수집"""
    rows = []

    # ── (A) 메인 페이지: Financial Highlight + DPS ──
    url_main = (
        f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
        f"?pGB=1&gicode=A{ticker}&stkGb=701"
    )
    main_tables = load_tables(url_main)

    highlight_annual, highlight_quarterly = False, False
    for t in main_tables:
        if not isinstance(t, pd.DataFrame) or t.shape[0] < 2 or t.shape[1] < 2:
            continue

        col1_list = [str(x) for x in t.iloc[:, 0].values]
        col1_text = " ".join(col1_list)

        # Financial Highlight 테이블 식별
        has_rev = "매출액" in col1_text or "영업수익" in col1_text
        has_roe = "ROE" in col1_text
        has_op = "영업이익" in col1_text
        if not (has_rev or has_roe or has_op):
            continue

        # 컬럼 헤더 월(month) 패턴으로 연간 vs 분기 구분
        cols = t.columns
        col_strs = [str(c[-1]) for c in cols] if isinstance(cols, pd.MultiIndex) else [str(c) for c in cols]
        months = [int(m.group(1)) for cs in col_strs if (m := re.search(r'\d{4}[./](\d{2})', cs))]

        is_annual = bool(months) and all(mo == 12 for mo in months)
        is_quarterly = bool(months) and any(mo in (3, 6, 9) for mo in months)

        if is_annual and not highlight_annual:
            rows += _extract_indicator_rows(t.copy(), ticker, "HIGHLIGHT", freq="y")
            highlight_annual = True
        elif is_quarterly and not highlight_quarterly:
            rows += _extract_indicator_rows(t.copy(), ticker, "HIGHLIGHT", freq="q")
            highlight_quarterly = True

        if highlight_annual and highlight_quarterly:
            break

    # DPS (배당금) — Highlight 테이블에서 별도 추출
    for t in main_tables:
        if not isinstance(t, pd.DataFrame) or t.shape[0] < 2:
            continue
        
        # [수정] 시리즈 변환 시에도 안전하게 처리
        col1 = t.iloc[:, 0].astype(str)
        
        dps_idx = col1[col1.str.contains("배당금|DPS", regex=True)].index
        if len(dps_idx) == 0:
            continue
        row_data = t.iloc[dps_idx[0]]
        for col_name, val in row_data.items():
            if col_name == t.columns[0]:
                continue
            
            # 1) Only use Annual columns for historical DPS
            col_str = str(col_name)
            if "Annual" not in col_str:
                continue
                
            # 2) Exclude Estimates/Preliminary values which are often incomplete
            if "(E)" in col_str or "(P)" in col_str:
                continue

            biz_date, _ = parse_period(col_name)
            if biz_date is None:
                continue

            # 3) Exclude future dates (결산 미완료 예비치 방지)
            if biz_date > pd.Timestamp.today():
                continue

            # 4) Exclude non-year-end months (분기 DPS 방지: 12월 결산 외 특수 결산은 허용)
            # Annual 컬럼이어도 분기 날짜(3,6,9월)가 생성될 경우 제외
            if biz_date.month in (3, 6, 9):
                continue

            v = safe_float(val)
            if v is not None:
                rows.append({
                    "종목코드": ticker,
                    "기준일": biz_date,
                    "지표구분": "DPS",
                    "계정": "주당배당금",
                    "값": v,
                })
        break

    # ── (B) 재무비율 페이지 ──
    url_ratio = (
        f"https://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp"
        f"?pGB=1&gicode=A{ticker}&stkGb=701"
    )
    ratio_tables = load_tables(url_ratio)
    if len(ratio_tables) >= 1:
        rows += _extract_indicator_rows(ratio_tables[0], ticker, "RATIO_Y")
    if len(ratio_tables) >= 2:
        rows += _extract_indicator_rows(ratio_tables[1], ticker, "RATIO_Q")

    return rows


# ═════════════════════════════════════════════
# 5. 주식수 (FnGuide)
# ═════════════════════════════════════════════

def fetch_shares(ticker: str) -> dict | None:
    """발행주식수, 자사주, 유통주식수, FICS 섹터 수집"""
    from bs4 import BeautifulSoup
    url = (
        f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
        f"?pGB=1&gicode=A{ticker}&stkGb=701"
    )
    # HTML 원문 저장 (테이블 파싱 + 섹터 파싱 공용)
    try:
        r = _session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        raw = r.content
    except Exception:
        return None

    # ── 테이블 파싱 (pd.read_html) ──
    tables = []
    for enc in ("cp949", "euc-kr", "utf-8"):
        try:
            html = raw.decode(enc, errors="strict")
            tables = pd.read_html(StringIO(html), displayed_only=False)
            break
        except Exception:
            continue
    if not tables:
        try:
            html = raw.decode("cp949", errors="replace")
            tables = pd.read_html(StringIO(html), displayed_only=False)
        except Exception:
            pass

    # 발행주식수
    issued = 0
    try:
        issued = safe_int(str(tables[0].iloc[6, 1]).split("/")[0])
    except Exception:
        pass

    # 자사주
    treasury = 0
    for t in tables:
        if isinstance(t, pd.DataFrame) and "보통주" in t.columns:
            try:
                val = safe_int(t["보통주"].iloc[4])
                if val is not None:
                    treasury = val
                    break
            except Exception:
                pass

    float_shares = max((issued or 0) - treasury, 0)

    # ── FICS 섹터 파싱 (span.stxt.stxt2) ──
    sector = None
    try:
        soup = BeautifulSoup(raw, "html.parser")
        fics_span = soup.find("span", class_="stxt2")
        if fics_span:
            text = fics_span.get_text(strip=True)
            # "FICS  반도체 및 관련장비" → "반도체 및 관련장비"
            sector = text.replace("FICS", "").strip()
    except Exception:
        pass

    return {
        "종목코드": ticker,
        "기준일": date.today().isoformat(),
        "발행주식수": issued,
        "자사주": treasury,
        "유통주식수": float_shares,
        "섹터": sector,
    }


# ═════════════════════════════════════════════
# 병렬 수집 래퍼
# ═════════════════════════════════════════════

def parallel_collect(func, tickers: list, desc: str, per_ticker_timeout: int = 60) -> list:
    """ThreadPoolExecutor 래퍼 — 결과를 리스트로 반환"""
    results = []
    total_timeout = len(tickers) * per_ticker_timeout / MAX_WORKERS + 120
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(func, t): t for t in tickers}
        done_count = 0
        try:
            for f in tqdm(as_completed(futures, timeout=total_timeout), total=len(tickers), desc=desc):
                ticker = futures[f]
                done_count += 1
                try:
                    res = f.result(timeout=per_ticker_timeout)
                    if res:
                        if isinstance(res, list):
                            results.extend(res)
                        else:
                            results.append(res)
                except TimeoutError:
                    log.warning(f"{desc}: {ticker} → 타임아웃 ({per_ticker_timeout}초 초과, 건너뜀)")
                except Exception as e:
                    log.warning(f"{desc}: {ticker} → 오류: {type(e).__name__}")
        except TimeoutError:
            pending = len(futures) - done_count
            log.warning(f"⚠️ {desc} 전체 타임아웃 — 완료 {done_count}/{len(futures)}, 미완료 {pending}건 건너뜀")
            for f in futures:
                f.cancel()
    return results


# ═════════════════════════════════════════════
# 테스트 함수
# ═════════════════════════════════════════════

def test_crawling():
    """3개 샘플 종목으로 크롤링 테스트"""
    log.info("=" * 60)
    log.info("🧪 테스트 모드 시작 (샘플 3개 종목)")
    log.info("=" * 60)
    
    # 마스터 데이터 로드 (종목명 확인용)
    master = collect_master()
    
    test_results = {}
    
    for ticker in TEST_TICKERS:
        stock_name = master[master["종목코드"] == ticker]["종목명"].values
        stock_name = stock_name[0] if len(stock_name) > 0 else "Unknown"
        
        log.info(f"\n{'='*60}")
        log.info(f"📌 [{ticker}] {stock_name} 테스트 중...")
        log.info(f"{'='*60}")
        
        test_results[ticker] = {
            "종목명": stock_name,
            "재무제표": False,
            "지표": False,
            "주식수": False,
        }
        
        # 1) 재무제표
        try:
            fs_data = fetch_fs(ticker)
            if fs_data and len(fs_data) > 0:
                test_results[ticker]["재무제표"] = True
                log.info(f"  ✅ 재무제표: {len(fs_data)}건 수집 성공")
                # 샘플 출력
                sample = pd.DataFrame(fs_data[:5])
                print(sample.to_string(index=False))
            else:
                log.warning(f"  ⚠️  재무제표: 데이터 없음")
        except Exception as e:
            log.error(f"  ❌ 재무제표 오류: {e}")
        
        # 2) 지표
        try:
            ind_data = fetch_indicators(ticker)
            if ind_data and len(ind_data) > 0:
                test_results[ticker]["지표"] = True
                log.info(f"  ✅ 핵심지표: {len(ind_data)}건 수집 성공")
                # 샘플 출력
                sample = pd.DataFrame(ind_data[:5])
                print(sample.to_string(index=False))
            else:
                log.warning(f"  ⚠️  핵심지표: 데이터 없음")
        except Exception as e:
            log.error(f"  ❌ 핵심지표 오류: {e}")
        
        # 3) 주식수
        try:
            share_data = fetch_shares(ticker)
            if share_data:
                test_results[ticker]["주식수"] = True
                log.info(f"  ✅ 주식수: 수집 성공")
                print(f"     발행주식수: {share_data['발행주식수']:,}주")
                print(f"     자사주: {share_data['자사주']:,}주")
                print(f"     유통주식수: {share_data['유통주식수']:,}주")
            else:
                log.warning(f"  ⚠️  주식수: 데이터 없음")
        except Exception as e:
            log.error(f"  ❌ 주식수 오류: {e}")
    
    # 결과 요약
    log.info("\n" + "=" * 60)
    log.info("📊 테스트 결과 요약")
    log.info("=" * 60)
    
    summary_df = pd.DataFrame(test_results).T
    print(summary_df.to_string())
    
    # 성공률 계산
    total_tests = len(TEST_TICKERS) * 3
    passed_tests = sum([
        sum([
            test_results[t]["재무제표"],
            test_results[t]["지표"],
            test_results[t]["주식수"]
        ])
        for t in TEST_TICKERS
    ])
    
    success_rate = (passed_tests / total_tests) * 100
    log.info(f"\n✅ 성공률: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
    
    if success_rate >= 80:
        log.info("🎉 테스트 통과! 전체 수집을 진행할 수 있습니다.")
        return True
    else:
        log.warning("⚠️  테스트 성공률이 낮습니다. 네트워크나 FnGuide 접근을 확인하세요.")
        return False


# ═════════════════════════════════════════════
# 메인 파이프라인
# ═════════════════════════════════════════════

def run_full(test_mode: bool = False, skip_price_history: bool = False, skip_investor: bool = False, daily_only: bool = False, progress_callback=None):
    """전체 데이터 수집 (SQLite DB 저장 + 이어하기)

    Args:
        test_mode: True이면 TEST_TICKERS(3개)만 수집
        skip_price_history: True이면 주가 히스토리 수집 건너뜀 (기술적 지표 계산 불가)
        skip_investor: True이면 투자자 매매동향 수집 건너뜀 (외국인/기관/개인 순매수)
        daily_only: True이면 매일 변하는 데이터만 수집
                    (daily, price_history, index_history, investor_trading)
                    재무제표/핵심지표/주식수는 건너뜀, 마스터는 DB 캐시만 사용
        progress_callback: Optional callable(stage: str, pct: int) for progress tracking
    """
    def _progress(stage: str, pct: int):
        """Call progress callback if provided."""
        if progress_callback:
            progress_callback(stage, pct)

    import db as _db
    _db.init_db()

    start = datetime.now()
    biz_day = get_biz_day()  # 예: '20260206'
    log.info(f"📅 기준 영업일: {biz_day}")
    if test_mode:
        log.info(f"🧪 테스트 모드: {len(TEST_TICKERS)}개 종목만 수집")

    _progress("마스터/일별시세 수집 중", 5)
    # ── 1) 마스터 ──
    if daily_only:
        # daily_only 모드: 마스터는 DB 캐시만 사용 (재수집 안 함)
        master = _db.load_latest("master")
        if master.empty:
            raise RuntimeError("daily_only 모드에서 master DB 캐시가 없습니다. 먼저 전체 수집을 한 번 실행하세요.")
        log.info(f"📂 [daily_only] master DB 캐시 사용 ({len(master)}개 종목)")
    elif _db.table_has_data("master", biz_day):
        log.info("📂 master 데이터가 DB에 있어 로드합니다.")
        master = _db.load_latest("master")
    else:
        master = collect_master()
        _db.save_df(master, "master", biz_day)

    # ── 2) 일별 시세 ──
    if _db.table_has_data("daily", biz_day):
        log.info("📂 daily 데이터가 DB에 있어 로드합니다.")
        daily = _db.load_latest("daily")
    else:
        daily = collect_daily(biz_day)
        _db.save_df(daily, "daily", biz_day)

    # 보통주만 추출 (FnGuide 크롤링 대상)
    targets = master.loc[
        (master["종목구분"] == "보통주") & (master["시장구분"].isin(["KOSPI", "KOSDAQ"])),
        "종목코드",
    ].tolist()

    # 종목코드 포맷 통일 (005930) + 비정상 코드 제거
    targets = [f"{x:06d}" if isinstance(x, (int, float)) else str(x) for x in targets]
    invalid = [t for t in targets if not t.isdigit()]
    if invalid:
        log.warning(f"⚠️ 비정상 종목코드 {len(invalid)}건 제외: {invalid[:10]}")
        targets = [t for t in targets if t.isdigit()]

    # 테스트 모드면 TEST_TICKERS만 수집
    if test_mode:
        targets = [t for t in targets if t in TEST_TICKERS]
        if not targets:
            targets = TEST_TICKERS
        log.info(f"🧪 테스트 대상: {targets}")
    elif daily_only:
        log.info(f"📅 [daily_only] 일간 데이터만 수집 ({len(targets)}개 종목 대상)")
    else:
        log.info(f"🎯 FnGuide 크롤링 대상: {len(targets)}개 보통주")

    # ── 3) 재무제표 ──
    _progress("재무제표 수집 중", 10)
    if daily_only:
        log.info("⏭️  [daily_only] 재무제표 수집 건너뜀")
    elif _db.table_has_data("financial_statements", biz_day):
        log.info("⏭️  financial_statements 이미 존재하여 수집 건너뜀")
    else:
        fs_rows = parallel_collect(fetch_fs, targets, "재무제표")
        if fs_rows:
            _db.save_df(pd.DataFrame(fs_rows), "financial_statements", biz_day)
        else:
            log.warning("⚠️ 재무제표 데이터 없음")

    # ── 4) 핵심 지표 ──
    _progress("핵심지표 수집 중", 20)
    if daily_only:
        log.info("⏭️  [daily_only] 핵심지표 수집 건너뜀")
    elif _db.table_has_data("indicators", biz_day):
        log.info("⏭️  indicators 이미 존재하여 수집 건너뜀")
    else:
        ind_rows = parallel_collect(fetch_indicators, targets, "핵심지표")
        if ind_rows:
            _db.save_df(pd.DataFrame(ind_rows), "indicators", biz_day)
        else:
            log.warning("⚠️ 핵심지표 데이터 없음")

    # ── 5) 주식수 ──
    _progress("주식수 수집 중", 30)
    if daily_only:
        log.info("⏭️  [daily_only] 주식수 수집 건너뜀")
    elif _db.table_has_data("shares", biz_day):
        log.info("⏭️  shares 이미 존재하여 수집 건너뜀")
    else:
        share_rows = parallel_collect(fetch_shares, targets, "주식수")
        if share_rows:
            _db.save_df(pd.DataFrame(share_rows), "shares", biz_day)
        else:
            log.warning("⚠️ 주식수 데이터 없음")

    # ── 6) 주가 히스토리 (52주 기술적 지표용) ──
    _progress("주가히스토리 수집 중", 40)
    if skip_price_history:
        log.info("⏭️  주가 히스토리 수집 건너뜀 (--skip-price-history)")
    elif _db.table_has_data("price_history", biz_day):
        log.info("⏭️  price_history 이미 존재하여 수집 건너뜀")
    else:
        ph_df = collect_price_history(targets)
        if not ph_df.empty:
            _db.save_df(ph_df, "price_history", biz_day)
        else:
            log.warning("⚠️ 주가 히스토리 데이터 없음")

    # ── 7) 지수 히스토리 (KOSPI/KOSDAQ, RS Rating용) ──
    _progress("지수히스토리 수집 중", 43)
    if not skip_price_history and _db.table_has_data("index_history", biz_day):
        log.info("⏭️  index_history 이미 존재하여 수집 건너뜀")
    elif not skip_price_history:
        idx_df = collect_index_history()
        if not idx_df.empty:
            _db.save_df(idx_df, "index_history", biz_day)
        else:
            log.warning("⚠️ 지수 히스토리 데이터 없음")

    # ── 8) 투자자 매매동향 (외국인/기관/개인 순매수) ──
    _progress("투자자매매동향 수집 중", 45)
    if skip_investor:
        log.info("⏭️  investor_trading 수집 건너뜀 (--skip-investor)")
    elif _db.table_has_data("investor_trading", biz_day):
        log.info("⏭️  investor_trading 이미 존재하여 수집 건너뜀")
    else:
        inv_df = collect_investor_trading(targets)
        if not inv_df.empty:
            _db.save_df(inv_df, "investor_trading", biz_day)
        else:
            log.warning("⚠️ 투자자 매매동향 데이터 없음")

    # ── 9) ETF/우선주 보조 가격 수집 ──
    _progress("ETF/우선주 가격 수집 중", 47)
    try:
        collect_supplement_prices()
    except Exception as e:
        log.warning(f"⚠️ ETF/우선주 가격 수집 실패 (무시): {e}")

    _progress("데이터 수집 완료", 48)

    elapsed = datetime.now() - start
    log.info(f"🎉 전체 수집 완료 (소요: {elapsed})")
    log.info(f"📁 DB: {_db.config.DB_PATH}")


def collect_supplement_prices():
    """포트폴리오에 있는 ETF/우선주/리츠 종목의 현재가를 FinanceDataReader로 수집하여
    price_supplement 테이블에 저장한다."""
    import FinanceDataReader as fdr
    import db as _db

    # 포트폴리오에서 종목코드 목록 확보
    portfolio = _db.load_portfolio()
    if not portfolio:
        log.info("포트폴리오 비어 있음 — supplement 수집 건너뜀")
        return

    portfolio_codes = [str(p["종목코드"]).zfill(6) for p in portfolio]

    # master 테이블에서 종목구분 확인
    master = _db.load_latest("master")
    if master.empty:
        log.warning("master 테이블 없음 — supplement 수집 건너뜀")
        return
    if "종목코드" in master.columns:
        master["종목코드"] = master["종목코드"].astype(str).str.zfill(6)

    # 포트폴리오 종목 중 dashboard_result에 없는 종목 = 보완 수집 대상
    # (ETF, 우선주, 리츠 또는 수집 안 된 보통주)
    dr = _db.load_dashboard()
    dr_codes: set = set()
    if not dr.empty and "종목코드" in dr.columns:
        dr_codes = set(dr["종목코드"].astype(str).str.zfill(6))

    # dashboard_result에 없는 포트폴리오 종목만 대상
    supp_codes = [c for c in portfolio_codes if c not in dr_codes]
    if not supp_codes:
        log.info("모든 포트폴리오 종목이 dashboard_result에 있음 — supplement 수집 건너뜀")
        return

    log.info(f"💹 보조 가격 수집 대상: {supp_codes}")

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")  # 7일 여유

    records = []
    for code in supp_codes:
        try:
            df_price = fdr.DataReader(code, yesterday, today)
            if df_price.empty:
                log.warning(f"  {code}: 가격 데이터 없음")
                continue
            last_row = df_price.iloc[-1]
            close = float(last_row.get("Close", last_row.get("종가", 0)) or 0)
            if close == 0:
                log.warning(f"  {code}: 종가 0 — 건너뜀")
                continue

            prev_close = float(df_price.iloc[-2].get("Close", df_price.iloc[-2].get("종가", close)) or close) if len(df_price) >= 2 else close
            change = close - prev_close
            change_pct = (change / prev_close * 100) if prev_close else 0

            # master에서 종목명/종목구분/시장구분 조회
            m_row = master[master["종목코드"] == code]
            name = m_row.iloc[0]["종목명"] if not m_row.empty else code
            gbn = m_row.iloc[0]["종목구분"] if not m_row.empty else "기타"
            mkt = m_row.iloc[0]["시장구분"] if not m_row.empty else ""

            records.append({
                "종목코드": code,
                "종목명": name,
                "종목구분": gbn,
                "시장구분": mkt,
                "현재가": close,
                "전일대비": round(change, 2),
                "등락률": round(change_pct, 2),
            })
            log.info(f"  {code} {name}: {close:,.0f}원 ({change_pct:+.2f}%)")
        except Exception as e:
            log.warning(f"  {code} 가격 수집 실패: {e}")

    if records:
        _db.upsert_price_supplement(records)
        log.info(f"✅ price_supplement 저장: {len(records)}건")


def main():
    parser = argparse.ArgumentParser(
        description="한국 주식 퀀트 데이터 수집기",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  테스트 모드 (3개 종목):  python quant_collector.py --test
  전체 실행:              python quant_collector.py
  테스트 후 전체 실행:     python quant_collector.py --test --auto-proceed
        """
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="테스트 모드 (샘플 3개 종목만 크롤링)"
    )
    parser.add_argument(
        "--auto-proceed",
        action="store_true",
        help="테스트 성공 시 자동으로 전체 수집 진행"
    )
    
    args = parser.parse_args()
    
    if args.test:
        test_passed = test_crawling()
        
        if test_passed and args.auto_proceed:
            log.info("\n자동 진행 모드: 전체 수집을 시작합니다...")
            run_full()
        elif test_passed:
            response = input("\n전체 수집을 진행하시겠습니까? (y/n): ").strip().lower()
            if response == 'y':
                run_full()
            else:
                log.info("전체 수집을 취소했습니다.")
        else:
            log.info("테스트를 통과하지 못했습니다. 문제를 해결 후 다시 시도하세요.")
    else:
        run_full()


if __name__ == "__main__":
    main()
