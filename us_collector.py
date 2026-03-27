# =========================================================
# us_collector.py  —  미국 주식 퀀트 데이터 수집기
# ---------------------------------------------------------
# 수집 항목:
#   1) 종목 마스터  (Russell 3000 + S&P 500/NASDAQ-100 멤버십 태그)
#   2) 일별 시세    (종가, 시가총액, EPS, BPS, DPS)
#   3) 재무제표     (IS / BS / CF — 연간 + 분기, yfinance)
#   4) 핵심 지표    (PER, PBR, ROE, 배당수익률 등)
#   5) 주식수/섹터  (floatShares, sector, industry)
#   6) 주가 히스토리 (2년 OHLCV)
#   7) 시장 지수    (S&P 500 ^GSPC, NASDAQ ^IXIC)
#
# 실행:
#   테스트 모드:  python run.py us-collect --test
#   전체 실행:    python run.py us-collect
# =========================================================

import io
import logging
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

import config
import db as _db

warnings.filterwarnings("ignore")
log = logging.getLogger("US_COLLECTOR")
# yfinance 내부 logger는 고빈도 수집에서 transient auth 오류를 과도하게 출력한다.
# 최종 실패는 우리 collector logger로 남기므로 여기서는 억제한다.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
US_MAX_WORKERS = 5        # yfinance 동시 요청 수 (보수적)
US_DELAY = 0.1            # 종목간 딜레이 (초)
US_BATCH_SIZE = 50        # N종목마다 잠시 휴식
US_BATCH_PAUSE = 2        # 배치 간 휴식 (초)
US_RETRY_COUNT = 3        # 재시도 횟수
REQUEST_TIMEOUT = 20      # 초
US_DAILY_CHUNK = 400      # bulk daily 다운로드 청크

RUSSELL3000_PRODUCT_URL = "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf"
RUSSELL3000_CSV_URLS = [
    f"{RUSSELL3000_PRODUCT_URL}/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund",
    f"{RUSSELL3000_PRODUCT_URL}/1449138789749.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund",
]
SOURCE_PRIORITY = {"RUSSELL3000": 0, "SP500": 1, "NASDAQ100": 2}
YAHOO_TICKER_ALIASES = {
    "BFA": "BF-A",
    "BFB": "BF-B",
    "BRKB": "BRK-B",
    "CWENA": "CWEN-A",
    "CWENB": "CWEN-B",
    "GEFB": "GEF-B",
    "HEIA": "HEI-A",
    "HEIB": "HEI-B",
    "LGFA": "LGF-A",
    "LGFB": "LGF-B",
    "MOGA": "MOG-A",
    "MOGB": "MOG-B",
    "WLYB": "WLY-B",
}

VALID_US_EXCHANGES = {"NYSE", "NASDAQ"}
INVALID_SECURITY_NAME_RE = re.compile(
    r"(?i)(?:\bCVR\b|\bCONTINGENT VALUE RIGHT\b|\bWARRANTS?\b|\bRIGHTS?\b|PRVT|VESTING)"
)

US_TEST_TICKERS = ["AAPL", "MSFT", "GOOGL"]

# 전역 requests 세션 (Wikipedia/iShares 스크래핑용)
_session = requests.Session()
_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
})

# ─────────────────────────────────────────────
# 계정명 매핑 (yfinance → 표준화된 계정명)
# ─────────────────────────────────────────────
US_ACCOUNT_MAP = {
    "revenue":              ["Total Revenue", "Revenue", "Operating Revenue"],
    "operating_income":     ["Operating Income", "EBIT", "Operating Income Loss"],
    "net_income":           ["Net Income", "Net Income Common Stockholders",
                             "Net Income Including Noncontrolling Interests"],
    "total_assets":         ["Total Assets"],
    "total_liabilities":    ["Total Liabilities Net Minority Interest", "Total Liabilities"],
    "total_equity":         ["Stockholders Equity", "Total Stockholder Equity",
                             "Common Stock Equity", "Total Equity Gross Minority Interest"],
    "operating_cf":         ["Operating Cash Flow",
                             "Cash Flow From Continuing Operating Activities",
                             "Net Cash Provided By Operating Activities"],
    "capex":                ["Capital Expenditure", "Purchase Of Fixed Assets",
                             "Capital Expenditures"],
    "interest_expense":     ["Interest Expense", "Interest Expense Non Operating",
                             "Net Interest Income"],
    "cash":                 ["Cash And Cash Equivalents",
                             "Cash Cash Equivalents And Short Term Investments"],
    "current_assets":       ["Current Assets", "Total Current Assets"],
    "current_liabilities":  ["Current Liabilities", "Total Current Liabilities"],
    "gross_profit":         ["Gross Profit"],
    "total_debt":           ["Total Debt", "Long Term Debt And Capital Lease Obligation",
                             "Long Term Debt"],
    "retained_earnings":    ["Retained Earnings"],
    "buyback":              ["Repurchase Of Capital Stock", "Common Stock Payments",
                             "Repurchase Of Common Stock"],
}


# ═════════════════════════════════════════════
# 공통 유틸리티
# ═════════════════════════════════════════════

def _get_ticker_ohlcv(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """yfinance 1.x MultiIndex bulk download에서 특정 ticker의 OHLCV 추출.

    Returns: DataFrame with columns [Open, High, Low, Close, Volume]
    """
    if raw is None or raw.empty:
        return pd.DataFrame()
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            # MultiIndex: (field, ticker) 구조
            available_tickers = raw.columns.get_level_values(1).unique()
            if ticker not in available_tickers:
                return pd.DataFrame()
            df = raw.xs(ticker, axis=1, level=1)
        else:
            df = raw
        return df.dropna(how="all")
    except Exception:
        return pd.DataFrame()


def safe_float(val) -> float:
    """안전한 float 변환 — NaN/None/문자열 처리"""
    if val is None:
        return np.nan
    try:
        if pd.isna(val):
            return np.nan
    except (TypeError, ValueError):
        pass
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return np.nan


def _find_account(df_index, aliases: list):
    """yfinance 재무제표 인덱스에서 계정명 검색 (복수 후보 중 첫 번째)"""
    for alias in aliases:
        if alias in df_index:
            return alias
    return None


def _extract_value(stmt_df, aliases: list, date_col) -> float:
    """재무제표 DataFrame에서 특정 계정의 특정 날짜 값 추출"""
    account = _find_account(stmt_df.index, aliases)
    if account is None:
        return np.nan
    try:
        val = stmt_df.loc[account, date_col]
        return safe_float(val)
    except (KeyError, TypeError):
        return np.nan


def _is_yf_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    needles = [
        "invalid crumb",
        "unauthorized",
        "user is unable to access this feature",
        "too many requests",
        "rate limit",
    ]
    return any(token in msg for token in needles)


def _is_yf_retryable_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    needles = [
        "invalid crumb",
        "unauthorized",
        "too many requests",
        "rate limit",
        "timed out",
        "timeout",
        "connection aborted",
        "connection reset",
        "temporarily unavailable",
    ]
    return any(token in msg for token in needles)


def _refresh_yfinance_session(reason: str = ""):
    """yfinance의 cookie/crumb 상태를 강제로 초기화한다."""
    try:
        from yfinance.data import YfData

        data = YfData()
        session = getattr(data, "_session", None)
        if session is not None and hasattr(session, "cookies"):
            session.cookies.clear()

        current = getattr(data, "_cookie_strategy", "basic")
        if hasattr(data, "_set_cookie_strategy"):
            target = "csrf" if current == "basic" else "basic"
            data._set_cookie_strategy(target)
        data._cookie = None
        data._crumb = None
        if reason:
            log.debug("yfinance 세션 갱신: %s", reason)
    except Exception as e:
        log.debug("yfinance 세션 갱신 실패: %s", e)


def _run_yf_call(call_fn, label: str, retries: int = 2):
    """yfinance 호출을 인증/일시 오류에 대해 재시도한다."""
    last_exc = None
    for attempt in range(retries):
        try:
            return call_fn()
        except Exception as e:
            last_exc = e
            if attempt < retries - 1 and _is_yf_auth_error(e):
                _refresh_yfinance_session(f"{label} auth")
                time.sleep(1.5 * (2 ** attempt))
                continue
            if attempt < retries - 1 and _is_yf_retryable_error(e):
                time.sleep(1.0 * (2 ** attempt))
                continue
            raise last_exc
    raise last_exc


def _normalize_us_ticker(value) -> str:
    """수집 소스별 티커 표기를 yfinance 친화 포맷으로 정규화."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    ticker = str(value).strip().upper()
    if not ticker or ticker == "NAN":
        return ""
    ticker = ticker.replace(".", "-").replace("/", "-")
    return YAHOO_TICKER_ALIASES.get(ticker, ticker)


def _normalize_us_exchange(value) -> str:
    """iShares/Wikipedia 교차소스 exchange 명칭을 NYSE/NASDAQ 중심으로 통일."""
    text = "" if value is None else str(value).strip()
    upper = text.upper()
    if "NASDAQ" in upper:
        return "NASDAQ"
    if "NYSE" in upper or "NEW YORK" in upper or "AMERICAN" in upper or "ARCA" in upper:
        return "NYSE"
    return text or "NYSE"


def _is_valid_us_equity_row(ticker: str, name: str, exchange: str) -> bool:
    """ETF holdings CSV에 섞인 비상장/권리증서/CVR 등을 제거."""
    if not ticker or ticker == "-" or set(ticker) == {"-"}:
        return False
    if exchange not in VALID_US_EXCHANGES:
        return False
    name_text = "" if name is None else str(name).strip()
    if INVALID_SECURITY_NAME_RE.search(name_text):
        return False
    return True


def _melt_financial_stmt(stmt_df: pd.DataFrame, ticker: str, period: str) -> pd.DataFrame:
    """yfinance 재무제표 DataFrame(wide) → long format 변환

    Args:
        stmt_df: yfinance가 반환한 재무제표 (index=계정명, columns=날짜)
        ticker: 종목 티커
        period: 'y' (연간) or 'q' (분기)

    Returns:
        (ticker, base_date, account, period, value, is_estimate) 컬럼 DataFrame
    """
    if stmt_df is None or stmt_df.empty:
        return pd.DataFrame()

    rows = []
    for date_col in stmt_df.columns:
        try:
            base_date = pd.Timestamp(date_col).strftime("%Y-%m-%d")
        except Exception:
            continue
        for account in stmt_df.index:
            val = safe_float(stmt_df.loc[account, date_col])
            if not np.isnan(val):
                rows.append({
                    "ticker":       ticker,
                    "base_date":    base_date,
                    "account":      str(account),
                    "period":       period,
                    "value":        val,
                    "is_estimate":  0,
                })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ═════════════════════════════════════════════
# Phase 1: 종목 마스터
# ═════════════════════════════════════════════

def _discover_russell3000_csv_url() -> str:
    """iShares IWV 페이지에서 holdings CSV 링크를 탐색."""
    try:
        resp = _session.get(RUSSELL3000_PRODUCT_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        html = resp.text.replace("&amp;", "&")
        patterns = [
            r'((?:https://www\.ishares\.com)?/us/products/239714/ishares-russell-3000-etf/\d+\.ajax\?[^"\']*fileType=csv[^"\']*fileName=IWV_holdings[^"\']*dataType=fund[^"\']*)',
            r'((?:https://www\.ishares\.com)?/us/products/239714/ishares-russell-3000-etf/\d+\.ajax\?[^"\']*dataType=fund[^"\']*fileName=IWV_holdings[^"\']*fileType=csv[^"\']*)',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                url = match.group(1)
                return requests.compat.urljoin("https://www.ishares.com", url)
    except Exception as e:
        log.debug("Russell 3000 CSV 링크 탐색 실패: %s", e)
    return ""


def _parse_ishares_holdings_csv(text: str) -> pd.DataFrame:
    """iShares holdings CSV에서 메타데이터/푸터를 제거하고 실제 보유종목만 파싱."""
    lines = text.splitlines()
    start_idx = next(
        (
            idx for idx, line in enumerate(lines)
            if line.lstrip("\ufeff").startswith("Ticker,")
        ),
        -1,
    )
    if start_idx < 0:
        return pd.DataFrame()

    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        if not lines[idx].replace("\xa0", "").strip():
            end_idx = idx
            break

    payload = "\n".join(lines[start_idx:end_idx]).strip()
    if not payload:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(payload))


def _fetch_russell3000_from_ishares() -> pd.DataFrame:
    """iShares IWV holdings CSV를 Russell 3000 프록시 유니버스로 사용."""
    url_candidates = []
    discovered = _discover_russell3000_csv_url()
    if discovered:
        url_candidates.append(discovered)
    for url in RUSSELL3000_CSV_URLS:
        if url not in url_candidates:
            url_candidates.append(url)

    for url in url_candidates:
        try:
            resp = _session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            raw = _parse_ishares_holdings_csv(resp.text)
            if raw.empty:
                continue

            raw.columns = [str(c).strip() for c in raw.columns]
            ticker_col = next((c for c in raw.columns if "ticker" in c.lower()), None)
            name_col = next((c for c in raw.columns if c.lower() == "name"), None)
            sector_col = next((c for c in raw.columns if c.lower() == "sector"), None)
            asset_class_col = next((c for c in raw.columns if "asset class" in c.lower()), None)
            exchange_col = next((c for c in raw.columns if c.lower() == "exchange"), None)
            location_col = next((c for c in raw.columns if c.lower() == "location"), None)
            if not ticker_col:
                continue

            cleaned = raw.copy()
            if asset_class_col:
                cleaned = cleaned[
                    cleaned[asset_class_col].fillna("").astype(str).str.contains("equity", case=False)
                ]
            if location_col:
                cleaned = cleaned[
                    cleaned[location_col].fillna("").astype(str).str.contains("united states", case=False)
                ]

            result = pd.DataFrame()
            result["ticker"] = cleaned[ticker_col].map(_normalize_us_ticker)
            result["name"] = (
                cleaned[name_col].fillna("").astype(str).str.strip()
                if name_col else result["ticker"]
            )
            result["exchange"] = (
                cleaned[exchange_col].map(_normalize_us_exchange)
                if exchange_col else "NYSE"
            )
            result["sector"] = (
                cleaned[sector_col].fillna("").astype(str).str.strip()
                if sector_col else ""
            )
            result["industry"] = ""
            result["stock_type"] = "Common"
            result["source"] = "RUSSELL3000"

            result = result[result["ticker"].astype(str).str.len() > 0].copy()
            valid_mask = result.apply(
                lambda row: _is_valid_us_equity_row(
                    str(row.get("ticker", "")),
                    row.get("name", ""),
                    str(row.get("exchange", "")),
                ),
                axis=1,
            )
            removed = int((~valid_mask).sum())
            result = result[valid_mask].reset_index(drop=True)
            if removed:
                log.info("Russell 3000 프록시 필터링: %d건 제거", removed)
            log.info("Russell 3000 (IWV holdings): %d종목", len(result))
            return result
        except Exception as e:
            log.warning("Russell 3000 iShares CSV 파싱 실패 (%s): %s", url, e)

    return pd.DataFrame()

def _fetch_sp500_from_wikipedia() -> pd.DataFrame:
    """Wikipedia에서 S&P 500 종목 목록 파싱"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        resp = _session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        tables = pd.read_html(resp.text, attrs={"id": "constituents"})
        df = tables[0]
        # 컬럼명 정규화
        df.columns = [c.strip() for c in df.columns]
        # Symbol, Security, GICS Sector, GICS Sub-Industry, Exchange
        ticker_col = next((c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower()), df.columns[0])
        name_col   = next((c for c in df.columns if "security" in c.lower() or "company" in c.lower()), df.columns[1])
        sector_col = next((c for c in df.columns if "sector" in c.lower()), None)
        industry_col = next((c for c in df.columns if "sub" in c.lower() and "industry" in c.lower()), None)
        exchange_col = next((c for c in df.columns if "exchange" in c.lower()), None)

        result = pd.DataFrame()
        result["ticker"]    = df[ticker_col].str.replace(".", "-", regex=False).str.strip()
        result["name"]      = df[name_col].str.strip()
        result["exchange"]  = df[exchange_col].str.strip() if exchange_col else "NYSE"
        result["sector"]    = df[sector_col].str.strip() if sector_col else ""
        result["industry"]  = df[industry_col].str.strip() if industry_col else ""
        result["stock_type"] = "Common"
        result["source"]    = "SP500"
        log.info("S&P 500: %d종목", len(result))
        return result
    except Exception as e:
        log.warning("S&P 500 Wikipedia 파싱 실패: %s", e)
        return pd.DataFrame()


def _fetch_nasdaq100_from_wikipedia() -> pd.DataFrame:
    """Wikipedia에서 NASDAQ-100 종목 목록 파싱"""
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    try:
        resp = _session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        # NASDAQ-100 구성종목 테이블 찾기 (Ticker 컬럼 있는 것)
        df = None
        for t in tables:
            cols = [str(c).lower() for c in t.columns]
            if any("ticker" in c or "symbol" in c for c in cols):
                df = t
                break
        if df is None:
            log.warning("NASDAQ-100 테이블을 찾지 못했습니다")
            return pd.DataFrame()

        df.columns = [c.strip() for c in df.columns]
        ticker_col = next((c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower()), df.columns[0])
        name_col   = next((c for c in df.columns if "company" in c.lower() or "name" in c.lower() or "security" in c.lower()), None)
        sector_col = next((c for c in df.columns if "sector" in c.lower() or "gics" in c.lower()), None)
        industry_col = next((c for c in df.columns if "industry" in c.lower() or "sub" in c.lower()), None)

        result = pd.DataFrame()
        result["ticker"]    = df[ticker_col].str.replace(".", "-", regex=False).str.strip()
        result["name"]      = df[name_col].str.strip() if name_col else ""
        result["exchange"]  = "NASDAQ"
        result["sector"]    = df[sector_col].str.strip() if sector_col else ""
        result["industry"]  = df[industry_col].str.strip() if industry_col else ""
        result["stock_type"] = "Common"
        result["source"]    = "NASDAQ100"
        # 빈 ticker 제거
        result = result[result["ticker"].str.len() > 0]
        log.info("NASDAQ-100: %d종목", len(result))
        return result
    except Exception as e:
        log.warning("NASDAQ-100 Wikipedia 파싱 실패: %s", e)
        return pd.DataFrame()


def collect_us_master(test_mode: bool = False) -> pd.DataFrame:
    """Russell 3000 기반 종목 마스터 + 대표 지수 멤버십 태그 수집."""
    if test_mode:
        rows = [
            {"ticker": t, "name": t, "exchange": "NASDAQ",
             "stock_type": "Common", "sector": "", "industry": "", "source": "RUSSELL3000,SP500,NASDAQ100"}
            for t in US_TEST_TICKERS
        ]
        return pd.DataFrame(rows)

    russell = _fetch_russell3000_from_ishares()
    sp500 = _fetch_sp500_from_wikipedia()
    nasdaq = _fetch_nasdaq100_from_wikipedia()

    if russell.empty and sp500.empty and nasdaq.empty:
        log.error("마스터 데이터 수집 실패")
        return pd.DataFrame()
    if russell.empty:
        log.warning("Russell 3000 확보 실패 — 기존 S&P 500/NASDAQ-100 유니버스로 폴백합니다")

    frames = [df for df in [sp500, nasdaq, russell] if not df.empty]
    combined = pd.concat(frames, ignore_index=True)
    if "source" not in combined.columns:
        combined["source"] = ""

    def _first_non_empty(series, default=""):
        for value in series:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return default

    def _merge_sources(series) -> str:
        values = []
        for raw in series:
            if not isinstance(raw, str):
                continue
            for token in raw.split(","):
                token = token.strip()
                if token and token not in values:
                    values.append(token)
        values.sort(key=lambda x: SOURCE_PRIORITY.get(x, 99))
        return ",".join(values)

    # 중복 ticker는 source를 합쳐 인덱스 멤버십 정보를 보존한다.
    combined = (
        combined.groupby("ticker", as_index=False)
        .agg({
            "name": lambda s: _first_non_empty(s, ""),
            "exchange": lambda s: _first_non_empty(s, "NYSE"),
            "stock_type": lambda s: _first_non_empty(s, "Common"),
            "sector": lambda s: _first_non_empty(s, ""),
            "industry": lambda s: _first_non_empty(s, ""),
            "source": _merge_sources,
        })
    )
    combined = combined[["ticker", "name", "exchange", "stock_type", "sector", "industry", "source"]]
    combined = combined.reset_index(drop=True)
    log.info("마스터 총 %d종목 (중복제거 후)", len(combined))
    return combined


# ═════════════════════════════════════════════
# Phase 2: 일별 시세 (bulk download)
# ═════════════════════════════════════════════

def collect_us_daily(tickers: list) -> pd.DataFrame:
    """yfinance bulk download로 최신 종가/시가총액 수집"""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance가 설치되지 않았습니다: pip install yfinance")
        return pd.DataFrame()

    log.info("일별 시세 수집 시작 (%d종목)...", len(tickers))
    today = datetime.now().strftime("%Y-%m-%d")

    rows = []

    def _empty_row(ticker: str) -> dict:
        return {
            "ticker": ticker,
            "name": ticker,
            "close": np.nan,
            "market_cap": np.nan,
            "shares_outstanding": np.nan,
            "eps": np.nan,
            "bps": np.nan,
            "dps": np.nan,
            "base_date": today,
        }

    for start in range(0, len(tickers), US_DAILY_CHUNK):
        chunk = tickers[start:start + US_DAILY_CHUNK]
        try:
            raw = _run_yf_call(
                lambda: yf.download(
                    chunk,
                    period="5d",
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                ),
                f"daily chunk {start}:{start + len(chunk)}",
                retries=3,
            )
        except Exception as e:
            log.warning("daily 청크 다운로드 실패 (%d~%d): %s", start, start + len(chunk) - 1, e)
            rows.extend(_empty_row(ticker) for ticker in chunk)
            continue

        for ticker in chunk:
            try:
                df_t = _get_ticker_ohlcv(raw, ticker)
                if df_t is None or df_t.empty:
                    rows.append(_empty_row(ticker))
                    continue

                df_t = df_t.dropna(subset=["Close"])
                if df_t.empty:
                    rows.append(_empty_row(ticker))
                    continue

                latest = df_t.iloc[-1]
                rows.append({
                    "ticker":             ticker,
                    "name":               ticker,
                    "close":              safe_float(latest.get("Close")),
                    "market_cap":         np.nan,  # info에서 채움
                    "shares_outstanding": np.nan,  # info에서 채움
                    "eps":                np.nan,
                    "bps":                np.nan,
                    "dps":                np.nan,
                    "base_date":          df_t.index[-1].strftime("%Y-%m-%d"),
                })
            except Exception as e:
                log.debug("daily 수집 실패 %s: %s", ticker, e)
                rows.append(_empty_row(ticker))

        log.info("  daily 진행: %d/%d", min(start + len(chunk), len(tickers)), len(tickers))
        if start + US_DAILY_CHUNK < len(tickers):
            time.sleep(1)

    df_daily = pd.DataFrame(rows)

    # bulk에서 누락된 종목 개별 fallback
    nan_close = df_daily[df_daily["close"].isna()]["ticker"].tolist()
    if nan_close:
        log.info("bulk 누락 종목 개별 재수집: %d건", len(nan_close))
        today = datetime.now().strftime("%Y-%m-%d")
        for ticker in nan_close:
            try:
                import yfinance as yf
                raw_single = _run_yf_call(
                    lambda t=ticker: yf.download(
                        t, period="5d", auto_adjust=True, progress=False, threads=False
                    ),
                    f"daily_fallback {ticker}",
                    retries=2,
                )
                if raw_single is not None and not raw_single.empty:
                    df_t = _get_ticker_ohlcv(raw_single, ticker)
                    if df_t is None or df_t.empty:
                        # single ticker download returns flat df
                        if isinstance(raw_single.columns, pd.MultiIndex):
                            df_t = raw_single.xs(ticker, axis=1, level=1)
                        else:
                            df_t = raw_single
                    df_t = df_t.dropna(subset=["Close"]) if not df_t.empty else df_t
                    if not df_t.empty:
                        latest = df_t.iloc[-1]
                        close_val = safe_float(latest.get("Close"))
                        if not np.isnan(close_val):
                            idx = df_daily[df_daily["ticker"] == ticker].index
                            df_daily.loc[idx, "close"] = close_val
                            df_daily.loc[idx, "base_date"] = df_t.index[-1].strftime("%Y-%m-%d")
                            log.debug("fallback 성공 %s: close=%.2f", ticker, close_val)
            except Exception as e:
                log.debug("fallback 실패 %s: %s", ticker, e)
            time.sleep(US_DELAY)

    log.info("일별 시세: %d건", len(df_daily))
    return df_daily


# ═════════════════════════════════════════════
# Phase 3: 종목별 통합 수집 (재무제표 + 지표 + 주식수)
# ═════════════════════════════════════════════

def fetch_us_ticker_data(ticker: str) -> dict:
    """yf.Ticker()에서 재무제표, 지표, 주식수 한번에 추출

    Returns:
        {
            'ticker': str,
            'fs_rows': list[dict],         → us_financial_statements
            'indicator_rows': list[dict],  → us_indicators
            'shares': dict,                → us_shares
            'daily_patch': dict,           → us_daily 보완 (market_cap, eps 등)
        }
    """
    try:
        import yfinance as yf
    except ImportError:
        return {}

    result = {
        "ticker": ticker,
        "fs_rows": [],
        "indicator_rows": [],
        "shares": {},
        "daily_patch": {},
    }

    # 주요 종목 리스트 — 이 종목들은 빈 데이터 시 더 공격적으로 재시도
    CRITICAL_TICKERS = {
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
        "AVGO", "LLY", "JPM", "V", "MA", "XOM", "UNH", "WMT", "JNJ",
        "COST", "HD", "PG", "NFLX", "CRM", "AMD", "ORCL", "BAC", "ABBV",
    }
    is_critical = ticker in CRITICAL_TICKERS
    effective_retries = US_RETRY_COUNT + 2 if is_critical else US_RETRY_COUNT

    for attempt in range(effective_retries):
        try:
            # ── 1. 재무제표 수집 ──────────────────────────
            for stmt_attr, period in [
                ("income_stmt",             "y"),
                ("quarterly_income_stmt",   "q"),
                ("balance_sheet",           "y"),
                ("quarterly_balance_sheet", "q"),
                ("cashflow",                "y"),
                ("quarterly_cashflow",      "q"),
            ]:
                for fs_attempt in range(3):
                    try:
                        stmt_df = _run_yf_call(
                            lambda attr=stmt_attr: getattr(yf.Ticker(ticker), attr),
                            f"{ticker} {stmt_attr}",
                            retries=2,
                        )
                        melted = _melt_financial_stmt(stmt_df, ticker, period)
                        if not melted.empty:
                            result["fs_rows"].extend(melted.to_dict("records"))
                        break  # 성공 (빈 데이터도 성공으로 처리)
                    except Exception as e:
                        if fs_attempt < 2 and _is_yf_retryable_error(e):
                            _refresh_yfinance_session(f"{ticker} {stmt_attr} retry")
                            time.sleep(1.5 * (2 ** fs_attempt))
                        else:
                            log.debug("%s %s 재무제표 실패: %s", ticker, stmt_attr, e)
                            break

            # ── 2. .info 지표/주식수 수집 ─────────────────
            info = {}
            for info_attempt in range(4 if is_critical else 2):
                try:
                    raw_info = _run_yf_call(
                        lambda: yf.Ticker(ticker).info or {},
                        f"{ticker} info",
                        retries=2,
                    )
                    # 빈 dict 또는 quoteType만 있는 최소 응답은 실패로 간주
                    meaningful_keys = {
                        "marketCap", "trailingPE", "returnOnEquity", "totalRevenue",
                        "trailingEps", "bookValue", "sharesOutstanding",
                    }
                    if raw_info and meaningful_keys.intersection(raw_info.keys()):
                        info = raw_info
                        break
                    else:
                        if info_attempt < (3 if is_critical else 1):
                            log.debug("%s info 빈 응답 (시도 %d) — 재시도", ticker, info_attempt + 1)
                            _refresh_yfinance_session(f"{ticker} info empty")
                            time.sleep(2.0 * (info_attempt + 1))
                        else:
                            log.warning("%s info 유효 데이터 없음 (빈 응답)", ticker)
                            info = raw_info or {}
                            break
                except Exception as e:
                    log.debug("%s info 조회 실패 (시도 %d): %s", ticker, info_attempt + 1, e)
                    if info_attempt < (3 if is_critical else 1):
                        _refresh_yfinance_session(f"{ticker} info error")
                        time.sleep(2.0 * (info_attempt + 1))
                    else:
                        info = {}

            today = datetime.now().strftime("%Y-%m-%d")

            # 핵심 지표 추출
            indicator_map = {
                "trailingPE":              "PER",
                "priceToBook":             "PBR",
                "returnOnEquity":          "ROE(%)",
                "debtToEquity":            "Debt_Ratio(%)",
                "operatingMargins":        "Operating_Margin(%)",
                "dividendYield":           "Dividend_Yield(%)",
                "priceToSalesTrailing12Months": "PSR",
                "returnOnAssets":          "ROA(%)",
                "revenueGrowth":           "Revenue_Growth(%)",
                "earningsGrowth":          "Earnings_Growth(%)",
                "grossMargins":            "Gross_Margin(%)",
                "ebitdaMargins":           "EBITDA_Margin(%)",
                "currentRatio":            "Current_Ratio",
                "quickRatio":              "Quick_Ratio",
                "totalDebt":               "Total_Debt",
                "freeCashflow":            "FCF",
                "operatingCashflow":       "Operating_CF",
                "marketCap":               "Market_Cap",
                "trailingEps":             "EPS",
                "bookValue":               "BPS",
                "dividendRate":            "DPS",
                "forwardPE":               "Forward_PER",
                "forwardEps":              "Forward_EPS",
                "pegRatio":                "PEG",
                "beta":                    "Beta",
                "fiftyTwoWeekHigh":        "52W_High",
                "fiftyTwoWeekLow":         "52W_Low",
                "sharesOutstanding":       "Shares_Outstanding",
                "floatShares":             "Float_Shares",
                # 애널리스트 컨센서스
                "targetMeanPrice":            "Target_Mean_Price",
                "targetHighPrice":            "Target_High_Price",
                "targetLowPrice":             "Target_Low_Price",
                "numberOfAnalystOpinions":    "Analyst_Count",
                "recommendationMean":         "Recommendation_Mean",
                # 보유 구조
                "heldPercentInsiders":        "Insider_Holdings_Pct",
                "heldPercentInstitutions":    "Institution_Holdings_Pct",
                # 공매도
                "shortRatio":                 "Short_Ratio",
                "shortPercentOfFloat":        "Short_Float_Pct",
                # EV 멀티플
                "enterpriseValue":            "Enterprise_Value",
                "enterpriseToRevenue":        "EV_Revenue",
                "enterpriseToEbitda":         "EV_EBITDA",
            }
            for info_key, account_name in indicator_map.items():
                val = safe_float(info.get(info_key))
                if not np.isnan(val):
                    # ROE, operatingMargins 등은 소수점 비율 → % 변환
                    if info_key in ("returnOnEquity", "operatingMargins", "dividendYield",
                                   "returnOnAssets", "revenueGrowth", "earningsGrowth",
                                   "grossMargins", "ebitdaMargins",
                                   "heldPercentInsiders", "heldPercentInstitutions",
                                   "shortPercentOfFloat"):
                        val = val * 100.0
                    result["indicator_rows"].append({
                        "ticker":         ticker,
                        "base_date":      today,
                        "indicator_type": "INFO",
                        "account":        account_name,
                        "value":          val,
                    })

            # ── 3. 주식수/섹터 ────────────────────────────
            def _safe_int(v):
                f = safe_float(v)
                return 0 if (f is None or np.isnan(f)) else int(f)

            result["shares"] = {
                "ticker":             ticker,
                "base_date":          today,
                "shares_outstanding": _safe_int(info.get("sharesOutstanding")),
                "float_shares":       _safe_int(info.get("floatShares")),
                "sector":             info.get("sector", ""),
                "industry":           info.get("industry", ""),
            }

            # ── 4. daily 보완 (market_cap, eps, bps, dps) ─
            result["daily_patch"] = {
                "ticker":             ticker,
                "name":               info.get("longName") or info.get("shortName") or ticker,
                "market_cap":         safe_float(info.get("marketCap")),
                "shares_outstanding": safe_float(info.get("sharesOutstanding")),
                "eps":                safe_float(info.get("trailingEps")),
                "bps":                safe_float(info.get("bookValue")),
                "dps":                safe_float(info.get("dividendRate")),
            }

            # ── 5. 배당 이력 (us_indicators DPS 이력) ─────
            try:
                divs = _run_yf_call(
                    lambda: yf.Ticker(ticker).dividends,
                    f"{ticker} dividends",
                    retries=2,
                )
                if divs is not None and not divs.empty:
                    for div_date, div_val in divs.items():
                        result["indicator_rows"].append({
                            "ticker":         ticker,
                            "base_date":      pd.Timestamp(div_date).strftime("%Y-%m-%d"),
                            "indicator_type": "DPS",
                            "account":        "DPS",
                            "value":          safe_float(div_val),
                        })
            except Exception:
                pass

            # ── 주요 종목 데이터 품질 검증 ─────────────────
            # 주요 종목인데 지표/재무 둘 다 비어있으면 재시도
            if is_critical and attempt < effective_retries - 1:
                has_indicators = len(result["indicator_rows"]) > 0
                has_fs = len(result["fs_rows"]) > 0
                if not has_indicators and not has_fs:
                    log.warning(
                        "%s (주요 종목) 지표/재무 모두 빈 데이터 — 재시도 %d/%d",
                        ticker, attempt + 1, effective_retries,
                    )
                    _refresh_yfinance_session(f"{ticker} critical retry")
                    time.sleep(3.0 * (attempt + 1))
                    # 누적된 빈 결과 리셋
                    result["fs_rows"] = []
                    result["indicator_rows"] = []
                    result["shares"] = {}
                    result["daily_patch"] = {}
                    continue  # 재시도

            break  # 성공시 루프 종료

        except Exception as e:
            if attempt < effective_retries - 1:
                wait = 2.0 ** attempt
                log.debug("%s 재시도 %d/%d (%.1fs): %s", ticker, attempt + 1, effective_retries, wait, e)
                _refresh_yfinance_session(f"{ticker} outer retry")
                time.sleep(wait)
            else:
                log.warning("%s 수집 최종 실패: %s", ticker, e)

    if is_critical:
        ind_count = len(result["indicator_rows"])
        fs_count = len(result["fs_rows"])
        if ind_count == 0 and fs_count == 0:
            log.warning("%s (주요 종목) 최종 결과 — 지표 0건, 재무 0건", ticker)
        else:
            log.debug("%s (주요 종목) 수집 완료 — 지표 %d건, 재무 %d건", ticker, ind_count, fs_count)

    return result


def parallel_collect_us(tickers: list, progress_cb=None) -> tuple:
    """ThreadPoolExecutor로 fetch_us_ticker_data 병렬 실행

    주요 종목(CRITICAL_TICKERS)을 앞쪽에 배치하여 rate limit 전에 수집 보장.

    Returns:
        (fs_df, indicators_df, shares_df, daily_patches_df)
    """
    # 주요 종목을 리스트 앞쪽으로 정렬
    CRITICAL_TICKERS = {
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
        "AVGO", "LLY", "JPM", "V", "MA", "XOM", "UNH", "WMT", "JNJ",
        "COST", "HD", "PG", "NFLX", "CRM", "AMD", "ORCL", "BAC", "ABBV",
    }
    critical = [t for t in tickers if t in CRITICAL_TICKERS]
    non_critical = [t for t in tickers if t not in CRITICAL_TICKERS]
    ordered_tickers = critical + non_critical
    if critical:
        log.info("주요 종목 %d개 우선 수집 배치: %s", len(critical), ", ".join(critical[:10]))

    all_fs, all_indicators, all_shares, all_daily_patches = [], [], [], []
    total = len(ordered_tickers)
    done = 0
    consecutive_empty = 0  # 연속 빈 결과 카운터 (rate limit 감지)
    failed_tickers = []    # 1차 수집에서 빈 결과를 반환한 종목

    log.info("병렬 수집 시작: %d종목 (workers=%d)", total, US_MAX_WORKERS)

    with ThreadPoolExecutor(max_workers=US_MAX_WORKERS) as executor:
        futures = {}
        for ticker in ordered_tickers:
            fut = executor.submit(fetch_us_ticker_data, ticker)
            futures[fut] = ticker
            time.sleep(US_DELAY)

        for fut in as_completed(futures):
            ticker = futures[fut]
            try:
                res = fut.result(timeout=90)
                if res:
                    all_fs.extend(res.get("fs_rows", []))
                    all_indicators.extend(res.get("indicator_rows", []))
                    if res.get("shares"):
                        all_shares.append(res["shares"])
                    if res.get("daily_patch"):
                        all_daily_patches.append(res["daily_patch"])

                    # 연속 빈 결과 감지 → rate limit 가능성
                    has_data = (
                        bool(res.get("fs_rows")) or
                        bool(res.get("indicator_rows")) or
                        bool(res.get("shares")) or
                        bool(res.get("daily_patch", {}).get("market_cap"))
                    )
                    if not has_data:
                        failed_tickers.append(ticker)
                        consecutive_empty += 1
                        if consecutive_empty >= 5:
                            log.warning(
                                "연속 빈 결과 %d건 감지 — rate limit 가능성. 세션 갱신 후 %ds 대기",
                                consecutive_empty, US_BATCH_PAUSE * 2,
                            )
                            _refresh_yfinance_session("rate limit detected")
                            time.sleep(US_BATCH_PAUSE * 2)
                            consecutive_empty = 0
                    else:
                        consecutive_empty = 0
                else:
                    failed_tickers.append(ticker)
            except Exception as e:
                log.debug("%s future 오류: %s", ticker, e)
                failed_tickers.append(ticker)

            done += 1
            if progress_cb:
                progress_cb(done, total, ticker)
            if done % US_BATCH_SIZE == 0:
                log.info("  진행: %d/%d (%.0f%%)", done, total, done / total * 100)
                time.sleep(US_BATCH_PAUSE)

    # ── 재시도: 1차에서 빈 결과를 반환한 종목만 ──────────────
    if failed_tickers:
        log.info("재시도 대상: %d종목 (1차 수집 실패) — 10초 대기 후 재수집", len(failed_tickers))
        time.sleep(10)
        _refresh_yfinance_session("retry pass")

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}
            for ticker in failed_tickers:
                fut = executor.submit(fetch_us_ticker_data, ticker)
                futures[fut] = ticker
                time.sleep(0.5)

            retry_ok, retry_fail = 0, 0
            for fut in as_completed(futures):
                ticker = futures[fut]
                try:
                    res = fut.result(timeout=90)
                    if res:
                        all_fs.extend(res.get("fs_rows", []))
                        all_indicators.extend(res.get("indicator_rows", []))
                        if res.get("shares"):
                            all_shares.append(res["shares"])
                        if res.get("daily_patch"):
                            all_daily_patches.append(res["daily_patch"])
                        has_data = (
                            bool(res.get("fs_rows")) or
                            bool(res.get("indicator_rows")) or
                            bool(res.get("shares")) or
                            bool(res.get("daily_patch", {}).get("market_cap"))
                        )
                        if has_data:
                            retry_ok += 1
                        else:
                            retry_fail += 1
                    else:
                        retry_fail += 1
                except Exception as e:
                    log.debug("%s 재시도 오류: %s", ticker, e)
                    retry_fail += 1

        log.info("재시도 완료 — 성공: %d, 최종 실패: %d", retry_ok, retry_fail)

    fs_df      = pd.DataFrame(all_fs)      if all_fs      else pd.DataFrame()
    ind_df     = pd.DataFrame(all_indicators) if all_indicators else pd.DataFrame()
    shares_df  = pd.DataFrame(all_shares)  if all_shares  else pd.DataFrame()
    patches_df = pd.DataFrame(all_daily_patches) if all_daily_patches else pd.DataFrame()

    log.info("재무제표: %d행, 지표: %d행, 주식수: %d행",
             len(fs_df), len(ind_df), len(shares_df))
    return fs_df, ind_df, shares_df, patches_df


# ═════════════════════════════════════════════
# Phase 4: 주가 히스토리 (bulk download)
# ═════════════════════════════════════════════

def collect_us_price_history(tickers: list, days: int = 730) -> pd.DataFrame:
    """yfinance bulk download로 2년치 OHLCV 수집"""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance가 없습니다")
        return pd.DataFrame()

    end = datetime.now()
    start = end - timedelta(days=days)
    start_str = start.strftime("%Y-%m-%d")
    end_str   = end.strftime("%Y-%m-%d")

    log.info("주가 히스토리 수집 (%d종목, %s~%s)...", len(tickers), start_str, end_str)

    # yfinance는 500개 이상 한번에 요청 시 불안정 → 청크 분할
    CHUNK = 200
    all_rows = []
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        try:
            raw = _run_yf_call(
                lambda: yf.download(
                    chunk,
                    start=start_str,
                    end=end_str,
                    auto_adjust=True,
                    progress=False,
                ),
                f"price_history chunk {i}:{i + len(chunk)}",
                retries=3,
            )
            if raw.empty:
                continue

            for ticker in chunk:
                try:
                    df_t = _get_ticker_ohlcv(raw, ticker)
                    if df_t is None or df_t.empty:
                        continue
                    df_t = df_t.dropna(subset=["Close"])
                    for dt, row in df_t.iterrows():
                        close = safe_float(row.get("Close"))
                        vol   = safe_float(row.get("Volume"))
                        all_rows.append({
                            "ticker":  ticker,
                            "date":    dt.strftime("%Y-%m-%d"),
                            "open":    safe_float(row.get("Open")),
                            "high":    safe_float(row.get("High")),
                            "low":     safe_float(row.get("Low")),
                            "close":   close,
                            "volume":  vol,
                            "amount":  close * vol if not (np.isnan(close) or np.isnan(vol)) else np.nan,
                        })
                except Exception as e:
                    log.debug("%s 주가이력 처리 실패: %s", ticker, e)
        except Exception as e:
            log.warning("주가 히스토리 청크 %d 실패: %s", i, e)

        if i + CHUNK < len(tickers):
            time.sleep(2)  # 청크 간 휴식

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    log.info("주가 히스토리: %d행 (%d종목)", len(df), df["ticker"].nunique())
    return df


# ═════════════════════════════════════════════
# Phase 5: 시장 지수 히스토리
# ═════════════════════════════════════════════

def collect_us_index_history(days: int = 730) -> pd.DataFrame:
    """S&P 500, NASDAQ Composite 지수 히스토리 수집"""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance가 없습니다")
        return pd.DataFrame()

    end = datetime.now()
    start = end - timedelta(days=days)
    indices = {"SP500": "^GSPC", "NASDAQ": "^IXIC"}

    all_rows = []
    for index_code, symbol in indices.items():
        try:
            raw = _run_yf_call(
                lambda sym=symbol: yf.download(
                    sym,
                    start=start.strftime("%Y-%m-%d"),
                    end=end.strftime("%Y-%m-%d"),
                    auto_adjust=True,
                    progress=False,
                ),
                f"index {index_code}",
                retries=3,
            )
            if raw.empty:
                log.warning("%s (%s) 데이터 없음", index_code, symbol)
                continue
            df_t = _get_ticker_ohlcv(raw, symbol)
            if df_t is None or df_t.empty:
                continue
            for dt, row in df_t.iterrows():
                close = safe_float(row.get("Close"))
                if not np.isnan(close):
                    all_rows.append({
                        "index_code": index_code,
                        "date":       dt.strftime("%Y-%m-%d"),
                        "close":      close,
                    })
            log.info("%s: %d행", index_code, len(df_t))
        except Exception as e:
            log.warning("%s 지수 수집 실패: %s", index_code, e)

    if not all_rows:
        return pd.DataFrame()
    return pd.DataFrame(all_rows)


# ═════════════════════════════════════════════
# 메인 오케스트레이터
# ═════════════════════════════════════════════

def run_full(
    test_mode: bool = False,
    skip_price_history: bool = False,
    daily_only: bool = False,
    progress_cb=None,
):
    """전체 US 수집 파이프라인 실행

    Args:
        test_mode: True이면 3개 테스트 종목만 수집
        skip_price_history: 주가 히스토리 수집 건너뜀
        daily_only: 일별 시세 + 지수만 수집 (재무제표 제외)
        progress_cb: (done, total, ticker) 형태 콜백
    """
    start_time = datetime.now()
    collected_date = start_time.strftime("%Y-%m-%d")

    log.info("=== US 퀀트 데이터 수집 시작 (mode=%s) ===",
             "test" if test_mode else ("daily_only" if daily_only else "full"))

    # DB 초기화
    _db.init_db()

    # ── Phase 1: 마스터 ──────────────────────────────────────
    if not test_mode and _db.table_has_data("us_master", collected_date):
        log.info("⏭️  [1/5] us_master 이미 존재하여 로드합니다.")
        master_df = _db.load_latest("us_master")
        if master_df.empty:
            log.error("마스터 로드 실패 — 수집 중단")
            return
    else:
        log.info("[1/5] 종목 마스터 수집...")
        master_df = collect_us_master(test_mode=test_mode)
        if master_df.empty:
            log.error("마스터 수집 실패 — 수집 중단")
            return
        _db.save_df(master_df, "us_master", collected_date)
    tickers = master_df["ticker"].tolist()
    log.info("대상 종목: %d개", len(tickers))

    # ── Phase 2: 일별 시세 ───────────────────────────────────
    _daily_cached = not test_mode and _db.table_has_data("us_daily", collected_date)
    if _daily_cached:
        log.info("⏭️  [2/5] us_daily 이미 존재하여 로드합니다.")
        daily_df = _db.load_latest("us_daily")
    else:
        log.info("[2/5] 일별 시세 수집...")
        daily_df = collect_us_daily(tickers)
    # daily는 Phase 3 daily_patch로 보완 후 저장

    # ── 페니스톡 / 거래 없는 종목 사전 필터 (test_mode 제외) ──
    if not test_mode and not daily_df.empty:
        _before = len(tickers)
        _valid = set(
            row["ticker"] for _, row in daily_df.iterrows()
            if pd.notna(row.get("close")) and float(row.get("close", 0) or 0) >= config.US_MIN_PRICE
        )
        tickers = [t for t in tickers if t in _valid]
        _removed = _before - len(tickers)
        if _removed:
            log.info("페니스톡/거래없음 필터: %d종목 제거 (주가 < $%.1f 또는 NaN)", _removed, config.US_MIN_PRICE)

    # ── Phase 3: 재무제표 + 지표 + 주식수 ───────────────────
    if not daily_only:
        _fs_cached = not test_mode and _db.table_has_data("us_financial_statements", collected_date)
        _ind_cached = not test_mode and _db.table_has_data("us_indicators", collected_date)
        _sh_cached = not test_mode and _db.table_has_data("us_shares", collected_date)

        if _daily_cached and _fs_cached and _ind_cached and _sh_cached:
            log.info("⏭️  [3/5] 재무/지표/주식수 이미 존재하여 수집 건너뜀")
        else:
            log.info("[3/5] 재무제표/지표/주식수 병렬 수집...")
            fs_df, ind_df, shares_df, patches_df = parallel_collect_us(
                tickers, progress_cb=progress_cb
            )

            # daily_df에 patches_df (name, market_cap, eps, bps, dps) 병합 (신규 수집 시에만)
            if not _daily_cached and not patches_df.empty and not daily_df.empty:
                patch_cols = ["ticker", "name", "market_cap", "shares_outstanding", "eps", "bps", "dps"]
                patch_cols = [c for c in patch_cols if c in patches_df.columns]
                daily_df = daily_df.drop(
                    columns=[c for c in patch_cols if c != "ticker" and c in daily_df.columns],
                    errors="ignore"
                )
                daily_df = daily_df.merge(patches_df[patch_cols], on="ticker", how="left")

            if not _daily_cached:
                _db.save_df(daily_df, "us_daily", collected_date)
            if not _fs_cached and not fs_df.empty:
                _db.save_df(fs_df, "us_financial_statements", collected_date)
            if not _ind_cached and not ind_df.empty:
                _db.save_df(ind_df, "us_indicators", collected_date)
            if not _sh_cached and not shares_df.empty:
                _db.save_df(shares_df, "us_shares", collected_date)
    else:
        log.info("[3/5] daily_only 모드 — 재무제표 수집 건너뜀")
        if not _daily_cached:
            _db.save_df(daily_df, "us_daily", collected_date)

    # ── Phase 4: 주가 히스토리 ───────────────────────────────
    if not skip_price_history:
        if not test_mode and _db.table_has_data("us_price_history", collected_date):
            log.info("⏭️  [4/5] us_price_history 이미 존재하여 수집 건너뜀")
        else:
            log.info("[4/5] 주가 히스토리 수집 (2년)...")
            ph_df = collect_us_price_history(tickers)
            if not ph_df.empty:
                _db.save_df(ph_df, "us_price_history", collected_date)
    else:
        log.info("[4/5] 주가 히스토리 건너뜀")

    # ── Phase 5: 지수 히스토리 ───────────────────────────────
    if not test_mode and _db.table_has_data("us_index_history", collected_date):
        log.info("⏭️  [5/5] us_index_history 이미 존재하여 수집 건너뜀")
    else:
        log.info("[5/5] 시장 지수 히스토리 수집...")
        idx_df = collect_us_index_history()
        if not idx_df.empty:
            _db.save_df(idx_df, "us_index_history", collected_date)

    elapsed = (datetime.now() - start_time).total_seconds()
    log.info("=== US 수집 완료 (%.0f초) ===", elapsed)


# ─────────────────────────────────────────────
# CLI 직접 실행 (테스트용)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description="US Stock Collector")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (3종목)")
    parser.add_argument("--daily-only", action="store_true", help="일별 데이터만 수집")
    parser.add_argument("--skip-price-history", action="store_true", help="주가 히스토리 건너뜀")
    args = parser.parse_args()
    run_full(
        test_mode=args.test,
        daily_only=args.daily_only,
        skip_price_history=args.skip_price_history,
    )
