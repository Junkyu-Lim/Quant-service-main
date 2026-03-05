# =========================================================
# db.py  —  DuckDB 데이터베이스 헬퍼
# ---------------------------------------------------------
# quant.duckdb 단일 파일로 모든 수집/스크리닝 데이터를 관리.
# collected_date 컬럼으로 날짜별 버전 관리 (기존 CSV 파일명 대체).
# SQLite → DuckDB 마이그레이션: 컬럼형 스토리지로 집계 쿼리 성능 향상.
# =========================================================

import logging
from contextlib import contextmanager

import duckdb
import pandas as pd

import config

log = logging.getLogger("DB")

# ─────────────────────────────────────────────
# 테이블 스키마
# ─────────────────────────────────────────────
_SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS master (
    종목코드      TEXT NOT NULL,
    종목명        TEXT,
    시장구분      TEXT,
    종목구분      TEXT,
    collected_date TEXT NOT NULL,
    PRIMARY KEY (종목코드, collected_date)
)""",
    """CREATE TABLE IF NOT EXISTS daily (
    종목코드      TEXT NOT NULL,
    종목명        TEXT,
    종가          DOUBLE,
    시가총액      DOUBLE,
    상장주식수    DOUBLE,
    EPS           DOUBLE,
    BPS           DOUBLE,
    주당배당금    DOUBLE,
    기준일        TEXT,
    collected_date TEXT NOT NULL,
    PRIMARY KEY (종목코드, collected_date)
)""",
    """CREATE TABLE IF NOT EXISTS financial_statements (
    종목코드      TEXT NOT NULL,
    기준일        TEXT,
    계정          TEXT,
    주기          TEXT,
    값            DOUBLE,
    추정치        INTEGER,
    collected_date TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS indicators (
    종목코드      TEXT NOT NULL,
    기준일        TEXT,
    지표구분      TEXT,
    계정          TEXT,
    값            DOUBLE,
    collected_date TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS shares (
    종목코드      TEXT NOT NULL,
    기준일        TEXT,
    발행주식수    BIGINT,
    자사주        BIGINT,
    유통주식수    BIGINT,
    섹터          TEXT,
    collected_date TEXT NOT NULL,
    PRIMARY KEY (종목코드, collected_date)
)""",
    """CREATE TABLE IF NOT EXISTS price_history (
    종목코드      TEXT NOT NULL,
    날짜          TEXT NOT NULL,
    시가          DOUBLE,
    고가          DOUBLE,
    저가          DOUBLE,
    종가          DOUBLE,
    거래량        DOUBLE,
    거래대금      DOUBLE,
    collected_date TEXT NOT NULL,
    PRIMARY KEY (종목코드, 날짜, collected_date)
)""",
    "CREATE INDEX IF NOT EXISTS idx_fs_code_date ON financial_statements (종목코드, collected_date)",
    "CREATE INDEX IF NOT EXISTS idx_ind_code_date ON indicators (종목코드, collected_date)",
    "CREATE INDEX IF NOT EXISTS idx_ph_code_date ON price_history (종목코드, collected_date)",
    """CREATE TABLE IF NOT EXISTS investor_trading (
    종목코드      TEXT NOT NULL,
    날짜          TEXT NOT NULL,
    외국인순매수  DOUBLE,
    기관순매수    DOUBLE,
    개인순매수    DOUBLE,
    collected_date TEXT NOT NULL,
    PRIMARY KEY (종목코드, 날짜, collected_date)
)""",
    "CREATE INDEX IF NOT EXISTS idx_inv_code_date ON investor_trading (종목코드, collected_date)",
    """CREATE TABLE IF NOT EXISTS index_history (
    지수코드      TEXT NOT NULL,
    날짜          TEXT NOT NULL,
    종가          DOUBLE,
    collected_date TEXT NOT NULL,
    PRIMARY KEY (지수코드, 날짜, collected_date)
)""",
    "CREATE INDEX IF NOT EXISTS idx_idx_code_date ON index_history (지수코드, collected_date)",
    """CREATE TABLE IF NOT EXISTS analysis_reports (
    종목코드      TEXT NOT NULL,
    종목명        TEXT,
    report_html   TEXT,
    scores_json   TEXT,
    model_used    TEXT,
    generated_date TEXT NOT NULL,
    PRIMARY KEY (종목코드)
)""",
    """CREATE TABLE IF NOT EXISTS portfolio (
    종목코드      TEXT PRIMARY KEY,
    수량          INTEGER NOT NULL DEFAULT 0,
    평균매입가    DOUBLE NOT NULL DEFAULT 0,
    매입일        TEXT,
    메모          TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS price_supplement (
    종목코드      TEXT PRIMARY KEY,
    종목명        TEXT,
    종목구분      TEXT,
    시장구분      TEXT,
    현재가        DOUBLE,
    전일대비      DOUBLE,
    등락률        DOUBLE,
    updated_at    TEXT
)""",
    """CREATE TABLE IF NOT EXISTS analysis_reports_history (
    id             INTEGER PRIMARY KEY,
    종목코드       TEXT NOT NULL,
    종목명         TEXT,
    report_html    TEXT,
    scores_json    TEXT,
    model_used     TEXT,
    generated_date TEXT NOT NULL
)""",
    "CREATE INDEX IF NOT EXISTS idx_history_code ON analysis_reports_history (종목코드, generated_date DESC)",
    """CREATE SEQUENCE IF NOT EXISTS seq_report_history START 1""",
    """CREATE TABLE IF NOT EXISTS portfolio_analysis (
    id              INTEGER PRIMARY KEY,
    report_html     TEXT,
    scores_json     TEXT,
    portfolio_hash  TEXT,
    model_used      TEXT,
    generated_date  TEXT NOT NULL,
    saved_at        TEXT
)""",
    """CREATE SEQUENCE IF NOT EXISTS seq_portfolio_analysis START 1""",
    """CREATE TABLE IF NOT EXISTS portfolio_cash (
    id         INTEGER PRIMARY KEY DEFAULT 1,
    amount     DOUBLE NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS dashboard_result (
    종목코드      TEXT PRIMARY KEY,
    종목명        TEXT,
    종가          DOUBLE,
    시가총액      DOUBLE,
    상장주식수    DOUBLE,
    TTM_매출      DOUBLE,
    TTM_순이익    DOUBLE,
    TTM_영업CF    DOUBLE,
    TTM_CAPEX     DOUBLE,
    TTM_FCF       DOUBLE,
    자본          DOUBLE,
    부채          DOUBLE,
    PER           DOUBLE,
    PBR           DOUBLE,
    "ROE(%)"      DOUBLE,
    "부채비율(%)" DOUBLE,
    "영업이익률(%)" DOUBLE,
    "배당수익률(%)" DOUBLE,
    EPS           DOUBLE,
    BPS           DOUBLE,
    DPS_최근      DOUBLE,
    PSR           DOUBLE,
    PEG           DOUBLE,
    "이익수익률(%)" DOUBLE,
    "FCF수익률(%)" DOUBLE,
    "현금전환율(%)" DOUBLE,
    "CAPEX비율(%)" DOUBLE,
    이익품질_양호 INTEGER,
    부채상환능력  DOUBLE,
    F스코어       DOUBLE,
    F1_수익성     INTEGER,
    F2_영업CF     INTEGER,
    F3_ROA개선    INTEGER,
    F4_이익품질   INTEGER,
    F5_레버리지   INTEGER,
    F6_유동성     INTEGER,
    F7_희석없음   INTEGER,
    "F8_매출총이익률" INTEGER,
    F9_자산회전율 INTEGER,
    적정주가_SRIM DOUBLE,
    "괴리율(%)"   DOUBLE,
    S_PER         DOUBLE,
    S_PBR         DOUBLE,
    S_ROE         DOUBLE,
    S_매출CAGR    DOUBLE,
    S_영업이익CAGR DOUBLE,
    S_순이익CAGR  DOUBLE,
    S_연속성장    DOUBLE,
    S_이익률개선  DOUBLE,
    S_배당수익률  DOUBLE,
    S_배당연속증가 DOUBLE,
    S_괴리율      DOUBLE,
    S_F스코어     DOUBLE,
    S_FCF수익률   DOUBLE,
    종합점수      DOUBLE,
    매출_CAGR     DOUBLE,
    영업이익_CAGR DOUBLE,
    순이익_CAGR   DOUBLE,
    매출_연속성장 DOUBLE,
    영업이익_연속성장 DOUBLE,
    순이익_연속성장 DOUBLE,
    이익률_변동폭 DOUBLE,
    배당_연속증가 DOUBLE,
    데이터_연수   INTEGER,
    순이익_전년음수 INTEGER,
    순이익_당기양수 INTEGER,
    PER_이상      INTEGER,
    시장구분      TEXT,
    종목구분      TEXT,
    RS_60d        DOUBLE,
    RS_120d       DOUBLE,
    RS_250d       DOUBLE,
    Composite_RS  DOUBLE,
    "RS_등급"     DOUBLE,
    "스마트머니_승률" DOUBLE,
    "양매수_비율" DOUBLE,
    VCP_신호      INTEGER,
    "영업이익_가속도" DOUBLE,
    "매출_가속도" DOUBLE,
    "실적가속_연속" INTEGER,
    "GPM_최근(%)" DOUBLE,
    "GPM_전년(%)" DOUBLE,
    "GPM_변화(pp)" DOUBLE,
    "ROIC(%)"     DOUBLE,
    "ROIC_전년(%)" DOUBLE,
    ROIC_개선     INTEGER,
    "퀄리티_턴어라운드" INTEGER,
    섹터          TEXT
)""",
]


# ─────────────────────────────────────────────
# 연결
# ─────────────────────────────────────────────

@contextmanager
def get_conn():
    """DuckDB 연결 컨텍스트 매니저 — with get_conn() as conn: 패턴으로 사용"""
    conn = duckdb.connect(str(config.DB_PATH))
    try:
        conn.begin()
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        for stmt in _SCHEMA_STATEMENTS:
            conn.execute(stmt)
        # portfolio_analysis 테이블 마이그레이션: saved_at 컬럼 추가
        try:
            cols = [r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='portfolio_analysis'"
            ).fetchall()]
            if "saved_at" not in cols:
                conn.execute("ALTER TABLE portfolio_analysis ADD COLUMN saved_at TEXT")
                log.info("portfolio_analysis: saved_at 컬럼 추가")
        except Exception as e:
            log.warning("portfolio_analysis 마이그레이션 실패: %s", e)
    log.info("DB 초기화 완료: %s", config.DB_PATH)


# ─────────────────────────────────────────────
# 쓰기
# ─────────────────────────────────────────────

def table_has_data(table: str, collected_date: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE collected_date = ?",
            [collected_date],
        )
        return cur.fetchone()[0] > 0


def _insert_df(conn, df: pd.DataFrame, table: str):
    """DataFrame을 DuckDB 테이블에 삽입"""
    cols = ", ".join(df.columns)
    conn.register("_insert_tmp", df)
    conn.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _insert_tmp")
    conn.unregister("_insert_tmp")


def save_df(df: pd.DataFrame, table: str, collected_date: str):
    if df.empty:
        return
    data = df.copy()
    data["collected_date"] = collected_date

    # Timestamp → "YYYY-MM-DD" 문자열 변환
    for col in data.columns:
        if pd.api.types.is_datetime64_any_dtype(data[col]):
            data[col] = data[col].dt.strftime("%Y-%m-%d")

    with get_conn() as conn:
        conn.execute(
            f"DELETE FROM {table} WHERE collected_date = ?",
            [collected_date],
        )
        _insert_df(conn, data, table)

    log.info("저장: %s (%d건, date=%s)", table, len(data), collected_date)


def save_dashboard(df: pd.DataFrame):
    if df.empty:
        return
    with get_conn() as conn:
        # 이전 배치 결과 보존: dashboard_result → dashboard_result_prev
        try:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'dashboard_result'"
            ).fetchone()[0]
            if cnt:
                conn.execute("DROP TABLE IF EXISTS dashboard_result_prev")
                conn.execute(
                    "CREATE TABLE dashboard_result_prev AS "
                    "SELECT * FROM dashboard_result"
                )
        except Exception:
            pass  # 최초 실행 시 이전 테이블 없음

        conn.execute("DROP TABLE IF EXISTS dashboard_result")
        conn.register("_dash_tmp", df)
        conn.execute("CREATE TABLE dashboard_result AS SELECT * FROM _dash_tmp")
        conn.unregister("_dash_tmp")
    log.info("저장: dashboard_result (%d건)", len(df))


# ─────────────────────────────────────────────
# 읽기
# ─────────────────────────────────────────────

def load_latest(table: str) -> pd.DataFrame:
    with get_conn() as conn:
        try:
            cur = conn.execute(f"SELECT MAX(collected_date) FROM {table}")
        except Exception:
            return pd.DataFrame()

        row = cur.fetchone()
        if row is None or row[0] is None:
            return pd.DataFrame()

        latest = row[0]
        df = conn.execute(
            f"SELECT * FROM {table} WHERE collected_date = ?",
            [latest],
        ).df()

    if "collected_date" in df.columns:
        df = df.drop(columns=["collected_date"])

    log.info("로드: %s (%d건, date=%s)", table, len(df), latest)
    return df


def load_dashboard() -> pd.DataFrame:
    with get_conn() as conn:
        try:
            df = conn.execute("SELECT * FROM dashboard_result").df()
        except Exception:
            return pd.DataFrame()
    return df


def load_dashboard_prev() -> pd.DataFrame:
    """이전 배치의 dashboard_result를 반환한다."""
    with get_conn() as conn:
        try:
            df = conn.execute("SELECT * FROM dashboard_result_prev").df()
        except Exception:
            return pd.DataFrame()
    return df


# ─────────────────────────────────────────────
# 상태 조회 (webapp용)
# ─────────────────────────────────────────────

def save_report(code: str, name: str, html: str, scores_json: str,
                 model: str, date: str):
    with get_conn() as conn:
        # 기존 보고서가 있으면 히스토리에 보관
        cur = conn.execute(
            "SELECT * FROM analysis_reports WHERE 종목코드 = ?", [code]
        )
        old = cur.fetchone()
        if old is not None:
            old_cols = [d[0] for d in cur.description]
            old_dict = dict(zip(old_cols, old))
            conn.execute(
                """INSERT INTO analysis_reports_history
                   (id, 종목코드, 종목명, report_html, scores_json, model_used, generated_date)
                   VALUES (nextval('seq_report_history'), ?, ?, ?, ?, ?, ?)""",
                [old_dict["종목코드"], old_dict.get("종목명", ""),
                 old_dict.get("report_html", ""), old_dict.get("scores_json", ""),
                 old_dict.get("model_used", ""), old_dict.get("generated_date", "")],
            )
        conn.execute(
            """INSERT OR REPLACE INTO analysis_reports
               (종목코드, 종목명, report_html, scores_json, model_used, generated_date)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [code, name, html, scores_json, model, date],
        )
    log.info("보고서 저장: %s %s (이전 버전 보관)", code, name)


def load_report(code: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM analysis_reports WHERE 종목코드 = ?",
            [code.zfill(6)],
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def list_reports() -> list[dict]:
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "SELECT 종목코드, 종목명, model_used, generated_date "
                "FROM analysis_reports ORDER BY generated_date DESC"
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception:
            return []


def delete_report(code: str):
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM analysis_reports WHERE 종목코드 = ?",
            [code.zfill(6)],
        )


def list_report_history(code: str) -> list[dict]:
    """특정 종목의 이전 분석 보고서 목록 (최신순, 최대 10건)."""
    with get_conn() as conn:
        try:
            cur = conn.execute(
                """SELECT id, 종목코드, 종목명, model_used, generated_date
                   FROM analysis_reports_history
                   WHERE 종목코드 = ?
                   ORDER BY generated_date DESC
                   LIMIT 10""",
                [code.zfill(6)],
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception:
            return []


def load_report_history(history_id: int) -> dict | None:
    """히스토리 ID로 이전 보고서 전체 내용 조회."""
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "SELECT * FROM analysis_reports_history WHERE id = ?",
                [history_id],
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        except Exception:
            return None


def load_stock_financials(code: str, period: str = "annual") -> pd.DataFrame:
    """특정 종목의 연간/분기 재무제표 시계열 (매출액/영업이익/당기순이익, 실적치만)"""
    주기 = "q" if period == "quarter" else "y"
    with get_conn() as conn:
        try:
            row = conn.execute(
                "SELECT MAX(collected_date) FROM financial_statements"
            ).fetchone()
            if not row or not row[0]:
                return pd.DataFrame()
            latest = row[0]
            df = conn.execute(
                """SELECT 기준일, 계정, 값
                   FROM financial_statements
                   WHERE 종목코드 = ?
                   AND collected_date = ?
                   AND 주기 = ?
                   AND 계정 IN ('매출액', '영업이익', '당기순이익')
                   AND 추정치 = 0
                   ORDER BY 기준일""",
                [code.zfill(6), latest, 주기],
            ).df()
        except Exception:
            return pd.DataFrame()
    return df


def load_portfolio() -> list[dict]:
    """포트폴리오 전체 조회"""
    with get_conn() as conn:
        try:
            cur = conn.execute("SELECT * FROM portfolio ORDER BY updated_at DESC")
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception:
            return []


def upsert_portfolio_item(code: str, qty: int, price: float,
                          buy_date: str, memo: str):
    """포트폴리오 항목 추가/수정 (INSERT OR REPLACE)"""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    code = code.zfill(6)
    with get_conn() as conn:
        # 기존 항목이 있으면 created_at 유지
        cur = conn.execute(
            "SELECT created_at FROM portfolio WHERE 종목코드 = ?", [code]
        )
        row = cur.fetchone()
        created = row[0] if row else now
        conn.execute(
            """INSERT OR REPLACE INTO portfolio
               (종목코드, 수량, 평균매입가, 매입일, 메모, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [code, qty, price, buy_date, memo, created, now],
        )
    log.info("포트폴리오 저장: %s (수량=%d, 단가=%.0f)", code, qty, price)


def delete_portfolio_item(code: str):
    """포트폴리오에서 종목 삭제"""
    code = code.zfill(6)
    with get_conn() as conn:
        conn.execute("DELETE FROM portfolio WHERE 종목코드 = ?", [code])
    log.info("포트폴리오 삭제: %s", code)


def load_cash() -> float:
    """예수금(현금) 잔고 조회. 미설정 시 0 반환."""
    with get_conn() as conn:
        row = conn.execute("SELECT amount FROM portfolio_cash WHERE id = 1").fetchone()
        return float(row[0]) if row else 0.0


def save_cash(amount: float):
    """예수금(현금) 잔고 저장."""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO portfolio_cash (id, amount, updated_at)
               VALUES (1, ?, ?)""",
            [max(0.0, float(amount)), now],
        )
    log.info("예수금 저장: %.0f원", amount)


def save_portfolio_analysis(html: str, scores_json: str, portfolio_hash: str,
                            model: str, date: str):
    """포트폴리오 분석 보고서 저장 (최대 5건 이력 유지)"""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        new_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM portfolio_analysis"
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO portfolio_analysis
               (id, report_html, scores_json, portfolio_hash, model_used, generated_date, saved_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [new_id, html, scores_json, portfolio_hash, model, date, now],
        )
        # 5건 초과 시 가장 오래된 것 삭제
        old_ids = conn.execute(
            "SELECT id FROM portfolio_analysis ORDER BY id DESC OFFSET 5"
        ).fetchall()
        if old_ids:
            ids_to_del = [r[0] for r in old_ids]
            conn.execute(
                f"DELETE FROM portfolio_analysis WHERE id IN ({','.join('?' * len(ids_to_del))})",
                ids_to_del,
            )
    log.info("포트폴리오 분석 보고서 저장 완료 (id=%d, model=%s)", new_id, model)


def load_portfolio_analysis() -> dict | None:
    """포트폴리오 분석 보고서 최신 1건 조회"""
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "SELECT * FROM portfolio_analysis ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        except Exception:
            return None


def load_portfolio_analysis_history() -> list[dict]:
    """포트폴리오 분석 이력 목록 조회 (id, generated_date, model_used, saved_at)"""
    with get_conn() as conn:
        try:
            cur = conn.execute(
                """SELECT id, generated_date, model_used, saved_at, portfolio_hash
                   FROM portfolio_analysis ORDER BY id DESC"""
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception:
            return []


def load_portfolio_analysis_by_id(report_id: int) -> dict | None:
    """특정 id의 포트폴리오 분석 보고서 조회"""
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "SELECT * FROM portfolio_analysis WHERE id = ?", [report_id]
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        except Exception:
            return None


def upsert_price_supplement(records: list[dict]):
    """ETF/우선주/리츠 현재가 보조 테이블 upsert"""
    if not records:
        return
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        for r in records:
            conn.execute(
                """INSERT OR REPLACE INTO price_supplement
                   (종목코드, 종목명, 종목구분, 시장구분, 현재가, 전일대비, 등락률, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    str(r.get("종목코드", "")).zfill(6),
                    r.get("종목명"),
                    r.get("종목구분"),
                    r.get("시장구분"),
                    r.get("현재가"),
                    r.get("전일대비"),
                    r.get("등락률"),
                    now,
                ],
            )
    log.info("price_supplement upsert: %d건", len(records))


def load_price_supplement() -> dict:
    """price_supplement 전체를 {종목코드: row_dict} 형태로 반환"""
    with get_conn() as conn:
        try:
            cur = conn.execute("SELECT * FROM price_supplement")
            cols = [d[0] for d in cur.description]
            return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}
        except Exception:
            return {}


def get_stock_info_from_master(code: str) -> dict | None:
    """master 테이블에서 종목명/종목구분/시장구분 조회"""
    code = code.zfill(6)
    with get_conn() as conn:
        try:
            cur = conn.execute(
                """SELECT 종목명, 종목구분, 시장구분
                   FROM master
                   WHERE 종목코드 = ?
                   ORDER BY collected_date DESC
                   LIMIT 1""",
                [code],
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {"종목명": row[0], "종목구분": row[1], "시장구분": row[2]}
        except Exception:
            return None


def load_price_history_multi(codes: list[str], n_days: int = 250) -> pd.DataFrame:
    """여러 종목의 최근 n_days 일간 종가를 Wide 포맷으로 반환.

    반환: DataFrame (날짜 index, 종목코드 columns, 종가 values)
    데이터가 없으면 빈 DataFrame 반환.
    """
    if not codes:
        return pd.DataFrame()
    codes = [c.zfill(6) for c in codes]
    placeholders = ", ".join(["?"] * len(codes))
    with get_conn() as conn:
        try:
            row = conn.execute(
                "SELECT MAX(collected_date) FROM price_history"
            ).fetchone()
            if not row or not row[0]:
                return pd.DataFrame()
            latest = row[0]
            df = conn.execute(
                f"""SELECT 종목코드, 날짜, 종가
                    FROM price_history
                    WHERE 종목코드 IN ({placeholders})
                      AND collected_date = ?
                    ORDER BY 날짜""",
                codes + [latest],
            ).df()
        except Exception:
            return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    pivot = df.pivot_table(index="날짜", columns="종목코드", values="종가", aggfunc="last")
    pivot = pivot.sort_index()
    # 최근 n_days 행만
    if len(pivot) > n_days:
        pivot = pivot.iloc[-n_days:]
    return pivot


def get_data_status() -> dict:
    tables = ["master", "daily", "financial_statements",
              "indicators", "shares", "price_history", "investor_trading",
              "index_history", "dashboard_result"]
    status = {}

    with get_conn() as conn:
        for t in tables:
            try:
                cur = conn.execute(f"SELECT COUNT(*) FROM {t}")
                total = cur.fetchone()[0]
            except Exception:
                continue

            if total == 0:
                continue

            if t == "dashboard_result":
                status[t] = {"rows": total, "collected_date": "-"}
            else:
                cur2 = conn.execute(f"SELECT MAX(collected_date) FROM {t}")
                latest = cur2.fetchone()[0]
                status[t] = {"rows": total, "collected_date": latest}

    return status
