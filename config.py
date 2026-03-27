import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# DuckDB database
DB_PATH = DATA_DIR / "quant.duckdb"

# Batch schedule (cron-style)
BATCH_HOUR = int(os.environ.get("BATCH_HOUR", "18"))   # 18시 KST (장 마감 후)
BATCH_MINUTE = int(os.environ.get("BATCH_MINUTE", "0"))

# Web server
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "5000"))
DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

# Claude API (for qualitative analysis reports)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANALYSIS_MODEL = os.environ.get("ANALYSIS_MODEL", "claude-sonnet-4-6")
PORTFOLIO_MODEL = os.environ.get("PORTFOLIO_MODEL", "claude-sonnet-4-6")


REPORT_DIR = DATA_DIR / "reports"
REPORT_DIR.mkdir(exist_ok=True)

# 사용자가 배치하는 사업보고서 PDF (예: 삼성전자_2024Q3.pdf)
# REPORT_DIR(AI 생성 HTML 저장소)와 별개 경로
PDF_REPORT_DIR = BASE_DIR / "report"
PDF_REPORT_DIR.mkdir(exist_ok=True)

# AI 분석 설정
ANALYSIS_TIMEOUT_SEC = float(os.environ.get("ANALYSIS_TIMEOUT_SEC", "720"))
ANALYSIS_MAX_TOKENS = int(os.environ.get("ANALYSIS_MAX_TOKENS", "12000"))
WEB_SEARCH_MAX_USES = int(os.environ.get("WEB_SEARCH_MAX_USES", "2"))

# S-RIM 할인율 설정 (동적 Ke = RISK_FREE_RATE + ERP)
# 환경변수로 재정의 가능: RISK_FREE_RATE=3.5, EQUITY_RISK_PREMIUM=5.5
RISK_FREE_RATE = float(os.environ.get("RISK_FREE_RATE", "3.5"))        # 국고채 3년물 기준 (%)
EQUITY_RISK_PREMIUM = float(os.environ.get("EQUITY_RISK_PREMIUM", "5.5"))  # 시장위험프리미엄 (%)

# ---------------------------------------------------------------------------
# US Stock Settings
# ---------------------------------------------------------------------------
US_RISK_FREE_RATE = float(os.environ.get("US_RISK_FREE_RATE", "4.3"))        # US 10Y Treasury (%)
US_EQUITY_RISK_PREMIUM = float(os.environ.get("US_EQUITY_RISK_PREMIUM", "5.0"))  # US ERP (%)

# US 수집/스크리닝 필터 (페니스톡/마이크로캡 제외)
US_MIN_PRICE = float(os.environ.get("US_MIN_PRICE", "1.0"))               # 최소 주가 ($)
US_MIN_MARKET_CAP = float(os.environ.get("US_MIN_MARKET_CAP", "50000000"))  # 최소 시총 ($50M)

# ---------------------------------------------------------------------------
# ETF 메타데이터 (하드코딩) — AI 포트폴리오 분석 시 활용
# ---------------------------------------------------------------------------
ETF_METADATA = {
    "449450": {
        "name": "PLUS K방산",
        "sector": "방위산업",
        "description": "FnGuide K-방위산업 지수 추종. 국내 방산 핵심 10개 기업 집중 투자",
        "constituents": [
            "한국항공우주(047810, 19%)", "한화에어로스페이스(012450, 19%)",
            "한화오션(042660, 18%)", "현대로템(064350, 17%)",
            "한화시스템(272210, 12%)", "LIG넥스원(079550, 10%)",
            "풍산(103140, 3%)", "STX엔진(077970)", "SNT다이내믹스(003570)",
            "엠앤씨솔루션(484870)",
        ],
    },
    "466920": {
        "name": "SOL 조선TOP3플러스",
        "sector": "조선/해운",
        "description": "FnGuide 조선 TOP3 Plus 지수 추종. 조선 빅3(HD한국조선해양, 한화오션, 삼성중공업) 각 20% + 조선 밸류체인 10종목 40%",
        "constituents": [
            "HD한국조선해양(329180, 20%)", "한화오션(042660, 20%)",
            "삼성중공업(010140, 20%)", "HD현대중공업(329180)",
            "HD현대미포(010620)", "HD현대마린솔루션(443060)",
            "한화엔진(082740)", "동성화인텍(033500)",
            "세진중공업(075580)", "하이록코리아(013030)",
        ],
    },
    "487240": {
        "name": "KODEX AI전력핵심설비",
        "sector": "전력/에너지",
        "description": "AI 전력 인프라 핵심 설비 기업 집중 투자 ETF. 삼성자산운용",
        "constituents": [],
    },
    "455850": {
        "name": "SOL AI반도체소부장",
        "sector": "반도체",
        "description": "AI 반도체 소재·부품·장비 기업 투자 ETF. 신한자산운용",
        "constituents": [],
    },
    "0072R0": {
        "name": "TIGER KRX금현물",
        "sector": "원자재/금",
        "description": "KRX 금현물 지수 추종 ETF. 미래에셋자산운용",
        "constituents": [],
    },
}
