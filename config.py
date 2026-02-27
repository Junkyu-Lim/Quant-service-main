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

# Gemini API (for AI analysis)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_RESEARCH_MODEL = os.environ.get("GEMINI_RESEARCH_MODEL", "gemini-2.0-flash")

REPORT_DIR = DATA_DIR / "reports"
REPORT_DIR.mkdir(exist_ok=True)

# S-RIM 할인율 설정 (동적 Ke = RISK_FREE_RATE + ERP)
# 환경변수로 재정의 가능: RISK_FREE_RATE=3.5, EQUITY_RISK_PREMIUM=5.5
RISK_FREE_RATE = float(os.environ.get("RISK_FREE_RATE", "3.5"))        # 국고채 3년물 기준 (%)
EQUITY_RISK_PREMIUM = float(os.environ.get("EQUITY_RISK_PREMIUM", "5.5"))  # 시장위험프리미엄 (%)
