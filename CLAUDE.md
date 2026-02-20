# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**👉 For detailed documentation, please refer to [README.md](README.md). This file is kept for quick reference only.**

## Quick Reference

### Commands

```bash
# Install
pip install -r requirements.txt

# Test (3 stocks)
python run.py pipeline --test

# Full pipeline
python run.py pipeline

# Web server
python run.py server

# Production
gunicorn -w 4 -b 0.0.0.0:5000 webapp.app:app
```

### Key Modules

| Module | Purpose |
|--------|---------|
| `db.py` | DuckDB database (`data/quant.duckdb`). Tables: master, daily, financial_statements, indicators, shares, price_history, dashboard_result, analysis_reports |
| `quant_collector_enhanced.py` | Data collection (FnGuide, KRX, FinanceDataReader) via ThreadPoolExecutor |
| `quant_screener.py` | Screening engine (TTM, CAGR, S-RIM, F-Score, technical indicators, 6 strategies) |
| `webapp/app.py` | Flask REST API + dashboard |
| `config.py` | Config (DB_PATH, BATCH_HOUR, HOST, PORT, ANTHROPIC_API_KEY) |
| `analysis/claude_analyzer.py` | AI analysis reports (Claude API, 5 investment gurus framework) |

### Critical Notes for Code Changes

⚠️ **Screening Consistency**: Filter logic exists in TWO places:
1. `quant_screener.py` (screening engine)
2. `webapp/app.py` → `_apply_screen_filter()`

**Update BOTH when changing screening criteria!**

### Important Patterns

- **Stock codes**: Always 6-digit zero-padded (`zfill(6)`)
- **Unit multiplier**: Inferred from Samsung (005930) revenue
- **Encoding**: Auto-detect cp949/euc-kr/utf-8 for Korean data
- **DB versioning**: Use `collected_date` column (replaces dated CSV filenames)
- **Scoring**: Percentile-based with strategy-specific weight vectors in `quant_screener.py`
- **Dashboard backup**: `save_dashboard()` auto-backs up previous batch to `dashboard_result_prev`

### Config

```python
# Environment variables
BATCH_HOUR=18              # Batch run time (KST)
BATCH_MINUTE=0
HOST=0.0.0.0
PORT=5000
DEBUG=false
ANTHROPIC_API_KEY=sk-ant-...  # Optional (AI analysis)
ANALYSIS_MODEL=claude-sonnet-4-5-20250929
```

---

**See README.md for full documentation.**

Last updated: 2026-02-18
