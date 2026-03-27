#!/usr/bin/env python3
"""
Entry point for the Quant service.

Usage:
    python run.py server                            – Start web server + batch scheduler
    python run.py pipeline                          – Full pipeline (collect + screen)
    python run.py pipeline --test                   – Test mode (3 sample stocks)
    python run.py pipeline --skip-collect           – Screen only (CSV data must exist)
    python run.py pipeline --skip-investor          – Skip investor trading collection
    python run.py pipeline --daily-only             – Daily data only + screen (fast, no FnGuide)
    python run.py update-prices                     – Update stock prices only (fast)
    python run.py collect                           – Run collector only
    python run.py collect --daily-only              – Collect daily data only (fast, no FnGuide)
    python run.py collect --skip-investor           – Collect without investor trading
    python run.py screen                            – Run screener only
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def cmd_server(args):
    from webapp.app import app
    import config

    import db as _db
    _db.init_db()

    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG, threaded=True)


def cmd_pipeline(args):
    from pipeline import run_pipeline
    run_pipeline(
        skip_collect=args.skip_collect,
        test_mode=args.test,
        skip_price_history=args.skip_price_history,
        skip_investor=args.skip_investor,
        daily_only=args.daily_only,
    )


def cmd_collect(args):
    from quant_collector_enhanced import run_full
    run_full(test_mode=args.test, skip_price_history=args.skip_price_history, skip_investor=args.skip_investor, daily_only=args.daily_only)


def cmd_update_prices(args):
    from pipeline import run_update_prices
    run_update_prices()


def cmd_screen(args):
    from quant_screener import run
    run()


def cmd_us_collect(args):
    from us_collector import run_full
    run_full(
        test_mode=args.test,
        skip_price_history=args.skip_price_history,
        daily_only=args.daily_only,
    )


def cmd_us_pipeline(args):
    from us_pipeline import run_pipeline
    run_pipeline(
        skip_collect=args.skip_collect,
        test_mode=args.test,
        skip_price_history=args.skip_price_history,
        daily_only=args.daily_only,
    )


def cmd_us_screen(args):
    from us_screener import run
    run()


def main():
    parser = argparse.ArgumentParser(description="Quant Service - KOSPI/KOSDAQ analysis")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("server", help="Start web server (manual pipeline via UI)")

    p_pipe = sub.add_parser("pipeline", help="Full pipeline (collect + screen)")
    p_pipe.add_argument("--test", action="store_true", help="Test mode (3 stocks only)")
    p_pipe.add_argument("--skip-collect", action="store_true", help="Skip collection, screen only")
    p_pipe.add_argument("--daily-only", action="store_true", help="일간 데이터만 수집 후 스크리닝 (daily, price_history, index_history, investor_trading)")
    p_pipe.add_argument("--skip-price-history", action="store_true", help="Skip price history collection (faster, but no technical indicators)")
    p_pipe.add_argument("--skip-investor", action="store_true", help="Skip investor trading collection (외국인/기관 매매동향)")

    p_col = sub.add_parser("collect", help="Run data collector only")
    p_col.add_argument("--test", action="store_true", help="Test mode (3 stocks only)")
    p_col.add_argument("--daily-only", action="store_true", help="일간 데이터만 수집 (daily, price_history, index_history, investor_trading)")
    p_col.add_argument("--skip-price-history", action="store_true", help="Skip price history collection")
    p_col.add_argument("--skip-investor", action="store_true", help="Skip investor trading collection (외국인/기관 매매동향)")

    sub.add_parser("update-prices", help="Update stock prices only (fast, no FnGuide crawling)")

    sub.add_parser("screen", help="Run screener only (requires existing CSVs)")

    p_us = sub.add_parser("us-collect", help="Collect US stock data (Russell 3000 base universe)")
    p_us.add_argument("--test", action="store_true", help="Test mode (AAPL, MSFT, GOOGL only)")
    p_us.add_argument("--daily-only", action="store_true", help="Daily data only (no financial statements)")
    p_us.add_argument("--skip-price-history", action="store_true", help="Skip price history collection")

    p_usp = sub.add_parser("us-pipeline", help="Full US pipeline (collect + screen)")
    p_usp.add_argument("--test", action="store_true", help="Test mode (AAPL, MSFT, GOOGL only)")
    p_usp.add_argument("--skip-collect", action="store_true", help="Skip collection, screen only")
    p_usp.add_argument("--daily-only", action="store_true", help="Daily data only")
    p_usp.add_argument("--skip-price-history", action="store_true", help="Skip price history collection")

    sub.add_parser("us-screen", help="Run US screener only (requires existing US data)")

    args = parser.parse_args()

    commands = {
        "server": cmd_server,
        "pipeline": cmd_pipeline,
        "collect": cmd_collect,
        "update-prices": cmd_update_prices,
        "screen": cmd_screen,
        "us-collect": cmd_us_collect,
        "us-pipeline": cmd_us_pipeline,
        "us-screen": cmd_us_screen,
    }
    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
