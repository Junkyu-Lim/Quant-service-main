#!/usr/bin/env python3
"""
Entry point for the Quant service.

Usage:
    python run.py server                            – Start web server + batch scheduler
    python run.py pipeline                          – Full pipeline (collect + screen)
    python run.py pipeline --test                   – Test mode (3 sample stocks)
    python run.py pipeline --skip-collect           – Screen only (CSV data must exist)
    python run.py pipeline --skip-investor          – Skip investor trading collection
    python run.py collect                           – Run collector only
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
    )


def cmd_collect(args):
    from quant_collector_enhanced import run_full
    run_full(test_mode=args.test, skip_price_history=args.skip_price_history, skip_investor=args.skip_investor)


def cmd_screen(args):
    from quant_screener import run
    run()


def main():
    parser = argparse.ArgumentParser(description="Quant Service - KOSPI/KOSDAQ analysis")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("server", help="Start web server (manual pipeline via UI)")

    p_pipe = sub.add_parser("pipeline", help="Full pipeline (collect + screen)")
    p_pipe.add_argument("--test", action="store_true", help="Test mode (3 stocks only)")
    p_pipe.add_argument("--skip-collect", action="store_true", help="Skip collection, screen only")
    p_pipe.add_argument("--skip-price-history", action="store_true", help="Skip price history collection (faster, but no technical indicators)")
    p_pipe.add_argument("--skip-investor", action="store_true", help="Skip investor trading collection (외국인/기관 매매동향)")

    p_col = sub.add_parser("collect", help="Run data collector only")
    p_col.add_argument("--test", action="store_true", help="Test mode (3 stocks only)")
    p_col.add_argument("--skip-price-history", action="store_true", help="Skip price history collection")
    p_col.add_argument("--skip-investor", action="store_true", help="Skip investor trading collection (외국인/기관 매매동향)")

    sub.add_parser("screen", help="Run screener only (requires existing CSVs)")

    args = parser.parse_args()

    commands = {
        "server": cmd_server,
        "pipeline": cmd_pipeline,
        "collect": cmd_collect,
        "screen": cmd_screen,
    }
    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
