"""
Batch scheduler using APScheduler.

Supports:
  - Scheduled daily pipeline run (after KRX market close, configurable).
  - On-demand trigger via function call.
"""

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import config
from pipeline import run_pipeline

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _daily_job():
    """Wrapper executed by the scheduler."""
    logger.info("Scheduled daily pipeline starting …")
    try:
        run_pipeline()
    except Exception:
        logger.exception("Scheduled pipeline run failed")


def start_scheduler() -> BackgroundScheduler:
    """Start the background scheduler with the daily pipeline job."""
    global _scheduler
    if _scheduler and _scheduler.running:
        logger.info("Scheduler already running")
        return _scheduler

    _scheduler = BackgroundScheduler(timezone="Asia/Seoul")
    _scheduler.add_job(
        _daily_job,
        trigger=CronTrigger(
            hour=config.BATCH_HOUR,
            minute=config.BATCH_MINUTE,
            timezone="Asia/Seoul",
        ),
        id="daily_pipeline",
        name="Daily KOSPI/KOSDAQ pipeline",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info(
        "Scheduler started – daily pipeline at %02d:%02d KST",
        config.BATCH_HOUR,
        config.BATCH_MINUTE,
    )
    return _scheduler


def stop_scheduler():
    """Gracefully shut down the scheduler."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
        _scheduler = None


def get_scheduler() -> BackgroundScheduler | None:
    return _scheduler
