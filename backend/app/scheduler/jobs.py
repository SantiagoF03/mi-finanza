from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.services.orchestrator import run_cycle

scheduler = BackgroundScheduler(job_defaults={"coalesce": True, "max_instances": 1})


def scheduled_cycle():
    db = SessionLocal()
    try:
        run_cycle(db, source="scheduler")
    finally:
        db.close()


def start_scheduler() -> None:
    settings = get_settings()
    if not settings.scheduler_enabled or scheduler.running:
        return
    scheduler.add_job(
        scheduled_cycle,
        "interval",
        days=settings.analysis_frequency_days,
        id="analysis_cycle",
        replace_existing=True,
        misfire_grace_time=120,
    )
    scheduler.start()
