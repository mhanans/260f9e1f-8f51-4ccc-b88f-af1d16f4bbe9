
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app.core.db import engine
from sqlmodel import Session, select
from app.models.all_models import ScanConfig
from app.scheduler.worker import scan_datasource_task
import logging

logger = logging.getLogger(__name__)

class SchedulerService:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        logger.info("Scheduler started.")

    def add_job(self, config_id: int, cron_expression: str, scan_scope: str = "full", timezone: str = "UTC"):
        """Adds or updates a job for a scan config."""
        job_id = f"scan_{config_id}"
        
        # Remove existing if any
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)

        try:
            self.scheduler.add_job(
                scan_datasource_task.delay, # Use Celery delay
                CronTrigger.from_crontab(cron_expression, timezone=timezone),
                id=job_id,
                args=[config_id, scan_scope],
                replace_existing=True
            )
            logger.info(f"Added job {job_id} with cron {cron_expression} (TZ: {timezone})")
        except Exception as e:
            logger.error(f"Failed to add job {job_id}: {e}")

    def remove_job(self, config_id: int):
        job_id = f"scan_{config_id}"
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed job {job_id}")

    def load_jobs_from_db(self):
        """Loads all active schedules from DB on startup."""
        with Session(engine) as session:
            statement = select(ScanConfig).where(ScanConfig.active == True).where(ScanConfig.schedule_cron != None)
            configs = session.exec(statement).all()
            
            for config in configs:
                if config.schedule_cron:
                    # Default to UTC if missing (though model defaults to UTC)
                    tz = config.schedule_timezone if config.schedule_timezone else "UTC"
                    self.add_job(config.id, config.schedule_cron, config.scan_scope, tz)

scheduler_service = SchedulerService()
