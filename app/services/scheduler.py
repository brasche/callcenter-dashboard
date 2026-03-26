from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.config import BLUEROCK_SYNC_INTERVAL, SHEETS_SYNC_INTERVAL

scheduler = AsyncIOScheduler()


def start_scheduler():
    # Import here to avoid circular imports before DB is ready
    from app.services.bluerock import sync_bluerock, poll_agent_status, sync_agent_daily_stats
    from app.services.sheets import sync_sheets

    scheduler.add_job(sync_bluerock,         "interval", seconds=BLUEROCK_SYNC_INTERVAL, id="bluerock_sync",   max_instances=1)
    scheduler.add_job(sync_sheets,           "interval", seconds=SHEETS_SYNC_INTERVAL,   id="sheets_sync",     max_instances=1)
    scheduler.add_job(poll_agent_status,     "interval", seconds=30,                     id="status_poll",     max_instances=1)
    scheduler.add_job(sync_agent_daily_stats,"interval", seconds=300,                    id="daily_stats_sync",max_instances=1)
    scheduler.start()


def stop_scheduler():
    scheduler.shutdown(wait=False)
