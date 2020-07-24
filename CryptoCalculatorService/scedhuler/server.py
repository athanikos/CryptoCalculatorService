from apscheduler.schedulers import SchedulerAlreadyRunningError
from pytz import utc
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

jobstores = {
    'mongo': MongoDBJobStore()
}
executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(5)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 1
}
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)


def start(cs):
    scheduler.add_job(cs.synchronize_transactions,'cron', second='*/5')
    try:
        scheduler.start()
    except SchedulerAlreadyRunningError:
        pass #log?


def stop():
    scheduler.shutdown()