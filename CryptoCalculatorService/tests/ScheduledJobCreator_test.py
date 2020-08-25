from datetime import datetime

import apscheduler
from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from cryptomodel.cryptostore import user_notification
from pytz import utc

from CryptoCalculatorService.BalanceService import BalanceService
from CryptoCalculatorService.config import configure_app
from CryptoCalculatorService.scheduler.ScheduledJobCreator import ScheduledJobCreator


def test_add_job():
    background_scheduler, bs, creator = setup_scheduler()

    user_not = user_notification()
    user_not.user_id = 1
    user_not.user_name = "name"
    user_not.user_email = "email"
    user_not.notification_type = "BALANCE"
    user_not.check_every = "00:01"
    user_not.start_date = datetime.now()
    user_not.end_date = datetime.now()
    user_not.is_active = False
    user_not.channel_type = "TELEGRAM"
    user_not.threshold_value = 1
    user_not.operation = "ADDED"
    creator.add_job(background_scheduler=background_scheduler, user_notification=user_not, balance_service=bs)
    assert (len(background_scheduler.get_jobs()) == 1)


def test_add_job_with_empty_check():
    background_scheduler, bs, creator = setup_scheduler()

    user_not = user_notification()
    user_not.user_id = 1
    user_not.user_name = "name"
    user_not.user_email = "email"
    user_not.notification_type = "BALANCE"
    user_not.check_every = "00:00"
    user_not.start_date = datetime.now()
    user_not.end_date = datetime.now()
    user_not.is_active = False
    user_not.channel_type = "TELEGRAM"
    user_not.threshold_value = 1
    user_not.operation = "ADDED"
    creator.add_job(background_scheduler=background_scheduler, user_notification=user_not, balance_service=bs)
    assert (len(background_scheduler.get_jobs()) == 1)
    assert (apscheduler.triggers.date.DateTrigger == type(background_scheduler.get_jobs()[0].trigger))


def setup_scheduler():
    config = configure_app()
    creator = ScheduledJobCreator()
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
    background_scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults,
                                               timezone=utc)
    bs = BalanceService(config)
    return background_scheduler, bs, creator
