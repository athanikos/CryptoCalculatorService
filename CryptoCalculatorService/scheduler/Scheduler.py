from datetime import datetime
import jsonpickle
from apscheduler.schedulers import SchedulerAlreadyRunningError
from apscheduler.schedulers.base import STATE_RUNNING
from calculator.BalanceCalculator import BalanceCalculator
from cryptodataaccess.Rates.RatesMongoStore import RatesMongoStore
from cryptodataaccess.Rates.RatesRepository import RatesRepository
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptomodel.operations import OPERATIONS
from flask import jsonify
from kafkaHelper.kafkaHelper import produce, consume
from pytz import utc
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from CryptoCalculatorService.helpers import log_error, log_info
from CryptoCalculatorService.BalanceService import DEFAULT_CURRENCY, DATE_FORMAT, PROJECT_NAME, BalanceService
from CryptoCalculatorService.scheduler.ScheduledJobCreator import ScheduledJobCreator

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

'''
Schedules jobs to Sync transactions and notifications
               to compute balance/prices save and produce  

'''


class Scheduler:

    def __init__(self, config, run_forever=True, consumer_time_out=5000):
        self.rates_store = RatesMongoStore(config, log_error)
        self.users_store = UsersMongoStore(config, log_error)
        self.trans_store = TransactionMongoStore(config, log_error)

        self.rates_repo = RatesRepository(self.rates_store)
        self.trans_repo = TransactionRepository(self.trans_store)
        self.users_repo = UsersRepository(self.users_store)
        self.bs = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults,
                                      timezone=utc)

        self.run_forever = run_forever
        self.consumer_time_out = consumer_time_out
        self.ran_once = False
        self.scheduled_job_creator = ScheduledJobCreator()

        self.balance_service = BalanceService(config)

    def start(self):
        self.bs.remove_all_jobs()
        self.bs.add_job(func=self.balance_service.synchronize_transactions_and_user_notifications, trigger='cron', second='*/59')
        self.bs.add_job(func=self.balance_service.schedule_user_notifications(), trigger='cron', second='*/59')


        try:
            self.bs.start()
        except SchedulerAlreadyRunningError:
            pass  # log?

    def stop(self):
        if self.bs.state == STATE_RUNNING:
            self.bs.shutdown()


