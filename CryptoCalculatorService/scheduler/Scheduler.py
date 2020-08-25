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
Schedules job to Sync transactions and notifications
Schedules job based on the notifications received 

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

    def start(self):
        self.bs.remove_all_jobs()
        self.bs.add_job(func=self.synchronize_transactions_and_user_notifications, trigger='cron', second='*/59')
        self.schedule_user_notifications()
        try:
            self.bs.start()
        except SchedulerAlreadyRunningError:
            pass  # log?

    def stop(self):
        if self.bs.state == STATE_RUNNING:
            self.bs.shutdown()

    def get_transactions(self, user_id):
        return jsonify(self.trans_repo.get_transactions(user_id).to_json())

    def insert_transaction(self, user_id, volume, symbol, value, price, date, source):
        trans = self.trans_repo.add_transaction(user_id=user_id, volume=volume, symbol=symbol, value=value,
                                                price=price,
                                                date=date, source=source, currency=DEFAULT_CURRENCY
                                                , source_id=None)  # fix get from user_settings
        self.trans_repo.commit()
        return trans

    def update_transaction(self, id, user_id, volume, symbol, value, price, date, source):
        trans = self.trans_repo.update_transaction(id, user_id, volume, symbol, value, price, DEFAULT_CURRENCY, date,
                                                   source,
                                                   source_id=id)  # fix get from user_settings
        self.trans_repo.commit()
        return trans

    def synchronize_transactions_and_user_notifications(self):
        while self.ran_once == False or self.run_forever:
            try:
                self.delete_and_insert_transactions(self.consume_transactions())
            except Exception as e:
                log_error(e, self.users_store.configuration)

            try:
                self.delete_and_insert_notifications(self.consume_notifications())
            except Exception as e:
                log_error(e, self.users_store.configuration)

            self.ran_once = True

    def consume_transactions(self):
        transactions = consume(topic=self.trans_store.configuration.TRANSACTIONS_TOPIC_NAME,
                               broker_names=self.trans_store.configuration.KAFKA_BROKERS,
                               consumer_group=PROJECT_NAME,
                               auto_offset_reset='largest',
                               consumer_timeout_ms=self.consumer_time_out
                               )
        return transactions

    def consume_notifications(self):
        notifications = consume(topic=self.trans_store.configuration.USER_NOTIFICATIONS_TOPIC_NAME,
                                broker_names=self.trans_store.configuration.KAFKA_BROKERS,
                                consumer_group=PROJECT_NAME,
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=self.consumer_time_out
                                )
        return notifications

    def delete_and_insert_notifications(self, notifications):
        for notification in notifications:
            un = jsonpickle.decode(notification, keys=False)
            self.users_repo.remove_notification_by_source_id(source_id=un.id)
            if un.operation == OPERATIONS.ADDED.name or un.operation == OPERATIONS.MODIFIED.name:
                self.users_repo.add_notification(user_id=un.user_id, user_name=un.user_name,
                                                 user_email=un.user_email,
                                                 threshold_value=un.threshold_value,
                                                 check_every=un.check_every,
                                                 start_date=un.start_date,
                                                 end_date=un.end_date,
                                                 is_active=un.is_active,
                                                 channel_type=un.channel_type,
                                                 notification_type=un.notification_type,
                                                 source_id=un.source_id)

                self.users_repo.commit()

    def delete_and_insert_transactions(self, transactions):
        for i in transactions:
            trans = jsonpickle.decode(i, keys=False)
            self.trans_repo.remove_transaction_by_source_id(source_id=trans.id)
            if trans.operation == OPERATIONS.ADDED.name or trans.operation == OPERATIONS.MODIFIED.name:
                self.trans_repo.add_transaction(symbol=trans.symbol, currency=trans.currency,
                                                user_id=trans.user_id, volume=trans.volume, value=trans.value,
                                                price=trans.price,
                                                date=trans.date, source=trans.source, source_id=trans.id,
                                                order_type=trans.order_type, transaction_type=trans.type
                                                )
                self.trans_repo.commit()

    def produce_compute_balances(self):
        for key, balances in self.get_all_users_computed_balances():
            produce(self.trans_repo.configuration.KAFKA_BROKERS,
                    self.trans_repo.configuration.BALANCES,
                    BalanceCalculator.compute(user_id=key,
                                              date=datetime.today().strftime(DATE_FORMAT)
                                              ))

    def schedule_user_notifications(self):
        for notif in self.schedule_user_notifications():#todo fix
            self.scheduled_job_creator.add_job(background_scheduler=self.bs, user_notification=notif)