from datetime import datetime

from cryptodataaccess.Calculator.CalculatorRepository import CalculatorRepository
from cryptodataaccess.helpers import convert_to_int_timestamp
from calculator.BalanceCalculator import BalanceCalculator
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptodataaccess.Rates.RatesRepository import RatesRepository
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Rates.RatesMongoStore import RatesMongoStore
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore
from cryptodataaccess.Calculator.CalculatorMongoStore import CalculatorMongoStore
from cryptomodel.operations import OPERATIONS
from flask import jsonify
from kafkaHelper.kafkaHelper import produce, consume
import jsonpickle
from CryptoCalculatorService.helpers import log_error

DEFAULT_CURRENCY = "EUR"
DATE_FORMAT = "%Y-%m-%d"
PROJECT_NAME = "CalculatorService"


class BalanceService:

    def __init__(self, config):
        self.rates_store = RatesMongoStore(config, log_error)
        self.users_store = UsersMongoStore(config, log_error)
        self.trans_store = TransactionMongoStore(config, log_error)
        self.calculator_store = CalculatorMongoStore(config, log_error)

        self.rates_repo = RatesRepository(self.rates_store)
        self.trans_repo = TransactionRepository(self.trans_store)
        self.users_repo = UsersRepository(self.users_store)
        self.calculator_repo = CalculatorRepository(self.calculator_store)

    def compute_balance_save_and_produce(self, user_id, user_notification):
        balance = self.compute_balance(user_id)
        cn = self.calculator_repo.add_computed_notification(user_id=user_id,
                                                            start_date=user_notification.start_date,
                                                            result=balance,
                                                            channel_type=user_notification.channel_type,
                                                            check_every=user_notification.check_every,
                                                            end_date=user_notification.end_date,
                                                            notification_type=user_notification.notification_type,
                                                            computed_date=user_notification.computed_date,
                                                            threshold_value=user_notification.threshold_value,
                                                            is_active=user_notification.is_active,
                                                            source_id=user_notification.source_id,
                                                            user_email=user_notification.user_email,
                                                            user_name=user_notification.user_name,
                                                            )
        self.calculator_repo.commit()

        produce(broker_names=self.users_store.configuration.KAFKA_BROKERS,
                topic=self.calculator_store.configuration.COMPUTED_NOTIFICATIONS_TOPIC_NAME
                , data_item=jsonpickle.encode(cn))

    def compute_balance(self, user_id):
        now = datetime.today()
        return self.compute_balance_with_upperbound_dates(user_id,
                                                          upper_bound_symbol_rates_date=convert_to_int_timestamp(now)
                                                          , upper_bound_transaction_date=now)

    def compute_balance_with_upperbound_dates(self, user_id, upper_bound_symbol_rates_date,
                                              upper_bound_transaction_date):
        preferred_currency = self.users_repo.get_user_settings(user_id)
        if preferred_currency is None:
            preferred_currency = DEFAULT_CURRENCY

        bc = BalanceCalculator(self.trans_repo.get_transactions_before_date(user_id, upper_bound_transaction_date),
                               self.rates_repo.fetch_symbol_rates_for_date(upper_bound_symbol_rates_date).rates,
                               self.rates_repo.fetch_latest_exchange_rates_to_date(upper_bound_symbol_rates_date),
                               preferred_currency,
                               upper_bound_symbol_rates_date=upper_bound_symbol_rates_date,
                               upper_bound_transaction_date=upper_bound_transaction_date
                               )

        return jsonpickle.encode(
            bc.compute(user_id=user_id, date=datetime.now()))

    def get_all_users_computed_balances(self):
        user_balances = []
        items = self.trans_repo.fetch_distinct_user_ids()
        for user_id in items:
            user_balances[user_id] = self.compute_balance(user_id)
        return user_balances

    def get_transactions(self, user_id):
        return jsonify(self.trans_repo.get_transactions(user_id).to_json())

    def insert_transaction(self, user_id, volume, symbol, value, price, date, source):
        trans = self.trans_repo.add_transaction(user_id=user_id, volume=volume, symbol=symbol, value=value,
                                                price=price,
                                                date=date, source=source, currency=DEFAULT_CURRENCY
                                                , source_id=None)  # todo  fix get from user_settings
        self.trans_repo.commit()
        return trans

    def update_transaction(self, id, user_id, volume, symbol, value, price, date, source):
        trans = self.trans_repo.update_transaction(id, user_id, volume, symbol, value, price, DEFAULT_CURRENCY, date,
                                                   source,
                                                   source_id=id)  # todo fix get from user_settings
        self.trans_repo.commit()
        return trans

    def synchronize_transactions_and_user_notifications(self):
        while self.ran_once == False or self.run_forever:
            try:
                transactions = self.consume_transactions()
                try:
                    self.delete_and_insert_transactions(transactions)
                except Exception as e:
                    (self.produce_transaction(t) for t in transactions)  # consumed but failed reproduce
                    log_error(e, self.users_store.configuration)
            except Exception as e:
                log_error(e, self.users_store.configuration)

            try:
                notifications = self.consume_notifications()
                try:
                    self.delete_and_insert_transactions(notifications)
                except Exception as e:
                    (self.produce_user_notification(n) for n in notifications)  # consumed but failed reproduce
                    log_error(e, self.users_store.configuration)

                self.delete_and_insert_notifications(notifications)
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
                    self.trans_repo.configuration.COMPUTED_NOTIFICATIONS_TOPIC_NAME,
                    BalanceCalculator.compute(user_id=key,
                                              date=datetime.today().strftime(DATE_FORMAT)
                                              ))

    def produce_user_notification(self, user_notification):
        produce(broker_names=self.users_store.configuration.KAFKA_BROKERS,
                topic=self.users_store.configuration.USER_NOTIFICATIONS_TOPIC_NAME
                , data_item=jsonpickle.encode(user_notification))

    def produce_transaction(self, transaction):
        produce(broker_names=self.users_store.configuration.KAFKA_BROKERS,
                topic=self.users_store.configuration.TRANSACTIONS_TOPIC_NAME
                , data_item=jsonpickle.encode(transaction))

    def schedule_user_notifications(self):
        # user_notifications = self.calculator_store
        for notif in self.schedule_user_notifications():  # todo fix get user_notifications and lookup with computed to get those pending
            self.scheduled_job_creator.add_job(background_scheduler=self.bs, user_notification=notif)
