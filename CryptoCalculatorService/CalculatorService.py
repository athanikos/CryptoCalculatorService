from datetime import datetime

from cryptomodel.operations import OPERATIONS
from flask import jsonify
from calculator.BalanceCalculator import BalanceCalculator
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptodataaccess.Rates.RatesRepository import RatesRepository
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository

from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Rates.RatesMongoStore import RatesMongoStore
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore

import jsonpickle
from CryptoCalculatorService.helpers import log_error
from kafkaHelper.kafkaHelper import consume, produce

DEFAULT_CURRENCY = "EUR"
DATE_FORMAT = "%Y-%m-%d"
SERVICE_NAME = "CalculatorService"


class CalculatorService:

    def __init__(self, config):
        self.rates_store = RatesMongoStore(config, log_error)
        self.users_store = UsersMongoStore(config, log_error)
        self.trans_store = TransactionMongoStore(config, log_error)

        self.rates_repo = RatesRepository(self.rates_store)
        self.trans_repo = TransactionRepository(self.trans_store)
        self.users_repo = UsersRepository(self.users_store)

    def compute(self, user_id):
        now = datetime.today().strftime(DATE_FORMAT)
        self.compute_with_upperbound_dates(user_id, now, now)

    def compute_with_upperbound_dates(self, user_id, upper_bound_symbol_rates_date, upper_bound_transaction_date):
        now = datetime.today().strftime(DATE_FORMAT)
        bc = BalanceCalculator(self.trans_repo.get_transactions(user_id),
                               self.rates_repo.fetch_symbol_rates_for_date(upper_bound_symbol_rates_date).rates,
                               self.rates_repo.fetch_latest_exchange_rates_to_date(upper_bound_symbol_rates_date),
                               DEFAULT_CURRENCY  # fix get from user_settings
                               )
        return jsonpickle.encode(bc.compute(user_id=user_id, upper_bound_symbol_rates_date= upper_bound_symbol_rates_date,
                                            date_time_calculated=now, upper_bound_transaction_date = upper_bound_transaction_date,

                                            ))


    def get_prices(self, items_count):
        now = datetime.today().strftime(DATE_FORMAT)
        return jsonify(self.rates_repo.fetch_latest_prices_to_date(before_date=now).to_json())

    def get_transactions(self, user_id):
        return jsonify(self.trans_repo.get_transactions(user_id).to_json())

    def insert_transaction(self, user_id, volume, symbol, value, price, date, source):
        trans =  self.trans_repo.add_transaction(user_id=user_id, volume=volume, symbol=symbol, value=value,
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

    def synchronize_transactions_and_user_notifications(cs):
        transactions = consume(topic=cs.trans_store.configuration.TRANSACTIONS_TOPIC_NAME,
                               broker_names=cs.trans_store.configuration.KAFKA_BROKERS,
                               consumer_group=SERVICE_NAME,
                               auto_offset_reset='largest',
                               consumer_timeout_ms=10000
                               )
        cs.delete_and_insert_transactions(cs, transactions)

        notifications = consume(topic=cs.trans_store.configuration.USER_NOTIFICATIONS_TOPIC_NAME,
                                broker_names=cs.trans_store.configuration.KAFKA_BROKERS,
                                consumer_group=SERVICE_NAME,
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=10000
                                )

        cs.delete_and_insert_notifications(notifications)

    def delete_and_insert_notifications(self, cs, notifications):
        for ntif in notifications:
            un = jsonpickle.decode(ntif, keys=False)
            cs.users_repo.delete_user_notification_by_source_id(source_id=un.id, throw_if_does_not_exist=False)
            if un.operation == OPERATIONS.ADDED.name or un.operation == OPERATIONS.MODIFIED.name:
                cs.users_repo.add_notification(user_id=un.user_id, user_name=un.user_name,
                                               user_email=un.user_email,
                                               expression_to_evaluate=un.expression_to_evaluate,
                                               check_every_seconds=un.check_every_seconds,
                                               check_times=un.check_times,
                                               is_active=un.is_active,
                                               channel_type=un.channel_type,
                                               fields_to_send=un.fields_to_send,
                                               source_id=un.source_id)

                cs.users_repo.commit()

    def delete_and_insert_transactions(self, cs, transactions):
        for i in transactions:
            trans = jsonpickle.decode(i, keys=False)
            cs.trans_repo.remove_transaction_by_source_id(source_id=trans.id)
            if trans.operation == OPERATIONS.ADDED.name or trans.operation == OPERATIONS.MODIFIED.name:
                cs.trans_repo.add_transaction(symbol=trans.symbol, currency=trans.currency,
                                              user_id=trans.user_id, volume=trans.volume, value=trans.value,
                                              price=trans.price,
                                              date=trans.date, source=trans.source, source_id=trans.id)
                cs.trans_repo.commit()


    def compute_balances_and_push(cs):
        items = cs.trans_repo.fetch_distinct_user_ids()
        for user_id in items:
            cs.compute(user_id)
        produce(cs.trans_repo.configuration.KAFKA_BROKERS,
                cs.trans_repo.configuration.BALANCES,
                BalanceCalculator.compute(user_id=user_id,
                                          date=datetime.today().strftime(DATE_FORMAT)
                                          ))
