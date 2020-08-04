from datetime import datetime

from cryptodataaccess.helpers import convert_to_int_timestamp
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
PROJECT_NAME = "CalculatorService"


class BalanceService:

    def __init__(self, config):
        self.rates_store = RatesMongoStore(config, log_error)
        self.users_store = UsersMongoStore(config, log_error)
        self.trans_store = TransactionMongoStore(config, log_error)

        self.rates_repo = RatesRepository(self.rates_store)
        self.trans_repo = TransactionRepository(self.trans_store)
        self.users_repo = UsersRepository(self.users_store)

    def compute_balance(self, user_id):
        now =   datetime.today()
        return self.compute_balance_with_upperbound_dates(user_id, upper_bound_symbol_rates_date=  convert_to_int_timestamp(now)
                                                          , upper_bound_transaction_date= now)

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
