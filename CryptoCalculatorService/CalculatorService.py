from datetime import datetime
from flask import jsonify
from calculator.BalanceCalculator import BalanceCalculator
from cryptodataaccess.RatesRepository import RatesRepository
from cryptodataaccess.TransactionRepository import TransactionRepository
import jsonpickle
from CryptoCalculatorService.helpers import log_error
from kafkaHelper.kafkaHelper import consume, produce

DEFAULT_CURRENCY = "EUR"
DATE_FORMAT = "%Y-%m-%d"


class CalculatorService:

    def __init__(self, config):
        self.rates_repo = RatesRepository(config, log_error)
        self.trans_repo = TransactionRepository(config, log_error)

    def compute(self, user_id):
        now = datetime.today().strftime(DATE_FORMAT)
        bc = BalanceCalculator(self.trans_repo.fetch_transactions(user_id),
                               self.rates_repo.fetch_symbol_rates().rates,
                               self.rates_repo.fetch_latest_exchange_rates_to_date(now),
                               DEFAULT_CURRENCY  # fix get from user_settings
                               )
        return jsonpickle.encode(bc.compute(user_id, now))

    def get_prices(self, items_count):
        now = datetime.today().strftime(DATE_FORMAT)
        return jsonify(self.rates_repo.fetch_latest_prices_to_date(before_date=now).to_json())

    def get_transactions(self, user_id):
        return jsonify(self.trans_repo.fetch_transactions(user_id).to_json())

    def insert_transaction(self, user_id, volume, symbol, value, price, date, source):
        return self.trans_repo.insert_transaction(user_id=user_id, volume=volume, symbol=symbol, value=value,
                                                  price=price,
                                                  date=date, source=source, currency=DEFAULT_CURRENCY
                                                  , source_id=None, operation='Added')  # fix get from user_settings

    def update_transaction(self, id, user_id, volume, symbol, value, price, date, source):
        return self.trans_repo.update_transaction(id, user_id, volume, symbol, value, price, DEFAULT_CURRENCY, date,
                                                  source,
                                                  source_id=id, operation='Modified')  # fix get from user_settings

    def synchronize_transactions(cs, test_mode=False):
        exit = False
        if 1 == 1 and exit == False:
            items = consume(topic=cs.trans_repo.configuration.TRANSACTIONS_TOPIC_NAME,
                            broker_names=cs.trans_repo.configuration.KAFKA_BROKERS,
                            consumer_group="CalculatorService",
                            auto_offset_reset='earliest',
                            consumer_timeout_ms=10000

                            )
            for i in items:
                trans = jsonpickle.decode(i, keys=False)
                cs.trans_repo.do_delete_transaction_by_source_id(source_id=trans.id, throw_if_does_not_exist=False)
                if trans.operation == "Added" or trans.operation == "Modified":
                    cs.trans_repo.insert_transaction(symbol=trans.symbol, currency=trans.currency,
                                                     user_id=trans.user_id, volume=trans.volume, value=trans.value,
                                                     price=trans.price,
                                                     date=trans.date, source=trans.source, source_id=trans.id,
                                                     operation=trans.operation)
            if test_mode:
                exit = True

    def compute_balances_and_push(cs):
        items = cs.trans_repo.fetch_distinct_user_ids()
        for user_id in items:
            cs.compute(user_id)
        produce(cs.trans_repo.configuration.KAFKA_BROKERS,
                cs.trans_repo.configuration.BALANCES,
                BalanceCalculator.compute(user_id=user_id,
                                          date=datetime.today().strftime(DATE_FORMAT)
                                          ))
