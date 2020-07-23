from datetime import datetime
from flask import jsonify
from calculator.BalanceCalculator import BalanceCalculator
from cryptodataaccess.Repository import Repository
from cryptodataaccess.TransactionRepository import TransactionRepository
import jsonpickle
from CryptoCalculatorService.helpers import log_error
from kafkaHelper.kafkaHelper import consume
from CryptoCalculatorService.scheduler.server import start

DATE_FORMAT = "%Y-%m-%d"


class CalculatorService:

    def __init__(self, config):
        self.repo = Repository(config, log_error)
        self.trans_repo = TransactionRepository(config, log_error)

    def compute(self, user_id):
        now = datetime.today().strftime(DATE_FORMAT)
        bc = BalanceCalculator(self.repo.fetch_transactions(user_id),
                               self.repo.fetch_symbol_rates().rates,
                               self.repo.fetch_latest_exchange_rates_to_date(now),
                               "EUR"  # fix get from user_settings
                               )
        return jsonpickle.encode(bc.compute(user_id, now))

    def get_prices(self, items_count):
        now = datetime.today().strftime(DATE_FORMAT)
        return jsonify(self.repo.fetch_latest_prices_to_date(before_date=now).to_json())

    def get_transactions(self, user_id):
        return jsonify(self.trans_repo.fetch_transactions(user_id).to_json())

    def insert_transaction(self, user_id, volume, symbol, value, price, date, source):
        return self.trans_repo.insert_transaction(user_id=user_id, volume=volume, symbol=symbol, value=value,
                                                  price=price,
                                                  date=date, source=source, currency="EUR"
                                                  , source_id=None, operation='Added')  # fix currency

    def update_transaction(self, id, user_id, volume, symbol, value, price, date, source):
        return self.trans_repo.update_transaction(id, user_id, volume, symbol, value, price, "EUR", date, source,
                                                  source_id=id, operation='Modified')  # fix

    def get_user_notifications(self, items_count):
        repo = Repository(self.configuration)
        return jsonify(repo.fetch_notifications(items_count).to_json())

    def get_user_channels(self, user_id, channel_type):
        repo = Repository(self.configuration)
        return jsonify(repo.fetch_user_channels(user_id, channel_type).to_json())

    def insert_user_notification(self, user_id, user_name, user_email, condition_value, field_name, operator,
                                 notify_times,
                                 notify_every_in_seconds, symbol, channel_type):
        repo = Repository(self.configuration)
        return repo.insert_notification(user_id, user_name, user_email, condition_value, field_name, operator,
                                        notify_times,
                                        notify_every_in_seconds, symbol, channel_type)

    def insert_user_channel(self, user_id, channel_type, chat_id):
        repo = Repository(self.configuration)
        return repo.insert_user_channel(user_id, channel_type, chat_id)

    def synchronize_transactions(cs, testmode=False):
        exit = False
        if 1 == 1 and exit == False:
            print("1==1")
            items = consume(topic=cs.repo.configuration.TRANSACTIONS_TOPIC_NAME,
                            broker_names=cs.repo.configuration.KAFKA_BROKERS,
                            consumer_group="CalculatorService",
                            auto_offset_reset='earliest',
                            consumer_timeout_ms = 10000

                            )
            for i in items:
                print("in i in items")
                trans = jsonpickle.decode(i, keys=False)
                cs.trans_repo.do_delete_transaction_by_source_id(source_id=trans.id, throw_if_does_not_exist=False)
                if trans.operation == "Added" or trans.operation == "Modified":
                    cs.trans_repo.insert_transaction(symbol=trans.symbol, currency=trans.currency,
                                                     user_id=trans.user_id, volume=trans.volume, value=trans.value,
                                                     price=trans.price,
                                                     date=trans.date, source=trans.source, source_id=trans.id,
                                                     operation=trans.operation)
            if testmode:
                exit = True