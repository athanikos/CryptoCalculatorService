from datetime import datetime
from flask import jsonify
from CryptoCalculatorService.calculator.BalanceCalculator import BalanceCalculator
from CryptoCalculatorService.data_access.Repository import Repository
DATE_FORMAT = "%Y-%m-%d"
import jsonpickle
from CryptoCalculatorService.helpers import log_error


class  CalculatorService:

    def __init__(self, config):
        self.repo = Repository(config, log_error)

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
        repo = Repository(self.configuration, log_error)
        return jsonify(repo.fetch_transactions(user_id).to_json())

    def insert_transaction(self, user_id, volume, symbol, value, price, date, source):
        repo = Repository(self.configuration, log_error)
        return repo.insert_transaction(user_id=user_id, volume=volume, symbol=symbol, value=value, price=price,
                                       date=date, source=source, currency="EUR")  # fix currency

    def update_transaction(self, id, user_id, volume, symbol, value, price, date, source):
        repo = Repository(self.configuration, log_error)
        return repo.update_transaction(id, user_id, volume, symbol, value, price, "EUR", date, source)  # fix

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
