from datetime import datetime

from cryptodataaccess.Rates.RatesRepository import RatesRepository
from cryptodataaccess.Rates.RatesMongoStore import RatesMongoStore
from flask import jsonify

from CryptoCalculatorService.helpers import log_error

DEFAULT_CURRENCY = "EUR"
DATE_FORMAT = "%Y-%m-%d"
PROJECT_NAME = "CalculatorService"


class PricesService:

    def __init__(self, config):
        self.rates_store = RatesMongoStore(config, log_error)
        self.rates_repo = RatesRepository(self.rates_store)

    def get_prices(self):
        now = datetime.today().strftime(DATE_FORMAT)
        return self.rates_repo.fetch_latest_prices_to_date(before_date=now)

    def get_price_per_date(self, symbol, date):
        rates = self.rates_repo.fetch_symbol_rates_for_date(date)
        if symbol not in rates.keys():
            raise ValueError("symbol does not exist")
        return rates[symbol].price

    def get_price_change(self, symbol, base_date, past_date):
        base_price = self.get_price_per_date(symbol, base_date)
        past_price = self.get_price_per_date(symbol, past_date)
        return (base_price - past_price) / base_price