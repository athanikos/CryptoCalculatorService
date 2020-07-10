from datetime import datetime
from CryptoCalculatorService.model.readonly import SymbolRates
from mongoengine import Q
from CryptoCalculatorService.model.coinmarket import prices
from CryptoCalculatorService.model.fixer import exchange_rates
from CryptoCalculatorService.data_access import helpers
DATE_FORMAT = "%Y-%m-%d"


class Repository:

    def __init__(self, config, log_error):
        self.configuration = config
        self.log_error = log_error

    def fetch_symbols(self):
        symbols = {}
        latest_prices = self.fetch_latest_prices_to_date(datetime.today().strftime(DATE_FORMAT))
        for coin in latest_prices[0].coins:
            symbols.update({coin.symbol: coin.name})
        return symbols

    def fetch_symbol_rates(self):
        dt_now = datetime.today().strftime(DATE_FORMAT)
        srs = SymbolRates(dt_now)
        latest_prices = self.fetch_latest_prices_to_date(dt_now)
        for coin in latest_prices[0].coins:
            srs.add_rate(coin.symbol, coin.quote.eur)
        return srs

    def fetch_latest_prices_to_date(self, before_date):
        return helpers.server_time_out_wrapper(self, self.do_fetch_latest_prices_to_date, before_date)

    def fetch_latest_exchange_rates_to_date(self, before_date):
        return helpers.server_time_out_wrapper(self, self.do_fetch_latest_exchange_rates_to_date, before_date)

    def fetch_transactions(self, user_id):
        return helpers.server_time_out_wrapper(self, self.do_fetch_transactions,  user_id)

    def do_fetch_latest_prices_to_date(self, before_date):
        helpers.do_connect(self.configuration)
        return prices.objects(Q(status__timestamp__lte=before_date)).order_by(
            '-status__timestamp')[:10]

    def do_fetch_latest_exchange_rates_to_date(self, before_date):
        helpers.do_connect(self.configuration)
        return exchange_rates.objects(Q(date__lte=before_date)).order_by(
            'date-')[:1]

    def do_fetch_transactions(self, user_id ):
        helpers.do_connect(self.configuration)
        return user_transaction.objects(Q(user_id=user_id))

