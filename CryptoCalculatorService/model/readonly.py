from mongoengine import Document


class SymbolRates:
    def __init__(self, date):
        self.date = date
        self.rates = {}

    def add_rate(self, symbol, metrics):
        self.rates.update({symbol: metrics})


class SymbolMetrics:
    def __init__(self, price, volume_24h, percent_change_1h,
                 percent_change_24h, percent_change_7d, market_cap):
        self.price = price
        self.volume_24h = volume_24h
        self.percent_change_1h = percent_change_1h
        self.percent_change_24h = percent_change_24h
        self.percent_change_7d = percent_change_7d
        self.market_cap = market_cap
