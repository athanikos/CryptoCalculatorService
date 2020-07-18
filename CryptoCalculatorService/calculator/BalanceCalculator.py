from CryptoModel.cryptostore_symbol_value import total_symbol_value

class BalanceCalculator:

    def __init__(self, transactions, symbol_rates, exchange_rates,preferred_currency):
        self.transactions = transactions
        self.symbol_rates = symbol_rates
        self.exchange_rates = exchange_rates
        self.preferred_currency = preferred_currency

    def compute(self, user_id, date):
        tsv = total_symbol_value(user_id, date,self.preferred_currency, self.symbol_rates, self.exchange_rates)
        return tsv.create(self.transactions)