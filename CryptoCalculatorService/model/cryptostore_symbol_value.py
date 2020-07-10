
from mongoengine import *
from CryptoCalculatorService.calculator.CurrencyConverter import CurrencyConverter


class user_settings(Document):
    user_id = IntField()
    preferred_currency = StringField()


class total_symbol_value:

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['exchange_rates']
        del state['symbol_prices']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __init__(self, user_id, date_time_calculated, preferred_currency, symbol_prices, exchange_rates):
        self.symbol_prices = symbol_prices
        self.exchange_rates = exchange_rates
        self.user_id = user_id
        self.converted_currency = preferred_currency
        self.converted_value = 0
        self.date_time_calculated = date_time_calculated
        self.user_grouped_symbol_values = []

    def create(self, transactions):
        for trans in transactions:  # pythonic ? iterators?
            self.add_transaction(trans)
        return self

    def add_transaction(self, transaction):
        computed_usv = user_symbol_value(user_id=self.user_id, transaction=transaction,
                                         present_price=self.symbol_prices[transaction.symbol].price,
                                         currency=self.converted_currency, exchange_rates=self.exchange_rates,
                                         date_time_calculated=self.date_time_calculated,
                                         converted_currency=self.converted_currency)

        ugsv = self.contains_symbol(transaction)

        if ugsv is not None:
            ugsv.volume += computed_usv.user_transaction.volume
            ugsv.value += computed_usv.converted_value
            ugsv.converted_currency = self.converted_currency
        else:
            ugsv = user_grouped_symbol_value(user_id=self.user_id, volume=transaction.volume,
                                             price=self.symbol_prices[transaction.symbol].price,
                                             currency=self.converted_currency,
                                             date_time_calculated=self.date_time_calculated, symbol=transaction.symbol)
            self.user_grouped_symbol_values.append(ugsv)

        self.converted_value += computed_usv.converted_value
        ugsv.user_symbol_values.append(computed_usv)

    def contains_symbol(self, transaction):
        if self.user_grouped_symbol_values is None:
            return None
        if len(self.user_grouped_symbol_values) == 0:
            return None

        for item in self.user_grouped_symbol_values:
            if item.symbol == transaction.symbol:
                return item
        return None


class user_grouped_symbol_value:
    def __init__(self, user_id, symbol, volume, price, currency, date_time_calculated):
        self.value = 0
        self.symbol = symbol
        self.volume = volume
        self.date_time_calculated = date_time_calculated
        self.user_id = user_id
        self.currency = currency
        self.price = price
        self.converted_currency = ""
        self.converted_value = 0
        self.user_symbol_values = []


class user_symbol_value:
    def __init__(self, user_id, transaction, present_price,
                 currency, date_time_calculated, converted_currency, exchange_rates):
        self.user_transaction = transaction
        self.present_price = present_price
        self.currency = currency
        self.date_time_calculated = date_time_calculated
        self.value = self.present_price * self.user_transaction.volume
        self.user_id = user_id
        self.converted_currency = converted_currency
        cc = CurrencyConverter(exchange_rates)  # fix it static / singleton
        self.converted_value = cc.convert_value(self.value, self.currency, self.converted_currency)
