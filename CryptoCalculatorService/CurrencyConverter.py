from CryptoCalculatorService.helpers import  if_none_raise, if_empty_string_raise
from enum import Enum


class ConversionOrder(Enum):
    Same = 1
    FromIsBase = 2
    ToIsBase = 3


class CurrencyConverter:

    def __init__(self, exchange_rate_item):
        self.exchange_rate_item = exchange_rate_item

    def convert_value(self, value, from_currency, to_currency):
        conversion_order = self.conversion_is_possible(from_currency, to_currency)

        if conversion_order == conversion_order.Same:
            return value
        elif conversion_order == conversion_order.FromIsBase:
            ratio = getattr(self.exchange_rate_item.rates,to_currency)
            return value * ratio
        elif conversion_order == conversion_order.ToIsBase:
            ratio = getattr(self.exchange_rate_item.rates, from_currency)
            return value / ratio

    def conversion_is_possible(self, from_currency, to_currency):
        if_none_raise(self.exchange_rate_item)
        if_empty_string_raise(from_currency)
        if_empty_string_raise(to_currency)

        if from_currency == to_currency:
            return ConversionOrder.Same
        elif from_currency == self.exchange_rate_item.base:
            return ConversionOrder.FromIsBase
        elif to_currency == self.exchange_rate_item.base:
            return ConversionOrder.ToIsBase
