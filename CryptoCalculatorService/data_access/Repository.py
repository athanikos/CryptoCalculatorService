from datetime import datetime
from CryptoModel.readonly import SymbolRates
from mongoengine import Q
from CryptoModel.coinmarket import prices
from CryptoModel.fixer import exchange_rates
from CryptoModel import helpers
from CryptoModel.cryptostore import  user_transaction
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

    def fetch_user_channels(self, user_id):
        return helpers.server_time_out_wrapper(self, self.do_fetch_user_channels, user_id)

    def fetch_notifications(self, items_count):
        return helpers.server_time_out_wrapper(self, self.do_fetch_notifications, items_count)

    def fetch_transactions(self, user_id):
        return helpers.server_time_out_wrapper(self, self.do_fetch_transactions,  user_id)

    def insert_transaction(self, user_id, volume, symbol, value, price, currency, date, source):
        return helpers.server_time_out_wrapper(self, self.do_insert_transaction, user_id, volume, symbol,
                                               value, price, currency, date, source)

    def insert_notification(self, user_id, user_name, user_email, condition_value, field_name, operator, notify_times,
                            notify_every_in_seconds, symbol, channel_type):
        return helpers.server_time_out_wrapper(self, self.do_insert_notification, user_id, user_name, user_email,
                                               condition_value, field_name, operator,
                                               notify_times,
                                               notify_every_in_seconds, symbol, channel_type)

    def update_notification(self, id, user_id, user_name, user_email, condition_value, field_name, operator, notify_times,
                            notify_every_in_seconds, symbol, channel_type):
        return helpers.server_time_out_wrapper(self, self.do_update_notification, id,  user_id, user_name, user_email,
                                               condition_value, field_name, operator,
                                               notify_times,
                                               notify_every_in_seconds, symbol, channel_type)

    def update_user_settings(self, user_id, preferred_currency):
        return helpers.server_time_out_wrapper(self, self.do_update_user_settings, user_id, preferred_currency)

    def insert_user_settings(self, user_id, preferred_currency):
        return helpers.server_time_out_wrapper(self, self.do_insert_user_channel, user_id, preferred_currency)

    def insert_user_channel(self, user_id, channel_type, chat_id):
        return helpers.server_time_out_wrapper(self, self.do_insert_user_channel, user_id, channel_type, chat_id)

    def update_transaction(self, id, user_id, volume, symbol, value, price, currency, date, source):
        return helpers.server_time_out_wrapper(self, self.do_update_transaction, id,
                                               user_id, volume, symbol, value, price, currency, date, source)

    def delete_transaction(self, id):
        helpers.server_time_out_wrapper(self, self.do_delete_transaction, id)

    def delete_notification(self, id):
        helpers.server_time_out_wrapper(self, self.do_delete_notification, id)

    def delete_user_settings(self, id):
        helpers.server_time_out_wrapper(self, self.do_delete_user_settings, id)

    def do_fetch_user_channels(self, user_id):
        helpers.do_connect(self.configuration)
        return user_notification.objects(Q(user_id=user_id))

    def do_fetch_notifications(self, items_count):
        helpers.do_connect(self.configuration)
        return user_notification.objects()[:items_count]

    def do_fetch_transactions(self, user_id ):
        helpers.do_connect(self.configuration)
        return user_transaction.objects(Q(user_id=user_id))

    def do_insert_transaction(self, user_id, volume, symbol, value, price, currency, date, source):
        helpers.do_connect(self.configuration)
        trans = user_transaction()
        trans.user_id = user_id
        trans.volume = volume
        trans.symbol = symbol
        trans.value = value
        trans.price = price
        trans.date = date
        trans.currency = currency
        trans.source = source
        trans.save()
        return user_transaction.objects(id=trans.id).first()

    def do_insert_notification(self, user_id, user_name, user_email, condition_value, field_name, operator,
                               notify_times,
                               notify_every_in_seconds, symbol, channel_type):
        helpers.do_connect(self.configuration)
        un = user_notification()
        un.userId = user_id
        un.is_active = True
        un.times_sent = 0
        un.channel_type = channel_type
        un.user_name = user_name
        un.user_email = user_email
        un.condition_value = condition_value
        un.field_name = field_name
        un.operator = operator
        un.notify_times = notify_times
        un.notify_every_in_seconds = notify_every_in_seconds
        un.symbol = symbol
        un.save()
        return user_notification.objects(id=un.id).first()

    def do_insert_user_channel(self, user_id, channel_type, chat_id):
        helpers.do_connect(self.configuration)
        uc = user_channel()
        uc.userId = user_id
        uc.channel_type = channel_type
        uc.chat_id = chat_id
        uc.save()
        return user_channel.objects(id=uc.id).first()

    def do_insert_user_settings(self, user_id, preferred_currency):
        helpers.do_connect(self.configuration)
        us = user_settings()
        us.userId = user_id
        us.preferred_currency= preferred_currency
        us.save()
        return user_settings.objects(id=us.id).first()

    def do_update_user_settings(self, id, user_id,  preferred_currency):
        helpers.do_connect(self.configuration)
        us = user_settings.objects(id=id).first()
        if_none_raise_with_id(id, us)
        us.user_id = user_id
        us.preferred_currency = preferred_currency
        us.save()
        return user_settings.objects(id=id).first()

    def do_update_transaction(self, id, user_id, volume, symbol, value, price, currency, date, source):
        helpers.do_connect(self.configuration)
        trans = user_transaction.objects(id=id).first()
        if_none_raise_with_id(id, trans)
        trans.user_id = user_id
        trans.volume = volume
        trans.symbol = symbol
        trans.value = value
        trans.price = price
        trans.date = date
        trans.source = source
        trans.currency = currency
        trans.save()
        return user_transaction.objects(id=id).first()

    def do_update_notification(self, id, user_id, user_name, user_email, condition_value, field_name, operator,
                               notify_times,
                               notify_every_in_seconds, symbol, channel_type):
        helpers.do_connect(self.configuration)
        un = user_notification.objects(id = id ).first()
        if_none_raise_with_id(id, un)
        un.id = id
        un.userId = user_id
        un.is_active = True
        un.times_sent = 0
        un.channel_type = channel_type
        un.user_name = user_name
        un.user_email = user_email
        un.condition_value = condition_value
        un.field_name = field_name
        un.operator = operator
        un.notify_times = notify_times
        un.notify_every_in_seconds = notify_every_in_seconds
        un.symbol = symbol
        un.save()
        return user_notification.objects(id=un.id).first()

    def do_delete_transaction(self, id):
        helpers.do_connect(self.configuration)
        trans = user_transaction.objects(id=id).first()
        if_none_raise_with_id(id, trans)
        trans.delete()

    def do_delete_notification(self, id):
        helpers.do_connect(self.configuration)
        un = user_notification.objects(id=id).first()
        if_none_raise_with_id(id, un)
        un.delete()

    def do_delete_user_settings(self, id):
        helpers.do_connect(self.configuration)
        us = user_settings.objects(id=id).first()
        if_none_raise_with_id(id, us)
        us.delete()