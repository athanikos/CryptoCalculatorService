from keyring import get_password
from mongoengine import *

operators = {'>': 'greater', '<': 'less', '=': 'equal'}
change_type = {'1': 'increase', '2': 'decrease', '3': 'increase_and_decrease'}
notification_type = {'telegram': 'telegram'}


class user_settings(Document):
    meta = {'strict': False}
    userId = IntField()
    preferred_currency = StringField()


class user_notification(Document):
    meta = {'strict': False}
    userId = IntField()
    user_name = StringField()
    user_email = StringField()
    condition_value = LongField()
    field_name = StringField()
    operator = StringField(choices=operators.keys())
    notify_times = LongField()
    notify_every_in_seconds = LongField()
    symbol = StringField()
    last_date_sent = DateField()
    is_active = BooleanField()
    times_sent = IntField()
    channel_type = StringField()


class user_transaction(Document):
    meta = {'strict': False}
    user_id = IntField()
    volume = LongField()
    symbol = StringField()
    value = LongField()
    price = LongField()
    date = DateField()
    source = StringField()
    currency = StringField()


class user_channel(Document):
    meta = {'strict': False}
    user_id = IntField()
    channel_type = StringField()
    chat_id = StringField()
    user_email = StringField()

    def set_token(self):
        self.token = get_password(self.notification_type,  'token')


