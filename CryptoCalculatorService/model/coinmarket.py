from mongoengine import *


class EUR(EmbeddedDocument):
    meta = {'strict': False}
    price = FloatField()
    volume_24h = FloatField()
    percent_change_1h = FloatField()
    percent_change_24h = FloatField()
    percent_change_7d = FloatField()
    market_cap = FloatField()
    last_updated = StringField()


class EURQuote(EmbeddedDocument):
    meta = {'strict': False}
    eur = EmbeddedDocumentField(EUR, db_field='EUR')


class Coin(EmbeddedDocument):
    meta = {'strict': False}
    id = IntField()
    name = StringField()
    symbol = StringField()
    quote = EmbeddedDocumentField(EURQuote, db_field='quote')


class Status(EmbeddedDocument):
    meta = {'strict': False}
    timestamp = StringField()


class prices(Document):
    meta = {'strict': False}
    _id = IntField(db_field='_id')
    coins = EmbeddedDocumentListField(Coin, db_field='data')
    status = ListField(EmbeddedDocumentListField(Status, db_field='status'))


