import time

import jsonpickle
from bson import ObjectId
from cryptomodel.cryptostore import user_transaction
from server import configure_app, create_app
import pytest
from kafkaHelper.kafkaHelper import produce
from CryptoCalculatorService.config import configure_app
from cryptodataaccess.TransactionRepository import TransactionRepository
from cryptodataaccess.helpers import do_connect, log_error


@pytest.fixture(scope='module')
def test_client():
    flask_app = create_app()
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client  # this is where the testing happens!
    ctx.pop()


def test_basic():
    cfg = configure_app()
    do_connect(cfg)
    user_transaction.objects.all().delete()

    tr = TransactionRepository(config=cfg, log_error=log_error)
    ut2 = user_transaction()
    ut2.source_id = ObjectId('666f6f2d6261722d71757578')
    ut2.id = ObjectId('666f6f2d6261722d71757578')
    ut2.operation = "Added"
    ut2.symbol = "OXT"
    ut2.source = "kraken"
    ut2.currency = "EUsdR"
    ut2.user_id = 1
    ut2.volume = 1000
    ut2.value = 1000
    ut2.price = 10
    ut2.date = "2020-01-01"
    produce(tr.configuration.KAFKA_BROKERS,   cfg.TRANSACTIONS_TOPIC_NAME , jsonpickle.encode(ut2))
    time.sleep(5)
    uts2 = tr.fetch_transactions(1)
    assert (len(uts2) == 1)
    assert (uts2[0].source_id == ObjectId('666f6f2d6261722d71757578'))
    assert (uts2[0].symbol == "OXT")

    ut1 = user_transaction()
    ut1.source_id = ObjectId('666f6f2d6261722d71757578')
    ut1.id = ObjectId('666f6f2d6261722d71757578')
    ut1.operation = "Added"
    ut1.symbol = "BBB"
    ut1.source = "kraken"
    ut1.currency = "EUsdR"
    ut1.user_id = 1
    ut1.volume = 1000
    ut1.value = 1000
    ut1.price = 10
    ut1.date = "2020-01-01"
    produce(tr.configuration.KAFKA_BROKERS,   cfg.TRANSACTIONS_TOPIC_NAME , jsonpickle.encode(ut1))
    time.sleep(5)

    uts3 = tr.fetch_transactions(1)
    assert (len(uts3) == 1)
    assert (uts3[0].source_id == ObjectId('666f6f2d6261722d71757578'))
    assert (uts3[0].symbol == "BBB")

