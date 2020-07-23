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
from CryptoCalculatorService.CalculatorService import CalculatorService
from CryptoCalculatorService.scheduler.server import start, stop


@pytest.fixture
def test_client():
    flask_app = create_app()
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client  # this is where the testing happens!
    ctx.pop()


def test_basic_2():
    cfg = configure_app()
    do_connect(cfg)
    user_transaction.objects.all().delete()
    tr = TransactionRepository(config=cfg, log_error=log_error)
    ut2 = user_transaction()
    ut2.source_id = ObjectId('666f6f2d6261722d71757578')
    ut2.id = ObjectId('666f6f2d6261722d71757578')
    ut2.operation = "Added"
    ut2.symbol = "OXqqT"
    ut2.source = "kraken"
    ut2.currency = "aaaaaa"
    ut2.user_id = 1
    ut2.volume = 1000
    ut2.value = 1000
    ut2.price = 10
    ut2.date = "2020-01-01"
    produce(tr.configuration.KAFKA_BROKERS, cfg.TRANSACTIONS_TOPIC_NAME, jsonpickle.encode(ut2))
    cc = CalculatorService(cfg)
    cc.synchronize_transactions(test_mode=True)
    uts2 = tr.fetch_transactions(1)
    assert (len(uts2) == 1)
    assert (uts2[0].source_id == ObjectId('666f6f2d6261722d71757578'))
    assert (uts2[0].symbol == "OXqqT")


def test_basic():
    cfg = configure_app()
    do_connect(cfg)
    cs = CalculatorService(cfg)
    start(cs)

    user_transaction.objects.all().delete()
    tr = TransactionRepository(config=cfg, log_error=log_error)
    ut2 = user_transaction()
    ut2.source_id = ObjectId('666f6f2d6261722d71757578')
    ut2.id = ObjectId('666f6f2d6261722d71757578')
    ut2.operation = "Added"
    ut2.symbol = "OXqqT"
    ut2.source = "kraken"
    ut2.currency = "aaaaaa"
    ut2.user_id = 1
    ut2.volume = 1000
    ut2.value = 1000
    ut2.price = 10
    ut2.date = "2020-01-01"
    produce(tr.configuration.KAFKA_BROKERS, cfg.TRANSACTIONS_TOPIC_NAME, jsonpickle.encode(ut2))
    time.sleep(20)
    uts2 = tr.fetch_transactions(1)
    assert (len(uts2) == 1)
    assert (uts2[0].source_id == ObjectId('666f6f2d6261722d71757578'))
    assert (uts2[0].symbol == "OXqqT")

def test_updates():
    cfg = configure_app()
    do_connect(cfg)
    cs = CalculatorService(cfg)
    user_transaction.objects.all().delete()
    tr = TransactionRepository(config=cfg, log_error=log_error)
    ut2 = user_transaction()
    ut2.source_id = ObjectId('666f6f2d6261722d71757578')
    ut2.id = ObjectId('666f6f2d6261722d71757578')
    ut2.operation = "Added"
    ut2.symbol = "OXqqT"
    ut2.source = "kraken"
    ut2.currency = "aaaaaa"
    ut2.user_id = 1
    ut2.volume = 1000
    ut2.value = 1000
    ut2.price = 10
    ut2.date = "2020-01-01"
    produce(tr.configuration.KAFKA_BROKERS, cfg.TRANSACTIONS_TOPIC_NAME, jsonpickle.encode(ut2))
    time.sleep(20)
    uts2 = tr.fetch_transactions(1)
    assert (len(uts2) == 1)
    assert (uts2[0].source_id == ObjectId('666f6f2d6261722d71757578'))
    assert (uts2[0].symbol == "OXqqT")

    tr = TransactionRepository(config=cfg, log_error=log_error)
    ut2 = user_transaction()
    ut2.source_id = ObjectId('666f6f2d6261722d71757578')
    ut2.id = ObjectId('666f6f2d6261722d71757578')
    ut2.operation = "Added"
    ut2.symbol = "OXqqT"
    ut2.source = "kraken"
    ut2.currency = "NEW"
    ut2.user_id = 1
    ut2.volume = 1000
    ut2.value = 1000
    ut2.price = 10
    ut2.date = "2020-01-01"
    produce(tr.configuration.KAFKA_BROKERS, cfg.TRANSACTIONS_TOPIC_NAME, jsonpickle.encode(ut2))
    time.sleep(20)
    uts2 = tr.fetch_transactions(1)
    assert (len(uts2) == 1)
    assert (uts2[0].currency == "NEW")
