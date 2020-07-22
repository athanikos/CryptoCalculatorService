import time

import jsonpickle
from bson import ObjectId
from cryptomodel.cryptostore import user_transaction
from server import configure_app, create_app
import pytest
from kafkaHelper.kafkaHelper import produce_with_action
from CryptoCalculatorService.config import  configure_app
from cryptodataaccess.TransactionRepository import  TransactionRepository
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
    cfg =  configure_app()
    do_connect(cfg)
    tr = TransactionRepository( config=cfg,log_error=log_error)
    ut = user_transaction()
    ut.source_id = ObjectId('666f6f2d6261722d71757578')
    ut.id = ObjectId('666f6f2d6261722d71757578')
    ut.operation= "Added"
    ut.symbol = "BTC"
    ut.source = "kraken"
    ut.currency = "EUR"
    ut.user_id = 1
    ut.volume = 1000
    ut.value = 1000
    ut.price = 10
    ut.date = "2020-01-01"
    user_transaction.objects.all().delete()
    produce_with_action(tr.configuration.KAFKA_BROKERS, tr.configuration.TRANSACTIONS_TOPIC_NAME, jsonpickle.encode(ut) )
    uts = tr.fetch_transactions(1)
    time.sleep(5)
    assert (len(uts) == 1 )
    assert (uts[0].source_id ==  ObjectId('666f6f2d6261722d71757578') )
