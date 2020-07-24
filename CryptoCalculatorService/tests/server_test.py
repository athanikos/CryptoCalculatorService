import pytest
import json
from bson import ObjectId
from server import configure_app, create_app
from cryptodataaccess.helpers import do_connect, log_error
from cryptomodel.cryptostore import user_transaction


@pytest.fixture(scope='module')
def test_client():
    flask_app = create_app()
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client  # this is where the testing happens!
    ctx.pop()


def test_fetch_transactions(test_client):
    config = configure_app()
    do_connect(config)
    user_transaction.objects.all().delete()
    ut = user_transaction()
    ut.symbol = "BTC"
    ut.date = "2020-01-01"
    ut.price = 10
    ut.value = 1
    ut.user_id = 1
    ut.volume = 10
    ut.currency = "EUR"
    ut.source = "kraken"
    ut.operation = "Added"
    ut.source_id = ObjectId('666f6f2d6261722d71757578')
    ut.save(force_insert=True, validate=False, clean=False)
    response = test_client.get('/api/v1/balance/1')
    data_json2 = json.dumps(response.get_json(silent=True, force=True))
    print(data_json2)
    assert response.status_code == 200

