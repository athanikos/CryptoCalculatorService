import time
from datetime import datetime

import jsonpickle
import mock
from bson import ObjectId
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptomodel.cryptostore import user_transaction
from cryptomodel.operations import OPERATIONS

from server import configure_app, create_app
import pytest
from kafkaHelper.kafkaHelper import produce, consume
from CryptoCalculatorService.config import configure_app
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore

from cryptodataaccess.helpers import do_connect, log_error
from CryptoCalculatorService.BalanceService import BalanceService, PROJECT_NAME
from CryptoCalculatorService.scedhuler.Scedhuler import Scedhuler

@pytest.fixture
def test_client():
    flask_app = create_app()
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client  # this is where the testing happens!
    ctx.pop()


@pytest.fixture(scope='module')
def mock_log():
    with mock.patch("cryptodataaccess.helpers.log_error"
                    ) as _mock:
        _mock.return_value = True
        yield _mock


def test_syncronize_transactions():
    cfg = configure_app()
    do_connect(cfg)
    cs = BalanceService(cfg)

    config = configure_app()
    store = TransactionMongoStore(config, mock_log)
    repo = TransactionRepository(store)
    do_connect(config)
    user_transaction.objects.all().delete()

    ut2 = user_transaction()
    ut2.source_id = ObjectId('666f6f2d6261722d71757578')
    ut2.id = ObjectId('666f6f2d6261722d71757578')
    ut2.operation = OPERATIONS.ADDED.name
    ut2.symbol = "BTC"
    ut2.source = "HEYEYEYEY"
    ut2.currency = "aaaaaa"
    ut2.user_id = 1
    ut2.volume = 1000
    ut2.value = 1000
    ut2.price = 10
    ut2.date = "2020-01-01"

    transactions = [jsonpickle.encode(ut2)]
    s =Scedhuler(config)
    s.delete_and_insert_transactions(transactions)
    uts2 = repo.get_transactions(1)
    assert (len(uts2) == 1)
