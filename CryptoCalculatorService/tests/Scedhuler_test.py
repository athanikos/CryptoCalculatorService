import time
from datetime import datetime

import jsonpickle
import mock
from bson import ObjectId
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptomodel.cryptostore import user_transaction, user_notification
from cryptomodel.operations import OPERATIONS

from CryptoCalculatorService.tests.helpers import setup_repos_and_clear_data
from server import configure_app, create_app
import pytest
from kafkaHelper.kafkaHelper import produce, consume
from CryptoCalculatorService.config import configure_app
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore

from cryptodataaccess.helpers import do_connect, log_error
from CryptoCalculatorService.BalanceService import BalanceService, PROJECT_NAME
from CryptoCalculatorService.scheduler.Scheduler import Scheduler
from cryptodataaccess.Memory import USER_NOTIFICATIONS_MEMORY_KEY


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




def test_create_Users_Repo():
    cfg = configure_app()
    do_connect(cfg)
    config = configure_app()
    store = UsersMongoStore(config, mock_log)
    repo = UsersRepository(store)
    assert (repo.memories[USER_NOTIFICATIONS_MEMORY_KEY] is not None)




def raise_Exception():
    raise Exception("test")
