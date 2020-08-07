import time
from datetime import datetime

import jsonpickle
import mock
from bson import ObjectId
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptomodel.cryptostore import user_transaction, user_notification
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
    ut2.order_type = "BUY"
    ut2.transaction_type = "TRADE"

    transactions = [jsonpickle.encode(ut2)]
    s = Scedhuler(config)
    s.delete_and_insert_transactions(transactions)
    uts2 = repo.get_transactions(1)
    assert (len(uts2) == 1)


def test_syncronize_notifications():
    config, users_repo, trans_repo = setup_repos_and_clear_data()

    un = user_notification()
    un.source_id = ObjectId('666f6f2d6261722d71757578')
    un.id = ObjectId('666f6f2d6261722d71757578')
    un.operation = OPERATIONS.ADDED.name
    un.channel_type = "tele"
    un.check_every_seconds = 1
    un.check_times = 3
    nots = [jsonpickle.encode(un)]
    s = Scedhuler(config)
    s.delete_and_insert_notifications(nots)
    uts2 = users_repo.get_notifications(1)
    assert (len(uts2) == 1)


def setup_repos_and_clear_data():
    cfg = configure_app()
    do_connect(cfg)
    config = configure_app()
    users_store = UsersMongoStore(config, mock_log)
    users_repo = UsersRepository(users_store)

    trans_store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(trans_store)
    do_connect(config)
    user_notification.objects.all().delete()
    return config, users_repo, trans_repo


def test_create_Users_Repo():
    cfg = configure_app()
    do_connect(cfg)
    config = configure_app()
    store = UsersMongoStore(config, mock_log)
    repo = UsersRepository(store)
    assert (repo.memories[USER_NOTIFICATIONS_MEMORY_KEY] is not None)


def test_produce_to_kafka_inserts_to_mongo():
    un = user_notification()
    un.source_id = ObjectId('666f6f2d6261722d71757578')
    un.id = ObjectId('666f6f2d6261722d71757578')
    un.operation = OPERATIONS.ADDED.name
    un.channel_type = "tele"
    un.check_every_seconds = 1
    un.check_times = 3
    config, users_repo, trans_repo = setup_repos_and_clear_data()

    users_repo.add_notification(un.user_id, un.user_name, un.user_email, un.expression_to_evaluate,
                                un.check_every_seconds, un.check_times,
                                un.is_active, un.channel_type, un.fields_to_send, un.source_id)
    users_repo.commit()
    produce(broker_names=users_repo.users_store.configuration.KAFKA_BROKERS,
            topic=users_repo.users_store.configuration.USER_NOTIFICATIONS_TOPIC_NAME
            , data_item=jsonpickle.encode(un))

    s = Scedhuler(config, run_forever=False, consumer_time_out=100)
    s.synchronize_transactions_and_user_notifications()

    assert (len(user_notification.objects()) == 1)


def test_on_consume_notifications_throws_exception_should_catch_and_log(mock_log):
    with mock.patch.object(Scedhuler, "consume_notifications"
                           ) as _mock:
        _mock.side_effect = raise_Exception
        config, users_repo, trans_repo = setup_repos_and_clear_data()
        un = user_notification()
        un.source_id = ObjectId('666f6f2d6261722d71757578')
        un.id = ObjectId('666f6f2d6261722d71757578')
        un.operation = OPERATIONS.ADDED.name
        un.channel_type = "tele"
        un.check_every_seconds = 1
        un.check_times = 3
        users_repo.add_notification(un.user_id, un.user_name, un.user_email, un.expression_to_evaluate,
                                    un.check_every_seconds, un.check_times,
                                    un.is_active, un.channel_type, un.fields_to_send, un.source_id)
        users_repo.commit()
        produce(broker_names=users_repo.users_store.configuration.KAFKA_BROKERS,
                topic=users_repo.users_store.configuration.USER_NOTIFICATIONS_TOPIC_NAME
                , data_item=jsonpickle.encode(un))

        s = Scedhuler(config, run_forever=False, consumer_time_out=5000)
        s.synchronize_transactions_and_user_notifications()
        assert _mock.called


def raise_Exception():
    raise Exception("test")