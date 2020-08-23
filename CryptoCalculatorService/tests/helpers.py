import json
import pymongo
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptodataaccess.helpers import get_url, do_connect
from cryptomodel.cryptomodel import user_notification

from CryptoCalculatorService.config import configure_app
import pytest
import mock

BASE_PATH = 'CryptoCalculatorService/tests/'

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

def get_exchange_rates_record():
    with open(BASE_PATH + 'sample_records/exchange_rate.json', 'r') as my_file:
        return json.load(my_file)


def get_prices_record():
    with open(BASE_PATH + 'sample_records/price.json', 'r') as my_file:
        return json.load(my_file)


def get_prices_2020706_record():
    with open(BASE_PATH + 'sample_records/prices_2020706.json', 'r') as my_file:
        return json.load(my_file)


def get_prices_2020731_record():
    with open(BASE_PATH + 'sample_records/prices_2020731.json', 'r') as my_file:
        return json.load(my_file)


def get_prices20200812039_record():
    with open(BASE_PATH + 'sample_records/prices_2038.json', 'r') as my_file:
        return json.load(my_file)


def get_prices20200801T2139_record():
    with open(BASE_PATH + 'sample_records/prices2039.json', 'r') as my_file:
        return json.load(my_file)


# refactor file use this only
def insert_prices_record_with_method(method_to_get):
    config = configure_app()
    client = pymongo.MongoClient(get_url(config))
    db = client[config.DATABASE]
    prices_col = db["prices"]
    prices_col.insert(method_to_get())


def insert_prices_record():
    config = configure_app()
    client = pymongo.MongoClient(get_url(config))
    db = client[config.DATABASE]
    prices_col = db["prices"]
    prices_col.insert(get_prices_record())


def delete_prices():
    config = configure_app()
    client = pymongo.MongoClient(get_url(config))
    db = client[config.DATABASE]
    prices_col = db["prices"]
    prices_col.delete_many({})


def insert_prices_2020706_record():
    config = configure_app()
    client = pymongo.MongoClient(get_url(config))
    db = client[config.DATABASE]
    prices_col = db["prices"]
    prices_col.insert(get_prices_2020706_record())


def insert_prices_2020731_record():
    config = configure_app()
    client = pymongo.MongoClient(get_url(config))
    db = client[config.DATABASE]
    prices_col = db["prices"]
    prices_col.insert(get_prices_2020731_record())


def insert_exchange_record():
    config = configure_app()
    client = pymongo.MongoClient(get_url(config))
    db = client[config.DATABASE]
    exchange_rates_col = db["exchange_rates"]
    exchange_rates_col.insert(get_exchange_rates_record())


@pytest.fixture(scope='module')
def mock_log():
    with mock.patch("Logger.logger.log_error"
                    ) as _mock:
        _mock.return_value = True
        yield _mock
