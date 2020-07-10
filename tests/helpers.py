import json
import pymongo
from CryptoCalculatorService.data_access.helpers import get_url
from CryptoCalculatorService.config import configure_app
import pytest
import mock


def get_exchange_rates_record():
    with open('../../sample_records/exchange_rate.json', 'r') as my_file:
        return json.load(my_file)


def get_prices_record():
    with open('../../sample_records/price.json', 'r') as my_file:
        return json.load(my_file)


def get_prices_2020706_record():
    with open('../../sample_records/prices_2020706.json', 'r') as my_file:
        return json.load(my_file)


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
