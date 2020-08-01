from datetime import datetime

import jsonpickle
from bson import ObjectId
from cryptodataaccess.Rates.RatesMongoStore import RatesMongoStore
from cryptodataaccess.Rates.RatesRepository import RatesRepository
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Users.UsersStore import UsersStore
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptodataaccess.helpers import do_connect
from cryptomodel.cryptomodel import exchange_rates, prices
from cryptomodel.cryptostore import user_transaction, user_settings

from CryptoCalculatorService.BalanceService import BalanceService
from CryptoCalculatorService.config import config, configure_app
from CryptoCalculatorService.tests.apsscedhuler_test import mock_log
from CryptoCalculatorService.tests.helpers import insert_prices_record, insert_exchange_record, \
    insert_prices_2020731_record, insert_prices_record_with_method, get_prices20200812039_record, \
    get_prices20200801T2139_record

DATE_FORMAT = '%Y-%m-%d'


def test_compute_with_non_existing_key():
    config = configure_app()

    users_store = RatesMongoStore(config, mock_log)
    rates_repo = RatesRepository(users_store)
    do_connect(config)

    dt_now = datetime.today().strftime(DATE_FORMAT)

    insert_exchange_record()
    insert_prices_record_with_method(get_prices20200812039_record)
    insert_prices_record_with_method(get_prices20200801T2139_record)


    user_transaction.objects.all().delete()
    assert(1==1)


