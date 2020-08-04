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
from cryptodataaccess.helpers import convert_to_int_timestamp

DATE_FORMAT = '%Y-%m-%d'


def test_fetch_symbol_rates_for_dat_with_two_entries_within_two_hours():
    config = configure_app()
    users_store = RatesMongoStore(config, mock_log)
    rates_repo = RatesRepository(users_store)
    do_connect(config)
    dt_now = datetime.today().strftime(DATE_FORMAT)
    user_transaction.objects.all().delete()
    prices.objects.all().delete()
    insert_exchange_record()
    insert_prices_record_with_method(get_prices20200812039_record)
    insert_prices_record_with_method(get_prices20200801T2139_record)
    rts = rates_repo.fetch_symbol_rates_for_date( convert_to_int_timestamp(datetime.today()))
                                                                        # 1596314291000        2020/08/01 20:38
                                                                        # 1596315611000        2020/08/01 21:00
    dt = datetime(year=2020, month=8 , day=1, hour=21, minute=0 ) #1596315600
    rts =  rates_repo.fetch_symbol_rates_for_date(convert_to_int_timestamp(dt))
    assert(rts.rates['BTC'].last_updated =='2013-04-28T00:00:00.000Z' )


