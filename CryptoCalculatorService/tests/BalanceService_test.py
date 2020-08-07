from datetime import date, datetime

import jsonpickle
from bson import ObjectId
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Users.UsersStore import UsersStore
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptodataaccess.helpers import do_connect, convert_to_int_timestamp
from cryptomodel.cryptomodel import exchange_rates, prices
from cryptomodel.cryptostore import user_transaction, user_settings

from CryptoCalculatorService.BalanceService import BalanceService
from CryptoCalculatorService.config import config, configure_app
from CryptoCalculatorService.tests.Scedhuler_test import mock_log
from CryptoCalculatorService.tests.helpers import insert_prices_record, insert_exchange_record, \
    insert_prices_2020731_record

DATE_FORMAT = '%Y-%m-%d'


def test_compute_with_non_existing_key():
    config, trans_repo, users_repo = delete_data_and_setup_repositories()
    cs = BalanceService(config)


    trans_repo.add_transaction(1, 1, 'OXT', 1, 1, "EUR", date(year=2020,month=1, day=1), "kraken",
                               source_id=ObjectId('666f6f2d6261722d71757578'), transaction_type="TRADE", order_type="BUY")
    trans_repo.commit()
    assert (len(user_transaction.objects) == 1)
    user_settings.objects.all().delete()
    users_repo.add_user_settings(user_id=1, preferred_currency='EUR', source_id=ObjectId('666f6f2d6261722d71757578'))
    users_repo.commit()
    out = jsonpickle.decode(cs.compute_balance(1))
    assert (out.transactions[0].is_valid == False)  # OXT does not exist


def delete_data_and_setup_repositories():
    user_transaction.objects.all().delete()
    exchange_rates.objects.all().delete()
    prices.objects.all().delete()
    user_settings.objects.all().delete()
    insert_prices_record()
    insert_exchange_record()
    config = configure_app()
    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)
    users_store = UsersMongoStore(config, mock_log)
    users_repo = UsersRepository(users_store)
    do_connect(config)
    return config, trans_repo, users_repo


def test_compute_with_existing_key():
    config, trans_repo, users_repo = delete_data_and_setup_repositories()

    insert_prices_record()
    insert_exchange_record()
    cs = BalanceService(config)
    trans_repo.add_transaction(1, 1, 'BTC', 1, 1, "EUR", date(year=2020, month=1,day=1), "kraken",
                               source_id=ObjectId('666f6f2d6261722d71757578'), transaction_type="TRADE", order_type="BUY")
    trans_repo.commit()
    assert (len(user_transaction.objects) == 1)
    user_settings.objects.all().delete()
    users_repo.add_user_settings(user_id=1, preferred_currency='EUR', source_id=ObjectId('666f6f2d6261722d71757578'))
    users_repo.commit()



    out = jsonpickle.decode(cs.compute_balance(1))
    assert (out.transactions[0].is_valid == True)  # OXT does not exist


def test_four_transactions_same_symbol():
    user_transaction.objects.all().delete()
    exchange_rates.objects.all().delete()
    prices.objects.all().delete()
    insert_prices_2020731_record()
    insert_exchange_record()
    config = configure_app()
    cs = BalanceService(config)
    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)
    users_store = UsersMongoStore(config, mock_log)
    users_repo = UsersRepository(users_store)
    do_connect(config)
    user_transaction.objects.all().delete()
    user_settings.objects.all().delete()

    trans_repo.add_transaction(user_id=1, source_id=None, currency="EUR", date="2020-07-30", volume=1000.71140621,
                               value=211, symbol="XRP",
                               price=0.21085, source="kraken",transaction_type="TRADE", order_type="BUY")
    trans_repo.add_transaction(user_id=1, source_id=None, currency="EUR", date="2020-07-29", volume=245.08602519,
                               value=50, symbol="XRP",
                               price=0.20401, source="kraken", transaction_type="TRADE", order_type="BUY")
    trans_repo.add_transaction(user_id=1, source_id=None, currency="EUR", date="2020-07-29", volume=487.16324840,
                               value=99.93179, symbol="XRP",
                               price=0.20527, source="kraken", transaction_type="TRADE", order_type="BUY")
    trans_repo.add_transaction(user_id=1, source_id=None, currency="EUR", date="2020-07-28", volume=500, value=96.70500,
                               symbol="XRP",
                               price=0.19344, source="kraken", transaction_type="TRADE", order_type="BUY")

    trans_repo.commit()
    assert (len(user_transaction.objects) == 4)
    user_settings.objects.all().delete()
    users_repo.add_user_settings(user_id=1, preferred_currency='EUR', source_id=ObjectId('666f6f2d6261722d71757578'))
    users_repo.commit()

    tdt = date(year=2020,month=8,day=1)
    sdt = convert_to_int_timestamp( datetime(year=2030,month=8,day=1))
    out = jsonpickle.decode(cs.compute_balance_with_upperbound_dates(1, upper_bound_symbol_rates_date=sdt,
                                                                     upper_bound_transaction_date=tdt))
    assert (len(out.transactions) == 4)

    assert (out.converted_value == 0.2134997315708581 * (500 + 487.16324840 + 245.08602519 + 1000.71140621))


