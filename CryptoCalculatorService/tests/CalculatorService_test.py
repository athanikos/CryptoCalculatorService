from bson import ObjectId
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore
from cryptodataaccess.Users.UsersMongoStore import UsersMongoStore
from cryptodataaccess.Users.UsersStore import UsersStore
from cryptodataaccess.Users.UsersRepository import UsersRepository
from cryptodataaccess.helpers import do_connect
from cryptomodel.cryptostore import user_transaction, user_settings


from CryptoCalculatorService.CalculatorService import CalculatorService
from CryptoCalculatorService.config import config, configure_app
from CryptoCalculatorService.tests.apsscedhuler_test import mock_log

DATE_FORMAT = '%Y-%m-%d'


def test_compute_with_non_existing_key():
    config = configure_app()
    cs  = CalculatorService(config)

    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)


    users_store = UsersMongoStore(config, mock_log)
    users_repo = UsersRepository(users_store)
    do_connect(config)
    user_transaction.objects.all().delete()
    user_settings.objects.all().delete()

    trans_repo.add_transaction(1, 1, 'OXT', 1, 1, "EUR", "2020-01-01", "kraken",
                         source_id=ObjectId('666f6f2d6261722d71757578'))
    trans_repo.commit()
    assert (len(user_transaction.objects) == 1)
    user_settings.objects.all().delete()
    users_repo.add_user_settings(user_id=1, preferred_currency='EUR', source_id=ObjectId('666f6f2d6261722d71757578'))
    users_repo.commit()
    out = cs.compute(1)
    print(out)
    assert (out)

def test_compute_with_existing_key():
    config = configure_app()
    cs  = CalculatorService(config)

    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)


    users_store = UsersMongoStore(config, mock_log)
    users_repo = UsersRepository(users_store)
    do_connect(config)
    user_transaction.objects.all().delete()
    user_settings.objects.all().delete()

    trans_repo.add_transaction(1, 1, 'BTC', 1, 1, "EUR", "2020-01-01", "kraken",
                         source_id=ObjectId('666f6f2d6261722d71757578'))
    trans_repo.commit()
    assert (len(user_transaction.objects) == 1)
    user_settings.objects.all().delete()
    users_repo.add_user_settings(user_id=1, preferred_currency='EUR', source_id=ObjectId('666f6f2d6261722d71757578'))
    users_repo.commit()
    out = cs.compute(1)
    print(out)
    assert (out)
