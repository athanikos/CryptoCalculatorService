import mock
from bson import ObjectId
from pymongo.errors import ServerSelectionTimeoutError
from cryptomodel.cryptostore import user_channel, user_transaction, user_notification
from cryptomodel.coinmarket import prices
from cryptomodel.fixer import exchange_rates
from CryptoCalculatorService.config import configure_app
from CryptoCalculatorService.data_access.Repository import Repository
import pytest
from CryptoCalculatorService.data_access import helpers
from tests.helpers import insert_prices_record, insert_exchange_record


@pytest.fixture(scope='module')
def mock_log():
    with mock.patch("Logger.logger.log_error"
                    ) as _mock:
        _mock.return_value = True
        yield _mock


def test_fetch_symbol_rates():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    prices.objects.all().delete()
    insert_prices_record()
    objs = repo.fetch_symbol_rates()
    assert (len(objs.rates) == 100)
    assert(objs.rates['BTC'].price ==    8101.799293468747)


def test_fetch_exchange_rates():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    exchange_rates.objects.all().delete()
    insert_exchange_record()
    objs = repo.fetch_latest_exchange_rates_to_date('1900-01-01')
    assert (len(objs) == 0)
    objs = repo.fetch_latest_exchange_rates_to_date('2020-07-04')
    assert (len(objs) == 1)
    objs = repo.fetch_latest_exchange_rates_to_date('2020-07-03')
    assert (len(objs) == 1)
    assert (objs[0].rates.AED == 4.127332)
    objs = repo.fetch_latest_exchange_rates_to_date('2020-07-02')
    assert (len(objs) == 0)


def test_fetch_prices_and_symbols():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    prices.objects.all().delete()
    insert_prices_record()
    objs = repo.fetch_latest_prices_to_date('2020-07-03')  # bound case : timestamp is saved as string so it cant find
    # it because  of the time stuff (either+1 day?)
    assert (len(objs) == 0)
    objs = repo.fetch_latest_prices_to_date('2020-07-04')
    assert (len(objs) == 1)
    symbols = repo.fetch_symbols()
    assert (len(symbols) == 100)


def test_insert_user_channel():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_channel.objects.all().delete()
    uc = repo.insert_user_channel(1, 'da', '1')
    assert (uc.channel_type == 'da')


def test_log_when_do_connect_raises_exception(mock_log):
    with mock.patch("CryptoModel.data_access.helpers.do_connect"
                    ) as _mock:
        _mock.side_effect = ServerSelectionTimeoutError("hi")
        with mock.patch("Logger.logger.log_error") as log:
            with pytest.raises(ServerSelectionTimeoutError):
                repo = Repository(configure_app(), mock_log)
                repo.insert_user_channel(1, "telegram", 1)
            mock_log.assert_called()


def test_insert_transaction():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_transaction.objects.all().delete()
    ut = repo.insert_transaction(1, 1, 'OXT', 1, 1, "USD", "2020-01-01", "kraken")
    assert (ut.user_id == 1)
    assert (ut.symbol == "OXT")
    assert (len(user_transaction.objects) == 1)


def test_update_transaction():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_transaction.objects.all().delete()
    ut = repo.insert_transaction(1, 1, 'OXT', 1, 1, "USD", "2020-01-01", "kraken")
    ut = repo.update_transaction(ut.id, 1, 1, 'OXT2', 1, 1, "EUR", "2020-01-01", "kraken")
    assert (ut.user_id == 1)
    assert (ut.symbol == "OXT2")
    assert (ut.currency == "EUR")


def test_update_transaction_when_does_not_exist_throws_ValueError():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_transaction.objects.all().delete()
    with pytest.raises(ValueError):
        repo.update_transaction(ObjectId('666f6f2d6261722d71757578'), 1, 1, 'OXT', "EUR", 1, 1, "2020-01-01",
                                     "kraken")


def test_update_notification_when_does_not_exist_throws_ValueError():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_notification.objects.all().delete()
    with pytest.raises(ValueError):
        repo.update_notification(ObjectId('666f6f2d6261722d71757578'), 1, 'nik', "nik@test.com", 1, "field_name",
                                      ">",1,1,"OXT","telegram")


def test_update_notification():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_notification.objects.all().delete()

    un = repo.insert_notification( 1, 'nik', "nik@test.com", 1, "field_name",
                                      ">",1,1,"OXT","telegram")
    un = repo.update_notification(un.id,  1, 'nik2', "nik@test.com", 1, "field_name",
                                      ">",1,1,"OXT","telegram")
    assert (un.user_name == "nik2")


def test_delete_transaction_when_does_not_exist_throws_ValueError():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_transaction.objects.all().delete()
    with pytest.raises(ValueError):
        repo.delete_transaction(ObjectId('666f6f2d6261722d71757578'))


def test_delete_transaction_when_exists():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    ut = repo.insert_transaction(1, 1, 'OXT', 1, 1, "EUR", "2020-01-01", "kraken")
    assert (len(user_transaction.objects) == 1)
    ut = repo.delete_transaction(ut.id)
    assert (len(user_transaction.objects) == 0)


def test_delete_notification_when_exists():
    config = configure_app()
    repo = Repository(config, mock_log)
    helpers.do_connect(config)
    user_notification.objects.all().delete()
    ut = repo.insert_notification(1, 'nik', 'nik@OXT.com', 100, 'field1', ">", 1, 1,"OXT","telegram")
    assert (len(user_notification.objects) == 1)
    ut = repo.delete_notification(ut.id)
    assert (len(user_notification.objects) == 0)



