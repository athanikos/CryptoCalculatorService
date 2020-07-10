from CryptoModel.calculator.CurrencyConverter import CurrencyConverter, ConversionOrder
from CryptoModel.data_access.Repository import Repository
from CryptoModel.config import  configure_app
from CryptoModel.test.helpers import mock_log, insert_exchange_record
from CryptoModel.data_access.helpers import do_connect


def test_convert():
    config = configure_app()
    repo = Repository(config, mock_log)
    do_connect(config)
    insert_exchange_record()
    objs = repo.fetch_latest_exchange_rates_to_date('2051-07-02')
    cc = CurrencyConverter(objs[0])
    assert(100 == cc.convert_value(100,"EUR","EUR"))
    assert(cc.conversion_is_possible("EUR","EUR") == ConversionOrder.Same)
    assert (1.123634 * 100  == cc.convert_value( 100, "EUR", "USD"))
    assert (cc.conversion_is_possible("EUR","USD") == ConversionOrder.FromIsBase)







