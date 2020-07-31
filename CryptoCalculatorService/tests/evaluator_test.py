import datetime

from cryptodataaccess.Rates.RatesMongoStore import RatesMongoStore
from cryptodataaccess.helpers import do_connect
import mock
import pytest
from CryptoCalculatorService.config import configure_app
from cryptodataaccess.Rates.RatesRepository import RatesRepository
from cryptomodel.coinmarket import prices
from tests.helpers import insert_prices_record
from CryptoCalculatorService.tests.helpers import mock_log, insert_exchange_record

@pytest.fixture(scope='module')
def mock_log():
    with mock.patch("cryptodataaccess.helpers.log_error"
                    ) as _mock:
        _mock.return_value = True
        yield _mock

