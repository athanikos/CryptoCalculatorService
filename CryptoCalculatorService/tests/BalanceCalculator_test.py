from datetime import date, datetime
from tests.helpers import insert_prices_record, \
    insert_prices_2020706_record, delete_prices
from cryptomodel.cryptostore import user_transaction
from calculator.BalanceCalculator import BalanceCalculator
from cryptodataaccess.RatesRepository import RatesRepository
from cryptodataaccess.TransactionRepository import TransactionRepository
from CryptoCalculatorService.config import  configure_app
from CryptoCalculatorService.tests.helpers import mock_log, insert_exchange_record
from cryptodataaccess.helpers import do_connect
DATE_FORMAT = '%Y-%m-%d'


def test_bc_create_1_item():
    config = configure_app()
    repo = TransactionRepository(config, mock_log)
    rates_repo = RatesRepository(config,mock_log)
    do_connect(config)
    user_transaction.objects.all().delete()
    delete_prices()
    insert_exchange_record()
    insert_prices_record()
    repo.insert_transaction(1,volume=10,symbol="BTC", value=2, price=1,currency="EUR",date="2020-01-01",source="kraken",
                            source_id=None,operation="Added"
                            )
    transactions = repo.fetch_transactions(1)
    symbols = rates_repo.fetch_symbol_rates()
    ers = rates_repo.fetch_latest_exchange_rates_to_date('2051-07-02')
    bc = BalanceCalculator(transactions, symbols.rates, ers,"EUR")
    sr = bc.symbol_rates["BTC"]
    assert (sr.price == 8101.799293468747)
    assert (sr.volume_24h == 13467618568.254385)
    assert (sr.percent_change_1h == -0.21539969)
    assert (sr.percent_change_24h == -0.85068831)
    assert (sr.percent_change_7d == -1.26435364)
    assert (sr.market_cap == 149249013266.08475)
    dt_now = datetime.today().strftime(DATE_FORMAT)
    out = bc.compute(1, dt_now)
    assert (len(out.user_grouped_symbol_values) == 1)
    assert (out.user_grouped_symbol_values[0].user_symbol_values[0].converted_value == 10 * 8101.799293468747)
    assert (out.user_grouped_symbol_values[0].user_symbol_values[0].date_time_calculated == dt_now)


def test_bc_create_2_items():
    config = configure_app()
    repo = TransactionRepository(config, mock_log)
    rates_repo = RatesRepository(config,mock_log)
    do_connect(config)
    insert_exchange_record()
    insert_prices_record()
    user_transaction.objects.all().delete()
    repo.insert_transaction(1,volume=10,symbol="BTC", value=2, price=1,currency="EUR",date="2020-01-01",source="kraken",
                            source_id=None, operation="Added"
                            )
    repo.insert_transaction(1, volume=2, symbol="BTC", value=2, price=1, currency="EUR", date="2020-01-01",
                            source="kraken", source_id=None,operation="Added" )

    transactions = repo.fetch_transactions(1)
    assert (len(transactions) == 2 )
    symbols = rates_repo.fetch_symbol_rates()
    ers = rates_repo.fetch_latest_exchange_rates_to_date('2051-07-02')
    bc = BalanceCalculator(transactions, symbols.rates, ers,"EUR")
    sr = bc.symbol_rates["BTC"]
    dt_now = datetime.today().strftime(DATE_FORMAT)
    tsv =  bc.compute(1, dt_now)
    assert (len(tsv.user_grouped_symbol_values) == 1)
    assert (tsv.user_grouped_symbol_values[0].volume == 12)
    assert ( len(tsv.user_grouped_symbol_values[0].user_symbol_values) == 2)
    bc.compute(tsv,"EUR")
    assert (tsv.converted_value == 8101.799293468747 * 12 )


def test_bc_create_ADA_19796():
    config = configure_app()
    repo = TransactionRepository(config, mock_log)
    rates_repo = RatesRepository(config,mock_log)
    do_connect(config)
    insert_exchange_record()
    insert_prices_2020706_record()
    user_transaction.objects.all().delete()
    repo.insert_transaction(1,volume=19796,symbol="ADA", value=69, price=1,currency="EUR",date="2020-07-13",source="kraken",
                            source_id=None,operation="Added"
                            )
    transactions = repo.fetch_transactions(1)
    assert (len(transactions) == 1 )
    symbols = rates_repo.fetch_symbol_rates()
    dt_now = datetime.today().strftime(DATE_FORMAT)
    ers = rates_repo.fetch_latest_exchange_rates_to_date(dt_now)
    bc = BalanceCalculator(transactions, symbols.rates, ers,"EUR")
    sr = bc.symbol_rates["BTC"]

    tsv =  bc.compute(1, dt_now)
    assert(bc.symbol_rates['ADA'].price == 0.08672453072885744  )
    assert (len(tsv.user_grouped_symbol_values) == 1)
    assert (tsv.user_grouped_symbol_values[0].volume == 19796)
    assert ( len(tsv.user_grouped_symbol_values[0].user_symbol_values) == 1)
    bc.compute(tsv,"EUR")
    assert (tsv.converted_value == 19796 * 0.08672453072885744  )


def test_fetch_latest_exchange_rates_to_date_returns_latest_record():
    config = configure_app()
    repo = RatesRepository(config, mock_log)
    trans_repo = TransactionRepository(config,mock_log)
    do_connect(config)
    user_transaction.objects.all().delete()
    delete_prices()

    insert_prices_record()#0.08410447380210428
    insert_prices_2020706_record()#0.08672453072885744

    symbols = repo.fetch_symbol_rates()

    trans_repo.insert_transaction(1, volume=19796, symbol="ADA", value=69, price=1, currency="EUR", date="2020-07-13",
                            source="kraken", source_id=None, operation="Added")
    transactions = trans_repo.fetch_transactions(1)
    assert (len(transactions) == 1)
    symbols = repo.fetch_symbol_rates()


    ers = repo.fetch_latest_exchange_rates_to_date('2051-07-15')
    bc = BalanceCalculator(transactions, symbols.rates, ers, "EUR")
    # should return 2020706 = 0.08672453072885744

    assert (bc.symbol_rates['ADA'].price == 0.08672453072885744)


