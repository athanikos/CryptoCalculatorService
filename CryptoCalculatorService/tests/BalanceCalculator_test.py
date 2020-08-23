from datetime import date, datetime
from cryptodataaccess.Rates.RatesMongoStore import RatesMongoStore
from cryptodataaccess.Transactions.TransactionMongoStore import TransactionMongoStore
from cryptomodel.order_types import ORDER_TYPES
from cryptomodel.transaction_types import TRANSACTION_TYPES
from CryptoCalculatorService.tests.helpers import  insert_prices_record, delete_prices, insert_prices_2020706_record
from cryptomodel.cryptostore import user_transaction
from calculator.BalanceCalculator import BalanceCalculator
from cryptodataaccess.Rates.RatesRepository import RatesRepository
from cryptodataaccess.Transactions.TransactionRepository import TransactionRepository

from CryptoCalculatorService.config import  configure_app
from CryptoCalculatorService.tests.helpers import mock_log, insert_exchange_record
from cryptodataaccess.helpers import do_connect, convert_to_int_timestamp
DATE_FORMAT = '%Y-%m-%d'


def test_bc_create_1_item():
    config = configure_app()

    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)
    users_store = RatesMongoStore(config, mock_log)
    rates_repo = RatesRepository(users_store)
    do_connect(config)
    user_transaction.objects.all().delete()
    delete_prices()
    insert_exchange_record()
    insert_prices_record()
    dt = date(year=2020,month=1, day =1)
    trans_repo.add_transaction(1,volume=10,symbol="BTC", value=2, price=1,currency="EUR",date=dt,source="kraken",
                            source_id=None, transaction_type=TRANSACTION_TYPES.TRADE.name, order_type=ORDER_TYPES.BUY.name)
    trans_repo.commit()
    transactions = trans_repo.get_transactions(1)
    symbols = rates_repo.fetch_symbol_rates()
    dt_now =  convert_to_int_timestamp(datetime.today())
    ers = rates_repo.fetch_latest_exchange_rates_to_date(dt_now)
    bc = BalanceCalculator(transactions, symbols.rates, ers,"EUR",
                           upper_bound_transaction_date=dt_now, upper_bound_symbol_rates_date=dt_now)
    sr = bc.symbol_rates["BTC"]
    assert (sr.price == 8101.799293468747)
    assert (sr.volume_24h == 13467618568.254385)
    assert (sr.percent_change_1h == -0.21539969)
    assert (sr.percent_change_24h == -0.85068831)
    assert (sr.percent_change_7d == -1.26435364)
    assert (sr.market_cap == 149249013266.08475)

    out = bc.compute(1, dt_now)
    assert (len(out.user_grouped_symbol_values.items()) == 1)
    assert (out.user_grouped_symbol_values['BTC'].user_symbol_values[0].converted_value == 10 * 8101.799293468747)
    assert (out.user_grouped_symbol_values['BTC'].user_symbol_values[0].date_time_calculated == dt_now)


def test_bc_create_2_items():
    config = configure_app()


    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)
    users_store = RatesMongoStore(config, mock_log)
    rates_repo = RatesRepository(users_store)
    do_connect(config)
    insert_exchange_record()
    insert_prices_record()

    dt = datetime(year=2020, day=1, month=1)
    user_transaction.objects.all().delete()
    trans_repo.add_transaction(1,volume=10,symbol="BTC", value=2, price=1,currency="EUR",date=dt,source="kraken",
                            source_id=None, transaction_type="TRADE", order_type="BUY"
                            )
    trans_repo.add_transaction(1, volume=2, symbol="BTC", value=2, price=1, currency="EUR", date=dt,
                            source="kraken", source_id=None,  transaction_type="TRADE", order_type="BUY")
    trans_repo.commit()
    transactions = trans_repo.get_transactions(1)
    assert (len(transactions) == 2 )
    symbols = rates_repo.fetch_symbol_rates()

    dt_now =  convert_to_int_timestamp(datetime.today())

    ers = rates_repo.fetch_latest_exchange_rates_to_date(dt_now)
    bc = BalanceCalculator(transactions, symbols.rates, ers,"EUR", upper_bound_transaction_date=dt_now,upper_bound_symbol_rates_date=dt_now)
    sr = bc.symbol_rates["BTC"]

    tsv =  bc.compute(user_id=1, date= dt_now)
    assert (len(tsv.user_grouped_symbol_values.items()) == 1)
    assert (tsv.user_grouped_symbol_values['BTC'].volume == 12)
    assert ( len(tsv.user_grouped_symbol_values['BTC'].user_symbol_values) == 2)
    bc.compute(tsv,"EUR")
    assert (tsv.converted_value == 8101.799293468747 * 12 )


def test_bc_create_ADA_19796():
    config = configure_app()
    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)
    users_store = RatesMongoStore(config, mock_log)
    rates_repo = RatesRepository(users_store)
    do_connect(config)

    dt_now = datetime.today().strftime(DATE_FORMAT)

    insert_exchange_record()
    insert_prices_2020706_record()
    user_transaction.objects.all().delete()


    dt = datetime(year=2020, month=7, day =13 )
    trans_repo.add_transaction(1,volume=19796,symbol="ADA", value=69, price=1,currency="EUR",date="2020-07-13",source="kraken",
                            source_id=None, transaction_type="TRADE", order_type="BUY"
                            )
    trans_repo.commit()

    transactions = trans_repo.get_transactions(1)
    assert (len(transactions) == 1 )
    symbols = rates_repo.fetch_symbol_rates()
    dt_now = convert_to_int_timestamp(datetime.today())
    ers = rates_repo.fetch_latest_exchange_rates_to_date(dt_now)
    bc = BalanceCalculator(transactions, symbols.rates, ers,"EUR",upper_bound_transaction_date=dt_now, upper_bound_symbol_rates_date=dt_now)
    sr = bc.symbol_rates["BTC"]

    tsv =  bc.compute(1, dt_now)
    assert(bc.symbol_rates['ADA'].price == 0.08410447380210428 )
    assert (len(tsv.user_grouped_symbol_values) == 1)
    assert (tsv.user_grouped_symbol_values['ADA'].volume == 19796)
    assert ( len(tsv.user_grouped_symbol_values['ADA'].user_symbol_values) == 1)
    bc.compute(tsv,"EUR")
    assert (tsv.converted_value == 19796 * 0.08410447380210428  )


def test_fetch_latest_exchange_rates_to_date_returns_latest_record():
    config = configure_app()
    store = TransactionMongoStore(config, mock_log)
    trans_repo = TransactionRepository(store)
    rates_store = RatesMongoStore(config, mock_log)
    rates_repo = RatesRepository(rates_store)

    user_transaction.objects.all().delete()
    delete_prices()
    insert_prices_record()#0.08410447380210428 later timestamp is higher
    insert_prices_2020706_record()#0.08672453072885744
    symbols = rates_repo.fetch_symbol_rates()
    dt = date(month=7,day=13,year=2020)
    trans_repo.add_transaction(1, volume=19796, symbol="ADA", value=69, price=1, currency="EUR", date=dt,
                            source="kraken", source_id=None, transaction_type="TRADE", order_type="BUY")
    trans_repo.commit()
    dt_now = datetime.today().strftime(DATE_FORMAT)
    transactions = trans_repo.get_transactions(1)
    assert (len(transactions) == 1)
    symbols = rates_repo.fetch_symbol_rates()
    dt_now = convert_to_int_timestamp(datetime.today())
    ers = rates_repo.fetch_latest_exchange_rates_to_date(dt_now)
    bc = BalanceCalculator(transactions, symbols.rates, ers, "EUR",
                           upper_bound_transaction_date=dt_now, upper_bound_symbol_rates_date=dt_now)
    assert (bc.symbol_rates['ADA'].price == 0.08410447380210428)



