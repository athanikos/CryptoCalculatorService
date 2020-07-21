import mock
import pytest
from cryptomodel.cryptostore import user_transaction
from server import configure_app, create_app
import mock
import pytest


@pytest.fixture(scope='module')
def test_client():
    flask_app = create_app()
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client  # this is where the testing happens!
    ctx.pop()


def test_basic():
    assert (1 == 1)
