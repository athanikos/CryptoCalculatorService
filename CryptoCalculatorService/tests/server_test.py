import pytest

from CryptoCalculatorService.scedhuler.Scedhuler import Scedhuler
from CryptoCalculatorService.tests.helpers import setup_repos_and_clear_data
from server import configure_app, create_app


@pytest.fixture(scope='module')
def test_client():
    flask_app = create_app()
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client  # this is where the testing happens!
    ctx.pop()

