import pytest
import json
from bson import ObjectId
from cryptomodel.operations import OPERATIONS

from server import configure_app, create_app
from cryptodataaccess.helpers import do_connect, log_error
from cryptomodel.cryptostore import user_transaction


@pytest.fixture(scope='module')
def test_client():
    flask_app = create_app()
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client  # this is where the testing happens!
    ctx.pop()


