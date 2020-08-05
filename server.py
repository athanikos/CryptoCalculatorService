import atexit

import pymongo
from flask import Flask, jsonify, request
from flask.blueprints import Blueprint

from CryptoCalculatorService.BalanceService import BalanceService
from CryptoCalculatorService.PricesService import PricesService
from CryptoCalculatorService.config import configure_app
from CryptoCalculatorService.scedhuler.Scedhuler import Scedhuler

bp = Blueprint(__name__.split('.')[0], __name__.split('.')[0])
bs = BalanceService(configure_app())
ps = PricesService(configure_app())
scedhuler = Scedhuler(configure_app())


def create_app():
    the_app = Flask(__name__.split('.')[0], instance_relative_config=True)
    the_app.register_blueprint(bp)
    scedhuler.start()
    return the_app


@bp.route("/api/v1/prices",
          methods=['GET'])
def get_prices():
    return jsonify(ps.get_prices(items_count=10)).to_json()


@bp.route("/api/v1/balance/<int:user_id>",
          methods=['GET'])
def get_balance(user_id):
    return jsonify(bs.compute_balance(user_id)).to_json()


@bp.app_errorhandler(pymongo.errors.ServerSelectionTimeoutError)
def handle_error(error):
    message = [str(x) for x in error.args]
    status_code = 500
    success = False
    response = {
        'success': success,
        'error': {
            'type': error.__class__.__name__,
            'message': message
        }
    }

    return jsonify(response), status_code


# Shut down the scedhuler when exiting the app
atexit.register(lambda: scedhuler.stop())

if __name__ == '__main__':
    create_app().run()
