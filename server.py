import pymongo
from flask import Flask, jsonify, request
from flask.blueprints import Blueprint
from CryptoCalculatorService.config import configure_app
from CryptoCalculatorService.CalculatorService import CalculatorService
import atexit
from CryptoCalculatorService.scheduler.server import stop
bp = Blueprint(__name__.split('.')[0], __name__.split('.')[0])
cs = CalculatorService(configure_app())
from CryptoCalculatorService.scheduler.server import start

def create_app():
    the_app = Flask(__name__.split('.')[0], instance_relative_config=True)
    the_app.register_blueprint(bp)
    start(cs)
    return the_app


@bp.route("/api/v1/prices",
          methods=['GET'])
def get_prices():
    return cs.get_prices(items_count=10)


@bp.route("/api/v1/transactions/<int:user_id>",
          methods=['GET'])
def get_transactions(user_id):
    return cs.get_transactions(request.json['user_id'])


@bp.route("/api/v1/transaction",
          methods=['POST'])
def insert_transaction():
    un = cs.insert_transaction(request.json['user_id'], request.json['volume'], request.json['symbol'],
                               request.json['value'], request.json['price'], request.json['date'],
                               request.json['source'])
    return jsonify(un.to_json())


@bp.route("/api/v1/transaction",
          methods=['PUT'])
def update_transaction():
    un = cs.update_transaction(request.json['id'],
                               request.json['user_id'], request.json['volume'], request.json['symbol'],
                               request.json['value'], request.json['price'], request.json['date'],
                               request.json['source'])
    return jsonify(un.to_json())


@bp.route("/api/v1/balance/<int:user_id>",
          methods=['GET'])
def get_balance(user_id):
    return cs.compute(user_id)


@bp.route("/api/v1/user-channels/<int:user_id>/<string:channel_type>",
          methods=['GET'])
def get_user_channels(user_id, channel_type):
    return cs.get_user_channels(user_id, channel_type)


@bp.route("/api/v1/user-notifications",
          methods=['GET'])
def get_user_notifications():
    return cs.get_user_notifications(10)


@bp.route("/api/v1/user-notifications",
          methods=['POST'])
def insert_notification():
    un = cs.insert_user_notification(request.json['user_id'],
                                     request.json['user_name'],
                                     request.json['user_email'],
                                     request.json['condition_value'],
                                     request.json['condition_value'],
                                     request.json['operator'],
                                     request.json['notify_times'],
                                     request.json['notify_every_in_seconds'],
                                     request.json['symbol'],
                                     request.json['channel_type']
                                     )
    return jsonify(un.to_json())


@bp.route("/api/v1/user-channel",
          methods=['POST'])
def insert_user_channel():
    un = cs.insert_user_channel(request.json['user_id'], request.json['channel_type'], request.json['chat_id'])

    return jsonify(un.to_json())


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


# Shut down the scheduler when exiting the app
atexit.register(lambda: stop())

if __name__ == '__main__':
    create_app().run()
