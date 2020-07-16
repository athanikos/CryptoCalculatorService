import pymongo
from flask import Flask, jsonify, request
from flask.blueprints import Blueprint
from CryptoCalculatorService.config import configure_app
from CryptoCalculatorService import CalculatorService

bp = Blueprint(__name__.split('.')[0], __name__.split('.')[0])


def create_app():
    the_app = Flask(__name__.split('.')[0], instance_relative_config=True)
    configure_app()
    the_app.register_blueprint(bp)
    return the_app


@bp.route("/api/v1/prices",
          methods=['GET'])
def get_prices():
    ps = CalculatorService(configure_app())
    return ps.get_prices(items_count=10)


@bp.route("/api/v1/transactions/<int:user_id>",
          methods=['GET'])
def get_transactions(user_id):
    ts = CalculatorService(configure_app())
    return ts.get_transactions(request.json['user_id'])


@bp.route("/api/v1/transaction",
          methods=['POST'])
def insert_transaction():
    ts = CalculatorService(configure_app())
    un = ts.insert_transaction(request.json['user_id'], request.json['volume'], request.json['symbol'],
                                request.json['value'], request.json['price'], request.json['date'],
                                request.json['source'])
    return jsonify(un.to_json())


@bp.route("/api/v1/transaction",
          methods=['PUT'])
def update_transaction():
    ts = CalculatorService(configure_app())
    un = ts.update_transaction( request.json['id'],
                                request.json['user_id'], request.json['volume'], request.json['symbol'],
                                request.json['value'], request.json['price'], request.json['date'],
                                request.json['source'])
    return jsonify(un.to_json())


@bp.route("/api/v1/balance/<int:user_id>",
          methods=['GET'])
def get_balance(user_id):
    bs = CalculatorService(configure_app())
    return bs.compute(user_id)


@bp.route("/api/v1/user-channels/<int:user_id>/<string:channel_type>",
          methods=['GET'])
def get_user_channels(user_id, channel_type):
    uns = CalculatorService(configure_app())
    return uns.get_user_channels(user_id, channel_type)


@bp.route("/api/v1/user-notifications",
          methods=['GET'])
def get_user_notifications():
    uns = CalculatorService(configure_app())
    return uns.get_user_notifications(10)


@bp.route("/api/v1/user-notifications",
          methods=['POST'])
def insert_notification():
    uns = CalculatorService(configure_app())
    un = uns.insert_user_notification(request.json['user_id'],
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
    uns = CalculatorService(configure_app())
    un = uns.insert_user_channel(request.json['user_id'], request.json['channel_type'], request.json['chat_id'])

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


if __name__ == '__main__':
    create_app().run()
