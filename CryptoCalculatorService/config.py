import os
from keyring import get_password
from werkzeug.utils import import_string

DB = "calculator_service"
PORT = 27017
MONGO_IP = "134.122.79.43"
KAFKA_BROKERS = "192.168.1.57:9092"
TRANSACTIONS_TOPIC_NAME = "transactions_2"


class BaseConfig(object):
    DEBUG = False
    TESTING = False
    SERVERNAME = "localhost"
    PORT = PORT
    DATABASE = DB
    USERNAME = ""
    PASSWORD = ""
    LOGS_PATH = '../CryptoCalculatorService/logs/CryptoModel.log'
    KAFKA_BROKERS = KAFKA_BROKERS
    TRANSACTIONS_TOPIC_NAME = TRANSACTIONS_TOPIC_NAME


class DevelopmentConfig(BaseConfig):
    DEBUG = True
    TESTING = True
    SERVERNAME = "localhost"
    PORT = PORT
    DATABASE = DB
    USERNAME = "test"
    PASSWORD = "test"
    LOGS_PATH = '../CryptoCalculatorService/logs/CryptoModel.log'
    KAFKA_BROKERS = KAFKA_BROKERS
    TRANSACTIONS_TOPIC_NAME = TRANSACTIONS_TOPIC_NAME


class ProductionConfig(BaseConfig):
    DEBUG = False
    TESTING = False
    SERVERNAME = MONGO_IP
    PORT = PORT
    DATABASE = DB
    USERNAME = ""
    PASSWORD = ""
    LOGS_PATH = '../CryptoCalculatorService/logs/CryptoUsersService.log'
    KAFKA_BROKERS = KAFKA_BROKERS
    TRANSACTIONS_TOPIC_NAME = TRANSACTIONS_TOPIC_NAME


config = {
    "development": "CryptoCalculatorService.config.DevelopmentConfig",
    "production": "CryptoCalculatorService.config.ProductionConfig",
    "default": "CryptoCalculatorService.config.DevelopmentConfig",
}


def configure_app():
    config_name = os.getenv('FLASK_ENV', 'default')
    cfg = import_string(config_name)()
    cfg.USERNAME = get_password('CryptoCalculatorService', 'USERNAME')
    cfg.PASSWORD = get_password('CryptoCalculatorService', cfg.USERNAME)
    return cfg
