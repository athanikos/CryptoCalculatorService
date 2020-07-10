import mongoengine
from mongoengine import connect, Q
from pymongo.errors import ServerSelectionTimeoutError


def do_connect(configuration):
    url = 'mongodb://' + configuration.USERNAME + ':' + configuration.PASSWORD + '@' \
          + configuration.SERVERNAME + ':' + str(configuration.PORT) + '/?authSource=admin'
    conn = connect(db=configuration.DATABASE, username=configuration.USERNAME, host=url)


def get_url(configuration):
    return 'mongodb://' + configuration.USERNAME + ':' + configuration.PASSWORD + '@' \
          + configuration.SERVERNAME + ':' + str(configuration.PORT) + '/?authSource=admin'


def server_time_out_wrapper(repository, method_to_call, *args):
    try:
        return method_to_call(*args)
    except ServerSelectionTimeoutError as inst:
        repository.log_error(exception=inst, web_method_name=str(method_to_call), cfg=repository.configuration)
        raise
    except mongoengine.connection.ConnectionFailure as cf:
        repository.log_error(exception=cf,  web_method_name=str(method_to_call),  cfg=repository.configuration)
        raise mongoengine.connection.ConnectionFailure(cf)


