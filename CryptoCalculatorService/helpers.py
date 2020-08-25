import logging


def if_none_raise_with_id(_id, trans):
    if trans is None:
        raise ValueError(str(_id) + " does not exist ")


def if_none_raise(obj):
    if obj is None:
        raise ValueError(" does not exist ")


def if_empty_string_raise(value):
    if value is None:
        raise ValueError(" does not exist ")
    if value == '':
        raise ValueError(" does not exist ")


def log_error(exception, pk_id, web_method_name, cfg):
    logging.basicConfig(filename=cfg.LOGS_PATH, level=logging.ERROR)
    logging.error(str(pk_id) + ' ' + str(web_method_name) + ' ' + str(exception))


def log_error(exception, cfg):
    logging.basicConfig(filename=cfg.LOGS_PATH, level=logging.ERROR)
    logging.error(getattr(exception, 'message', repr(exception)))


def log_info(message, cfg):
    logging.basicConfig(filename=cfg.LOGS_PATH, level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    logging.info(message)
