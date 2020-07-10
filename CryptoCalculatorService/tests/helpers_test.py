from CryptoModel.model.helpers import from_dict


def test_from_dict():
    my_dict = {
        "user_id": 1,
        "user_name": "1",
        "user_email": "sdsds",
        "condition_value": "1",
        "field_name": "sdds",
        "operator": ">",
        "notify_times": "1",
        "notify_every_in_seconds": "1",
        "symbol": "btc",
        "channel_type": "telegram"
    }

    un = from_dict(_dict = my_dict, class_name = 'user_notification')
    assert (un.user_id == 1)
    assert (un.user_name == "1")


def test_replace_underscore_id_from_dict():
    my_dict = {
        "_id": 1,
        }

    un = from_dict(_dict = my_dict, class_name = 'user_notification')
    assert (un.id == 1)
