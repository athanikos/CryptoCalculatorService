from collections import namedtuple


def from_dict( _dict, class_name):
    if _dict is None:
        raise TypeError
    if class_name == "":
        raise ValueError

    if "_id" in _dict.keys():
        _dict["id"] = _dict.pop("_id")

    return namedtuple(class_name, _dict.keys(),rename=True)(*_dict.values())

