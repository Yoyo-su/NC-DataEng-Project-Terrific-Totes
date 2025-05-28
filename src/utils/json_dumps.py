import json


def dump_to_json(data):
    return json.dumps(data, default=str)
