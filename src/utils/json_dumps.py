import json

def dump_to_json(data):
    """
    Util function that takes in a dictionary and converts it to json format for storage
    Args:
        data (dictionary): a dictionary representing data from a table
    Returns:
        a json file
    """
    return json.dumps(data, default=str) 

print(type(dump_to_json({'hello':'oops'})))