import json


def json_pretty(obj):
    """A simple wrapper for the json pretty string given in the json docs"""
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
