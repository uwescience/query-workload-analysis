from json import dumps, JSONEncoder


class SetEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, list):
            return sorted(obj)
        return JSONEncoder.default(self, obj)


def json_pretty(obj):
    """A simple wrapper for the json pretty string given in the json docs"""
    return dumps(obj, sort_keys=True, indent=4, separators=(',', ': '),
                 cls=SetEncoder)
