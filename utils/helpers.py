
import json
import os
import pathlib
from definitions import CONFIG_DIR

LOGS_PATH = os.path.join(CONFIG_DIR, "metadata.json")
if not os.path.exists(LOGS_PATH):
    open(LOGS_PATH, 'w+').close()

def write_metadata(obj):
    logs = read_metadata()
    logs.update(obj)
    with open(LOGS_PATH, 'w+') as file:
        file.write(json.dumps(logs, indent=4, sort_keys=True))

    return True


def read_metadata():
    if not os.path.exists(LOGS_PATH):
        open(LOGS_PATH, 'w+').close()

    metadata = open(LOGS_PATH, 'r').read()
    return json.loads(metadata) if metadata else {}


def write_json(obj, path):
    with open(path, 'w+') as file:
        file.write(json.dumps(obj, indent=4, sort_keys=True))

    return True


def read_json(path):
    data = open(path, 'w+').read()
    return json.loads(data) if data else {}


def merge_dict_structures(a, b, _path=None):
    """
    Merges two JSON structures

    Args:
        a: first JSON structure
        b: second JSON structure
        _path: Do not use. Private variable to keep track of location in JSON

    Returns:
        Dictionary structure comprised of all key/values from both A and B
    """
    _path = _path or []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dict_structures(a[key], b[key], _path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(_path + [str(key)]))
        else:
            a[key] = b[key]
    return a