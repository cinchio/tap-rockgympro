import json
import os
from time import sleep


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        schema_name = filename.replace('.json', '')
        with open(path) as file:
            schemas[schema_name] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema in raw_schemas.items():
        streams.append(schema)

    return {'streams': streams}


def rate_handler(func, args, kwargs):
    # If we get a 429 rate limit error exception wait exponentially longer until the rate limit ends
    timer = 1
    while True:
        response = func(*args, **kwargs)
        response = response.json()

        if response.get('status') == 429:
            sleep(timer)
            # Double the next wait time
            timer *= 2
        else:
            return response
