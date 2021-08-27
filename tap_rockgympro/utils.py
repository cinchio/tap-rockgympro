import json
import pkg_resources
from time import sleep


# Load schemas from schemas folder
def load_schemas():
    schema_names = ['bookings', 'customers', 'facilities']
    schemas = {}

    for schema_name in schema_names:
        schemas[schema_name] = json.load(pkg_resources.resource_stream('tap_rockgympro', f'schemas/{schema_name}.json'))

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
