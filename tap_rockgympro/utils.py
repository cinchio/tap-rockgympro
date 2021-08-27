import json
import pkg_resources
from time import sleep
from datetime import datetime
from pytz import UTC

from tap_rockgympro.consts import ORDERED_STREAM_NAMES


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for schema_name in ORDERED_STREAM_NAMES:
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

def format_date(item):
    if item == '0000-00-00 00:00:00' or not item:
        return None

    return datetime.strptime(item, "%Y-%m-%d %H:%M:%S").astimezone(UTC)


def format_date_iso(item):
    date = format_date(item)
    return None if not date else date.isoformat()
