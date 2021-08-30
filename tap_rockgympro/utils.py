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
    # If we get a 429 rate limit error exception wait until the rate limit ends
    while True:
        response = func(*args, **kwargs)
        response_json = response.json()

        if response_json.get('status') == 429:
            sleep(int(response.headers.get('retry-after') or 1))
        else:
            return response_json

def format_date(item, timezone=None):
    if item == '0000-00-00 00:00:00' or not item:
        return None

    return datetime.strptime(item, "%Y-%m-%d %H:%M:%S").astimezone(timezone or UTC)


def format_date_iso(item, timezone=None):
    date = format_date(item, timezone)
    return None if not date else date.isoformat()
