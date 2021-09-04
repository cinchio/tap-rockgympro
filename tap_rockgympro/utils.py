import json
import pkg_resources
from time import sleep
from datetime import datetime
from pytz import UTC
from singer import logger

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
            seconds = max(int(response.headers.get('retry-after') or 1), 1)
            logger.log_info(f'Hit rate limit. Waiting {seconds} seconds')
            sleep(seconds)
        else:
            return response_json

def format_date(item, timezone=None):
    if item == '0000-00-00 00:00:00' or not item:
        return None

    return datetime.strptime(item, "%Y-%m-%d %H:%M:%S").astimezone(timezone or UTC)


def format_date_iso(item, timezone=None):
    date = format_date(item, timezone)
    return None if not date else date.isoformat()

def nested_set(record, target, value):
    """
        Using dot-notation set the value of a dictionary

        Example:

        obj = {
            "foo": {
                "bar": 4
            }
        }

        nested_set(obj, 'foo.bar', 7)
        Returns:
        {
            "foo": {
                "bar": 7
            }
        }

        nested_set(obj, 'foo.zaz', 12)
        Returns:
        {
            "foo": {
                "bar": 7,
                "zaz": 12
            }
        }
    """

    if '.' in target:
        next_level, extra_levels = target.split('.', 1)

        if next_level not in record:
            record[next_level] = {}

        record[next_level] = nested_set(record[next_level], extra_levels, value)
    else:
        record[target] = value

    return record


def nested_get(record: dict, target: str):
    """
        Using dot-notation get the value of a dictionary

        Example:

        obj = {
            "foo": {
                "bar": 4
            }
        }

        nested_get(obj, 'foo.bar')  # returns 4
        nested_get(obj, 'foo.zaz')  # returns None
    """

    if '.' in target:
        next_level, extra_levels = target.split('.', 1)
        return nested_get(record.get(next_level, {}), extra_levels)

    return record.get(target)
