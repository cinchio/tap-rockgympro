from datetime import datetime

import requests
import singer
from pytz import UTC

from tap_rockgympro.utils import rate_handler

# Customer endpoint only allows 25 at a time.
BATCH_SIZE = 25


class Customers:
    # Keep track of which IDs we've already sent so we don't send them needlessly
    cached_ids = None
    has_sent_schema = False

    def __init__(self, stream, config, state):
        self.stream = stream
        self.config = config
        self.state = state
        self.cached_ids = set()

    def process(self, ids):
        # RockGymPro's API requires a customer ID to get customers.  They have no endpoint for looping through all customers
        ids_to_sync = list(ids - self.cached_ids)

        for start in range(0, len(ids_to_sync), BATCH_SIZE):

            response = rate_handler(requests.get, (
                f"https://api.rockgympro.com/v1/customers?customerGuid={','.join(ids_to_sync[start:start+BATCH_SIZE])}",
            ), {"auth": (self.config['api_user'], self.config['api_key'])})

            for record in response['customer']:
                if not self.has_sent_schema:
                    singer.write_schema(self.stream['stream'], self.stream['schema'], self.stream['key_properties'])
                    self.has_sent_schema = True

                if record['lastRecordEdit'] and record['lastRecordEdit'] == '0000-00-00 00:00:00':
                    record['lastRecordEdit'] = None
                else:
                    record['lastRecordEdit'] = datetime.strptime(record['lastRecordEdit'],
                                                                 "%Y-%m-%d %H:%M:%S").astimezone(UTC).isoformat()

                # Format records
                singer.write_record(self.stream['stream'], record)

        self.cached_ids = self.cached_ids & ids
