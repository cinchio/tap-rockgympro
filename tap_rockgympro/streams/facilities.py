from datetime import datetime

import requests
import singer
from pytz import UTC

from ..utils import rate_handler


class Facilities:

    def __init__(self, stream, config, state):
        self.stream = stream
        self.config = config
        self.state = state

    def process(self):
        response = rate_handler(requests.get, ('https://api.rockgympro.com/v1/facilities',),
                                {"auth": (self.config['api_user'], self.config['api_key'])})

        time_extracted = datetime.strptime(response['rgpApiTime'], "%Y-%m-%d %H:%M:%S").astimezone(UTC)

        needs_state_update = False
        has_sent_schema = False

        if 'facilities' not in self.state:
            self.state['facilities'] = {
                "codes": []
            }

        for facility_code, record in response['facilities'].items():
            if facility_code not in self.state['facilities']['codes']:
                needs_state_update = True
                self.state['facilities']['codes'].append(facility_code)

                if not has_sent_schema:
                    singer.write_schema(self.stream['stream'], self.stream['schema'], self.stream['key_properties'])
                    has_sent_schema = True

                singer.write_record(self.stream['stream'], record, time_extracted=time_extracted)

        if needs_state_update:
            singer.write_state(self.state)
