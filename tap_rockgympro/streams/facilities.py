import requests
import singer

from tap_rockgympro.utils import rate_handler, format_date
from tap_rockgympro.mixins import Stream

class Facilities(Stream):
    def process(self):
        response = rate_handler(requests.get, ('https://api.rockgympro.com/v1/facilities',),
                                {"auth": (self.config['api_user'], self.config['api_key'])})

        time_extracted = format_date(response['rgpApiTime'])

        needs_state_update = False
        has_sent_schema = False

        if 'facilities' not in self.state:
            self.state['facilities'] = {
                "codes": []
            }

        for facility_code, record in response['facilities'].items():
            needs_state_update = True
            if facility_code not in self.state['facilities']['codes']:
                self.state['facilities']['codes'].append(facility_code)

            if not has_sent_schema:
                singer.write_schema(self.stream['stream'], self.stream['schema'], self.stream['key_properties'])
                has_sent_schema = True

            singer.write_record(self.stream['stream'], record, time_extracted=time_extracted)

        if needs_state_update:
            singer.write_state(self.state)
