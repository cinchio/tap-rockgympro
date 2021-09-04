from dateutil import parser
import requests
import singer
from singer import logger
import pytz

from tap_rockgympro.utils import rate_handler

class Stream:
    stream = None
    config = None
    state = None
    timezones = None

    def __init__(self, stream, config, state):
        self.stream = stream
        self.config = config
        self.state = state

        self.timezones = {}
        for timezone in config.get('timezones', []):
            self.timezones[timezone['code']] = timezone['timezone']

    def get_timezone(self, facility_code):
        tz = self.timezones.get(facility_code)
        if tz:
            return pytz.timezone(tz)

        return pytz.UTC

class FacilityStream(Stream):
    customer_stream = None

    def __init__(self, stream, config, state, customer_stream):
        self.customer_stream = customer_stream
        super().__init__(stream, config, state)

    def get_bookmark_time(self, facility_code):
        # We save bokomarks based on facility code.
        time = self.state.get(self.stream['stream'], {}).get('bookmark_time', {}).get(facility_code)

        return parser.isoparse(time) if time else None

    def set_bookmark_time(self, facility_code, bookmark_time):
        if self.stream['stream'] not in self.state:
            self.state[self.stream['stream']] = {}

        if 'bookmark_time' not in self.state[self.stream['stream']]:
            self.state[self.stream['stream']]['bookmark_time'] = {}

        self.state[self.stream['stream']]['bookmark_time'][facility_code] = bookmark_time.isoformat()

    def format_record(self, record, facility_code):
        return record

    def get_updated_time(self, record, facility_code):
        raise NotImplementedError

    def get_created_time(self, record, facility_code):
        raise NotImplementedError

    def get_url(self, code, page, bookmark_time):
        return f"https://api.rockgympro.com/v1/{self.stream['stream']}/facility/{code}?page={page}"

    def process(self):
        facility_codes = self.state.get('facilities', {}).get('codes', [])
        has_sent_schema = False

        for code in facility_codes:
            page = 1
            total_page = None

            # bookmark_time is the current bookmark time that we for querying the API
            # new_bookmark_time is our cached bookmark time that we update and send to the state
            new_bookmark_time = bookmark_time = self.get_bookmark_time(code)

            logger.log_info(f"Syncing stream: {self.stream['stream']} facility code: {code}")
            logger.log_info(f"Using bookmark time of {bookmark_time}")

            while not total_page or page < total_page:
                # Loop through all of the pages.
                records = []
                response = rate_handler(requests.get,
                                        (self.get_url(code, page, bookmark_time),),
                                        {"auth": (self.config['api_user'], self.config['api_key'])})

                if not total_page:
                    total_page = response['rgpApiPaging']['pageTotal'] or 1

                for record in response[self.stream['stream']]:
                    # format record
                    updated_time = self.get_updated_time(record, code)
                    created_time = self.get_created_time(record, code)
                    record = self.format_record(record, code)

                    if not record:
                        continue

                    if not new_bookmark_time or created_time > new_bookmark_time:
                        # We've hit a new record.
                        new_bookmark_time = created_time

                    if not bookmark_time or updated_time > bookmark_time:
                        # Only include records that are after the state's bookmark time
                        # Instead of sending records straight to the stream batch them so we can check the customers first
                        records.append(record)

                if records:
                    # Fetch and output customers for these records
                    customers = {record['customerGuid'] for record in records}
                    if customers:
                        self.customer_stream.process(customers, code)

                    if not has_sent_schema:
                        # Output schema if we haven't yet
                        singer.write_schema(self.stream['stream'], self.stream['schema'], self.stream['key_properties'])
                        has_sent_schema = True

                    # Output records
                    singer.write_records(self.stream['stream'], records)

                if not bookmark_time or new_bookmark_time > bookmark_time:
                    # If we have a new bookmark time set it to the state
                    self.set_bookmark_time(code, new_bookmark_time)
                    singer.write_state(self.state)

                page += 1
