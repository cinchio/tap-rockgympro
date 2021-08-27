from datetime import datetime

import requests
import singer
from pytz import UTC

from tap_rockgympro.utils import rate_handler


class Bookings:
    """
    Processing bookings is challenging because there's no way to filter by updated bookings. As
    far as I can tell bookings are created with a bookingDate and can be later cancelled which
    updates the cancelledOn.

    To be able to only process new/updated bookings we have to pull all booking information from the
    RockGymPro API and filter the bookingDate and the cancelledOn by the last bookmark time

    Also there isn't any way to get all customers so we are fetching customers from the API in batches
    from the batches of bookings we pull.
    """

    def __init__(self, stream, config, state, customer_stream):
        self.stream = stream
        self.config = config
        self.state = state
        self.customer_stream = customer_stream

    def process(self):
        facility_codes = self.state.get('facilities', []).get('codes', [])
        has_sent_schema = False

        for code in facility_codes:
            page = 1
            total_page = None

            new_bookmark_time = bookmark_time = self.config.get('bookings', {}).get('bookmark_time')

            while not total_page or page < total_page:
                # Loop through all of the pages.
                bookings = []
                response = rate_handler(requests.get,
                                        (f'https://api.rockgympro.com/v1/bookings/facility/{code}?page={page}',),
                                        {"auth": (self.config['api_user'], self.config['api_key'])})

                if not total_page:
                    total_page = response['rgpApiPaging']['pageTotal'] or 1

                for record in response['bookings']:
                    # format record
                    record['bookingDate'] = datetime.strptime(record['bookingDate'],
                                                              "%Y-%m-%d %H:%M:%S").astimezone(UTC).isoformat()
                    record['originalBookedTime'] = datetime.strptime(record['originalBookedTime'],
                                                                     "%Y-%m-%d %H:%M:%S").astimezone(UTC).isoformat()

                    if record['cancelledOn'] == '0000-00-00 00:00:00':
                        record['cancelledOn'] = None
                    else:
                        record['cancelledOn'] = datetime.strptime(record['cancelledOn'],
                                                                  "%Y-%m-%d %H:%M:%S").astimezone(UTC).isoformat()

                    latest_time = record['bookingDate'] if not record[
                        'cancelledOn'] or record['bookingDate'] > record['cancelledOn'] else record['cancelledOn']

                    if not new_bookmark_time or record['bookingDate'] > new_bookmark_time:
                        new_bookmark_time = record['bookingDate']

                    if not bookmark_time or latest_time > bookmark_time:
                        # Only include bookings that are after the state's bookmark time
                        # Instead of sending bookings straight to the stream batch them so we can check the customers first
                        bookings.append(record)

                if bookings:
                    # Fetch and output customers for these bookings
                    customers = {record['customerGuid'] for record in bookings}
                    if customers:
                        self.customer_stream.process(customers)

                    # Then output bookings
                    if not has_sent_schema:
                        singer.write_schema(self.stream['stream'], self.stream['schema'], self.stream['key_properties'])
                        has_sent_schema = True

                    singer.write_records('bookings', bookings)

                if not bookmark_time or new_bookmark_time > bookmark_time:
                    # If we have a new bookmark time set it to the state
                    self.state['bookings'] = {"bookmark_time": new_bookmark_time}
                    singer.write_state(self.state)

                page += 1
