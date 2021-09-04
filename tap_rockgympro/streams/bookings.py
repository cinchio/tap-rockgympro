import singer
import pytz
from datetime import datetime
from singer import logger
from tap_rockgympro.utils import format_date, format_date_iso, nested_get, nested_set
from tap_rockgympro.mixins import FacilityStream
import copy

def is_active(record, timezone):
    return record['cancelledOn'] != '0000-00-00 00:00:00' and format_date(record['originalBookedTime'], timezone) > datetime.now().astimezone(pytz.UTC)

class Bookings(FacilityStream):
    """
    Processing bookings is challenging because there's no way to filter by updated bookings. As
    far as I can tell bookings are created with a bookingDate and can be later cancelled which
    updates the cancelledOn.

    To be able to only process new/updated bookings we have to pull all booking information from the
    RockGymPro API and filter the bookingDate and the cancelledOn by the last bookmark time

    Also there isn't any way to get all customers so we are fetching customers from the API in batches
    from the batches of bookings we pull.
    """

    def format_record(self, record, facility_code):
        # If state's oldest active record doesn't exist or is inactive and this record is active or its bookingDate is closer to now set it on the state
        start_record = nested_get(self.state, f"{self.stream['stream']}.start_date.{facility_code}")
        if (
            not start_record or
            (not is_active(record, self.get_timezone(facility_code)) and record['bookingDate'] > start_record['bookingDate']) or
            (is_active(record, self.get_timezone(facility_code)) and is_active(start_record, self.get_timezone(facility_code)) and record['bookingDate'] < start_record['bookingDate'])
        ):
            start_record = copy.deepcopy(record)
            nested_set(self.state, f"{self.stream['stream']}.start_date.{facility_code}", start_record)
            singer.write_state(self.state)

        record['bookingDate'] = format_date_iso(record['bookingDate'], self.get_timezone(facility_code))
        record['originalBookedTime'] = format_date_iso(record['originalBookedTime'], self.get_timezone(facility_code))
        record['cancelledOn'] = format_date_iso(record['cancelledOn'], self.get_timezone(facility_code))

        return record

    def get_updated_time(self, record, facility_code):
        booking_date = format_date(record['bookingDate'], self.get_timezone(facility_code))
        cancelled_on = format_date(record['cancelledOn'], self.get_timezone(facility_code))
        return booking_date if not cancelled_on or booking_date > cancelled_on else cancelled_on

    def get_created_time(self, record, facility_code):
        return format_date(record['bookingDate'], self.get_timezone(facility_code))


    def get_url(self, code, page, bookmark_time):
        url = f"https://api.rockgympro.com/v1/{self.stream['stream']}/facility/{code}?page={page}"

        # We're using the original state so our query doesn't return different results
        start_date_time = nested_get(self.original_state, f"{self.stream['stream']}.start_date.{code}.bookingDate")
        if start_date_time:
            url += f'&startDateTime={start_date_time}'

        logger.log_info(f'Querying page: {url}')

        return url
