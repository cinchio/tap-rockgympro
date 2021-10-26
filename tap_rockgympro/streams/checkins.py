from datetime import timedelta
from singer import logger
import requests

from tap_rockgympro.utils import format_date, format_date_iso, rate_handler
from tap_rockgympro.mixins import FacilityStream

class Checkins(FacilityStream):
    def format_record(self, record, facility_code):
        if record['customerId'] == -100:
            # Fetch remote checkin.
            checkin_url = f"https://api.rockgympro.com/v1/checkins/facility/{record['remoteDatabaseTag']}/{record['remoteCheckinId']}"
            response = rate_handler(requests.get, (checkin_url,), {"auth": (self.config['api_user'], self.config['api_key'])})

            if 'checkin' in response:
                record = response['checkin']

        record['postDate'] = format_date_iso(record['postDate'], self.get_timezone(facility_code))
        record['checkoutPostDate'] = format_date_iso(record['checkoutPostDate'], self.get_timezone(facility_code))
        record['facilityCode'] = facility_code
        return record

    def get_updated_time(self, record, facility_code):
        post_date = format_date(record['postDate'], self.get_timezone(facility_code))
        checkout_post_date = format_date(record['checkoutPostDate'], self.get_timezone(facility_code))
        return post_date if not checkout_post_date or post_date > checkout_post_date else checkout_post_date

    def get_created_time(self, record, facility_code):
        return format_date(record['postDate'], self.get_timezone(facility_code))

    def get_url(self, code, page, bookmark_time):
        url = super().get_url(code, page, bookmark_time)

        if bookmark_time:
            url += '&startDateTime=' + (bookmark_time - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

        return url
