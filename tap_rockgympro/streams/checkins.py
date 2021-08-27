from datetime import timedelta

from tap_rockgympro.utils import format_date, format_date_iso
from tap_rockgympro.mixins import FacilityStream

class Checkins(FacilityStream):
    def format_record(self, record):
        record['postDate'] = format_date(record['postDate']).isoformat()
        record['checkoutPostDate'] = format_date_iso(record['checkoutPostDate'])
        return record

    def get_updated_time(self, record):
        post_date = format_date(record['postDate'])
        checkout_post_date = format_date(record['checkoutPostDate'])
        return post_date if not checkout_post_date or post_date > checkout_post_date else checkout_post_date

    def get_created_time(self, record):
        return format_date(record['postDate'])

    def get_url(self, code, page, bookmark_time):
        url = super().get_url(code, page, bookmark_time)

        if bookmark_time:
            url += '&startDateTime=' + (bookmark_time - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

        return url
