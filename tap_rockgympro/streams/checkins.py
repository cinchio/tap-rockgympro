from tap_rockgympro.utils import format_date
from tap_rockgympro.mixins import FacilityStream

class Checkins(FacilityStream):
    def format_record(self, record):
        record['postDate'] = format_date(record['postDate'])
        record['checkoutPostDate'] = format_date(record['checkoutPostDate'])
        return record

    def get_updated_time(self, record):
        return record['postDate'] if not record['checkoutPostDate'] or record['postDate'] > record['checkoutPostDate'] else record['checkoutPostDate']

    def get_created_time(self, record):
        return record['postDate']


    def get_url(self, code, page, bookmark_time):
        url = super().get_url(code, page, bookmark_time)

        if bookmark_time:
            url += '&startDateTime=' + bookmark_time.strftime('%Y-%m-%d %H:%M:%S')

        return url
