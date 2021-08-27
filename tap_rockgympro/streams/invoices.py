from datetime import timedelta

from tap_rockgympro.utils import format_date
from tap_rockgympro.mixins import FacilityStream


class Invoices(FacilityStream):
    def format_record(self, record):
        record['invoicePostDate'] = format_date(record['invoicePostDate']).isoformat()
        record['payment']['postdate'] = format_date(record['payment']['postdate']).isoformat()
        return record

    def get_updated_time(self, record):
        return format_date(record['invoicePostDate'])

    def get_created_time(self, record):
        return format_date(record['invoicePostDate'])

    def get_url(self, code, page, bookmark_time):
        url = super().get_url(code, page, bookmark_time)

        if bookmark_time:
            url += '&startDateTime=' + (bookmark_time - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

        return url
