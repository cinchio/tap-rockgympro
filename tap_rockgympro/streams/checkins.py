from datetime import timedelta
from dateutil import parser
from singer import logger

from tap_rockgympro.utils import format_date, format_date_iso, nested_set, nested_get
from tap_rockgympro.mixins import FacilityStream

class Checkins(FacilityStream):
    # We have two different endpoints. One to grab remote checkins and one to grab regular checkins.
    # We're probably not doing this in the best way but we set this class variable to True when
    # we're running in the remote context.  We write the records all as "checkins" but we use a
    # different value for state.
    context_remote = False

    def format_record(self, record, facility_code):

        # OLD code for fetching remote checkins
        #if record['customerId'] == -100:
        #    # Fetch remote checkin.
        #    checkin_url = f"https://api.rockgympro.com/v1/checkins/facility/{record['remoteDatabaseTag']}/{record['remoteCheckinId']}"
        #    response = rate_handler(requests.get, (checkin_url,), {"auth": (self.config['api_user'], self.config['api_key'])})
        #    if 'checkin' in response:
        #        record = response['checkin']

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

    def get_bookmark_time(self, facility_code):
        # We save bookmarks based on facility code.
        stream_name = self.stream['stream']

        if self.context_remote:
            # If we're running in the remote context then change the stream name to use the remote
            stream_name = f'remote_{stream_name}'

        time = nested_get(self.state, f"{stream_name}.bookmark_time.{facility_code}")
        return parser.isoparse(time) if time else None

    def set_bookmark_time(self, facility_code, bookmark_time):
        stream_name = self.stream['stream']

        if self.context_remote:
            # If we're running in the remote context then change the stream name to use the remote
            stream_name = f'remote_{stream_name}'

        nested_set(self.state, f"{stream_name}.bookmark_time.{facility_code}", bookmark_time.isoformat())

    def get_url(self, code, page, bookmark_time):
        url = super().get_url(code, page, bookmark_time)

        if self.context_remote:
            # If we're running in the remote context then change the stream name to use the remote
            url += '&remoteOnly=1'

        if bookmark_time:
            url += '&startDateTime=' + (bookmark_time - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

        return url

    def process(self):
        # Sync remote checkins
        self.context_remote = True
        logger.log_info("Syncing remote checkins")
        super().process()

        # Sync normal checkins
        self.context_remote = False
        logger.log_info("Syncing regular checkins")
        super().process()
