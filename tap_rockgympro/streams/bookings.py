from tap_rockgympro.utils import format_date
from tap_rockgympro.mixins import FacilityStream

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

    def format_record(self, record):
        record['bookingDate'] = format_date(record['bookingDate'])
        record['originalBookedTime'] = format_date(record['originalBookedTime'])
        record['cancelledOn'] = format_date(record['cancelledOn'])
        return record

    def get_updated_time(self, record):
        return record['bookingDate'] if not record['cancelledOn'] or record['bookingDate'] > record['cancelledOn'] else record['cancelledOn']

    def get_created_time(self, record):
        return record['bookingDate']
