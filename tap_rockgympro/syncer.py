import singer

from tap_rockgympro.streams import Bookings, Customers, Facilities
from tap_rockgympro.utils import discover


class Syncer:
    config = None
    catalog = None
    state = None
    customer_stream = None

    def __init__(self, args):
        self.config = args.config

        if args.catalog:
            self.catalog = args.catalog.to_dict()
        else:
            self.catalog = discover()

        self.sort_catalog()

        self.state = args.state

    def sort_catalog(self):
        stream_order = ['facilities', 'customers', 'bookings']
        streams = []
        for stream_name in stream_order:
            stream = next((s for s in self.catalog['streams'] if s[0] == stream_name), None)
            if stream:
                streams.append(stream)

        self.catalog['streams'] = streams

    def get_stream(self, stream_name, stream):
        if stream_name == 'facilities':
            return Facilities(stream, self.config, self.state)
        elif stream_name == 'bookings':
            if not self.customer_stream:
                raise Exception('Catalog needs to include customer stream if it includes booking stream')
            return Bookings(stream, self.config, self.state, self.customer_stream)

    def sync(self):
        # Loop through streams
        for stream_name, stream in self.catalog['streams']:
            if stream_name == 'customers':
                # We can't directly stream customers, they're a side effect from other streams
                self.customer_stream = Customers(stream, self.config, self.state)
                continue

            processor = self.get_stream(stream_name, stream)

            if not processor:
                # We couldn't find a streamer for this stream
                continue

            processor.process()


"""
Notes:

- Bookings
-- Needs a facility code to even get bookings
-- Needs a customer already loaded but we can't load customers without a guid





"""
