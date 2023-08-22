import json
from uuid import uuid4
from singer import logger

from tap_rockgympro.streams import Bookings, Checkins, Customers, Facilities, Invoices
from tap_rockgympro.utils import discover
from tap_rockgympro.consts import ORDERED_STREAM_NAMES


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
        streams = []
        for stream_name in ORDERED_STREAM_NAMES:
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
        elif stream_name == 'checkins':
            if not self.customer_stream:
                raise Exception('Catalog needs to include customer stream if it includes checkins stream')
            return Checkins(stream, self.config, self.state, self.customer_stream)
        if stream_name == 'invoices':
            if not self.customer_stream:
                raise Exception('Catalog needs to include customer stream if it includes invoices stream')
            return Invoices(stream, self.config, self.state, self.customer_stream)
        if stream_name == 'customers':
            self.customer_stream = Customers(stream, self.config, self.state)
            return self.customer_stream

    def sync(self):
        # Loop through streams
        for stream_name, stream in self.catalog['streams']:
            processor = self.get_stream(stream_name, stream)

            if not processor:
                # We couldn't find a streamer for this stream
                continue

            log_id = str(uuid4())
            logger.log_info(json.dumps({
                "id": log_id,
                "event": "START",
                "stream": stream["stream"],
            }))

            processor.process()

            logger.log_info(json.dumps({
                "id": log_id,
                "event": "END",
                "stream": stream["stream"],
            }))
