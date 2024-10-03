import requests
import singer
from datetime import datetime, timedelta

from singer import logger

from tap_rockgympro.utils import (
    nested_set,
    rate_handler,
    format_date,
    format_date_iso,
    nested_get,
)
from tap_rockgympro.mixins import Stream

# Customer endpoint only allows 25 at a time.
BATCH_SIZE = 25


class Customers(Stream):
    # Keep track of which IDs we've already sent so we don't send them needlessly
    cached_ids = None
    has_sent_schema = False

    def __init__(self, stream, config, state):
        super().__init__(stream, config, state)
        self.cached_ids = set()

    def process_state(self):
        """
        Due to the RockGymPro's many shortcomings in the API we are unable to get updates to customers unless they have a checkin
        or similar API event.  We are now going to store in the cache/state customers that have had an event in the last 30 days
        and query those customers.

        TODO [10/3/2024] - after speaking with Jared at standup we may need to rethink how we're getting information
        for customers as the 30 day cache / reprocessing flow may be too much for both their API / our db every hour.
        """

        thirty_days_ago = str(datetime.now() - timedelta(days=30))

        # Loop over each facility
        facility_codes = nested_get(self.state, "facilities.codes") or {}

        for code in facility_codes:
            # Clear out customer IDs that are over 30 days old
            stripped_guids = {
                guid: last_date
                for guid, last_date in (
                    nested_get(self.state, f"customers.guids.{code}") or {}
                ).items()
                if last_date > thirty_days_ago
            }

            # Update state
            nested_set(self.state, f"customers.guids.{code}", stripped_guids)
            singer.write_state(self.state)

            # Process all other customer IDs
            self.process(stripped_guids.keys(), code)

    def process(self, ids=None, facility_code=None):
        # RockGymPro's API requires a customer ID to get customers.  They have no endpoint for looping through all customers
        if ids is None:
            return self.process_state()

        ids_to_sync = list(ids - self.cached_ids)

        for start in range(0, len(ids_to_sync), BATCH_SIZE):
            response = rate_handler(
                requests.get,
                (
                    f"https://api.rockgympro.com/v1/customers?customerGuid={','.join(ids_to_sync[start:start+BATCH_SIZE])}",
                ),
                {"auth": (self.config["api_user"], self.config["api_key"])},
            )

            time_extracted = format_date(
                response["rgpApiTime"], self.get_timezone(facility_code)
            )

            if "customer" not in response:
                logger.log_error(
                    f"https://api.rockgympro.com/v1/customers?customerGuid={','.join(ids_to_sync[start:start+BATCH_SIZE])}"
                )
                logger.log_error(f"Missing customer key: {response}")

            for record in response["customer"]:
                if not self.has_sent_schema:
                    singer.write_schema(
                        self.stream["stream"],
                        self.stream["schema"],
                        self.stream["key_properties"],
                    )
                    self.has_sent_schema = True

                # Format records
                record["lastRecordEdit"] = format_date_iso(
                    record["lastRecordEdit"], self.get_timezone(facility_code)
                )

                if record["bday"] == "0000-00-00":
                    # Remove bad birthdays
                    record["bday"] = None

                singer.write_record(
                    self.stream["stream"], record, time_extracted=time_extracted
                )

        self.cached_ids = self.cached_ids & ids
