import json

from singer import utils, get_logger
from tap_rockgympro.syncer import Syncer
from tap_rockgympro.utils import discover

LOGGER = get_logger()
REQUIRED_CONFIG_KEYS = ['api_user', 'api_key']


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
        return

    syncer = Syncer(args)
    syncer.sync()


if __name__ == "__main__":
    main()
