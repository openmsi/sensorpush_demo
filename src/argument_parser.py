"""The ArgumentParser to use in the SensorPush programs"""

# imports
from openmsistream.utilities import OpenMSIStreamArgumentParser
from openmsitoolbox.argument_parsing.parser_callbacks import positive_int


class SensorPushArgumentParser(OpenMSIStreamArgumentParser):
    """ArgumentParser for SensorPush activities"""

    ARGUMENTS = {
        **OpenMSIStreamArgumentParser.ARGUMENTS,
        "device_address": [
            "positional",
            {
                "help": "The address (MAC or UUID) of the SensorPush device to connect to"
            },
        ],
        "device_addresses": [
            "optional",
            {
                "nargs": "+",
                "default": "all",
                "help": (
                    "The addresses (MAC or UUID) of the SensorPush device to connect to. "
                    "Set this to 'all' to automatically scan for SensorPush devices to use."
                ),
            },
        ],
        "sampling_interval": [
            "optional",
            {
                "help": (
                    "How often samples should be read from the sensor and written out "
                    "to CSV files (seconds). Inexact, as there is some lag in "
                    "communicating with sensors."
                ),
                "default": 5,
                "type": positive_int,
            },
        ],
        "n_connection_retries": [
            "optional",
            {
                "help": "Number of retries to use for connecting to a SensorPush device",
                "default": 5,
                "type": positive_int,
            },
        ],
        "name": [
            "optional",
            {
                "help": "An informal name to associate with the given SensorPush device",
            },
        ],
    }
