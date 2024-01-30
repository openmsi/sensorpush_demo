"""Connect to a group of SensorPush devices and produce json-formatted string messages
with readings from them at some set interval
"""

# imports
import asyncio, pathlib, json  # pylint: disable=multiple-imports
from datetime import datetime
from bleak import BleakScanner, BleakClient
from bleak.exc import BleakDeviceNotFoundError
from sensorpush import sensorpush as sp
from openmsitoolbox import ControlledProcessAsync, Runnable
from openmsistream.kafka_wrapper.producer_group import ProducerGroup
from argument_parser import SensorPushArgumentParser  # pylint: disable=import-error


class SensorPushProducer(ProducerGroup, ControlledProcessAsync, Runnable):
    """Connect to a group of SensorPush devices and produce messages with readings from
    them to a Kafka topic
    """

    ARGUMENT_PARSER_TYPE = SensorPushArgumentParser
    SENSOR_NAME_START = (
        "SensorPush HT.w"  # The start of all SensorPush HT.w devices' names
    )
    SCAN_TIME = 10  # How long to scan for new devices each iteration
    TIMESTAMP_FORMAT = "%Y-%m-%d-%H-%M-%S"

    #################### PUBLIC METHODS ####################

    def __init__(
        self,
        device_addresses,
        sampling_interval,
        n_connection_retries,
        topic_name,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.device_addresses = device_addresses
        self.found_addresses = set()
        self.sampling_interval = sampling_interval
        self.n_connection_retries = n_connection_retries
        self.topic_name = topic_name
        msg = (
            f"Will produce SensorPush readings every {self.sampling_interval} seconds "
            f"to the {self.topic_name} topic"
        )
        self.logger.info(msg)
        self.producer = self.get_new_producer()
        self.n_msgs_produced = 0
        self._lock = asyncio.Lock()
        self._device_addresses = set()
        self._clients_by_address = {}
        self._readings_queue = asyncio.Queue()

    async def run_task(self):
        "Triggers all of the program's constituent async functions"
        await asyncio.gather(
            self._scan_for_devices(),
            self._connect_clients(),
            self._enqueue_readings(),
            self._produce_readings(),
            self._poll_producer(),
        )

    #################### PRIVATE METHODS ####################

    async def _scan_for_devices(self):
        """If the program is automatically scanning for all devices found, this function
        scans for new devices and registers them when they're recognized.
        """
        if isinstance(self.device_addresses, list):
            async with self._lock:
                self.found_addresses = set(self.device_addresses)
            return

        async def callback(device, _):
            if device.name and device.name.startswith(self.SENSOR_NAME_START):
                async with self._lock:
                    if device.address not in self.found_addresses:
                        self.logger.info(
                            f"Found new device with address {device.address}"
                        )
                    self.found_addresses.add(device.address)

        while self.alive:
            async with BleakScanner(callback):
                self.logger.debug(
                    f"Scanning for SensorPush devices for {self.SCAN_TIME}s"
                )
                await asyncio.sleep(self.SCAN_TIME)

    async def _connect_clients(self):
        "Creates connected clients for any new devices found"
        while self.alive:
            if len(self.found_addresses) <= 0:
                await asyncio.sleep(1)
                continue
            async with self._lock:
                tasks = [
                    self._connect_client(addr)
                    for addr in self.found_addresses
                    if addr not in self._clients_by_address
                ]
            if len(tasks) > 0:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(1)

    async def _connect_client(self, device_address):
        "Create a BleakClient connected to the device at the given address"
        self.logger.info(f"Connecting to SensorPush device at {device_address}...")
        client = BleakClient(device_address)
        for iretry in range(self.n_connection_retries):
            try:
                await client.connect()
                break
            except BleakDeviceNotFoundError as exc:
                if iretry < self.n_connection_retries:
                    msg = (
                        f"Failed a connection attempt to {device_address}. "
                        f"Retrying ({self.n_connection_retries-iretry} left)..."
                    )
                    self.logger.debug(msg)
                else:
                    errmsg = (
                        f"Failed to connect to device after {self.n_connection_retries} "
                        "retries!"
                    )
                    self.logger.error(errmsg, exc_info=exc, reraise=False)
        batt_mv, _ = await sp.read_batt_info(client)
        self.logger.info(
            f"Connected to device at {device_address} (battery power={batt_mv}V)"
        )
        async with self._lock:
            self._clients_by_address[device_address] = client

    async def _enqueue_readings(self):
        """While the process is running, get readings every sampling interval and add
        them to the reading queue
        """
        while self.alive:
            if len(self._clients_by_address) <= 0:
                await asyncio.sleep(1)
            async with self._lock:
                tasks = [
                    self._enqueue_reading_for_client(addr, client)
                    for addr, client in self._clients_by_address.items()
                ]
            if len(tasks) > 0:
                await asyncio.gather(*tasks)
                await asyncio.sleep(self.sampling_interval)
            else:
                await asyncio.sleep(1)

    async def _enqueue_reading_for_client(self, address, client):
        """Gets and enqueues timestamped temperature and humidity readings for
        a given client"""
        timestamp = datetime.now()
        temp_c, hum = await asyncio.gather(
            sp.read_temperature(client), sp.read_humidity(client)
        )
        self.logger.debug(f"Enqueing {address} reading of {temp_c} degC, {hum}%")
        await self._readings_queue.put((timestamp, address, temp_c, hum))

    def _producer_callback(self, err, msg):
        """Callback function registered with the producer for each message sent. Calls
        "produce" again for any failed messages, and otherwise increments the message
        counter
        """
        if err is None and msg.error() is not None:
            err = msg.error()
        if err is not None:
            warnmsg = (
                f'WARNING: Producer {"fatally" if err.fatal() else ""} failed to '
                f"deliver message with key {msg.key()}"
                f'{" and cannot retry" if not err.retriable() else ""}. '
                f"This message will be re-enqeued. Error reason: {err.str()}"
            )
            self.logger.warning(warnmsg)
            self.producer.produce(
                topic=msg.topic(),
                key=msg.key(),
                value=msg.value(),
                on_delivery=self._producer_callback,
            )
        else:
            self.n_msgs_produced += 1

    async def _produce_readings(self):
        """While the process is running, produce readings from the queue as
        json-formatted strings
        """
        while self.alive:
            if self._readings_queue.empty():
                await asyncio.sleep(1)
                continue
            reading = await self._readings_queue.get()
            timestamp = datetime.strftime(reading[0], self.TIMESTAMP_FORMAT)
            address = reading[1]
            temp_c = reading[2]
            hum = reading[3]
            msg_key = f"{address}_reading_{timestamp}"
            msg_value = json.dumps(
                {
                    "timestamp": timestamp,
                    "address": address,
                    "temperature": temp_c,
                    "humidity": hum,
                }
            )
            self.logger.debug(f"Producing {msg_key}")
            self.producer.produce(
                topic=self.topic_name,
                key=msg_key,
                value=msg_value,
                on_delivery=self._producer_callback,
            )

    async def _poll_producer(self):
        "Calls 'poll' on the producer periodically to trigger the callback"
        while self.alive:
            await asyncio.sleep(self.SCAN_TIME)
            self.producer.poll(1)

    def _on_check(self):
        self.logger.info(
            f"{self.n_msgs_produced} messages produced to {self.topic_name} so far"
        )

    def _on_shutdown(self):
        self.producer.flush()
        self.producer.close()
        self.logger.info(
            f"{self.n_msgs_produced} messages produced to {self.topic_name} in total"
        )
        return super()._on_shutdown()

    #################### CLASS METHODS ####################

    @classmethod
    def get_command_line_arguments(cls):
        super_args, super_kwargs = super().get_command_line_arguments()
        cl_args = [
            *super_args,
            "device_addresses",
            "sampling_interval",
            "n_connection_retries",
            "topic_name",
        ]
        cl_kwargs = {**super_kwargs}
        cl_kwargs["config"] = str(
            pathlib.Path(__file__).parent.parent
            / "config_files"
            / "sensorpush_json.config"
        )
        remove_args = ["logger_file_path", "logger_file_level", "update_seconds"]
        for rem_arg in remove_args:
            cl_kwargs[rem_arg] = None
        return cl_args, cl_kwargs

    @classmethod
    def run_from_command_line(cls, args=None):
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        init_args, init_kwargs = cls.get_init_args_kwargs(args)
        sensorpush_producer = cls(
            args.device_addresses,
            args.sampling_interval,
            args.n_connection_retries,
            args.topic_name,
            *init_args,
            **init_kwargs,
        )
        asyncio.run(sensorpush_producer.run_loop())


def main(args=None):
    "Run the SensorPushProducer"
    SensorPushProducer.run_from_command_line(args)


if __name__ == "__main__":
    main()
