"""Plot raw temperature/humidity measurements as they're received from a topic"""

# imports
import pathlib, json, time
from datetime import datetime
from threading import Thread
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import animation
from openmsitoolbox import Runnable, ControlledProcessMultiThreaded
from openmsistream.kafka_wrapper import ConsumerGroup
from argument_parser import SensorPushArgumentParser  # pylint: disable=import-error
from sensor_push_producer import SensorPushProducer  # pylint: disable=import-error


class SensorPushConsumer(ConsumerGroup, ControlledProcessMultiThreaded, Runnable):
    """Makes and live-updates a plot of raw temperature/humidity readings read
    from a topic
    """

    ARGUMENT_PARSER_TYPE = SensorPushArgumentParser

    #################### PUBLIC METHODS ####################

    def __init__(self, config_file, topic_name, **kwargs):
        super().__init__(config_file, topic_name, **kwargs)
        self.measurements_df = None
        self.n_msgs_read = 0
        self.n_msgs_processed = 0
        self.consumers_by_thread_id = {}
        self.fig, self.ax = plt.subplots(2, 1, sharex=True, figsize=(12.0, 9.0))
        self._format_plot()

    def update_plot(self, _):
        """Called as part of a FuncAnimation to update an animated plot based on the
        current state of the measurement dataframe
        """
        if self.measurements_df is None or len(self.measurements_df) < 1:
            return
        for axs in self.ax:
            axs.clear()
        self._format_plot()
        for dev_id in self.measurements_df["device_id"].unique():
            device_df = (
                self.measurements_df[self.measurements_df["device_id"] == dev_id]
            ).copy()
            device_df.sort_values("timestamp", inplace=True)
            timestamps = device_df["timestamp"]
            temps = device_df["temperature"]
            hums = device_df["humidity"]
            self.ax[0].plot(timestamps, temps, marker="o", label=dev_id)
            self.ax[1].plot(timestamps, hums, marker="o", label=dev_id)
        for axs in self.ax:
            axs.relim()
            axs.autoscale_view()
        n_devices = len(self.measurements_df["device_id"].unique())
        if n_devices > 0:
            self.ax[0].legend(
                loc="upper left",
                bbox_to_anchor=(1.0, 1.0),
                ncol=max(1, int(n_devices / 8)),
            )
        self.fig.tight_layout()

    #################### PRIVATE METHODS ####################

    def _format_plot(self):
        self.ax[0].set_title("Temperature measurements")
        self.ax[0].set_ylabel("temperature (deg C)")
        self.ax[1].set_title("Humidity measurements")
        self.ax[1].set_ylabel("humidity (%)")
        self.ax[1].set_xlabel("timestamp")

    def _run_worker(self):
        consumer = self.get_new_subscribed_consumer()
        while self.alive:
            msg = consumer.get_next_message(0)
            if msg is None:
                time.sleep(0.1)
                continue
            with self.lock:
                self.n_msgs_read+=1
            msg_dict = json.loads(msg.value())
            dev_id = msg_dict["address"]
            timestamp = datetime.strptime(
                msg_dict["timestamp"],
                SensorPushProducer.TIMESTAMP_FORMAT,
            )
            temp = msg_dict["temperature"]
            hum = msg_dict["humidity"]
            new_df = pd.DataFrame(
                [
                    {
                        "device_id": dev_id,
                        "timestamp": timestamp,
                        "temperature": temp,
                        "humidity": hum,
                    }
                ]
            )
            with self.lock:
                if self.measurements_df is None:
                    self.measurements_df = new_df
                else:
                    self.measurements_df = pd.concat(
                        [self.measurements_df, new_df], ignore_index=True
                    )
                self.n_msgs_processed+=1
        consumer.close()
            
    def _on_check(self):
        msg = (
            f"{self.n_msgs_read} messages read and {self.n_msgs_processed} messages "
            f"processed so far"
        )
        self.logger.info(msg)

    #################### CLASS METHODS ####################

    @classmethod
    def get_command_line_arguments(cls):
        super_args, super_kwargs = super().get_command_line_arguments()
        cl_kwargs = {**super_kwargs}
        cl_kwargs["config"] = str(
            pathlib.Path(__file__).parent.parent
            / "config_files"
            / "sensorpush_json.config"
        )
        cl_kwargs["n_threads"]=2
        remove_args = ["logger_file_path", "logger_file_level", "update_seconds"]
        for rem_arg in remove_args:
            cl_kwargs[rem_arg] = None
        return super_args, cl_kwargs

    @classmethod
    def run_from_command_line(cls, args=None):
        parser = cls.get_argument_parser()
        args = parser.parse_args(args)
        init_args, init_kwargs = cls.get_init_args_kwargs(args)
        consumer = cls(*init_args, **init_kwargs)
        consumer_thread = Thread(target=consumer.run)
        consumer_thread.start()
        _ = animation.FuncAnimation(
            consumer.fig, consumer.update_plot, interval=500, save_count=100
        )
        plt.show()
        consumer_thread.join()
        msg = (
            f"Processed {consumer.n_msgs_processed} message"
            f"{'s' if consumer.n_msgs_processed!=1 else ''} in total"
        )
        consumer.logger.info(msg)


def main(args=None):
    """Run the Consumer"""
    SensorPushConsumer.run_from_command_line(args)


if __name__ == "__main__":
    main()
