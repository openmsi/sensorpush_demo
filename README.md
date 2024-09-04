# SensorPush Demos

Scripts for demos using SensorPush devices

## Installation

In a new conda environment with Python>=3.9, clone this repository and then run:

    cd sensorpush_demo
    pip install -r requirements.txt

## Programs

There are several programs available in this repository. Add "`-h`" to any of the commands below to display the help text for each tool with complete descriptions of all recognized arguments.

### Utilities

To find devices:

    python src/get_device_addresses.py --all

The above will run for 30 seconds and print out the addresses of all SensorPush HT.w devices it can find within that time.

To test connection to a device:

    python src/blink_device.py [device_address]

The above will connect to the device with the given address and blink the LED on it 10 times.

To periodically write out CSV files with measurements in them:

    python src/sensor_push_csv_writer.py [device_address] [path_to_output_dir]

The above will periodically write out small CSV files containing single readings from the device with the given address, adding them to the directory at `[path_to_output_dir]`. This program is useful to write out files that can then be produced by a regular OpenMSIStream producer with a DataFileUploadDirectory with a command like:

    DataFileUploadDirectory [path_to_output_dir] --topic_name [topic_name]

for example.

### Bare producer

To produce bare reading messages to a topic:

    python src/sensor_push_producer.py --device_addresses [addr_1] [addr_2] --topic_name [topic_name] --sampling_interval [sample_secs]

The above will launch a long-running producer program to produce readings to a topic as JSON-formatted strings. Add device addresses to pull readings from with the "`--device_addresses`" argument (space separated) or leave that argument out to produce readings from all devices detected (they will automatically connect and disconnect as they enter/leave range). Topic name and sampling interval are configurable, as well as the config file to use (should contain `[broker]` and `[producer]` sections; the default value is a good example of where to start to write a new one).

### Consumers

To live-plot readings that were produced with the `sensor_push_producer` program:

    python src/sensor_push_consumer.py --topic_name [topic_name]

(reads JSON-formatted messages).

To run the file-based "StreamProcessor"-type program:

    python src/sensor_push_stream_plotter.py --topic_name [topic_name]

The above will read OpenMSIStream DataFileChunk-formatted messages produced by the `sensor_push_csv_writer` program paired with an OpenMSIStream DataFileUploadDirectory (or similar).

Both of the programs references above are more configurable; see all options available by adding "`-h`" to the commands listed.
