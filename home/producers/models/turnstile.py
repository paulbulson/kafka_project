"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    """Defines a single turnstile"""
    
    logger.info(f"loading {Path(__file__).parents[0]}/schemas/turnstile_key.json")
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    logger.info(f"loading {Path(__file__).parents[0]}/schemas/turnstile_value.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        topic_name = "turnstile" # TODO: Come up with a better topic name (defined as listed in ruberic)
        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, # TODO: Uncomment once schema is defined
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #
        logger.info(f"turnstile entries {num_entries} at station {self.station.name} for line {self.station.color.name}")
        
        # a topic is created for each turnstile for each station in Kafka to track the turnstile events
        for entry in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    'station_id': self.station.station_id,
                    'station_name': self.station.name,
                    'line': self.station.color.name
            },

            )
        # the station emits a turnstyle event to Kafka whenever the Turnstile.run() is called
        # events emitted to Kafka are paired with the Avro key and value schemas
        
