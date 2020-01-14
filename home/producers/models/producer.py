"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        logger.info(f"Creating {topic_name} producer with key schema {key_schema} and value schema {value_schema}")
        
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://localhost:9092',
            'schema.registry.url': 'http://localhost:8081'
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            logger.info(f"creating topic {self.topic_name}")
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
            logger.info(f"created topic {self.topic_name}")
        else:
            logger.info(f"Topic {self.topic_name} already exists")

        # TODO: Configure the AvroProducer
        # See below link for parameters
        # https://github.com/confluentinc/confluent-kafka-python/blob/fe8596887f62cfc11b688ece11378ae815b7e45f/confluent_kafka/avro/__init__.py#L16
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema = self.key_schema,
            default_value_schema = self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        # See Lesson 2 Topic 14 'Topic Creation'
        
        logger.info(f"creating topic {self.topic_name}")
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})

        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )

        for topic, future in futures.items():
            logger.info(f"confirming topic creation")
            try:
                future.result()
                logger.info(f"created topic {self.topic_name} confirmed")
            except Exception as e:
                logger.info(f"failed to create topic {self.topic_name}: {e}")
        
    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        
        if self.producer is None:
            return

        logger.info(f"flushing producer {self.topic_name}")
        self.producer.flush()        
        logger.info(f"producer {self.topic_name} closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
