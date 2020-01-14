"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        logger.info(f"Creating kafka consumer for {topic_name_pattern}")
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
                #
                # TODO
                #
            'bootstrap.servers': 'PLAINTEXT://localhost:9092',
            "default.topic.config": {"auto.offset.reset": "earliest"},
            'group.id': f'{topic_name_pattern}'
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8001"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        logger.info(f"Attempting to subscribe to topic {self.topic_name_pattern}")
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)
        logger.info(f"Successful subscription to topic {self.topic_name_pattern}")

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        logger.info(f"on_assign {self.topic_name_pattern} consumer {consumer} and partitions {partitions}")
        for partition in partitions:
            # TODO
            logger.info(f"setting offset ...")
            if self.offset_earliest is True:
#                logger.info("xxx")
                #partition.offset = OFFSET_BEGINNING
                partition.offset = 0 # hanging when using OFFSET_BEGINNING
#                logger.info("yyy")
            logger.info(f"done setting offset")

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        logger.info(f"Consuming data {self.topic_name_pattern}")
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            logger.info("Sleeping ...")
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.

        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.

        logger.info(f"poll message from {self.topic_name_pattern}")
        try:
            message = self.consumer.poll(self.consume_timeout)
            logger.info(f"message returned from poll call is {message}")
        except error:
            logger.info(f'Could not poll message from {self.topic_name_pattern}')
            return 0
                
        if message is None:
            logger.info(f"No message received")
            return 0
        elif message.error() is not None:
            logger.info(f"Message error: {message.error()}")
            return 0
        else:
            logger.info(f"consumed message {message.topic()}")
            self.message_handler(message)
        return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        # TODO: Cleanup the kafka consumer
        logger.info(f"closing {self.topic_name_pattern}")
        self.consumer.close()