import pika
import json
import logging
from typing import Any, Dict
from functools import lru_cache
from rabbitmq_producer import RABBITMQ_HOST, RABBITMQ_QUEUE

# Constants
PREFETCH_COUNT: int = 10000
BATCH_SIZE: int = 10000
LOG_FORMAT : str = "%(asctime)s - %(message)s" 

# Logging setup
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


class RabbitMQConsumer:
    """ RabbitMQ connection and message consumption """

    def __init__(self) -> None:
        self.host = RABBITMQ_HOST
        self.queue = RABBITMQ_QUEUE
        self.connection =  None
        self.channel = None
        self.batch = [] # Buffer to store batches

    def connect(self) -> None:
        """ Establish connection and channel with RabbitMQ """

        logging.info("Connecting to RabbitMQ")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)   
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT) # Limit unprocessed messages

    def process_message(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """ Processes incoming messages from RabbitMQ """

        try:
            data: Dict[str, Any] = json.loads(body)
            data["delivery_tag"] = method.delivery_tag
            self.batch.append(data)

            if len(self.batch) >= BATCH_SIZE:
                self.process_batch(ch)

        except Exception as e:
            logging.error(f"Error processing message: {e}")
    
    def process_batch(self, ch: Any) -> None:
        """ Process batch of messages """

        logging.info(f"Processing batch of {len(self.batch)} messages")
        # Simulate storing in MongoDB or Snowflake
        # db.store(self.batch)
        # Acknowledge all messages in batch
        for message in self.batch:
            ch.basic_ack(delivery_tag=message["delivery_tag"])
        self.batch.clear()


    def consume(self) -> None:
        """ consumes messages from RabbitMQ """
        self.connect()
        logging.info(f"Listening for messages on '{self.queue}'")
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.process_message)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.info("Stopping consumer")
            self.connection.close()

if __name__ == "__main__":
    consumer = RabbitMQConsumer()
    consumer.consume()



