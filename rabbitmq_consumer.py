import pika
import json
import logging

# Constants
RABBITMQ_HOST = "localhost"
QUEUE_NAME = "ecommerce_data"

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def process_message(ch, method, properties, body):
    """Processes incoming messages from RabbitMQ."""
    data = json.loads(body)
    logging.info(f"✅ Received message: {data}")

    # Simulate processing (e.g., store in DB)
    # db.store(data)

    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message

def main():
    """Consumes messages from RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    logging.info(f"🎧 Waiting for messages on '{QUEUE_NAME}'...")
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)

    channel.start_consuming()  # Keep listening

if __name__ == "__main__":
    main()

