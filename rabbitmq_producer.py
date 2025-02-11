import pika
import json 
import time 
import logging
from pyspark.sql import SparkSession


DATA_FILE = 'Sales Transaction v.4a.csv'
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'ecommerce_data'
DELAY = 0.01

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def init_spark():
    return SparkSession.builder.appName('Ecommerce Data').getOrCreate()

def read_data(spark):
    """ Read csv into a spark dataframe """
    logging.info(f"Reading data from {DATA_FILE}")
    return spark.read.csv(DATA_FILE, header=True, inferSchema=True)

def send_data(row, channel):
    """ Sends a row of data to RabbitMQ """
    message = json.dumps(row.asDict())
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=message)
    logging.info(f"Sent message: {message}")

def main():
    spark = init_spark()
    df = read_data(spark)
    
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)

    for i, row in enumerate(df.toLocalIterator()):
        if i >= 10:
            break

        send_data(row, channel)
        time.sleep(DELAY)
    

    connection.close()
    spark.stop()
    connection.close()

    logging.info("Finished sending data")


if  __name__ == '__main__':
    main()