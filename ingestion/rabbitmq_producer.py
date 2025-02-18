import pika
import json 
import time 
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict, List

# Constants
DATA_FILE : str = './ingestion/Sales Transaction v.4a.csv'
RABBITMQ_HOST: str = 'localhost'
RABBITMQ_QUEUE: str = 'ecommerce_data'
BATCH_SIZE: int = 10000

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

class SparkSessionFactory:
    """ Factory for creating and managing SparkSession """

    _instance: SparkSession = None 

    @classmethod
    def spark_session(cls) -> SparkSession:
        """ Singleton implementation to get SparkSession """
        if cls._instance is None:
            cls._instance = SparkSession.builder.appName('Ecommerce Data').getOrCreate()
        return cls._instance

class DataCleaner:

    """ Data cleaning operations """

    @staticmethod
    def clean_data(df: DataFrame) -> DataFrame:
        logging.info("Cleaning data")

        df = df.dropna(subset=["TransactionNo", "ProductNo", "ProductName", "Price", "Quantity", "CustomerNo", "Country"])
        df = df.dropDuplicates(["TransactionNo", "ProductNo", "CustomerNo"])
        df = df.withColumn("Price", col("Price").cast("float")) \
            .withColumn("Quantity", col("Quantity").cast("int"))
        logging.info("Data cleaned")
        return df

class RabbitMQProducer:
    """ RabbitMQ connection and message publishing in batches for faster read """

    def __init__(self, host: str, queue: str) -> None:
        self.host = host
        self.queue = queue 
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)

    def send_batch(self, batch: list[dict]) -> None:
        for m in batch:
            self.channel.basic_publish(exchange='', routing_key=self.queue, body=json.dumps(m))
        logging.info(f"Sent {len(batch)} messages")
    
    def close_connection(self) -> None:
        self.connection.close()


def load_data(file: str) -> DataFrame:
    """ Read csv into a spark dataframe """
    logging.info(f"Reading data from {file}")
    spark = SparkSessionFactory.spark_session()
    df = spark.read.csv(file, header=True, inferSchema=True)
    return df


def main() -> None:
    """ Main function to execute the ETL pipeline """ 
    spark = SparkSessionFactory.spark_session()
    df = load_data(DATA_FILE)
    df = DataCleaner.clean_data(df)

    producer = RabbitMQProducer(RABBITMQ_HOST, RABBITMQ_QUEUE)

    batch: List[dict] = []
    for row in df.collect():  # Collects all rows efficiently
        batch.append(row.asDict())

        if len(batch) >= BATCH_SIZE:
            producer.send_batch(batch)
            batch.clear()
        
    if batch:
        producer.send_batch(batch)
        
    producer.close_connection()
    spark.stop()
    logging.info("Finished sending data")

if __name__ == '__main__': 
    main()






