from pymongo import MongoClient
import os
from dotenv import load_dotenv

class MongoConnection:

    _instance: MongoClient = None  # Singleton instance

    def __new__(cls, uri=None, db_name=None):
        if cls._instance is None:
            cls._instance = super(MongoConnection, cls).__new__(cls)
            cls._instance.client = MongoClient(uri)
            cls._instance.db = cls._instance.client[db_name]
        return cls._instance

load_dotenv()

uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB")
mongo_conn = MongoConnection(uri, db_name)
db = mongo_conn.db
