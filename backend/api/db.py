import os
from pymongo import MongoClient

def get_db():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB", "bitcoin_db")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    return client[db_name]
