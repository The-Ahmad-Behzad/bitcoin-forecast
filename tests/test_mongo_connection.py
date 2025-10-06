# quick test snippet
from pymongo import MongoClient
import os
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client.get_database("bitcoin_db")
res = db.test_collection.insert_one({"test": "ok"})
print("Inserted id:", res.inserted_id)
count = db.test_collection.count_documents({})
print("Count in test_collection:", count)
# optionally: db.test_collection.drop()
