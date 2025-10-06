from pymongo import MongoClient
c = MongoClient("mongodb://localhost:27017")
db = c.bitcoin_db
print("Count:", db.btc_historical.count_documents({}))
doc = db.btc_historical.find_one({}, {"date": 1, "_id": 0})
print("Example date:", doc)