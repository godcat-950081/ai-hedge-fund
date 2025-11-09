from pymongo import MongoClient

MONGO_URI = "mongodb://admin:Z!cxz098-lz@localhost:27017/"
DB_NAME = "ai-hedge-fund"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]