import pandas as pd
from transform import transform
from pymongo import MongoClient

def load_to_mongo():
    # Step 1: Get transformed data
    df = transform()

    # Step 2: Connect to MongoDB
    # Replace the URI with your MongoDB connection string
    client = MongoClient("mongodb://localhost:27017/")  # default local MongoDB
    db = client["salesdb"]  # database name
    collection = db["orders"]  # collection name

    # Step 3: Convert DataFrame to dictionary and insert into MongoDB
    data_dict = df.to_dict("records")
    collection.delete_many({})  # optional: clear collection before inserting
    collection.insert_many(data_dict)

    print("✅ Data Loaded Successfully into MongoDB")
    print(f"Database: {db.name}, Collection: {collection.name}")

if __name__ == "__main__":
    load_to_mongo()
