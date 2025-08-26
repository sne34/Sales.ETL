from transform import transform  # import your transform function
from pymongo import MongoClient

def load_to_mongo():
    # Step 1: Get transformed data
    df = transform()  # your transformed DataFrame

    # Step 2: Connect to local MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["salesdb"]           # Database name
    collection = db["orders"]        # Collection name

    # Step 3: Convert DataFrame to dictionary and insert into MongoDB
    data_dict = df.to_dict("records")
    collection.delete_many({})       # Optional: clear collection before inserting
    collection.insert_many(data_dict)

    print(" Data Loaded Successfully into MongoDB")

# Step 4: Run the script
if __name__ == "__main__":
    load_to_mongo()
