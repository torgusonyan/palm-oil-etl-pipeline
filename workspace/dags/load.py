'''
=================================================
Data Loading Script (Memory Optimized for Docker)
Author  : Ryan Farid Pratama

This program loads the transformed PySpark DataFrame into MongoDB Atlas 
safely without using toPandas() to prevent Java OutOfMemoryErrors in Docker.
=================================================
'''

import os
import certifi

from pymongo import MongoClient
from pyspark.sql import DataFrame
from dotenv import load_dotenv
load_dotenv()



MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME = "portfolio3-db"
COLLECTION_NAME = "palm_oil_supply_chain"

def load_data(
    df: DataFrame, 
    mongo_uri: str = MONGO_URI, 
    db_name: str = DB_NAME, 
    collection_name: str = COLLECTION_NAME, 
    batch_size: int = 1000
) -> None:
    '''
    Loads a transformed PySpark DataFrame into MongoDB Atlas using safe partition
    streaming to avoid Java OutOfMemoryError in Docker environments.
    '''
    
    print("Connecting to MongoDB Atlas...")
    try:
        client = MongoClient(mongo_uri, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
    except Exception as e:
        # Surface the REAL error instead of mislabeling every failure as SSL,
        # and do not retry with the same URI (which would fail identically).
        print(f"MongoDB connection failed ({type(e).__name__}: {e})")
        raise

    # Clear existing collection to keep it clean during re-runs
    client[db_name][collection_name].delete_many({})
    client.close()

    print("Streaming DataFrame partitions and inserting in batches...")

    # Split the data into many small partitions so no single chunk is large
    # enough to exhaust the JVM heap. Each partition is processed on a Spark
    # worker (not pulled back to the driver), which is what prevents the
    # Java OutOfMemoryError that toLocalIterator()/toPandas() caused.
    df = df.repartition(64)

    def insert_partition(rows):
        '''Runs on each Spark worker: opens its own Mongo connection and
        inserts that partition's rows in small batches.'''
        worker_client = MongoClient(
            mongo_uri, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=10000
        )
        worker_collection = worker_client[db_name][collection_name]

        batch = []
        for row in rows:
            batch.append(row.asDict())
            if len(batch) >= batch_size:
                worker_collection.insert_many(batch)
                batch = []

        # Insert any remaining records in this partition
        if batch:
            worker_collection.insert_many(batch)

        worker_client.close()

    # foreachPartition streams the data on the Spark side; nothing large is
    # ever collected into the driver's Python/JVM memory at once.
    df.foreachPartition(insert_partition)

    # Verify the final document count from the driver
    verify_client = MongoClient(mongo_uri, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=10000)
    inserted_count = verify_client[db_name][collection_name].count_documents({})
    verify_client.close()

    print(f"Successfully inserted all {inserted_count} documents into MongoDB database '{db_name}', collection '{collection_name}'.")


if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data

    raw_path = "raw_dataset.csv"
    
    print("Running extraction...")
    df_raw = extract_data(raw_path)
    print("Running transformation...")
    df_clean = transform_data(df_raw)
    print("Running loading process...")
    load_data(df_clean)