"""
Hadoop Batch Processing - Extract from MongoDB and upload to HDFS.

Demonstrates the MongoDB -> HDFS data flow for batch analytics.
Uses lazy imports to avoid crash at module load if dependencies are not available.
"""

import logging
import os
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "iot_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sensor_readings")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_PATH = "/user/hadoop/iot_data"


def extract_from_mongodb_to_hdfs():
    """Extract batch data from MongoDB and upload to HDFS."""
    import pandas as pd
    from hdfs import InsecureClient
    from pymongo import MongoClient

    logger.info("[INPUT EXPECTED] Source: MongoDB collection '%s.%s' at %s", MONGODB_DB, MONGODB_COLLECTION, MONGODB_URI)
    logger.info("[OUTPUT EXPECTED] Destination: HDFS path '%s' via NameNode %s", HDFS_PATH, HDFS_NAMENODE)

    mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    try:
        mongo_client.admin.command("ping")
        logger.info("[OUTPUT RECEIVED] Connected to MongoDB at %s", MONGODB_URI)
    except Exception as e:
        logger.error("Failed to connect to MongoDB: %s", e)
        return

    logger.info("[PROCESSING] Querying MongoDB documents and projecting fields...")
    data = list(collection.find({}, {"_id": 0}))
    if not data:
        logger.warning("[INPUT RECEIVED] Zero records found in MongoDB for extraction.")
        return

    logger.info("[INPUT RECEIVED] Extracted %d records from MongoDB.", len(data))
    df = pd.DataFrame(data)
    csv_filename = f"/tmp/iot_data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_filename, index=False)
    logger.info("[PROCESSING] Staged intermediate CSV to %s", csv_filename)

    try:
        logger.info("[PROCESSING] Streaming serialized CSV to HDFS NameNode...")
        hdfs_client = InsecureClient(HDFS_NAMENODE, user=HDFS_USER)
        hdfs_file_path = f"{HDFS_PATH}/{os.path.basename(csv_filename)}"
        with hdfs_client.write(hdfs_file_path, encoding="utf-8") as writer:
            df.to_csv(writer, index=False)
        logger.info("[OUTPUT RECEIVED] Uploaded to HDFS successfully: %s", hdfs_file_path)
    except Exception as e:
        logger.error("HDFS upload failed: %s", e)


if __name__ == "__main__":
    extract_from_mongodb_to_hdfs()
