"""
MongoDB Streaming Sink for IoT Sensor Events.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "iot_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sensor_readings")
MAX_RETRIES = 5
RETRY_BACKOFF = 5


def validate_and_transform_sensor_reading(data):
    """Validate and format a raw sensor reading event for MongoDB persistence."""
    if not isinstance(data, dict):
        return None
    device_id = data.get("device_id")
    reading_value = data.get("reading_value")

    if device_id is None or reading_value is None:
        logger.warning("[PROCESSING] Skipping malformed message missing device_id or reading_value: %s", data)
        return None

    return {
        "event_id": data.get("event_id"),
        "device_id": device_id,
        "reading_value": float(reading_value),
        "timestamp": data.get("timestamp", int(time.time())),
        "ingested_at": datetime.now(timezone.utc),
    }


def store_sensor_reading(data, collection):
    """Store a single sensor reading document in MongoDB collection."""
    doc = validate_and_transform_sensor_reading(data)
    if doc:
        collection.insert_one(doc)
        logger.debug("[OUTPUT RECEIVED] MongoDB document stored for device_id %s.", doc["device_id"])
        return True
    return False


def consume_kafka_to_mongodb():
    """Consume Kafka messages and store in MongoDB."""
    from pymongo import MongoClient
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    logger.info("[INPUT EXPECTED] Source: Kafka topic '%s' on broker '%s' | Schema: {event_id, device_id, reading_value, timestamp}", KAFKA_TOPIC, KAFKA_BROKER)
    logger.info("[OUTPUT EXPECTED] Destination: MongoDB database '%s', collection '%s' at %s", MONGODB_DB, MONGODB_COLLECTION, MONGODB_URI)

    mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    try:
        mongo_client.admin.command("ping")
        logger.info("[OUTPUT RECEIVED] Connected to MongoDB at %s", MONGODB_URI)
    except Exception as e:
        logger.error("Failed to connect to MongoDB: %s", e)
        sys.exit(1)

    consumer = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="mongodb_consumer",
            )
            logger.info("[OUTPUT RECEIVED] Connected to Kafka, listening on topic: %s", KAFKA_TOPIC)
            break
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d). Retrying in %ds...",
                attempt,
                MAX_RETRIES,
                RETRY_BACKOFF,
            )
            time.sleep(RETRY_BACKOFF)

    if consumer is None:
        logger.error("Failed to connect to Kafka.")
        sys.exit(1)

    logger.info("[PROCESSING] Commencing streaming consumption into MongoDB document collection...")
    messages_stored = 0
    for message in consumer:
        logger.debug("[INPUT RECEIVED] Received message from Kafka: %s", message.value)
        if store_sensor_reading(message.value, collection):
            messages_stored += 1
            if messages_stored % 100 == 0:
                logger.info("[OUTPUT RECEIVED] Stored %d documents in MongoDB '%s.%s'.", messages_stored, MONGODB_DB, MONGODB_COLLECTION)


if __name__ == "__main__":
    consume_kafka_to_mongodb()
