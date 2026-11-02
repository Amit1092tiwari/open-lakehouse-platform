"""
Redis In-Memory Caching and Running Statistics Aggregator for Streaming Events.
"""

import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "processed_readings")


def process_reading(data, redis_client):
    """Process a single sensor reading event, update Redis running stats and cache."""
    if not isinstance(data, dict):
        return None

    device_id = data.get("device_id")
    reading_value = data.get("reading_value")

    if device_id is None or reading_value is None:
        return None

    try:
        reading_val = float(reading_value)
    except (ValueError, TypeError):
        return None

    # Cache latest reading in Redis
    cache_key = f"device:{device_id}:latest"
    redis_client.setex(cache_key, 3600, json.dumps(data))

    # Track running stats
    stats_key = f"device:{device_id}:stats"
    redis_client.hincrby(stats_key, "count", 1)
    redis_client.hincrbyfloat(stats_key, "total", reading_val)

    count_val = redis_client.hget(stats_key, "count")
    total_val = redis_client.hget(stats_key, "total")

    count = int(count_val) if count_val is not None else 1
    total = float(total_val) if total_val is not None else reading_val
    avg_reading = total / count if count > 0 else reading_val

    processed_data = {
        "device_id": device_id,
        "reading_value": reading_val,
        "avg_reading": round(avg_reading, 4),
        "message_count": count,
    }

    redis_client.lpush("processed_queue", json.dumps(processed_data))
    return processed_data


def process_streaming_data():
    """Consume Kafka sensor readings, cache in Redis, and forward processed data."""
    import redis
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import NoBrokersAvailable

    logger.info("[INPUT EXPECTED] Source: Kafka topic '%s' | Schema: {device_id, reading_value}", KAFKA_INPUT_TOPIC)
    logger.info("[OUTPUT EXPECTED] Redis Cache: keys 'device:{id}:latest', 'device:{id}:stats' & Forward Topic: '%s'", KAFKA_OUTPUT_TOPIC)

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

    try:
        redis_client.ping()
        logger.info("[OUTPUT RECEIVED] Connected to Redis at %s:%d", REDIS_HOST, REDIS_PORT)
    except redis.ConnectionError:
        logger.error("Failed to connect to Redis.")
        sys.exit(1)

    try:
        consumer = KafkaConsumer(
            KAFKA_INPUT_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="redis_processor",
        )
    except NoBrokersAvailable:
        logger.error("Kafka broker not available at %s", KAFKA_BROKER)
        sys.exit(1)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
        )
    except NoBrokersAvailable:
        logger.error("Kafka producer cannot connect to %s", KAFKA_BROKER)
        sys.exit(1)

    logger.info("[PROCESSING] Commencing streaming processing, Redis cache invalidation, and statistics computation...")
    count = 0
    for message in consumer:
        logger.debug("[INPUT RECEIVED] Ingested message from topic '%s'", KAFKA_INPUT_TOPIC)
        result = process_reading(message.value, redis_client)
        if result:
            producer.send(KAFKA_OUTPUT_TOPIC, result)
            count += 1
            if count % 100 == 0:
                logger.info("[OUTPUT RECEIVED] Processed %d messages: Cached stats in Redis and forwarded to '%s'.", count, KAFKA_OUTPUT_TOPIC)


if __name__ == "__main__":
    process_streaming_data()
