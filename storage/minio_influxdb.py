"""
MinIO & InfluxDB Open-Source Time-Series and Object Storage Sink.

100% Open-Source stack:
- MinIO: High-performance, S3-compatible open-source object store (AGPLv3)
- InfluxDB: Open-source time-series database for high-throughput IoT metrics
- Apache Kafka: Distributed event streaming platform (Apache 2.0)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2024")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw-data")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "pipeline-influx-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pipeline-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")


def build_influx_point(data):
    """Construct an InfluxDB Point from raw sensor dictionary."""
    if not isinstance(data, dict):
        return None
    device_id = data.get("device_id")
    reading_value = data.get("reading_value")
    timestamp = data.get("timestamp", int(time.time()))

    if device_id is None or reading_value is None:
        return None

    try:
        from influxdb_client import Point, WritePrecision
        return (
            Point("sensor_readings")
            .tag("device_id", str(device_id))
            .field("reading_value", float(reading_value))
            .time(timestamp, WritePrecision.S)
        )
    except ImportError:
        # Fallback dictionary for testing when influxdb_client is not installed
        return {
            "measurement": "sensor_readings",
            "tags": {"device_id": str(device_id)},
            "fields": {"reading_value": float(reading_value)},
            "time": timestamp,
        }


def write_sensor_reading_to_influx(data, write_api, bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG):
    """Write a single reading point to InfluxDB write_api."""
    point = build_influx_point(data)
    if point is not None:
        write_api.write(bucket=bucket, org=org, record=point)
        return True
    return False


def consume_kafka_to_influx():
    """Consume Kafka sensor data and store in InfluxDB."""
    from influxdb_client import InfluxDBClient
    from kafka import KafkaConsumer

    logger.info("[INPUT EXPECTED] Source: Kafka topic '%s' on broker '%s' | Schema: {device_id, reading_value, timestamp}", KAFKA_TOPIC, KAFKA_BROKER)
    logger.info("[OUTPUT EXPECTED] Destination: InfluxDB at %s | Org: '%s' | Bucket: '%s'", INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET)

    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx_client.write_api()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="influxdb_consumer",
    )

    logger.info("[PROCESSING] Commencing streaming consumption from Kafka into InfluxDB...")
    messages_stored = 0

    for message in consumer:
        logger.debug("[INPUT RECEIVED] Consumed Kafka message: offset=%s, key=%s", message.offset, message.key)
        if write_sensor_reading_to_influx(message.value, write_api):
            messages_stored += 1
            if messages_stored % 100 == 0:
                logger.info("[OUTPUT RECEIVED] InfluxDB Sink: Ingested %d sensor points successfully.", messages_stored)


def extract_from_influx_and_upload_minio():
    """Extract data from InfluxDB and upload to MinIO S3 object store."""
    import boto3
    import pandas as pd
    from influxdb_client import InfluxDBClient

    logger.info("[INPUT EXPECTED] Source: InfluxDB bucket '%s' (range: last 24h) | Measurement: 'sensor_readings'", INFLUXDB_BUCKET)
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

    query = f"""
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "sensor_readings")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """

    logger.info("[PROCESSING] Querying InfluxDB and pivoting time-series records to tabular shape...")
    query_api = influx_client.query_api()
    tables = query_api.query(query, org=INFLUXDB_ORG)

    data = []
    for table in tables:
        for record in table.records:
            data.append(record.values)

    df = pd.DataFrame(data)
    if df.empty:
        logger.warning("[INPUT RECEIVED] No data found in InfluxDB for the last 24h window.")
        return

    logger.info("[INPUT RECEIVED] Extracted %d records from InfluxDB.", len(df))

    csv_filename = f"/tmp/iot_data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    logger.info("[PROCESSING] Serializing %d records to CSV: %s", len(df), csv_filename)
    df.to_csv(csv_filename, index=False)

    logger.info("[OUTPUT EXPECTED] Destination: MinIO bucket s3://%s/iot_data/ at %s", MINIO_BUCKET, MINIO_ENDPOINT)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    s3_key = f"iot_data/{os.path.basename(csv_filename)}"
    s3.upload_file(csv_filename, MINIO_BUCKET, s3_key)
    logger.info("[OUTPUT RECEIVED] InfluxDB snapshot uploaded successfully to MinIO at s3://%s/%s", MINIO_BUCKET, s3_key)


# Backward compatibility alias
extract_from_influx_and_upload_s3 = extract_from_influx_and_upload_minio


if __name__ == "__main__":
    consume_kafka_to_influx()
