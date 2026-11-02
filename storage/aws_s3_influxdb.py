"""
Storage Integration: S3/MinIO & InfluxDB.

Maintains backward-compatibility aliases while delegating to the open-source
minio_influxdb module.
"""

from storage.minio_influxdb import (
    INFLUXDB_BUCKET,
    INFLUXDB_ORG,
    INFLUXDB_TOKEN,
    INFLUXDB_URL,
    KAFKA_BROKER,
    KAFKA_TOPIC,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET as S3_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    build_influx_point,
    consume_kafka_to_influx,
    extract_from_influx_and_upload_minio,
    extract_from_influx_and_upload_s3,
    write_sensor_reading_to_influx,
)

__all__ = [
    "KAFKA_BROKER",
    "KAFKA_TOPIC",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "S3_BUCKET",
    "INFLUXDB_URL",
    "INFLUXDB_TOKEN",
    "INFLUXDB_ORG",
    "INFLUXDB_BUCKET",
    "build_influx_point",
    "write_sensor_reading_to_influx",
    "consume_kafka_to_influx",
    "extract_from_influx_and_upload_minio",
    "extract_from_influx_and_upload_s3",
]

if __name__ == "__main__":
    consume_kafka_to_influx()
