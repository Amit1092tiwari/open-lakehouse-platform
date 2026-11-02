"""Unit tests for polyglot storage sinks (MongoDB, InfluxDB, Redis)."""

import os
import sys
from unittest.mock import MagicMock
import pytest

# Add storage directory to sys.path
STORAGE_DIR = os.path.join(os.path.dirname(__file__), "..", "storage")
sys.path.insert(0, STORAGE_DIR)

from mongodb_streaming import validate_and_transform_sensor_reading, store_sensor_reading
from aws_s3_influxdb import build_influx_point, write_sensor_reading_to_influx
from redis_integration import process_reading


# ==============================================================================
# MongoDB Sink Tests
# ==============================================================================

def test_mongodb_validate_and_transform_success():
    """Verify valid sensor reading is formatted properly for MongoDB."""
    data = {
        "event_id": "evt-123",
        "device_id": 1001,
        "reading_value": 45.8,
        "timestamp": 1700000000,
    }
    result = validate_and_transform_sensor_reading(data)
    assert result is not None
    assert result["device_id"] == 1001
    assert result["reading_value"] == 45.8
    assert result["timestamp"] == 1700000000
    assert "ingested_at" in result


def test_mongodb_validate_and_transform_malformed():
    """Verify malformed messages return None."""
    assert validate_and_transform_sensor_reading(None) is None
    assert validate_and_transform_sensor_reading("invalid string") is None
    assert validate_and_transform_sensor_reading({"device_id": 1001}) is None
    assert validate_and_transform_sensor_reading({"reading_value": 50.0}) is None


def test_mongodb_store_sensor_reading():
    """Verify store_sensor_reading inserts document into collection."""
    mock_collection = MagicMock()
    data = {"device_id": 1002, "reading_value": 78.4}
    success = store_sensor_reading(data, mock_collection)

    assert success is True
    mock_collection.insert_one.assert_called_once()
    args, _ = mock_collection.insert_one.call_args
    assert args[0]["device_id"] == 1002
    assert args[0]["reading_value"] == 78.4


# ==============================================================================
# InfluxDB Sink Tests
# ==============================================================================

def test_influxdb_build_point_success():
    """Verify InfluxDB point construction from valid sensor payload."""
    data = {"device_id": 505, "reading_value": 62.3, "timestamp": 1700001000}
    point = build_influx_point(data)
    assert point is not None


def test_influxdb_build_point_malformed():
    """Verify malformed payloads return None."""
    assert build_influx_point({}) is None
    assert build_influx_point({"device_id": 505}) is None
    assert build_influx_point({"reading_value": 62.3}) is None


def test_influxdb_write_point():
    """Verify writing point to mock write_api."""
    mock_write_api = MagicMock()
    data = {"device_id": 505, "reading_value": 62.3}
    success = write_sensor_reading_to_influx(data, mock_write_api, bucket="test_bucket", org="test_org")

    assert success is True
    mock_write_api.write.assert_called_once()


# ==============================================================================
# Redis Sink Tests
# ==============================================================================

def test_redis_process_reading():
    """Verify Redis caching, running stats, and queueing logic."""
    mock_redis = MagicMock()
    # Mock return values for hget (count and total)
    mock_redis.hget.side_effect = lambda key, field: "2" if field == "count" else "100.0"

    data = {"device_id": 2001, "reading_value": 50.0}
    result = process_reading(data, mock_redis)

    assert result is not None
    assert result["device_id"] == 2001
    assert result["reading_value"] == 50.0
    assert result["message_count"] == 2
    assert result["avg_reading"] == 50.0

    # Verify calls
    mock_redis.setex.assert_called_once()
    mock_redis.hincrby.assert_called_with("device:2001:stats", "count", 1)
    mock_redis.hincrbyfloat.assert_called_with("device:2001:stats", "total", 50.0)
    mock_redis.lpush.assert_called_once()

