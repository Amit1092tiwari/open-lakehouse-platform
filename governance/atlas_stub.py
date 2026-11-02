"""
Apache Atlas Metadata & Data Lineage Integration.
"""

import json
import logging
import os

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

ATLAS_API_URL = os.getenv("ATLAS_API_URL", "http://atlas:21000/api/atlas/v2")
ATLAS_USERNAME = os.getenv("ATLAS_USERNAME", "admin")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD", "admin")

HEADERS = {"Content-Type": "application/json"}


def check_dataset_exists(dataset_name):
    """Check if a dataset exists in Apache Atlas."""
    url = f"{ATLAS_API_URL}/entities?type=Dataset&name={dataset_name}"
    try:
        response = requests.get(
            url,
            auth=(ATLAS_USERNAME, ATLAS_PASSWORD),
            headers=HEADERS,
            timeout=10,
        )
        if response.status_code == 200:
            data = response.json()
            exists = bool(data.get("entities"))
            if exists:
                logger.info("[OUTPUT RECEIVED] Dataset '%s' exists in Atlas catalog.", dataset_name)
            else:
                logger.warning("[OUTPUT RECEIVED] Dataset '%s' not found in Atlas catalog.", dataset_name)
            return exists
        logger.error("Atlas API error: %d - %s", response.status_code, response.text)
        return False
    except requests.RequestException as e:
        logger.error("Atlas connectivity error: %s", str(e))
        return False


def register_dataset_lineage(source_name, target_name, extra_info=None):
    """Register dataset lineage in Apache Atlas."""
    logger.info("[INPUT EXPECTED] Lineage nodes: Source='%s' -> Target='%s'", source_name, target_name)
    logger.info("[PROCESSING] Verifying entity registration in Atlas before constructing lineage edge...")
    if not check_dataset_exists(source_name) or not check_dataset_exists(target_name):
        logger.error("Cannot register lineage: source or target not found in Atlas.")
        return False

    payload = {
        "guidEntityMap": {},
        "relations": [
            {
                "typeName": "Process",
                "fromEntityId": source_name,
                "toEntityId": target_name,
                "relationshipAttributes": extra_info or {},
            }
        ],
    }

    logger.info("[OUTPUT EXPECTED] Destination: Apache Atlas API endpoint %s/entities", ATLAS_API_URL)
    try:
        response = requests.post(
            f"{ATLAS_API_URL}/entities",
            auth=(ATLAS_USERNAME, ATLAS_PASSWORD),
            headers=HEADERS,
            data=json.dumps(payload),
            timeout=10,
        )
        if response.status_code in (200, 201):
            logger.info("[OUTPUT RECEIVED] Lineage registered successfully: '%s' -> '%s'", source_name, target_name)
            return True
        logger.error(
            "Lineage registration failed: %d - %s",
            response.status_code,
            response.text,
        )
        return False
    except requests.RequestException as e:
        logger.error("Lineage registration error: %s", str(e))
        return False


if __name__ == "__main__":
    register_dataset_lineage(
        "mysql.orders",
        "minio.raw-data.orders",
        {"job": "batch_ingestion_dag", "transformation": "cleaning, enrichment"},
    )
