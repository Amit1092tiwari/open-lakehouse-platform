"""Unit tests for ML tracking and Data Governance (Atlas Lineage)."""

import os
import sys
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest

ROOT_DIR = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT_DIR, "governance"))
sys.path.insert(0, os.path.join(ROOT_DIR, "ml"))

from atlas_stub import check_dataset_exists, register_dataset_lineage


# ==============================================================================
# Atlas Governance Lineage Tests
# ==============================================================================

@patch("requests.get")
def test_atlas_check_dataset_exists(mock_get):
    """Verify dataset existence verification via Atlas API."""
    # When dataset exists
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"entities": [{"guid": "123", "typeName": "Dataset"}]}
    assert check_dataset_exists("orders_table") is True

    # When dataset does not exist
    mock_get.return_value.json.return_value = {"entities": []}
    assert check_dataset_exists("unknown_table") is False

    # When API returns 500 error
    mock_get.return_value.status_code = 500
    mock_get.return_value.text = "Internal Server Error"
    assert check_dataset_exists("error_table") is False


@patch("requests.post")
@patch("atlas_stub.check_dataset_exists")
def test_atlas_register_lineage(mock_check, mock_post):
    """Verify lineage registration request payload and handling."""
    # Case 1: source or target does not exist
    mock_check.side_effect = lambda name: name == "source_ds"
    assert register_dataset_lineage("source_ds", "missing_target") is False
    mock_post.assert_not_called()

    # Case 2: both exist and registration succeeds
    mock_check.side_effect = lambda name: True
    mock_post.return_value.status_code = 200
    res = register_dataset_lineage("source_ds", "target_ds", {"pipeline": "batch_etl"})
    assert res is True
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    assert "data" in kwargs
    assert "source_ds" in kwargs["data"]
    assert "target_ds" in kwargs["data"]


# ==============================================================================
# ML Model Tracking Tests
# ==============================================================================

def test_ml_feature_store_and_training():
    """Verify ML training logic and metrics calculation."""
    pytest.importorskip("mlflow")
    pytest.importorskip("sklearn")
    from mlflow_tracking import train_and_log_model

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.set_experiment"), \
         patch("mlflow.start_run"), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.sklearn.log_model"):

        features = pd.DataFrame({
            "avg_reading": [30.0, 45.0, 60.0, 75.0, 90.0] * 10,
            "max_reading": [50.0, 65.0, 80.0, 95.0, 110.0] * 10,
        })
        model, mse, r2 = train_and_log_model(features)

        assert model is not None
        assert mse >= 0.0
        assert r2 <= 1.0

