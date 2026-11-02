"""Unit tests for Market Telemetry Ingestion Agent (Nifty NSE Market Ingestion)."""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

AGENTS_DIR = os.path.join(os.path.dirname(__file__), "..", "agents")
sys.path.insert(0, AGENTS_DIR)

from nifty50_agent import NiftyNSEAgent


def test_nifty_agent_parse_data_success():
    """Verify NiftyNSEAgent parses standard NSE index payload."""
    agent = NiftyNSEAgent()
    mock_payload = {
        "data": [
            {
                "index": "NIFTY 50",
                "last": "24,500.50",
                "net": "+150.25",
                "percnt": "+0.62%",
                "high": "24,550.00",
                "low": "24,380.10",
                "open": "24,400.00",
                "close": "24,350.25",
            }
        ]
    }
    parsed = agent.parse_nifty_data(mock_payload)

    assert parsed is not None
    assert parsed["index_name"] == "NIFTY 50"
    assert parsed["current_value"] == "24,500.50"
    assert parsed["change"] == "+150.25"
    assert parsed["high"] == "24,550.00"
    assert "timestamp" in parsed


def test_nifty_agent_parse_data_empty():
    """Verify parser returns None on empty or malformed API payload."""
    agent = NiftyNSEAgent()
    assert agent.parse_nifty_data(None) is None
    assert agent.parse_nifty_data({}) is None
    assert agent.parse_nifty_data({"data": []}) is None


def test_nifty_agent_save_to_csv():
    """Verify saving parsed market ticks into temporary CSV file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test_nifty.csv"
        agent = NiftyNSEAgent()
        agent.csv_file = csv_path
        agent.file_exists = False

        data = {
            "timestamp": "2026-11-02 10:00:00",
            "index_name": "NIFTY 50",
            "current_value": "24,500.00",
            "change": "+100.00",
            "change_percent": "+0.41%",
            "high": "24,550.00",
            "low": "24,450.00",
            "open": "24,450.00",
            "close": "24,400.00",
        }

        success = agent.save_to_csv(data)
        assert success is True
        assert csv_path.exists()

        content = csv_path.read_text(encoding="utf-8-sig")
        assert "NIFTY 50" in content
        assert "24,500.00" in content


@patch("urllib.request.urlopen")
def test_nifty_agent_run_once_mocked(mock_urlopen):
    """Verify end-to-end execution flow of run_once with mocked network response."""
    mock_response = MagicMock()
    mock_response.read.return_value = b'{"data": [{"index": "NIFTY 50", "last": "24500", "net": "100", "percnt": "0.4", "high": "24600", "low": "24400", "open": "24450", "close": "24400"}]}'
    mock_urlopen.return_value.__enter__.return_value = mock_response

    with tempfile.TemporaryDirectory() as tmpdir:
        agent = NiftyNSEAgent()
        agent.csv_file = Path(tmpdir) / "test_ticks.csv"
        agent.file_exists = False

        result = agent.run_once()
        assert result is True
        assert agent.csv_file.exists()

