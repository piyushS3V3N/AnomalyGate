import pytest
from src.utils.data_generator import generate_log

def test_generate_log_structure():
    """Test that generated logs contain all necessary keys."""
    log = generate_log()
    expected_keys = [
        "timestamp", "source_ip", "event_type", 
        "action", "bytes_transferred", "severity", "is_anomaly"
    ]
    for key in expected_keys:
        assert key in log, f"Missing key: {key}"

def test_anomaly_flag():
    """Test that anomalies are properly flagged."""
    log = generate_log()
    assert log["is_anomaly"] in [0, 1], "is_anomaly must be 0 or 1"
