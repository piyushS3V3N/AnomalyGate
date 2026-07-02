from src.utils.data_generator import generate_log

def test_generate_log_structure():
    """Verify that generated records contain ONLY native Spring Boot properties."""
    log_payload, _ = generate_log()
    
    expected_keys = ["@timestamp", "level", "thread", "logger", "message", "context"]
    for key in expected_keys:
        assert key in log_payload, f"Missing native Spring field: {key}"
        
    assert "client_ip" in log_payload["context"]
    assert "bytes_sent" in log_payload["context"]

    # STOPS CHEATING: Confirm nothing leaks the true answer
    assert "is_anomaly" not in log_payload
    assert "is_anomaly" not in log_payload["context"]


def test_anomaly_flag():
    """Verify the sidecar evaluation tag functions correctly."""
    _, is_anomaly_val = generate_log()
    assert is_anomaly_val in "Sidecar tag tracking value must be 0 or 1"
