# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

from pathlib import Path

from coreason_etl_faers.utils.logger import logger


def test_logger_initialization() -> None:
    """Test that the logger is initialized correctly and creates the log directory."""
    # Since the logger is initialized on import, we check side effects

    # Check if logs directory creation is handled
    # Note: running this test might actually create the directory in the test environment
    # if it doesn't exist.

    log_path = Path("logs")
    assert log_path.exists()
    assert log_path.is_dir()

    # Verify app.log creation if it was logged to (it might be empty or not created until log)
    # logger.info("Test log")
    # assert (log_path / "app.log").exists()


def test_logger_exports() -> None:
    """Test that logger is exported."""
    assert logger is not None


def test_get_robust_session_configuration() -> None:
    """Test that the robust session is properly configured with retries and adapters."""
    import requests

    from coreason_etl_faers.utils.http import get_robust_session

    session = get_robust_session(retries=5, backoff_factor=0.5, status_forcelist=(500, 502))

    assert isinstance(session, requests.Session)

    # Verify adapter mounts
    assert "http://" in session.adapters
    assert "https://" in session.adapters

    # Verify HTTPAdapter properties
    from requests.adapters import HTTPAdapter

    adapter = session.adapters["https://"]
    assert isinstance(adapter, HTTPAdapter)
    assert adapter.max_retries.total == 5
    assert adapter.max_retries.backoff_factor == 0.5
    assert list(adapter.max_retries.status_forcelist) == [500, 502]
