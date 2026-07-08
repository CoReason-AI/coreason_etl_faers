# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import sys

from pytest_mock import MockerFixture

from coreason_etl_faers.config import FaersExtractionPolicy
from coreason_etl_faers.main import build_postgres_uri_from_env, main


def test_build_postgres_uri_from_env_defaults(mocker: MockerFixture) -> None:
    """Test that the URI builder correctly uses default values when env vars are missing."""
    mocker.patch.dict("os.environ", {}, clear=True)
    uri = build_postgres_uri_from_env()
    assert uri == "postgresql://postgres:postgres@localhost:5432/coreason"


def test_build_postgres_uri_from_env_custom(mocker: MockerFixture) -> None:
    """Test that the URI builder correctly maps all provided standard environment variables."""
    mocker.patch.dict(
        "os.environ",
        {
            "PGHOST": "db.coreason.ai",
            "PGPORT": "5433",
            "PGUSER": "etl_user",
            "PGPASSWORD": "supersecretpassword",
            "PGDATABASE": "faers_db",
        },
        clear=True,
    )
    uri = build_postgres_uri_from_env()
    assert uri == "postgresql://etl_user:supersecretpassword@db.coreason.ai:5433/faers_db"


def test_main_cli_execution_defaults(mocker: MockerFixture) -> None:
    """Test the main CLI execution with default arguments."""
    # Mock sys.argv to simulate running without arguments
    mocker.patch.object(sys, "argv", ["main.py"])

    # Mock the execution task
    mock_execute = mocker.patch("coreason_etl_faers.main.execute_faers_etl_transmutation_task")

    main()

    # The expected default policy
    expected_policy = FaersExtractionPolicy(
        source_quarter="2023q4", base_url="https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
    )

    mock_execute.assert_called_once()
    args, _kwargs = mock_execute.call_args
    assert args[0] == expected_policy
    assert "postgresql://" in args[1]


def test_main_cli_execution_custom_args(mocker: MockerFixture) -> None:
    """Test the main CLI execution with custom arguments."""
    # Mock sys.argv to simulate running with specific arguments
    test_url = "file:///tmp/fda.html"
    mocker.patch.object(sys, "argv", ["main.py", "--quarter", "2024q1", "--url", test_url])

    # Mock the execution task
    mock_execute = mocker.patch("coreason_etl_faers.main.execute_faers_etl_transmutation_task")

    main()

    expected_policy = FaersExtractionPolicy(source_quarter="2024q1", base_url=test_url)

    mock_execute.assert_called_once()
    args, _kwargs = mock_execute.call_args
    assert args[0] == expected_policy
    assert "postgresql://" in args[1]
