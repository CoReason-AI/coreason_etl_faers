# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import polars as pl
import pytest
from pytest_mock import MockerFixture

from coreason_etl_faers.silver_deduplication import extract_deduplicated_cases_task


def test_extract_deduplicated_cases_task_success(mocker: MockerFixture) -> None:
    """Test standard pushdown execution successfully extracts mock Polars DataFrame."""
    connection_uri = "postgresql://user:pass@localhost:5432/db"
    source_table = "faers_bronze"
    source_schema = "test_schema"

    # The expected df returned from the database
    expected_df = pl.DataFrame(
        [
            {"data": '{"caseid": "1", "primaryid": "100"}', "rn": 1},
            {"data": '{"caseid": "2", "primaryid": "200"}', "rn": 1},
        ]
    )

    mock_read_db = mocker.patch("polars.read_database_uri", return_value=expected_df)

    result_df = extract_deduplicated_cases_task(connection_uri, source_table, source_schema)

    # Validate output
    assert len(result_df) == 2
    assert "data" in result_df.columns
    assert "rn" not in result_df.columns

    # Validate mock parameters
    mock_read_db.assert_called_once()
    called_query = mock_read_db.call_args[0][0]
    called_conn = mock_read_db.call_args[1]["uri"]

    assert called_conn == connection_uri
    assert "ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY CAST(primaryid AS NUMERIC) DESC)" in called_query
    assert f"{source_schema}.{source_table}" in called_query


def test_extract_deduplicated_cases_task_empty(mocker: MockerFixture) -> None:
    """Test standard pushdown execution successfully extracts an empty Polars DataFrame."""
    connection_uri = "postgresql://user:pass@localhost:5432/db"
    source_table = "empty_table"

    expected_df = pl.DataFrame({"data": []}, schema={"data": pl.String})
    mock_read_db = mocker.patch("polars.read_database_uri", return_value=expected_df)

    result_df = extract_deduplicated_cases_task(connection_uri, source_table)

    assert len(result_df) == 0
    assert result_df.equals(expected_df)
    mock_read_db.assert_called_once()


def test_extract_deduplicated_cases_task_connection_error(mocker: MockerFixture) -> None:
    """Test behavior when the database connection fails."""
    connection_uri = "postgresql://user:pass@invalid:5432/db"
    source_table = "faers_bronze"

    mocker.patch("polars.read_database_uri", side_effect=Exception("Connection refused"))

    with pytest.raises(Exception, match="Connection refused"):
        extract_deduplicated_cases_task(connection_uri, source_table)


def test_extract_deduplicated_cases_task_invalid_table() -> None:
    """Test defensive validation blocks invalid table names to prevent SQL injection."""
    connection_uri = "postgresql://user:pass@localhost:5432/db"

    invalid_tables = [
        "faers_bronze; DROP TABLE users;",
        "faers-bronze",
        "faers bronze",
        "faers_bronze'",
        "123_test_table_#",
    ]

    for invalid_table in invalid_tables:
        with pytest.raises(ValueError, match="Must contain only alphanumeric characters and underscores"):
            extract_deduplicated_cases_task(connection_uri, invalid_table)
