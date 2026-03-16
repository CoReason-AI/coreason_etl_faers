# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import pytest
from pytest_mock import MockerFixture

from coreason_etl_faers.config import FaersExtractionPolicy
from coreason_etl_faers.orchestrator_bronze import execute_bronze_extraction_task


def test_execute_bronze_extraction_task_success(mocker: MockerFixture) -> None:
    """Tests the successful execution of the Bronze extraction orchestration."""
    mock_resolve_url = mocker.patch("coreason_etl_faers.orchestrator_bronze.resolve_faers_url")
    mock_resolve_url.return_value = "https://example.com/resolved.zip"

    mock_stream_data = mocker.patch("coreason_etl_faers.orchestrator_bronze.stream_faers_data")
    # Return an empty generator to simulate streaming
    mock_stream_data.return_value = (x for x in [])  # type: ignore

    mock_load_bronze = mocker.patch("coreason_etl_faers.orchestrator_bronze.load_faers_to_bronze")

    policy = FaersExtractionPolicy(source_quarter="2023q4")

    execute_bronze_extraction_task(policy)

    mock_resolve_url.assert_called_once_with(
        "2023q4", "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
    )

    assert mock_stream_data.call_count == 3

    # Extract the args it was called with
    calls = mock_stream_data.call_args_list
    assert calls[0][0] == ("https://example.com/resolved.zip", "DEMO23Q4.txt")
    assert calls[1][0] == ("https://example.com/resolved.zip", "DRUG23Q4.txt")
    assert calls[2][0] == ("https://example.com/resolved.zip", "REAC23Q4.txt")

    assert mock_load_bronze.call_count == 3
    load_calls = mock_load_bronze.call_args_list
    assert load_calls[0][0][1] == "faers_bronze_demo"
    assert load_calls[1][0][1] == "faers_bronze_drug"
    assert load_calls[2][0][1] == "faers_bronze_reac"


def test_execute_bronze_extraction_task_error(mocker: MockerFixture) -> None:
    """Tests that errors in URL resolution propagate and halt the pipeline."""
    mock_resolve_url = mocker.patch("coreason_etl_faers.orchestrator_bronze.resolve_faers_url")
    mock_resolve_url.side_effect = Exception("Resolution failed")

    mock_stream_data = mocker.patch("coreason_etl_faers.orchestrator_bronze.stream_faers_data")
    mock_load_bronze = mocker.patch("coreason_etl_faers.orchestrator_bronze.load_faers_to_bronze")

    policy = FaersExtractionPolicy(source_quarter="2022q1", base_url="file://local/test.html")

    with pytest.raises(Exception, match="Resolution failed"):
        execute_bronze_extraction_task(policy)

    mock_resolve_url.assert_called_once_with("2022q1", "file://local/test.html")
    mock_stream_data.assert_not_called()
    mock_load_bronze.assert_not_called()
