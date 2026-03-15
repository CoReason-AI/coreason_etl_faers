# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

from collections.abc import Iterator
from typing import Any

from pytest_mock import MockerFixture

from coreason_etl_faers.bronze_ingestion import load_faers_to_bronze


def dummy_stream_generator() -> Iterator[dict[str, Any]]:
    yield {"caseid": "123", "drugname": "ASPIRIN"}
    yield {"caseid": "456", "drugname": "TYLENOL"}


def test_load_faers_to_bronze(mocker: MockerFixture) -> None:
    """
    Test that the dlt pipeline is configured and executed correctly with
    the required constraints (max_table_nesting=0) without hitting a live DB.
    """
    # Mock the pipeline object returned by dlt.pipeline
    mock_pipeline = mocker.MagicMock()

    # When dlt.pipeline is called, return our mock pipeline
    mock_dlt_pipeline = mocker.patch("dlt.pipeline", return_value=mock_pipeline)

    # Mock dlt.resource to verify its arguments
    mock_resource_obj = mocker.MagicMock()
    mock_resource = mocker.patch("dlt.resource", return_value=mock_resource_obj)

    table_name = "raw_demo"
    generator = dummy_stream_generator()

    # Execute the bronze ingestion function
    load_faers_to_bronze(generator, table_name)

    # Verify pipeline configuration
    mock_dlt_pipeline.assert_called_once_with(
        pipeline_name="faers_bronze_pipeline",
        destination="postgres",
        dataset_name="faers_bronze",
    )

    # Verify that resource was created with max_table_nesting=0
    mock_resource.assert_called_once_with(generator, name=table_name, max_table_nesting=0)

    # Verify pipeline execution
    mock_pipeline.run.assert_called_once_with(mock_resource_obj)
