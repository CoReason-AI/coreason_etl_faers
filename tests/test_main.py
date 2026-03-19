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

from coreason_etl_faers.config import FaersExtractionPolicy
from coreason_etl_faers.main import execute_faers_etl_transmutation_task
from coreason_etl_faers.orchestrator_gold import GoldManifoldManifest
from coreason_etl_faers.orchestrator_silver import SilverManifoldManifest


@pytest.fixture
def mock_policy() -> FaersExtractionPolicy:
    """Fixture to provide a valid FaersExtractionPolicy."""
    return FaersExtractionPolicy(
        source_quarter="2023q4",
        base_url="https://example.com/faers",
    )


@pytest.fixture
def mock_silver_manifest() -> SilverManifoldManifest:
    """Fixture to provide a mocked SilverManifoldManifest."""
    return SilverManifoldManifest(
        demo_df=pl.DataFrame({"caseid": ["1", "2"]}),
        drug_df=pl.DataFrame({"caseid": ["1", "2"], "coreason_id": ["id1", "id2"]}),
        reac_df=pl.DataFrame({"caseid": ["1", "2"], "normalized_pt": ["reac1", "reac2"]}),
    )


@pytest.fixture
def mock_gold_manifest() -> GoldManifoldManifest:
    """Fixture to provide a mocked GoldManifoldManifest."""
    return GoldManifoldManifest(
        fact_adverse_event_df=pl.DataFrame({"caseid": ["1", "2"]}),
        bridge_case_drug_df=pl.DataFrame({"caseid": ["1", "2"], "coreason_id": ["id1", "id2"]}),
        bridge_case_reaction_df=pl.DataFrame({"caseid": ["1", "2"], "normalized_pt": ["reac1", "reac2"]}),
    )


def test_execute_faers_etl_transmutation_task_success(
    mocker: MockerFixture,
    mock_policy: FaersExtractionPolicy,
    mock_silver_manifest: SilverManifoldManifest,
    mock_gold_manifest: GoldManifoldManifest,
) -> None:
    """Test successful execution of the full ETL pipeline."""
    connection_uri = "postgresql://user:pass@localhost:5432/db"

    mock_bronze = mocker.patch("coreason_etl_faers.main.execute_bronze_extraction_task")
    mock_silver = mocker.patch(
        "coreason_etl_faers.main.execute_silver_transmutation_task", return_value=mock_silver_manifest
    )
    mock_gold = mocker.patch("coreason_etl_faers.main.execute_gold_transmutation_task", return_value=mock_gold_manifest)

    result = execute_faers_etl_transmutation_task(mock_policy, connection_uri)

    mock_bronze.assert_called_once_with(mock_policy)
    mock_silver.assert_called_once_with(connection_uri)
    mock_gold.assert_called_once_with(mock_silver_manifest, connection_uri)

    assert result == mock_gold_manifest


def test_execute_faers_etl_transmutation_task_bronze_failure(
    mocker: MockerFixture,
    mock_policy: FaersExtractionPolicy,
) -> None:
    """Test failure during the Bronze extraction phase."""
    connection_uri = "postgresql://user:pass@localhost:5432/db"

    mock_bronze = mocker.patch(
        "coreason_etl_faers.main.execute_bronze_extraction_task", side_effect=Exception("Bronze error")
    )
    mock_silver = mocker.patch("coreason_etl_faers.main.execute_silver_transmutation_task")
    mock_gold = mocker.patch("coreason_etl_faers.main.execute_gold_transmutation_task")

    with pytest.raises(Exception, match="Bronze error"):
        execute_faers_etl_transmutation_task(mock_policy, connection_uri)

    mock_bronze.assert_called_once_with(mock_policy)
    mock_silver.assert_not_called()
    mock_gold.assert_not_called()


def test_execute_faers_etl_transmutation_task_silver_failure(
    mocker: MockerFixture,
    mock_policy: FaersExtractionPolicy,
) -> None:
    """Test failure during the Silver transmutation phase."""
    connection_uri = "postgresql://user:pass@localhost:5432/db"

    mock_bronze = mocker.patch("coreason_etl_faers.main.execute_bronze_extraction_task")
    mock_silver = mocker.patch(
        "coreason_etl_faers.main.execute_silver_transmutation_task", side_effect=Exception("Silver error")
    )
    mock_gold = mocker.patch("coreason_etl_faers.main.execute_gold_transmutation_task")

    with pytest.raises(Exception, match="Silver error"):
        execute_faers_etl_transmutation_task(mock_policy, connection_uri)

    mock_bronze.assert_called_once_with(mock_policy)
    mock_silver.assert_called_once_with(connection_uri)
    mock_gold.assert_not_called()


def test_execute_faers_etl_transmutation_task_gold_failure(
    mocker: MockerFixture,
    mock_policy: FaersExtractionPolicy,
    mock_silver_manifest: SilverManifoldManifest,
) -> None:
    """Test failure during the Gold transmutation phase."""
    connection_uri = "postgresql://user:pass@localhost:5432/db"

    mock_bronze = mocker.patch("coreason_etl_faers.main.execute_bronze_extraction_task")
    mock_silver = mocker.patch(
        "coreason_etl_faers.main.execute_silver_transmutation_task", return_value=mock_silver_manifest
    )
    mock_gold = mocker.patch(
        "coreason_etl_faers.main.execute_gold_transmutation_task", side_effect=Exception("Gold error")
    )

    with pytest.raises(Exception, match="Gold error"):
        execute_faers_etl_transmutation_task(mock_policy, connection_uri)

    mock_bronze.assert_called_once_with(mock_policy)
    mock_silver.assert_called_once_with(connection_uri)
    mock_gold.assert_called_once_with(mock_silver_manifest, connection_uri)
