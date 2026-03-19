# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import json
from unittest.mock import MagicMock

import polars as pl

from coreason_etl_faers.orchestrator_silver import execute_silver_transmutation_task


def test_execute_silver_transmutation_task_happy_path(mocker: MagicMock) -> None:
    """Test standard Silver layer orchestration."""
    mock_extract = mocker.patch("coreason_etl_faers.orchestrator_silver.extract_deduplicated_cases_task")

    # Mock the return values of the extract function for demo, drug, reac
    # Demo dataframe needs a JSON string containing caseid, patient_id, event_dt
    demo_df = pl.DataFrame({"data": [json.dumps({"caseid": "1", "patient_id": "P1", "event_dt": "2023-01-01"})]})

    # Drug dataframe needs a JSON string containing caseid, drugname, role_cod
    drug_df = pl.DataFrame({"data": [json.dumps({"caseid": "1", "drugname": "Aspirin", "role_cod": "PS"})]})

    # Reac dataframe needs a JSON string containing caseid, pt
    reac_df = pl.DataFrame({"data": [json.dumps({"caseid": "1", "pt": "Headache"})]})

    mock_extract.side_effect = [demo_df, drug_df, reac_df]

    mock_generate_ids = mocker.patch("coreason_etl_faers.orchestrator_silver.generate_coreason_ids")
    mock_generate_ids.return_value = pl.DataFrame(
        {"caseid": ["1"], "drugname": ["Aspirin"], "coreason_id": ["id1"], "role_cod": ["PS"]}
    )

    mock_normalize_pts = mocker.patch("coreason_etl_faers.orchestrator_silver.normalize_meddra_pts")
    mock_normalize_pts.return_value = pl.DataFrame({"caseid": ["1"], "pt": ["Headache"], "normalized_pt": ["HEADACHE"]})

    manifest = execute_silver_transmutation_task("postgresql://fake_uri")

    assert mock_extract.call_count == 3
    mock_extract.assert_any_call("postgresql://fake_uri", "faers_bronze_demo")
    mock_extract.assert_any_call("postgresql://fake_uri", "faers_bronze_drug")
    mock_extract.assert_any_call("postgresql://fake_uri", "faers_bronze_reac")

    assert "caseid" in manifest.demo_df.columns
    assert "patient_id" in manifest.demo_df.columns
    assert "event_dt" in manifest.demo_df.columns
    assert manifest.demo_df["caseid"][0] == "1"
    assert manifest.demo_df["patient_id"][0] == "P1"
    assert manifest.demo_df["event_dt"][0] == "2023-01-01"

    assert manifest.drug_df is mock_generate_ids.return_value
    assert manifest.reac_df is mock_normalize_pts.return_value


def test_execute_silver_transmutation_task_complex_malformed_json(mocker: MagicMock) -> None:
    """Test Silver layer orchestration resilience against structurally malformed JSON payloads
    that might occur in raw Bronze layer data."""
    mock_extract = mocker.patch("coreason_etl_faers.orchestrator_silver.extract_deduplicated_cases_task")

    # Demo dataframe with partially missing fields and completely unparsable structure
    demo_df = pl.DataFrame(
        {
            "data": [
                json.dumps({"caseid": "1", "event_dt": "2023-01-01"}),  # Missing patient_id
                json.dumps({"patient_id": "P2"}),  # Missing caseid and event_dt
                "{malformed_json: true, caseid:",  # Unparsable JSON
                json.dumps(
                    {"caseid": "3", "patient_id": "P3", "event_dt": "2023-01-03", "extra_noise": {"nested": "value"}}
                ),  # Extra unexpected nested noise
            ]
        }
    )

    # Drug dataframe with similar issues
    drug_df = pl.DataFrame(
        {
            "data": [
                json.dumps({"caseid": "1", "drugname": "Aspirin"}),  # Missing role_cod
                "{bad json",
                json.dumps({"caseid": "3", "role_cod": "PS"}),  # Missing drugname
            ]
        }
    )

    # Reac dataframe with similar issues
    reac_df = pl.DataFrame(
        {
            "data": [
                json.dumps({"caseid": "1"}),  # Missing pt
                json.dumps({"pt": "Headache"}),  # Missing caseid
                "null",
            ]
        }
    )

    mock_extract.side_effect = [demo_df, drug_df, reac_df]

    # Use the real generate_ids and normalize_pts functions to test full integration resilience
    # No patching of these internal functions.

    manifest = execute_silver_transmutation_task("postgresql://fake_uri")

    assert mock_extract.call_count == 3

    # Check Demo DF
    assert "caseid" in manifest.demo_df.columns
    assert "patient_id" in manifest.demo_df.columns
    assert "event_dt" in manifest.demo_df.columns

    # Check Drug DF
    assert "caseid" in manifest.drug_df.columns
    assert "drugname" in manifest.drug_df.columns
    assert "role_cod" in manifest.drug_df.columns
    assert "coreason_id" in manifest.drug_df.columns
    assert "normalized_drugname" in manifest.drug_df.columns

    # Check Reac DF
    assert "caseid" in manifest.reac_df.columns
    assert "pt" in manifest.reac_df.columns
    assert "normalized_pt" in manifest.reac_df.columns


def test_execute_silver_transmutation_task_empty_dfs(mocker: MagicMock) -> None:
    """Test Silver layer orchestration when dataframes are empty."""
    mock_extract = mocker.patch("coreason_etl_faers.orchestrator_silver.extract_deduplicated_cases_task")

    empty_df = pl.DataFrame({"data": []}, schema={"data": pl.String})
    mock_extract.return_value = empty_df

    mocker.patch("coreason_etl_faers.orchestrator_silver.generate_coreason_ids", return_value=empty_df)
    mocker.patch("coreason_etl_faers.orchestrator_silver.normalize_meddra_pts", return_value=empty_df)

    manifest = execute_silver_transmutation_task("postgresql://fake_uri")

    assert mock_extract.call_count == 3
    # Check if fallback columns are correctly added
    assert "caseid" in manifest.demo_df.columns
    assert "patient_id" in manifest.demo_df.columns
    assert "event_dt" in manifest.demo_df.columns
    assert manifest.drug_df is empty_df
    assert manifest.reac_df is empty_df


def test_execute_silver_transmutation_task_missing_columns(mocker: MagicMock) -> None:
    """Test handling of missing drugname and pt columns."""
    mock_extract = mocker.patch("coreason_etl_faers.orchestrator_silver.extract_deduplicated_cases_task")

    demo_df = pl.DataFrame({"data": [json.dumps({"caseid": "1"})]})
    drug_df = pl.DataFrame({"data": [json.dumps({"caseid": "1"})]})
    reac_df = pl.DataFrame({"data": [json.dumps({"caseid": "1"})]})

    mock_extract.side_effect = [demo_df, drug_df, reac_df]

    def check_drug_df(df: pl.DataFrame) -> pl.DataFrame:
        assert "drugname" in df.columns
        return df

    def check_reac_df(df: pl.DataFrame) -> pl.DataFrame:
        assert "pt" in df.columns
        return df

    mock_generate_ids = mocker.patch(
        "coreason_etl_faers.orchestrator_silver.generate_coreason_ids", side_effect=check_drug_df
    )
    mock_normalize_pts = mocker.patch(
        "coreason_etl_faers.orchestrator_silver.normalize_meddra_pts", side_effect=check_reac_df
    )

    execute_silver_transmutation_task("postgresql://fake_uri")

    mock_generate_ids.assert_called_once()
    mock_normalize_pts.assert_called_once()
