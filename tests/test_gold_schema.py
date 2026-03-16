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

from coreason_etl_faers.gold_schema import (
    build_bridge_case_drug_manifold,
    build_bridge_case_reaction_manifold,
    build_fact_adverse_event_manifold,
)


def test_build_fact_adverse_event_manifold_happy_path() -> None:
    # Arrange
    df = pl.DataFrame(
        {
            "caseid": ["123", "456", "789"],
            "patient_id": ["p1", "p2", "p3"],
            "event_dt": ["2023-01-01", "2023-02-01", "2023-03-01"],
            "extra_col": ["a", "b", "c"],
        }
    )

    # Act
    result = build_fact_adverse_event_manifold(df)

    # Assert
    assert result.columns == ["caseid", "patient_id", "event_dt"]
    assert result["caseid"].to_list() == ["123", "456", "789"]
    assert result["patient_id"].to_list() == ["p1", "p2", "p3"]
    assert result["event_dt"].to_list() == ["2023-01-01", "2023-02-01", "2023-03-01"]


def test_build_fact_adverse_event_manifold_missing_columns() -> None:
    # Arrange
    df = pl.DataFrame(
        {
            "caseid": ["123", "456", "789"]
            # Missing patient_id and event_dt
        }
    )

    # Act
    result = build_fact_adverse_event_manifold(df)

    # Assert
    assert result.columns == ["caseid", "patient_id", "event_dt"]
    assert result["caseid"].to_list() == ["123", "456", "789"]
    assert result["patient_id"].to_list() == [None, None, None]
    assert result["event_dt"].to_list() == [None, None, None]


def test_build_fact_adverse_event_manifold_empty_dataframe() -> None:
    # Arrange
    df = pl.DataFrame({"caseid": [], "patient_id": [], "event_dt": []})

    # Act
    result = build_fact_adverse_event_manifold(df)

    # Assert
    assert result.columns == ["caseid", "patient_id", "event_dt"]
    assert len(result) == 0


def test_build_bridge_case_drug_manifold_happy_path() -> None:
    # Arrange
    df = pl.DataFrame(
        {
            "caseid": ["123", "456", "789"],
            "coreason_id": ["c1", "c2", "c3"],
            "role_cod": ["r1", "r2", "r3"],
            "extra_col": ["a", "b", "c"],
        }
    )

    # Act
    result = build_bridge_case_drug_manifold(df)

    # Assert
    assert result.columns == ["caseid", "coreason_id", "role_cod"]
    assert result["caseid"].to_list() == ["123", "456", "789"]
    assert result["coreason_id"].to_list() == ["c1", "c2", "c3"]
    assert result["role_cod"].to_list() == ["r1", "r2", "r3"]


def test_build_bridge_case_drug_manifold_missing_columns() -> None:
    # Arrange
    df = pl.DataFrame(
        {
            "caseid": ["123", "456", "789"]
            # Missing coreason_id and role_cod
        }
    )

    # Act
    result = build_bridge_case_drug_manifold(df)

    # Assert
    assert result.columns == ["caseid", "coreason_id", "role_cod"]
    assert result["caseid"].to_list() == ["123", "456", "789"]
    assert result["coreason_id"].to_list() == [None, None, None]
    assert result["role_cod"].to_list() == [None, None, None]


def test_build_bridge_case_drug_manifold_empty_dataframe() -> None:
    # Arrange
    df = pl.DataFrame({"caseid": [], "coreason_id": [], "role_cod": []})

    # Act
    result = build_bridge_case_drug_manifold(df)

    # Assert
    assert result.columns == ["caseid", "coreason_id", "role_cod"]
    assert len(result) == 0


def test_build_bridge_case_drug_manifold_completely_empty() -> None:
    # Arrange
    df = pl.DataFrame()

    # Act
    result = build_bridge_case_drug_manifold(df)

    # Assert
    assert result.columns == ["caseid", "coreason_id", "role_cod"]
    assert len(result) == 0


def test_build_bridge_case_reaction_manifold_happy_path() -> None:
    # Arrange
    df = pl.DataFrame(
        {
            "caseid": ["123", "456", "789"],
            "normalized_pt": ["HEADACHE", "NAUSEA", "FEVER"],
            "extra_col": ["a", "b", "c"],
        }
    )

    # Act
    result = build_bridge_case_reaction_manifold(df)

    # Assert
    assert result.columns == ["caseid", "normalized_pt"]
    assert result["caseid"].to_list() == ["123", "456", "789"]
    assert result["normalized_pt"].to_list() == ["HEADACHE", "NAUSEA", "FEVER"]


def test_build_bridge_case_reaction_manifold_missing_columns() -> None:
    # Arrange
    df = pl.DataFrame(
        {
            "caseid": ["123", "456", "789"]
            # Missing normalized_pt
        }
    )

    # Act
    result = build_bridge_case_reaction_manifold(df)

    # Assert
    assert result.columns == ["caseid", "normalized_pt"]
    assert result["caseid"].to_list() == ["123", "456", "789"]
    assert result["normalized_pt"].to_list() == [None, None, None]


def test_build_bridge_case_reaction_manifold_empty_dataframe() -> None:
    # Arrange
    df = pl.DataFrame({"caseid": [], "normalized_pt": []})

    # Act
    result = build_bridge_case_reaction_manifold(df)

    # Assert
    assert result.columns == ["caseid", "normalized_pt"]
    assert len(result) == 0


def test_build_bridge_case_reaction_manifold_completely_empty() -> None:
    # Arrange
    df = pl.DataFrame()

    # Act
    result = build_bridge_case_reaction_manifold(df)

    # Assert
    assert result.columns == ["caseid", "normalized_pt"]
    assert len(result) == 0


def test_build_fact_adverse_event_manifold_completely_empty() -> None:
    # Arrange
    df = pl.DataFrame()

    # Act
    result = build_fact_adverse_event_manifold(df)

    # Assert
    assert result.columns == ["caseid", "patient_id", "event_dt"]
    assert len(result) == 0
