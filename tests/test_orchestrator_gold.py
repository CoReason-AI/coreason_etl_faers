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

from coreason_etl_faers.orchestrator_gold import execute_gold_transmutation_task
from coreason_etl_faers.orchestrator_silver import SilverManifoldManifest


def test_execute_gold_transmutation_task_happy_path() -> None:
    # Arrange
    silver_manifest = SilverManifoldManifest(
        demo_df=pl.DataFrame(
            {
                "caseid": ["123"],
                "patient_id": ["p1"],
                "event_dt": ["2023-01-01"],
            }
        ),
        drug_df=pl.DataFrame(
            {
                "caseid": ["123"],
                "coreason_id": ["c1"],
                "role_cod": ["r1"],
            }
        ),
        reac_df=pl.DataFrame(
            {
                "caseid": ["123"],
                "normalized_pt": ["HEADACHE"],
            }
        ),
    )

    # Act
    gold_manifest = execute_gold_transmutation_task(silver_manifest)

    # Assert
    assert gold_manifest.fact_adverse_event_df.columns == ["caseid", "patient_id", "event_dt"]
    assert len(gold_manifest.fact_adverse_event_df) == 1
    assert gold_manifest.fact_adverse_event_df["caseid"].to_list() == ["123"]

    assert gold_manifest.bridge_case_drug_df.columns == ["caseid", "coreason_id", "role_cod"]
    assert len(gold_manifest.bridge_case_drug_df) == 1
    assert gold_manifest.bridge_case_drug_df["caseid"].to_list() == ["123"]

    assert gold_manifest.bridge_case_reaction_df.columns == ["caseid", "normalized_pt"]
    assert len(gold_manifest.bridge_case_reaction_df) == 1
    assert gold_manifest.bridge_case_reaction_df["caseid"].to_list() == ["123"]


def test_execute_gold_transmutation_task_empty_dfs() -> None:
    # Arrange
    silver_manifest = SilverManifoldManifest(
        demo_df=pl.DataFrame(),
        drug_df=pl.DataFrame(),
        reac_df=pl.DataFrame(),
    )

    # Act
    gold_manifest = execute_gold_transmutation_task(silver_manifest)

    # Assert
    assert gold_manifest.fact_adverse_event_df.columns == ["caseid", "patient_id", "event_dt"]
    assert len(gold_manifest.fact_adverse_event_df) == 0

    assert gold_manifest.bridge_case_drug_df.columns == ["caseid", "coreason_id", "role_cod"]
    assert len(gold_manifest.bridge_case_drug_df) == 0

    assert gold_manifest.bridge_case_reaction_df.columns == ["caseid", "normalized_pt"]
    assert len(gold_manifest.bridge_case_reaction_df) == 0


def test_execute_gold_transmutation_task_missing_columns() -> None:
    # Arrange
    # Missing columns will be populated with null values by the gold_schema functions
    silver_manifest = SilverManifoldManifest(
        demo_df=pl.DataFrame({"caseid": ["123"]}),
        drug_df=pl.DataFrame({"caseid": ["123"]}),
        reac_df=pl.DataFrame({"caseid": ["123"]}),
    )

    # Act
    gold_manifest = execute_gold_transmutation_task(silver_manifest)

    # Assert
    assert gold_manifest.fact_adverse_event_df.columns == ["caseid", "patient_id", "event_dt"]
    assert len(gold_manifest.fact_adverse_event_df) == 1
    assert gold_manifest.fact_adverse_event_df["patient_id"].to_list() == [None]

    assert gold_manifest.bridge_case_drug_df.columns == ["caseid", "coreason_id", "role_cod"]
    assert len(gold_manifest.bridge_case_drug_df) == 1
    assert gold_manifest.bridge_case_drug_df["coreason_id"].to_list() == [None]

    assert gold_manifest.bridge_case_reaction_df.columns == ["caseid", "normalized_pt"]
    assert len(gold_manifest.bridge_case_reaction_df) == 1
    assert gold_manifest.bridge_case_reaction_df["normalized_pt"].to_list() == [None]
