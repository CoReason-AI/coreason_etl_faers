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


def _enforce_schema(df: pl.DataFrame, expected_cols: list[str]) -> pl.DataFrame:
    """
    Private helper function to enforce a strict tabular schema.
    It guarantees all expected columns exist (filling missing ones with nulls)
    and trims away any extra columns.
    """
    if len(df) == 0:
        return pl.DataFrame(schema=dict.fromkeys(expected_cols, pl.String))

    missing_cols = [pl.lit(None).alias(col) for col in expected_cols if col not in df.columns]
    if missing_cols:
        df = df.with_columns(missing_cols)

    return df.select(expected_cols)


def build_fact_adverse_event_manifold(df: pl.DataFrame) -> pl.DataFrame:
    """
    AGENT INSTRUCTION: This function strictly adheres to the Layer 3 Gold (The Product) architecture for fact tables.
    It builds the granular fact table linking CASEID to patient_id and dates.
    All logic is verified and idempotent.

    Transforms the deduplicated Silver layer DataFrame into the Gold layer fact_adverse_event state.

    Args:
        df: A Polars DataFrame representing the deduplicated Silver case state.
            Must contain columns 'caseid', 'patient_id', 'event_dt'.

    Returns:
        A Polars DataFrame representing the fact_adverse_event schema.
    """
    return _enforce_schema(df, ["caseid", "patient_id", "event_dt"])


def build_bridge_case_drug_manifold(df: pl.DataFrame) -> pl.DataFrame:
    """
    AGENT INSTRUCTION: This function strictly adheres to the Layer 3 Gold (The Product)
    architecture for bridging tables.
    It links CASEID to coreason_id (Drug) while preserving role_cod.

    Transforms the Silver layer DataFrame into the Gold layer bridge_case_drug state.

    Args:
        df: A Polars DataFrame representing the Silver case-drug state.
            Must contain columns 'caseid', 'coreason_id', 'role_cod'.

    Returns:
        A Polars DataFrame representing the bridge_case_drug schema.
    """
    return _enforce_schema(df, ["caseid", "coreason_id", "role_cod"])


def build_bridge_case_reaction_manifold(df: pl.DataFrame) -> pl.DataFrame:
    """
    AGENT INSTRUCTION: This function strictly adheres to the Layer 3 Gold (The Product)
    architecture for bridging tables.
    It links CASEID to the MedDRA Preferred Term (normalized_pt).

    Transforms the Silver layer DataFrame into the Gold layer bridge_case_reaction state.

    Args:
        df: A Polars DataFrame representing the Silver case-reaction state.
            Must contain columns 'caseid', 'normalized_pt'.

    Returns:
        A Polars DataFrame representing the bridge_case_reaction schema.
    """
    return _enforce_schema(df, ["caseid", "normalized_pt"])
