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
    # Defensive check: ensure all expected columns exist, fill with nulls if absent
    expected_cols = ["caseid", "patient_id", "event_dt"]

    # Check if empty
    if len(df) == 0:
        return pl.DataFrame(schema={"caseid": pl.String, "patient_id": pl.String, "event_dt": pl.String})

    for col in expected_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # Extract just the relevant columns
    return df.select(["caseid", "patient_id", "event_dt"])


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
    expected_cols = ["caseid", "coreason_id", "role_cod"]

    # Check if empty
    if len(df) == 0:
        return pl.DataFrame(
            schema={
                "caseid": pl.String,
                "coreason_id": pl.String,
                "role_cod": pl.String,
            }
        )

    for col in expected_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # Extract just the relevant columns
    return df.select(expected_cols)


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
    expected_cols = ["caseid", "normalized_pt"]

    # Check if empty
    if len(df) == 0:
        return pl.DataFrame(
            schema={
                "caseid": pl.String,
                "normalized_pt": pl.String,
            }
        )

    for col in expected_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # Extract just the relevant columns
    return df.select(expected_cols)
