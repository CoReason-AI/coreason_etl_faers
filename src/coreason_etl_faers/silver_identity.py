# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import uuid

import polars as pl

# Define a specific namespace for FAERS Drugs
NAMESPACE_FAERS_DRUG = uuid.UUID("7c5d4b4a-1c8a-4b9f-8a2e-3d1f5c6b9a8d")


def generate_coreason_ids(df: pl.DataFrame) -> pl.DataFrame:
    """
    AGENT INSTRUCTION: This function implements the Polars UUID5 Generation Pattern exactly as specified.
    Do not alter the UUID generation logic, UUID namespace, or column aliases.

    Applies deterministic UUID5 hashing to the 'drugname' column to generate idempotent 'coreason_id's.
    The string is first stripped of whitespace and converted to uppercase.

    Args:
        df: A Polars DataFrame containing at least a 'drugname' column of type String.

    Returns:
        A new Polars DataFrame containing 'normalized_drugname' and 'coreason_id' columns.
    """
    # 1. Normalize the drug name
    df = df.with_columns(pl.col("drugname").str.strip_chars().str.to_uppercase().alias("normalized_drugname"))

    # 2. Shift-Left UUID5 Generation via map_batches
    return df.with_columns(
        pl.col("normalized_drugname")
        .map_batches(
            lambda s: pl.Series([str(uuid.uuid5(NAMESPACE_FAERS_DRUG, str(x))) if x else None for x in s]),
            return_dtype=pl.String,
        )
        .alias("coreason_id")
    )
