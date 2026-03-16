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


def normalize_meddra_pts(df: pl.DataFrame) -> pl.DataFrame:
    """
    AGENT INSTRUCTION: This function implements MedDRA normalization.
    It strips whitespace and converts the 'pt' column to uppercase.

    Applies deterministic string normalization to the 'pt' (MedDRA Preferred Term)
    column.

    Args:
        df: A Polars DataFrame containing at least a 'pt' column of type String.

    Returns:
        A new Polars DataFrame containing a 'normalized_pt' column.
    """
    return df.with_columns(pl.col("pt").str.strip_chars().str.to_uppercase().alias("normalized_pt"))
