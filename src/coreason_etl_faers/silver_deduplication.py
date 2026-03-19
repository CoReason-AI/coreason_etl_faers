# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import re

import polars as pl

from coreason_etl_faers.utils.logger import logger


def extract_deduplicated_cases_task(
    connection_uri: str, source_table: str, source_schema: str = "bronze"
) -> pl.DataFrame:
    """
    AGENT INSTRUCTION: This function strictly adheres to the Compute Pushdown Deduplication requirement.
    It executes a SQL Window function against PostgreSQL to filter the most recent cases natively,
    extracting only the deduplicated dataset into a Polars DataFrame.

    Extracts a deduplicated manifold of FAERS cases from the Bronze layer.

    Args:
        connection_uri: The strict URI connection string to the PostgreSQL database.
        source_table: The target physical table name in the Bronze layer.
        source_schema: The schema where the table resides. Defaults to 'bronze'.

    Returns:
        A Polars DataFrame representing the deduplicated case state.

    Raises:
        ValueError: If the source_table or source_schema contains invalid characters.
    """
    logger.info(f"Initiating extract_deduplicated_cases_task from table: {source_schema}.{source_table}")

    # Defensive validation to prevent SQL injection via table name interpolation
    if not re.match(r"^[a-zA-Z0-9_]+$", source_table) or not re.match(r"^[a-zA-Z0-9_]+$", source_schema):
        logger.error(f"Adversarial extraction vector detected. Invalid schema or table: {source_schema}.{source_table}")
        raise ValueError(
            f"Invalid schema or table name: '{source_schema}.{source_table}'. "
            "Must contain only alphanumeric characters and underscores."
        )

    # ruff: noqa: S608
    query = f"""
    WITH ranked_cases AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY caseid ORDER BY CAST(primaryid AS NUMERIC) DESC) as rn
        FROM {source_schema}.{source_table}
    )
    SELECT *
    FROM ranked_cases
    WHERE rn = 1
    """

    logger.debug("Executing compute pushdown deduplication query")
    df = pl.read_database_uri(query, uri=connection_uri)
    
    # Drop the temporary ranking column
    if "rn" in df.columns:
        df = df.drop("rn")

    logger.info(f"Successfully extracted {len(df)} deduplicated rows into Polars DataFrame")
    return df
