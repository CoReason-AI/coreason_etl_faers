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
from pydantic import BaseModel, Field

from coreason_etl_faers.silver_deduplication import extract_deduplicated_cases_task
from coreason_etl_faers.silver_identity import generate_coreason_ids
from coreason_etl_faers.silver_meddra import normalize_meddra_pts
from coreason_etl_faers.utils.logger import logger


class SilverManifoldManifest(BaseModel):
    """
    AGENT INSTRUCTION: This object represents the ephemeral state of the Silver layer.
    It holds the DataFrames extracted from Bronze after transmutation.
    """

    demo_df: pl.DataFrame = Field(description="The deduplicated demographics manifold (DEMO).")
    drug_df: pl.DataFrame = Field(description="The deduplicated and identified drug manifold (DRUG).")
    reac_df: pl.DataFrame = Field(description="The deduplicated and normalized reactions manifold (REAC).")

    model_config = {"arbitrary_types_allowed": True}


def execute_silver_transmutation_task(connection_uri: str) -> SilverManifoldManifest:
    """
    AGENT INSTRUCTION: This orchestrates the Silver layer manifold transmutation.
    It extracts deduplicated cases from the Bronze layer tables natively in PostgreSQL,
    then applies Silver-layer structural normalizations in Polars (UUID5 coreason_id generation for drugs,
    and MedDRA PT normalization for reactions).

    Args:
        connection_uri: The strict URI connection string to the PostgreSQL database.

    Returns:
        A SilverManifoldManifest containing the unified Silver DataFrames.
    """
    logger.info("Initiating Silver layer transmutation task.")

    logger.info("Extracting and transmuting DEMO manifold.")
    demo_df = extract_deduplicated_cases_task(connection_uri, "faers_bronze_demo")

    if "data" in demo_df.columns and len(demo_df) > 0:
        demo_df = demo_df.with_columns(
            pl.col("data").str.json_path_match("$.caseid").alias("caseid"),
            pl.col("data").str.json_path_match("$.patient_id").alias("patient_id"),
            pl.col("data").str.json_path_match("$.event_dt").alias("event_dt"),
        )

    # If the columns missing, add them to avoid crash
    for col in ["caseid", "patient_id", "event_dt"]:
        if col not in demo_df.columns:
            demo_df = demo_df.with_columns(pl.lit(None).cast(pl.String).alias(col))

    logger.info("Extracting and transmuting DRUG manifold.")
    drug_raw_df = extract_deduplicated_cases_task(connection_uri, "faers_bronze_drug")

    # Silver ID Generation requires 'drugname' field. The data from `extract_deduplicated_cases_task`
    # is packed inside a 'data' struct column due to the dlt ingestion format into JSONB.
    # We must unpack the JSON string into columns to apply Polars native functions.

    # We first parse the JSON string in the 'data' column into a struct, then unnest it.
    if "data" in drug_raw_df.columns and len(drug_raw_df) > 0:
        drug_raw_df = drug_raw_df.with_columns(
            pl.col("data").str.json_path_match("$.drugname").alias("drugname"),
            pl.col("data").str.json_path_match("$.caseid").alias("caseid"),
            pl.col("data").str.json_path_match("$.role_cod").alias("role_cod"),
        )

    # If the columns missing, add them to avoid crash
    for col in ["drugname", "caseid", "role_cod"]:
        if col not in drug_raw_df.columns:
            drug_raw_df = drug_raw_df.with_columns(pl.lit(None).cast(pl.String).alias(col))

    drug_df = generate_coreason_ids(drug_raw_df)

    logger.info("Extracting and transmuting REAC manifold.")
    reac_raw_df = extract_deduplicated_cases_task(connection_uri, "faers_bronze_reac")

    if "data" in reac_raw_df.columns and len(reac_raw_df) > 0:
        reac_raw_df = reac_raw_df.with_columns(
            pl.col("data").str.json_path_match("$.pt").alias("pt"),
            pl.col("data").str.json_path_match("$.caseid").alias("caseid"),
        )

    if "pt" not in reac_raw_df.columns:
        reac_raw_df = reac_raw_df.with_columns(pl.lit(None).cast(pl.String).alias("pt"))

    reac_df = normalize_meddra_pts(reac_raw_df)

    logger.info("Silver layer transmutation task completed successfully.")
    return SilverManifoldManifest(
        demo_df=demo_df,
        drug_df=drug_df,
        reac_df=reac_df,
    )
