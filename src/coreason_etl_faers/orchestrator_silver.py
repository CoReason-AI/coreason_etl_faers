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


def _extract_json_fields(df: pl.DataFrame, fields: list[str]) -> pl.DataFrame:
    """
    Private helper function to safely unpack JSON fields from the Bronze layer's
    'data' column. It handles missing columns by injecting nulls, preventing
    crashes on malformed upstream payloads.
    """
    if "data" in df.columns and len(df) > 0:
        exprs = [pl.col("data").str.json_path_match(f"$.{field}").alias(field) for field in fields]
        df = df.with_columns(exprs)

    for field in fields:
        if field not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.String).alias(field))

    return df


def execute_silver_transmutation_task(connection_uri: str) -> SilverManifoldManifest:
    """
    AGENT INSTRUCTION: This orchestrates the Silver layer manifold transmutation.
    It extracts deduplicated cases from the Bronze layer tables natively in PostgreSQL,
    then applies Silver-layer structural normalizations in Polars (UUID5 coreason_id generation for drugs,
    and MedDRA PT normalization for reactions), and finally persists them to the Silver schema.

    Args:
        connection_uri: The strict URI connection string to the PostgreSQL database.

    Returns:
        A SilverManifoldManifest containing the unified Silver DataFrames.
    """
    logger.info("Initiating Silver layer transmutation task.")

    logger.info("Extracting and transmuting DEMO manifold.")
    demo_df = extract_deduplicated_cases_task(connection_uri, "coreason_etl_faers_bronze_demo", source_schema="bronze")
    demo_df = _extract_json_fields(demo_df, ["caseid", "patient_id", "event_dt"])

    logger.info("Extracting and transmuting DRUG manifold.")
    drug_raw_df = extract_deduplicated_cases_task(
        connection_uri, "coreason_etl_faers_bronze_drug", source_schema="bronze"
    )
    drug_raw_df = _extract_json_fields(drug_raw_df, ["drugname", "caseid", "role_cod"])
    drug_df = generate_coreason_ids(drug_raw_df)

    logger.info("Extracting and transmuting REAC manifold.")
    reac_raw_df = extract_deduplicated_cases_task(
        connection_uri, "coreason_etl_faers_bronze_reac", source_schema="bronze"
    )
    reac_raw_df = _extract_json_fields(reac_raw_df, ["pt", "caseid"])
    reac_df = normalize_meddra_pts(reac_raw_df)

    logger.info("Persisting Silver manifolds to PostgreSQL (silver schema).")

    # Save the transmutated frames to the postgres Silver schema
    if len(demo_df) > 0:
        demo_df.write_database(
            "coreason_etl_faers_silver_demo", connection=connection_uri, engine="adbc", if_table_exists="replace"
        )
    if len(drug_df) > 0:
        drug_df.write_database(
            "coreason_etl_faers_silver_drug", connection=connection_uri, engine="adbc", if_table_exists="replace"
        )
    if len(reac_df) > 0:
        reac_df.write_database(
            "coreason_etl_faers_silver_reac", connection=connection_uri, engine="adbc", if_table_exists="replace"
        )

    logger.info("Silver layer transmutation task completed successfully.")
    return SilverManifoldManifest(
        demo_df=demo_df,
        drug_df=drug_df,
        reac_df=reac_df,
    )
