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

from coreason_etl_faers.gold_schema import (
    build_bridge_case_drug_manifold,
    build_bridge_case_reaction_manifold,
    build_fact_adverse_event_manifold,
)
from coreason_etl_faers.orchestrator_silver import SilverManifoldManifest
from coreason_etl_faers.utils.logger import logger


class GoldManifoldManifest(BaseModel):
    """
    AGENT INSTRUCTION: This object represents the ephemeral state of the Gold layer.
    It holds the DataFrames extracted from Silver after transmutation.
    """

    fact_adverse_event_df: pl.DataFrame = Field(description="The core adverse event fact manifold.")
    bridge_case_drug_df: pl.DataFrame = Field(description="The case-drug bridging manifold.")
    bridge_case_reaction_df: pl.DataFrame = Field(description="The case-reaction bridging manifold.")

    model_config = {"arbitrary_types_allowed": True}


def execute_gold_transmutation_task(
    silver_manifest: SilverManifoldManifest, connection_uri: str
) -> GoldManifoldManifest:
    """
    AGENT INSTRUCTION: This orchestrates the Gold layer manifold transmutation.
    It takes the Silver layer manifest and transmutates it into the Gold layer star schema using
    the defined structural transformations, then persists them to PostgreSQL.

    Args:
        silver_manifest: The strictly typed SilverManifoldManifest from the previous pipeline stage.
        connection_uri: The strict URI connection string to the PostgreSQL database.

    Returns:
        A GoldManifoldManifest containing the unified Gold DataFrames.
    """
    logger.info("Initiating Gold layer transmutation task.")

    logger.info("Transmuting Silver DEMO to Gold fact_adverse_event manifold.")
    fact_adverse_event_df = build_fact_adverse_event_manifold(silver_manifest.demo_df)

    logger.info("Transmuting Silver DRUG to Gold bridge_case_drug manifold.")
    bridge_case_drug_df = build_bridge_case_drug_manifold(silver_manifest.drug_df)

    logger.info("Transmuting Silver REAC to Gold bridge_case_reaction manifold.")
    bridge_case_reaction_df = build_bridge_case_reaction_manifold(silver_manifest.reac_df)

    logger.info("Persisting Gold manifolds to PostgreSQL (gold schema).")

    if len(fact_adverse_event_df) > 0:
        fact_adverse_event_df.write_database(
            "coreason_etl_faers_gold_fact_adverse_event",
            connection=connection_uri,
            engine="adbc",
            if_table_exists="replace",
        )
    if len(bridge_case_drug_df) > 0:
        bridge_case_drug_df.write_database(
            "coreason_etl_faers_gold_bridge_case_drug",
            connection=connection_uri,
            engine="adbc",
            if_table_exists="replace",
        )
    if len(bridge_case_reaction_df) > 0:
        bridge_case_reaction_df.write_database(
            "coreason_etl_faers_gold_bridge_case_reaction",
            connection=connection_uri,
            engine="adbc",
            if_table_exists="replace",
        )

    logger.info("Gold layer transmutation task completed successfully.")
    return GoldManifoldManifest(
        fact_adverse_event_df=fact_adverse_event_df,
        bridge_case_drug_df=bridge_case_drug_df,
        bridge_case_reaction_df=bridge_case_reaction_df,
    )
