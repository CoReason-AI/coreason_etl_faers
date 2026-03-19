# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import argparse
import os

from coreason_etl_faers.config import FaersExtractionPolicy
from coreason_etl_faers.orchestrator_bronze import execute_bronze_extraction_task
from coreason_etl_faers.orchestrator_gold import GoldManifoldManifest, execute_gold_transmutation_task
from coreason_etl_faers.orchestrator_silver import execute_silver_transmutation_task
from coreason_etl_faers.utils.logger import logger


def execute_faers_etl_transmutation_task(policy: FaersExtractionPolicy, connection_uri: str) -> GoldManifoldManifest:
    """
    AGENT INSTRUCTION: This orchestrates the complete ETL pipeline from Bronze extraction to Gold transmutation.
    It strictly adheres to the Epistemic/Transmutation naming convention and accepts validated policy contracts.

    Executes the end-to-end FAERS ETL pipeline.

    Args:
        policy: The semantic contract dictating the extraction configuration.
        connection_uri: The strict URI connection string to the PostgreSQL database.

    Returns:
        The final materialized GoldManifoldManifest containing the star schema DataFrames.
    """
    logger.info("Initiating FAERS ETL transmutation task.")
    logger.info(f"Policy configuration: target_quarter={policy.source_quarter}")

    try:
        # Step 1: Bronze Extraction
        logger.info("Starting Bronze Layer Extraction.")
        execute_bronze_extraction_task(policy)
        logger.info("Bronze Layer Extraction completed.")

        # Step 2: Silver Transmutation
        logger.info("Starting Silver Layer Transmutation.")
        silver_manifest = execute_silver_transmutation_task(connection_uri)
        logger.info("Silver Layer Transmutation completed.")

        # Step 3: Gold Transmutation
        logger.info("Starting Gold Layer Transmutation.")
        gold_manifest = execute_gold_transmutation_task(silver_manifest, connection_uri)
        logger.info("Gold Layer Transmutation completed.")

        logger.info("FAERS ETL transmutation task completed successfully.")
        return gold_manifest

    except Exception:
        logger.exception("Failed to execute FAERS ETL transmutation task.")
        raise


def build_postgres_uri_from_env() -> str:
    """
    AGENT INSTRUCTION: Strictly builds the PostgreSQL connection URI from standard environment variables.
    Requires PGHOST, PGUSER, PGPASSWORD, PGDATABASE. PGPORT defaults to 5432.
    """
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    database = os.environ.get("PGDATABASE", "coreason")

    # The URI structure expected by ADBC / Polars:
    # postgresql://user:password@host:port/database
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def main() -> None:
    """
    AGENT INSTRUCTION: Entry point for the ETL CLI.
    Parses command line arguments, builds the policy and connection URI,
    and initiates the ETL transmutation task.
    """
    parser = argparse.ArgumentParser(description="CoReason FAERS ETL Pipeline")
    parser.add_argument(
        "--quarter",
        type=str,
        default="2023q4",
        help="The target chronological quarter, formatted exactly as 'YYYYq[1-4]' (e.g., '2023q1').",
    )
    parser.add_argument(
        "--url",
        type=str,
        default="https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html",
        help="The base URL or file path for the extraction.",
    )

    args = parser.parse_args()

    policy = FaersExtractionPolicy(source_quarter=args.quarter, base_url=args.url)
    connection_uri = build_postgres_uri_from_env()

    execute_faers_etl_transmutation_task(policy, connection_uri)


if __name__ == "__main__":  # pragma: no cover
    main()
