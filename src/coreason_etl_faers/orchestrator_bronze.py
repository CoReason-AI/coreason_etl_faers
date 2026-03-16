# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

from coreason_etl_faers.bronze_ingestion import load_faers_to_bronze
from coreason_etl_faers.config import FaersExtractionPolicy
from coreason_etl_faers.extractor import resolve_faers_url
from coreason_etl_faers.streamer import stream_faers_data
from coreason_etl_faers.utils.logger import logger


def execute_bronze_extraction_task(policy: FaersExtractionPolicy) -> None:
    """
    AGENT INSTRUCTION: This orchestrates the Bronze layer ingestion manifold.
    It takes a strictly typed extraction policy, resolves the URL, and
    iterates through the core datasets (DEMO, DRUG, REAC).

    Coordinates the streaming extraction of raw FAERS data and
    persists it via the Bronze ingestion pipeline.

    Args:
        policy: The semantic contract dictating the extraction configuration.
    """
    logger.info(f"Initiating Bronze extraction task for quarter: {policy.source_quarter}")

    resolved_url = resolve_faers_url(policy.source_quarter, policy.base_url)

    # Format the quarter for the target filenames (e.g., '2023q4' -> '23Q4')
    # According to TRD, source_quarter is YYYYq[1-4]
    year = policy.source_quarter[:4]
    quarter = policy.source_quarter[-2:]

    # Files in the zip use the 2-digit year, so '2023q4' -> '23Q4'
    file_quarter = f"{year[2:]}{quarter.upper()}"

    # Define the core FAERS datasets and their target tables
    core_datasets = {
        f"DEMO{file_quarter}.txt": "faers_bronze_demo",
        f"DRUG{file_quarter}.txt": "faers_bronze_drug",
        f"REAC{file_quarter}.txt": "faers_bronze_reac",
    }

    for filename, table_name in core_datasets.items():
        logger.info(f"Streaming and loading dataset: {filename} into table: {table_name}")
        stream_generator = stream_faers_data(resolved_url, filename)
        load_faers_to_bronze(stream_generator, table_name)

    logger.info("Bronze extraction task completed successfully")
