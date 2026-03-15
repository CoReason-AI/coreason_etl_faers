# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

from collections.abc import Iterator
from typing import Any

import dlt

from coreason_etl_faers.utils.logger import logger


def load_faers_to_bronze(stream_generator: Iterator[dict[str, Any]], table_name: str) -> None:
    """
    AGENT INSTRUCTION: This function implements the dlt chunking strategy.
    It strictly uses max_table_nesting=0 to ensure single JSONB row creation.

    Loads the streamed FAERS data generator into PostgreSQL using dlt.

    Args:
        stream_generator: The generator yielding single-record dictionaries.
        table_name: The destination table name in PostgreSQL.
    """
    logger.info(f"Configuring dlt pipeline for target table: {table_name}")

    pipeline = dlt.pipeline(
        pipeline_name="faers_bronze_pipeline",
        destination="postgres",
        dataset_name="faers_bronze",
    )

    logger.info("Executing dlt pipeline run with max_table_nesting=0")

    # Run the pipeline with max_table_nesting=0 as strictly required by TRD
    # It ensures the data lands as exactly one row with one JSONB column per FAERS record
    # We create a dlt resource with max_table_nesting=0
    resource = dlt.resource(stream_generator, name=table_name, max_table_nesting=0)
    load_info = pipeline.run(resource)

    logger.info(f"dlt pipeline loaded data successfully. Info:\n{load_info}")
