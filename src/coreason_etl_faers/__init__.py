# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

"""
ETL pipeline processing FDA Adverse Event Reporting System (FAERS) data
"""

__version__ = "0.1.0"
__author__ = "Akshaya M"
__email__ = "akshaya.movvar@coreason.ai"

from .config import FaersExtractionPolicy
from .main import execute_faers_etl_transmutation_task
from .orchestrator_gold import GoldManifoldManifest
from .orchestrator_silver import SilverManifoldManifest

__all__ = [
    "FaersExtractionPolicy",
    "GoldManifoldManifest",
    "SilverManifoldManifest",
    "execute_faers_etl_transmutation_task",
]
