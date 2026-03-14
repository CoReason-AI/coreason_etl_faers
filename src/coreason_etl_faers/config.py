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
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FaersExtractionPolicy(BaseModel):
    """
    AGENT INSTRUCTION: This class defines the strictly validated boundary condition
    for an extraction orchestration sequence.

    A semantic contract dictating the structure and validation of the external
    source coordinates required for a quarterly FAERS data extraction manifold.

    This schema guarantees deterministic state variables and protects the extraction pipeline
    from adversarial URL vectors or structurally invalid chronological targets.
    """

    source_quarter: str = Field(
        ..., description="The target chronological quarter, formatted exactly as 'YYYYq[1-4]' (e.g., '2023q1')."
    )

    base_url: str = Field(
        "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html",
        description="The primary HTTP extraction vector for the FAERS index page or a local file:// vector.",
    )

    # Precompiled regex pattern as a class variable for efficient validation
    _QUARTER_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^\d{4}q[1-4]$")

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    @field_validator("source_quarter")
    @classmethod
    def validate_source_quarter(cls, value: str) -> str:
        """
        Validates the structure of the target chronological quarter.

        Args:
            value: The candidate string intended to represent a target quarter.

        Returns:
            The immutable, validated string representation of the target quarter.

        Raises:
            ValueError: If the string deviates from the deterministic 'YYYYq[1-4]' manifold constraint.
        """
        if not cls._QUARTER_PATTERN.match(value):
            raise ValueError(
                f"Invalid chronological constraint for 'source_quarter': '{value}'. "
                "Must strictly conform to 'YYYYq[1-4]' topology."
            )
        return value

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        """
        Enforces acceptable protocol schemas for the extraction target vector.

        Args:
            value: The candidate string intended to represent the extraction target URL.

        Returns:
            The validated URI string.

        Raises:
            ValueError: If the scheme is not explicitly permitted (must be http://, https://, or file://).
        """
        if not value.startswith(("http://", "https://", "file://")):
            raise ValueError(
                f"Adversarial extraction vector detected: '{value}'. "
                "Target URL must begin with 'http://', 'https://', or 'file://'."
            )
        return value
