# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from coreason_etl_faers.config import FaersExtractionPolicy


def test_faers_extraction_policy_valid_defaults() -> None:
    """
    Validates that the policy instantiates successfully given nominal
    chronological parameters and default extraction vectors.
    """
    policy = FaersExtractionPolicy(source_quarter="2023q1")
    assert policy.source_quarter == "2023q1"
    assert policy.base_url == "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"


def test_faers_extraction_policy_custom_url() -> None:
    """
    Verifies that the schema accepts overriding of the default vector
    with a valid explicit deterministic URL or file scheme.
    """
    custom_url = "file:///local/path/to/fda/html"
    policy = FaersExtractionPolicy(source_quarter="2024q4", base_url=custom_url)
    assert policy.base_url == custom_url


@pytest.mark.parametrize(
    "invalid_quarter",
    [
        "2023q5",  # Out of bounds quarter
        "2023q0",  # Out of bounds quarter
        "99q1",  # Bad year format
        "2023-q1",  # Bad separator
        "2023Q1",  # Strict casing enforcement (must be lowercase)
        "",  # Empty string
        " 2023q1",  # Before strip whitespace (Pydantic strips automatically by config)
    ],
)
def test_faers_extraction_policy_invalid_quarter(invalid_quarter: str) -> None:
    """
    Enforces the chronological boundaries. Any quarter string not
    matching exactly 'YYYYq[1-4]' must raise a validation error.
    """
    if invalid_quarter == " 2023q1":
        # The Pydantic model_config has str_strip_whitespace=True
        # so this specific one should pass after stripping
        policy = FaersExtractionPolicy(source_quarter=invalid_quarter)
        assert policy.source_quarter == "2023q1"
    else:
        with pytest.raises(ValidationError, match="Invalid chronological constraint"):
            FaersExtractionPolicy(source_quarter=invalid_quarter)


@pytest.mark.parametrize(
    "invalid_url",
    [
        "ftp://fis.fda.gov",
        "sftp://fis.fda.gov",
        "localhost/html",
        "//fis.fda.gov",
        "",
    ],
)
def test_faers_extraction_policy_invalid_url(invalid_url: str) -> None:
    """
    Verifies that the policy fiercely rejects adversarial or unexpected
    protocols for the extraction target vector.
    """
    with pytest.raises(ValidationError, match="Adversarial extraction vector detected"):
        FaersExtractionPolicy(source_quarter="2023q1", base_url=invalid_url)


def test_faers_extraction_policy_forbid_extra_fields() -> None:
    """
    Verifies that injecting arbitrary data variables outside of the schema
    raises an error due to the `extra='forbid'` constraint.
    """
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        FaersExtractionPolicy(source_quarter="2023q1", unknown_field="test")  # type: ignore


@given(st.text())
def test_faers_extraction_policy_hypothesis_fuzzing(random_string: str) -> None:
    """
    Property-based testing to guarantee that completely arbitrary text manifolds
    cannot trick the schema instantiator into returning a valid policy.
    """
    import re

    # If the random string accidentally generates a valid quarter, skip this iteration's failure assertion
    if re.match(r"^\d{4}q[1-4]$", random_string.strip()):
        return

    with pytest.raises(ValidationError):
        FaersExtractionPolicy(source_quarter=random_string)
