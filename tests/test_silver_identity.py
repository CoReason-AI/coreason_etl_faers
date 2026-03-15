# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import uuid

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_faers.silver_identity import NAMESPACE_FAERS_DRUG, generate_coreason_ids


def test_generate_coreason_ids_happy_path() -> None:
    """Test standard drug names resolve correctly."""
    df = pl.DataFrame({"drugname": ["aspirin", " IBUPROFEN  ", "Tylenol"]})
    result = generate_coreason_ids(df)

    assert "normalized_drugname" in result.columns
    assert "coreason_id" in result.columns

    normalized = result["normalized_drugname"].to_list()
    assert normalized == ["ASPIRIN", "IBUPROFEN", "TYLENOL"]

    coreason_ids = result["coreason_id"].to_list()
    # verify determinism
    expected_aspirin = str(uuid.uuid5(NAMESPACE_FAERS_DRUG, "ASPIRIN"))
    expected_ibuprofen = str(uuid.uuid5(NAMESPACE_FAERS_DRUG, "IBUPROFEN"))
    expected_tylenol = str(uuid.uuid5(NAMESPACE_FAERS_DRUG, "TYLENOL"))

    assert coreason_ids == [expected_aspirin, expected_ibuprofen, expected_tylenol]


def test_generate_coreason_ids_empty_and_null() -> None:
    """Test handling of null, empty strings, and whitespace-only strings."""
    df = pl.DataFrame({"drugname": [None, "", "   "]})
    result = generate_coreason_ids(df)

    normalized = result["normalized_drugname"].to_list()
    assert normalized == [None, "", ""]

    coreason_ids = result["coreason_id"].to_list()

    # Empty string is falsy so it should map to None
    assert coreason_ids == [None, None, None]


@given(st.lists(st.text(), min_size=1, max_size=50))
def test_generate_coreason_ids_property(drug_names: list[str]) -> None:
    """Property-based testing for arbitrary string inputs to ensure no crashes and consistent schemas."""
    df = pl.DataFrame({"drugname": drug_names})

    try:
        result = generate_coreason_ids(df)

        # Verify schema
        assert "normalized_drugname" in result.columns
        assert "coreason_id" in result.columns
        assert result["coreason_id"].dtype == pl.String
        assert result["normalized_drugname"].dtype == pl.String

        # Lengths should remain the same
        assert len(result) == len(df)
    except Exception as e:
        pytest.fail(f"Unexpected exception with inputs {drug_names}: {e}")
