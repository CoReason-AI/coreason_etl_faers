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

from coreason_etl_faers.silver_meddra import normalize_meddra_pts


def test_normalize_meddra_pts_happy_path() -> None:
    df = pl.DataFrame({"pt": ["Headache", "Nausea", "Fever"]})
    result = normalize_meddra_pts(df)

    expected = ["HEADACHE", "NAUSEA", "FEVER"]
    assert result["normalized_pt"].to_list() == expected


def test_normalize_meddra_pts_whitespace() -> None:
    df = pl.DataFrame({"pt": ["  Headache  ", "\tNausea\n", " Fever "]})
    result = normalize_meddra_pts(df)

    expected = ["HEADACHE", "NAUSEA", "FEVER"]
    assert result["normalized_pt"].to_list() == expected


def test_normalize_meddra_pts_nulls_and_empty() -> None:
    df = pl.DataFrame({"pt": ["", None, "  "]})
    result = normalize_meddra_pts(df)

    expected = ["", None, ""]
    assert result["normalized_pt"].to_list() == expected


def test_normalize_meddra_pts_mixed_case() -> None:
    df = pl.DataFrame({"pt": ["hEaDaChE", "nAuSeA"]})
    result = normalize_meddra_pts(df)

    expected = ["HEADACHE", "NAUSEA"]
    assert result["normalized_pt"].to_list() == expected
