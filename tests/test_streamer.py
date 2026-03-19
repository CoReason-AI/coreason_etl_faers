# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import os
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
import requests
from pytest_mock import MockerFixture

from coreason_etl_faers.streamer import stream_faers_data


@pytest.fixture
def mock_faers_zip_content() -> bytes:
    """Fixture to generate a valid in-memory ZIP archive containing FAERS-like test data."""
    mock_data = "col1$col2$col3\nval1$val2$val3\nval4$val5$val6"
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test_file.txt", mock_data.encode("latin-1"))
    return zip_buffer.getvalue()


@pytest.fixture
def mock_faers_zip_content_latin1() -> bytes:
    """Fixture to generate a valid in-memory ZIP archive containing latin-1 encoding issues."""
    mock_data = "col1$col2\nval1$vål2"  # 'å' is a latin-1 specific character, not ascii
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test_file.txt", mock_data.encode("latin-1"))
    return zip_buffer.getvalue()


def test_stream_faers_data_http_success(mocker: MockerFixture, mock_faers_zip_content: bytes) -> None:
    """Test successful streaming and parsing from an HTTP URL."""
    url = "https://example.com/faers.zip"
    target_filename = "test_file.txt"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [mock_faers_zip_content]

    # Mock context manager
    mock_response.__enter__ = mocker.Mock(return_value=mock_response)
    mock_response.__exit__ = mocker.Mock(return_value=None)

    mock_get = mocker.patch("requests.get", return_value=mock_response)

    results = list(stream_faers_data(url, target_filename))

    mock_get.assert_called_once_with(url, stream=True, timeout=30)
    assert len(results) == 2
    assert results[0] == {"col1": "val1", "col2": "val2", "col3": "val3"}
    assert results[1] == {"col1": "val4", "col2": "val5", "col3": "val6"}


def test_stream_faers_data_local_file_success(tmp_path: Path, mock_faers_zip_content: bytes) -> None:
    """Test successful streaming and parsing from a local file:// URL."""
    local_file_path = tmp_path / "test_faers.zip"
    with open(local_file_path, "wb") as f:
        f.write(mock_faers_zip_content)

    url = f"file://{local_file_path}"
    target_filename = "test_file.txt"

    results = list(stream_faers_data(url, target_filename))

    assert len(results) == 2
    assert results[0] == {"col1": "val1", "col2": "val2", "col3": "val3"}


def test_stream_faers_data_latin1_encoding(mocker: MockerFixture, mock_faers_zip_content_latin1: bytes) -> None:
    """Test that latin-1 encoding is properly decoded and parsed."""
    url = "https://example.com/faers.zip"
    target_filename = "test_file.txt"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [mock_faers_zip_content_latin1]

    mock_response.__enter__ = mocker.Mock(return_value=mock_response)
    mock_response.__exit__ = mocker.Mock(return_value=None)
    mocker.patch("requests.get", return_value=mock_response)

    results = list(stream_faers_data(url, target_filename))

    assert len(results) == 1
    assert results[0] == {"col1": "val1", "col2": "vål2"}


def test_stream_faers_data_http_failure(mocker: MockerFixture) -> None:
    """Test that HTTP errors are propagated correctly and temp files are cleaned up."""
    url = "https://example.com/fail.zip"
    target_filename = "test_file.txt"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.side_effect = requests.RequestException("HTTP Error")

    mock_response.__enter__ = mocker.Mock(return_value=mock_response)
    mock_response.__exit__ = mocker.Mock(return_value=None)
    mocker.patch("requests.get", return_value=mock_response)

    mock_unlink = mocker.patch("os.unlink", side_effect=os.unlink)

    with pytest.raises(requests.RequestException, match="HTTP Error"):
        list(stream_faers_data(url, target_filename))

    # Verify cleanup happened despite the exception
    assert mock_unlink.call_count == 1


def test_stream_faers_data_complex_inconsistent_columns_and_quotes(mocker: MockerFixture) -> None:
    """Test streamer resilience against inconsistent columns and unescaped quotes which
    often appear in legacy medical text fields."""
    mock_data = (
        "col1$col2$col3\n"
        'val1$"quoted" value$val3\n'  # Unescaped quotes
        "val4$val5\n"  # Missing a column
        "val7$val8$val9$extra_val\n"  # Extra column
    )
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test_file.txt", mock_data.encode("latin-1"))

    url = "https://example.com/faers.zip"
    target_filename = "test_file.txt"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [zip_buffer.getvalue()]

    mock_response.__enter__ = mocker.Mock(return_value=mock_response)
    mock_response.__exit__ = mocker.Mock(return_value=None)
    mocker.patch("requests.get", return_value=mock_response)

    results = list(stream_faers_data(url, target_filename))

    assert len(results) == 3
    # Quotes should be preserved as part of the string (csv.QUOTE_NONE)
    assert results[0] == {"col1": "val1", "col2": '"quoted" value', "col3": "val3"}
    # Missing columns become None
    assert results[1] == {"col1": "val4", "col2": "val5", "col3": None}
    # Extra columns are assigned to the None key by DictReader
    assert results[2]["col1"] == "val7"
    assert results[2]["col2"] == "val8"
    assert results[2]["col3"] == "val9"
    assert results[2].get(None) == ["extra_val"]  # type: ignore[call-overload]


def test_stream_faers_data_empty_file(mocker: MockerFixture) -> None:
    """Test streamer behavior when the target txt file is completely empty."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test_file.txt", b"")

    url = "https://example.com/faers.zip"
    target_filename = "test_file.txt"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [zip_buffer.getvalue()]

    mock_response.__enter__ = mocker.Mock(return_value=mock_response)
    mock_response.__exit__ = mocker.Mock(return_value=None)
    mocker.patch("requests.get", return_value=mock_response)

    results = list(stream_faers_data(url, target_filename))

    assert len(results) == 0


def test_stream_faers_data_bad_zip_file(mocker: MockerFixture) -> None:
    """Test that a non-zip file throws BadZipFile and cleans up correctly."""
    url = "https://example.com/bad.zip"
    target_filename = "test_file.txt"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [b"this is not a zip file"]

    mock_response.__enter__ = mocker.Mock(return_value=mock_response)
    mock_response.__exit__ = mocker.Mock(return_value=None)
    mocker.patch("requests.get", return_value=mock_response)

    mock_unlink = mocker.patch("os.unlink", side_effect=os.unlink)

    with pytest.raises(zipfile.BadZipFile):
        list(stream_faers_data(url, target_filename))

    assert mock_unlink.call_count == 1


def test_stream_faers_data_missing_target_file(mocker: MockerFixture, mock_faers_zip_content: bytes) -> None:
    """Test that searching for a non-existent file inside the zip throws KeyError."""
    url = "https://example.com/faers.zip"
    target_filename = "missing_file.txt"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [mock_faers_zip_content]

    mock_response.__enter__ = mocker.Mock(return_value=mock_response)
    mock_response.__exit__ = mocker.Mock(return_value=None)
    mocker.patch("requests.get", return_value=mock_response)

    with pytest.raises(KeyError, match=r"There is no item named 'missing_file.txt' in the archive"):
        list(stream_faers_data(url, target_filename))
