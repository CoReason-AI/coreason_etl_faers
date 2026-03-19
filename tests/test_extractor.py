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
import requests
from pytest_mock import MockerFixture

from coreason_etl_faers.extractor import FAERSUrlResolutionError, resolve_faers_url


def test_resolve_faers_url_local_file_fallback() -> None:
    """Test that a local file:// URL bypasses HTTP requests and returns immediately."""
    local_url = "file:///tmp/faers_test_data/FPD-QDE-FAERS.html"
    result = resolve_faers_url("2023q4", url=local_url)
    assert result == local_url


def test_resolve_faers_url_success_absolute_link(mocker: MockerFixture) -> None:
    """Test successful resolution of an absolute ZIP URL with standard double quotes."""
    html_content = """
    <html>
        <body>
            <p>Some text</p>
            <a href="https://example.com/faers/ASCII_2023Q4_data.zip">Download FAERS</a>
        </body>
    </html>
    """
    mock_response = mocker.Mock()
    mock_response.text = html_content
    mock_response.raise_for_status.return_value = None

    mock_get = mocker.patch("requests.get", return_value=mock_response)

    result = resolve_faers_url("2023q4", url="https://example.com/faers.html")

    assert result == "https://example.com/faers/ASCII_2023Q4_data.zip"
    mock_get.assert_called_once_with("https://example.com/faers.html", timeout=30)


def test_resolve_faers_url_success_relative_link_single_quotes(mocker: MockerFixture) -> None:
    """Test successful resolution of a relative ZIP URL with single quotes and case insensitivity."""
    html_content = """
    <div>
        <a class="download-link" href='/downloads/ASCII_2023q4_xyz.zip'>Download</a>
    </div>
    """
    mock_response = mocker.Mock()
    mock_response.text = html_content
    mock_response.raise_for_status.return_value = None

    mock_get = mocker.patch("requests.get", return_value=mock_response)

    # If the URL resolves a relative link, it should use urljoin
    result = resolve_faers_url("2023Q4", url="https://example.com/base/")

    assert result == "https://example.com/downloads/ASCII_2023q4_xyz.zip"
    mock_get.assert_called_once_with("https://example.com/base/", timeout=30)


def test_resolve_faers_url_default_url(mocker: MockerFixture) -> None:
    """Test that the default FDA URL is used when no URL is provided."""
    html_content = '<a href="https://fis.fda.gov/downloads/ASCII_2024q1_extract.zip">Data</a>'
    mock_response = mocker.Mock()
    mock_response.text = html_content
    mock_response.raise_for_status.return_value = None

    mock_get = mocker.patch("requests.get", return_value=mock_response)

    result = resolve_faers_url("2024q1")

    assert result == "https://fis.fda.gov/downloads/ASCII_2024q1_extract.zip"
    mock_get.assert_called_once_with("https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html", timeout=30)


def test_resolve_faers_url_complex_page_structure(mocker: MockerFixture) -> None:
    """Test defensive regex against a complex HTML page with multiple misleading links,
    different formats, and various DOM structures."""
    html_content = """
    <html>
        <body>
            <div>
                <!-- Misleading XML Link -->
                <a href="https://example.com/faers/XML_2023Q4_data.zip">XML 2023Q4</a>

                <!-- Misleading Link for a Different Quarter -->
                <a class="btn" href="https://example.com/faers/ASCII_2023Q3_data.zip">ASCII 2023Q3</a>

                <!-- Misleading Non-ZIP Link -->
                <a href="https://example.com/faers/ASCII_2023Q4_data.pdf">ASCII 2023Q4 PDF</a>

                <!-- Target Link buried in weird structure -->
                <span>
                    <a data-url="target" href="https://fis.fda.gov/downloads/ASCII_2023Q4_extract.zip"
                       target="_blank">Download Target</a>
                </span>

                <!-- Another Misleading Link later in the document -->
                <a href="https://example.com/faers/ASCII_2024Q1_data.zip">ASCII 2024Q1</a>
            </div>
        </body>
    </html>
    """
    mock_response = mocker.Mock()
    mock_response.text = html_content
    mock_response.raise_for_status.return_value = None

    mock_get = mocker.patch("requests.get", return_value=mock_response)

    result = resolve_faers_url("2023q4", url="https://fis.fda.gov/extensions.html")

    assert result == "https://fis.fda.gov/downloads/ASCII_2023Q4_extract.zip"
    mock_get.assert_called_once()


def test_resolve_faers_url_no_match(mocker: MockerFixture) -> None:
    """Test that an error is raised when the HTML content does not contain a matching ZIP link."""
    html_content = """
    <html>
        <body>
            <a href="https://example.com/faers/ASCII_2023Q3_data.zip">Wrong Quarter</a>
            <a href="https://example.com/faers/XML_2023Q4_data.zip">Wrong Format</a>
        </body>
    </html>
    """
    mock_response = mocker.Mock()
    mock_response.text = html_content
    mock_response.raise_for_status.return_value = None

    mocker.patch("requests.get", return_value=mock_response)

    with pytest.raises(FAERSUrlResolutionError, match="Could not find matching ZIP link for quarter: 2023q4"):
        resolve_faers_url("2023q4", url="https://example.com/faers.html")


def test_resolve_faers_url_http_error(mocker: MockerFixture) -> None:
    """Test that an error is raised when the HTTP request fails."""
    mock_get = mocker.patch("requests.get", side_effect=requests.RequestException("Connection timeout"))

    with pytest.raises(FAERSUrlResolutionError, match="HTTP request failed: Connection timeout"):
        resolve_faers_url("2023q4", url="https://example.com/faers.html")

    mock_get.assert_called_once()
