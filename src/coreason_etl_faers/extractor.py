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

import requests

from coreason_etl_faers.utils.logger import logger


class FAERSUrlResolutionError(Exception):
    """Event raised when the FAERS URL cannot be resolved."""


def resolve_faers_url(source_quarter: str, url: str | None = None) -> str:
    """
    AGENT INSTRUCTION: This function strictly adheres to the defensive regex and fallback logic.
    Do not use BeautifulSoup or rely on DOM structure.

    Fetches the FDA HTML page and resolves the target ZIP file URL.

    Args:
        source_quarter: The quarter identifier (e.g., '2023q4', '2023Q4').
        url: The source URL or file path. If it starts with 'file://', it is returned as is.
             Defaults to the FDA FIS extension URL.

    Returns:
        The resolved URL to the ZIP file.

    Raises:
        FAERSUrlResolutionEvent: If the URL cannot be resolved or the HTTP request fails.
    """
    if url is None:
        url = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"

    if url.startswith("file://"):
        logger.info(f"Using local file fallback: {url}")
        return url

    logger.info(f"Fetching FAERS HTML page from {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch FAERS HTML page: {e}")
        raise FAERSUrlResolutionError(f"HTTP request failed: {e}") from e

    html_content = response.text

    # Defensive regex to find the zip link regardless of DOM structure
    # Matches href="...ASCII...{source_quarter}....zip" (case-insensitive for the quarter)
    pattern = rf'href=["\']([^"\']*ASCII[^"\']*?{re.escape(source_quarter)}[^"\']*?\.zip)["\']'

    match = re.search(pattern, html_content, re.IGNORECASE)
    if not match:
        logger.error(f"Could not resolve FAERS ZIP URL for quarter {source_quarter}")
        raise FAERSUrlResolutionError(f"Could not find matching ZIP link for quarter: {source_quarter}")

    resolved_url = match.group(1)

    # Handle relative URLs if necessary (though the regex typically captures absolute if provided that way,
    # but the FDA site often has absolute links in the href)
    if resolved_url.startswith("/"):
        from urllib.parse import urljoin

        resolved_url = urljoin(url, resolved_url)

    logger.info(f"Successfully resolved FAERS ZIP URL: {resolved_url}")
    return resolved_url
