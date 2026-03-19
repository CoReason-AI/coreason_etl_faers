# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import csv
import os
import tempfile
import zipfile
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

import requests

from coreason_etl_faers.utils.logger import logger


@contextmanager
def _get_zip_filepath(url: str) -> Generator[str]:
    """
    Yields a local file path to the ZIP archive. If the source is an HTTP URL,
    it streams the content to a temporary file which is cleaned up upon exit.
    If the source is a local 'file://' URL, it yields the resolved path directly
    without redundant copying.
    """
    parsed_url = urlparse(url)

    if parsed_url.scheme == "file":
        # Strip `file://` scheme carefully to support both Unix paths and
        # Windows drive letters natively, applying standard unquoting.
        path_part = url[7:] if url.startswith("file://") else parsed_url.path
        local_path = unquote(url2pathname(path_part))
        logger.info(f"Using local file directly: {local_path}")
        yield local_path
    else:
        # Handle HTTP(S) URLs
        # fmt: off
        # ruff: noqa: SIM115
        tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        # fmt: on
        try:
            logger.info(f"Streaming FAERS data from {url} to {tmp_zip.name}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    tmp_zip.write(chunk)
            tmp_zip.close()  # Release OS lock
            yield tmp_zip.name
        finally:
            import contextlib

            with contextlib.suppress(Exception):
                tmp_zip.close()
            logger.debug(f"Cleaning up temporary file: {tmp_zip.name}")
            if os.path.exists(tmp_zip.name):
                os.unlink(tmp_zip.name)


def stream_faers_data(url: str, target_filename: str) -> Iterator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: This function strictly adheres to the Safe Disk-Backed ZIP Streaming Pattern.
    Do not hold the ZIP in memory. Enforce 'latin-1' encoding and '$' delimiter.

    Streams a FAERS ZIP archive from a URL to a local temporary file (or uses the local
    path directly if file:// scheme), extracts a specific text file, and yields
    single-record dictionaries for downstream processing.

    Args:
        url: The HTTP or file URL of the FAERS ZIP archive.
        target_filename: The name of the specific text file within the ZIP archive to extract.

    Yields:
        Dictionaries representing single records parsed from the target file.

    Raises:
        requests.RequestException: If the HTTP request fails.
        zipfile.BadZipFile: If the downloaded file is not a valid ZIP archive.
        KeyError: If the target_filename is not found in the ZIP archive.
    """
    with _get_zip_filepath(url) as zip_path:
        logger.info(f"Opening ZIP archive: {zip_path} for file: {target_filename}")
        with zipfile.ZipFile(zip_path, "r") as z, z.open(target_filename) as f:
            # Enforce latin-1 for legacy medical text
            decoded_stream = (line.decode("latin-1", errors="replace") for line in f)
            # Prevent quote swallowing on $ delimiters
            # ruff: noqa: UP028
            reader = csv.DictReader(decoded_stream, delimiter="$", quoting=csv.QUOTE_NONE)
            for row in reader:
                yield row  # Yield single dict to let dlt handle batching
