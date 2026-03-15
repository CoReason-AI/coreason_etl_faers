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
from collections.abc import Iterator
from typing import Any

import requests

from coreason_etl_faers.utils.logger import logger


def stream_faers_data(url: str, target_filename: str) -> Iterator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: This function strictly adheres to the Safe Disk-Backed ZIP Streaming Pattern.
    Do not hold the ZIP in memory. Enforce 'latin-1' encoding and '$' delimiter.

    Streams a FAERS ZIP archive from a URL to a local temporary file, extracts a specific text file,
    and yields single-record dictionaries for downstream processing.

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
    # fmt: off
    # ruff: noqa: SIM115
    tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    # fmt: on
    try:
        logger.info(f"Streaming FAERS data from {url} to {tmp_zip.name}")

        # Handle file:// local URLs explicitly for the fallback
        if url.startswith("file://"):
            local_path = url[7:]  # Remove 'file://'
            logger.info(f"Reading from local file: {local_path}")
            with open(local_path, "rb") as f_in:
                while chunk := f_in.read(8192):
                    tmp_zip.write(chunk)
        else:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    tmp_zip.write(chunk)

        tmp_zip.close()  # Release OS lock

        logger.info(f"Opening ZIP archive: {tmp_zip.name} for file: {target_filename}")
        with zipfile.ZipFile(tmp_zip.name, "r") as z, z.open(target_filename) as f:
            # Enforce latin-1 for legacy medical text
            decoded_stream = (line.decode("latin-1", errors="replace") for line in f)
            # Prevent quote swallowing on $ delimiters
            # ruff: noqa: UP028
            reader = csv.DictReader(decoded_stream, delimiter="$", quoting=csv.QUOTE_NONE)
            for row in reader:
                yield row  # Yield single dict to let dlt handle batching

    finally:
        # Ensure file handle is closed (e.g., if an exception occurred before explicit close)
        import contextlib

        with contextlib.suppress(Exception):
            tmp_zip.close()

        logger.debug(f"Cleaning up temporary file: {tmp_zip.name}")
        if os.path.exists(tmp_zip.name):
            os.unlink(tmp_zip.name)  # Cross-platform cleanup
