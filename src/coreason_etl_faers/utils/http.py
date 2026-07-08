# Copyright (c) 2026 CoReason Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_faers

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_robust_session(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: tuple[int, ...] = (500, 502, 503, 504),
) -> requests.Session:
    """
    AGENT INSTRUCTION: This capability uses the standard library approach to handle
    ephemeral network failures deterministically, rejecting custom while/try blocks.

    Configures and returns a requests.Session capable of retrying HTTP requests
    automatically on transient failures.

    Args:
        retries: Total number of retries to allow.
        backoff_factor: A backoff factor to apply between attempts after the second try.
        status_forcelist: A set of integer HTTP status codes that we should force a retry on.

    Returns:
        A fully configured requests.Session.
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
