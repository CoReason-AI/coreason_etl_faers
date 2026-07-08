import sys
from unittest.mock import MagicMock

# Mock dlt module globally for tests to avoid needing to install the heavy package
# which breaks on Python 3.14t and Windows
sys.modules["dlt"] = MagicMock()
