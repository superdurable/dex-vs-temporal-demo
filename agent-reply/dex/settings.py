"""Test-only settings read by app.py; the Flow code does not use them.

SEND_REPLY_SECONDS makes sending the reply take that long (default 0).
SEND_REPLY_FAILED_ATTEMPTS makes that many first attempts fail (default 0).
"""

import os

SEND_REPLY_SECONDS = float(os.environ.get("SEND_REPLY_SECONDS", "0"))
SEND_REPLY_FAILED_ATTEMPTS = int(os.environ.get("SEND_REPLY_FAILED_ATTEMPTS", "0"))
