"""Test-only settings read by app.py; the Flow code does not use them.

SEND_REPLY_FAILED_ATTEMPTS makes that many first SendReplyStep attempts fail (default 0).
"""

import os

SEND_REPLY_FAILED_ATTEMPTS = int(os.environ.get("SEND_REPLY_FAILED_ATTEMPTS", "0"))
