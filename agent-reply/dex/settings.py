"""Settings shared by the Worker, the client, and the Flow code.

AGENT_REPLY_AUTO_SEND_SECONDS sets AUTO_SEND_AFTER (default five minutes).
SEND_REPLY_SECONDS (test only) makes sending the reply take that long (default 0).
SEND_REPLY_FAILED_ATTEMPTS (test only) makes that many first attempts fail (default 0).
"""

import os
from datetime import timedelta

AUTO_SEND_AFTER = timedelta(
    seconds=int(os.environ.get("AGENT_REPLY_AUTO_SEND_SECONDS", "300"))
)
SEND_REPLY_SECONDS = float(os.environ.get("SEND_REPLY_SECONDS", "0"))
SEND_REPLY_FAILED_ATTEMPTS = int(os.environ.get("SEND_REPLY_FAILED_ATTEMPTS", "0"))
