"""Settings shared by the Worker, the client, and the Flow code.

AGENT_REPLY_AUTO_SEND_SECONDS sets AUTO_SEND_AFTER (default five minutes).
"""

import os
from datetime import timedelta

AUTO_SEND_AFTER = timedelta(
    seconds=int(os.environ.get("AGENT_REPLY_AUTO_SEND_SECONDS", "300"))
)
