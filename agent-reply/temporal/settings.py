"""Settings shared by the worker, the client, and the workflow code.

AGENT_REPLY_AUTO_SEND_SECONDS sets AUTO_SEND_AFTER (default five minutes).
TEMPORAL_ADDRESS sets the server (default localhost:7233).
"""

import os
from datetime import timedelta

AUTO_SEND_AFTER = timedelta(
    seconds=int(os.environ.get("AGENT_REPLY_AUTO_SEND_SECONDS", "300"))
)
TASK_QUEUE = "agent-reply"
