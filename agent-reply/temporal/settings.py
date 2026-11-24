"""Settings shared by the worker, the client, and the workflow code.

SEND_REPLY_SECONDS (test only) makes the send_reply activity take that long (default 0).
SEND_REPLY_FAILED_ATTEMPTS (test only) makes that many first send_reply attempts fail (default 0).
TEMPORAL_ADDRESS sets the server (default localhost:7233).
"""

import os

SEND_REPLY_SECONDS = float(os.environ.get("SEND_REPLY_SECONDS", "0"))
SEND_REPLY_FAILED_ATTEMPTS = int(os.environ.get("SEND_REPLY_FAILED_ATTEMPTS", "0"))
TASK_QUEUE = "agent-reply"
