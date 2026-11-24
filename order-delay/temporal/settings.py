"""Settings shared by the worker, the client, and the workflow code.

ORDER_DELAY_SECONDS sets the change window (default 60 seconds).
TEMPORAL_ADDRESS sets the server (default localhost:7233).
"""

import os
from datetime import timedelta

DELAY = timedelta(seconds=int(os.environ.get("ORDER_DELAY_SECONDS", "60")))
TASK_QUEUE = "order-delay"
