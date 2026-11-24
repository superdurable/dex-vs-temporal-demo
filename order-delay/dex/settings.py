"""Settings shared by the Worker, the client, and the Flow code.

ORDER_DELAY_SECONDS sets the change window (default 60 seconds).
"""

import os
from datetime import timedelta

DELAY = timedelta(seconds=int(os.environ.get("ORDER_DELAY_SECONDS", "60")))
