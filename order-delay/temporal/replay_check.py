"""Replay a saved OrderWorkflow history against other workflow code, offline.

Usage: replay_check.py <history.json> <module>
<module> is a module name such as flow_v2. Prints whether the replay
succeeded and the exact error otherwise.
"""

import asyncio
import importlib
import sys
from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer


async def main(history_path: str, module_name: str) -> None:
    workflow_class = importlib.import_module(module_name).OrderWorkflow
    history = WorkflowHistory.from_json(
        Path(history_path).stem, Path(history_path).read_text()
    )
    replayer = Replayer(workflows=[workflow_class])
    result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
    if result.replay_failure is None:
        print(f"replay of {history_path} with {module_name}: OK")
    else:
        failure = result.replay_failure
        print(f"replay of {history_path} with {module_name}: FAILED")
        print(f"{type(failure).__name__}: {failure}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2]))
