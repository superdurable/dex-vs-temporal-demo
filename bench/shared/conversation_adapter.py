"""Engine-neutral interface the load driver uses to talk to a conversation engine.

An adapter is a class with a no-argument constructor. It reads its own settings
(server address, identities) from environment variables. The load driver loads
it with --adapter <module>:<ClassName>; the module must be importable through
PYTHONPATH.

Contract:
- At most one customer message per conversation is outstanding at a time. The
  driver always waits for the reply before it posts the next message.
- start_conversation creates the conversation with its first customer message.
  The driver then calls wait_for_reply for that first message.
- observe_conversation prepares the adapter to see the next reply of an
  existing conversation (for example, by catching up with its event stream).
  The driver calls it before the timed part of a returning conversation, so
  its cost is not counted as reply latency. It must not make the engine's
  worker load the conversation.
- wait_for_reply returns when the reply to the latest posted message is
  visible to a reader of the conversation, or raises TimeoutError.
"""

from __future__ import annotations

from typing import Protocol


class ConversationAdapter(Protocol):
    engine_name: str

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def start_conversation(self, conversation_id: str, first_message: str) -> None: ...

    async def observe_conversation(self, conversation_id: str) -> None: ...

    async def post_message(self, conversation_id: str, text: str) -> None: ...

    async def wait_for_reply(self, conversation_id: str, timeout_seconds: float) -> None: ...
