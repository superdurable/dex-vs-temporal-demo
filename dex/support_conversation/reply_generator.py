"""Reply generation: the seam where an LLM call with internal tools would go."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Protocol

from support_conversation.transcript import DEMO_REPLY_BYTES

REPLY_BYTES = DEMO_REPLY_BYTES or 200


@dataclass(frozen=True)
class GeneratedReply:
    text: str
    tool_calls: list[str] = field(default_factory=list)


class ReplyGenerator(Protocol):
    def generate_reply(
        self, conversation_id: str, customer_messages: list[str]
    ) -> GeneratedReply: ...


class MockReplyGenerator:
    """Deterministic local stand-in for a model: no network and no sleep.

    It hashes the conversation ID and the message with SHA-256, picks one or
    two tool names from the digest, and fills a fixed reply template. The
    tools are names only; nothing is looked up or sent.
    """

    TOOLS = ("lookup_order", "check_refund_policy", "check_shipping_status")

    def generate_reply(
        self, conversation_id: str, customer_messages: list[str]
    ) -> GeneratedReply:
        digest = hashlib.sha256(
            "\n".join([conversation_id, *customer_messages]).encode()
        ).digest()
        tool_calls = [self.TOOLS[digest[0] % len(self.TOOLS)]]
        if digest[1] % 2 == 0:
            tool_calls.append(self.TOOLS[(digest[0] + 1) % len(self.TOOLS)])
        order_number = int.from_bytes(digest[2:5], "big") % 1_000_000
        text = (
            f"Thanks for your message. I checked order {order_number:06d} using "
            f"{' and '.join(tool_calls)}. Your request is covered. "
            "Is there anything else I can help with today?"
        )
        if DEMO_REPLY_BYTES:
            # The memory benchmark fills the reply to exactly DEMO_REPLY_BYTES.
            text = (text + " ") * (DEMO_REPLY_BYTES // (len(text) + 1) + 1)
        return GeneratedReply(text[:REPLY_BYTES], tool_calls)
