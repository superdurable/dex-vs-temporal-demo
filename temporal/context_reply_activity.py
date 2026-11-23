"""The reply activity of ConversationWithContextWorkflow (T8) and its request.

generate_reply_from_context returns the same deterministic mock reply as
generate_reply, from a request that carries the conversation context.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from temporalio import activity

from activities import build_mock_reply
from conversation_shared import ROLE_CUSTOMER, GeneratedReply, TranscriptMessage


@dataclass
class ContextReplyRequest:
    conversation_id: str
    customer_sequence: int
    # Every transcript record appended so far, as a model call would need it.
    context: list[TranscriptMessage] = field(default_factory=list)


@activity.defn
async def generate_reply_from_context(request: ContextReplyRequest) -> GeneratedReply:
    """The same mock, called with the conversation context a real model needs.

    It reads only the customer message being answered; the context is
    passed to show what a real model call puts into the activity input.
    """
    customer_text = next(
        (
            message.text
            for message in request.context
            if message.sequence == request.customer_sequence
            and message.role == ROLE_CUSTOMER
        ),
        None,
    )
    if customer_text is None:
        raise ValueError(
            f"customer message {request.customer_sequence} is not in the context "
            f"of {len(request.context)} messages"
        )
    return build_mock_reply(request.customer_sequence, customer_text)
