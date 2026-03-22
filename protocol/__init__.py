"""
bet365 ZAP WebSocket protocol parser.

Provides parsing of raw ZAP frames into structured Python objects,
maintains an in-memory state tree, and emits change callbacks.
"""

from .zap_parser import (
    ZapParser,
    MessageType,
    DeltaOp,
    Classification,
    Competition,
    Event,
    Market,
    Participant,
    ChangeEvent,
    ChangeType,
)

__all__ = [
    "ZapParser",
    "MessageType",
    "DeltaOp",
    "Classification",
    "Competition",
    "Event",
    "Market",
    "Participant",
    "ChangeEvent",
    "ChangeType",
]
