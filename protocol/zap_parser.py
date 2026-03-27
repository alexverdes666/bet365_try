"""
ZAP WebSocket protocol parser for bet365 live data.

Parses raw WebSocket frames into structured Python objects, maintains an
in-memory state tree (sports > competitions > events > markets > selections),
applies incremental deltas, and emits callbacks on data changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Protocol constants
# ---------------------------------------------------------------------------

SOH = "\x01"  # Separates topic name from payload
STX = "\x02"  # Separates protocol version from connection ID
ETX = "\x03"  # Handshake: after '#', before topic count
BS  = "\x08"  # Frame separator between batched updates
NUL = "\x00"  # Message terminator

BYTE_TOPIC_STATUS   = 0x23  # '#'
BYTE_CLIENT_SUB     = 0x16
BYTE_INITIAL_LOAD   = 0x14
BYTE_DELTA           = 0x15
BYTE_SERVER_ACK      = 0x31  # ASCII '1'


class MessageType(IntEnum):
    TOPIC_STATUS_NOTIFICATION = BYTE_TOPIC_STATUS
    CLIENT_SUBSCRIBE = BYTE_CLIENT_SUB
    INITIAL_TOPIC_LOAD = BYTE_INITIAL_LOAD
    DELTA = BYTE_DELTA
    SERVER_ACK = BYTE_SERVER_ACK
    UNKNOWN = 0xFF


class DeltaOp(str, Enum):
    FULL = "F"
    UPDATE = "U"
    INSERT = "I"
    DELETE = "D"


class ChangeType(str, Enum):
    EVENT_ADDED = "event_added"
    EVENT_REMOVED = "event_removed"
    SCORE_CHANGE = "score_change"
    ODDS_CHANGE = "odds_change"
    MARKET_ADDED = "market_added"
    MARKET_REMOVED = "market_removed"
    SELECTION_ADDED = "selection_added"
    SELECTION_REMOVED = "selection_removed"
    EVENT_UPDATED = "event_updated"
    MARKET_UPDATED = "market_updated"
    SELECTION_UPDATED = "selection_updated"
    SPORT_ADDED = "sport_added"
    SPORT_REMOVED = "sport_removed"
    COMPETITION_ADDED = "competition_added"
    COMPETITION_REMOVED = "competition_removed"
    COMPETITION_UPDATED = "competition_updated"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ChangeEvent:
    """Emitted when the state tree changes."""
    change_type: ChangeType
    entity_type: str          # "CL", "CT", "EV", "MA", "PA"
    entity_id: str
    topic: str = ""
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None


@dataclass
class Classification:
    """CL entity -- a sport / top-level classification."""
    id: str = ""
    name: str = ""
    sport_id: str = ""
    topic: str = ""
    raw: Dict[str, str] = field(default_factory=dict)


@dataclass
class Competition:
    """CT entity -- a competition / league."""
    id: str = ""
    name: str = ""
    short_name: str = ""
    order: str = ""
    sport_id: str = ""
    topic: str = ""
    raw: Dict[str, str] = field(default_factory=dict)


@dataclass
class Event:
    """EV entity -- a live event / match."""
    id: str = ""
    name: str = ""
    score: str = ""
    minute: str = ""
    seconds: str = ""
    time_type: str = ""       # 0=stopped, 1=running
    timestamp: str = ""       # YYYYMMDDHHmmss
    sport_id: str = ""
    competition_id: str = ""
    fixture_id: str = ""
    topic: str = ""
    update_context: str = ""
    goals: str = ""
    raw: Dict[str, str] = field(default_factory=dict)


@dataclass
class Market:
    """MA entity -- a betting market."""
    id: str = ""
    market_type: str = ""
    name: str = ""
    fixture_id: str = ""
    columns: str = ""
    suspended: str = ""
    event_id: str = ""
    topic: str = ""
    raw: Dict[str, str] = field(default_factory=dict)


@dataclass
class Participant:
    """PA entity -- a selection / outcome within a market."""
    id: str = ""
    name: str = ""
    odds: str = ""
    order: str = ""
    fixture_id: str = ""
    handicap: str = ""
    handicap_display: str = ""
    suspended: str = ""
    market_id: str = ""
    topic: str = ""
    raw: Dict[str, str] = field(default_factory=dict)


# Mapping from ZAP two-letter type codes to dataclass constructors.
_ENTITY_TYPE_MAP: Dict[str, type] = {
    "CL": Classification,
    "CT": Competition,
    "EV": Event,
    "MA": Market,
    "PA": Participant,
}


# ---------------------------------------------------------------------------
# Parsed-message container
# ---------------------------------------------------------------------------

@dataclass
class ParsedMessage:
    """Result of parsing a single ZAP sub-frame."""
    msg_type: MessageType
    topic: str = ""
    operation: Optional[DeltaOp] = None
    entities: List[Dict[str, str]] = field(default_factory=list)
    raw: str = ""
    # For server-ack / handshake
    connection_id: str = ""
    protocol_version: str = ""
    extra: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# State tree
# ---------------------------------------------------------------------------

class StateTree:
    """
    In-memory hierarchical state of the live data feed.

    Structure:
        sports (CL)  ->  competitions (CT)  ->  events (EV)
                                                  ->  markets (MA)
                                                        ->  selections (PA)
    """

    def __init__(self) -> None:
        # Primary lookup tables keyed by entity ID.
        self.sports: Dict[str, Classification] = {}
        self.competitions: Dict[str, Competition] = {}
        self.events: Dict[str, Event] = {}
        self.markets: Dict[str, Market] = {}
        self.selections: Dict[str, Participant] = {}

        # Relationship indexes  (parent_id -> set of child_ids).
        self.sport_competitions: Dict[str, set] = {}   # sport_id -> comp ids
        self.comp_events: Dict[str, set] = {}           # comp_id  -> event ids
        self.event_markets: Dict[str, set] = {}          # event_id -> market ids
        self.market_selections: Dict[str, set] = {}      # market_id -> selection ids

        # Reverse: topic string -> (entity_type, entity_id)
        self.topic_index: Dict[str, Tuple[str, str]] = {}

    # -- helpers -------------------------------------------------------------

    def _link_sport(self, sport: Classification) -> None:
        self.sport_competitions.setdefault(sport.id, set())

    def _link_competition(self, comp: Competition) -> None:
        self.comp_events.setdefault(comp.id, set())
        if comp.sport_id:
            self.sport_competitions.setdefault(comp.sport_id, set()).add(comp.id)

    def _link_event(self, ev: Event) -> None:
        self.event_markets.setdefault(ev.id, set())
        if ev.competition_id:
            self.comp_events.setdefault(ev.competition_id, set()).add(ev.id)

    def _link_market(self, mkt: Market) -> None:
        self.market_selections.setdefault(mkt.id, set())
        if mkt.event_id:
            self.event_markets.setdefault(mkt.event_id, set()).add(mkt.id)

    def _link_selection(self, sel: Participant) -> None:
        if sel.market_id:
            self.market_selections.setdefault(sel.market_id, set()).add(sel.id)

    # -- public mutators -----------------------------------------------------

    def upsert_sport(self, sport: Classification) -> Optional[Classification]:
        old = self.sports.get(sport.id)
        self.sports[sport.id] = sport
        self._link_sport(sport)
        if sport.topic:
            self.topic_index[sport.topic] = ("CL", sport.id)
        return old

    def upsert_competition(self, comp: Competition) -> Optional[Competition]:
        old = self.competitions.get(comp.id)
        self.competitions[comp.id] = comp
        self._link_competition(comp)
        if comp.topic:
            self.topic_index[comp.topic] = ("CT", comp.id)
        return old

    def upsert_event(self, ev: Event) -> Optional[Event]:
        old = self.events.get(ev.id)
        self.events[ev.id] = ev
        self._link_event(ev)
        if ev.topic:
            self.topic_index[ev.topic] = ("EV", ev.id)
        return old

    def upsert_market(self, mkt: Market) -> Optional[Market]:
        old = self.markets.get(mkt.id)
        self.markets[mkt.id] = mkt
        self._link_market(mkt)
        if mkt.topic:
            self.topic_index[mkt.topic] = ("MA", mkt.id)
        return old

    def upsert_selection(self, sel: Participant) -> Optional[Participant]:
        old = self.selections.get(sel.id)
        self.selections[sel.id] = sel
        self._link_selection(sel)
        if sel.topic:
            self.topic_index[sel.topic] = ("PA", sel.id)
        return old

    def remove_sport(self, sport_id: str) -> Optional[Classification]:
        sport = self.sports.pop(sport_id, None)
        if sport and sport.topic:
            self.topic_index.pop(sport.topic, None)
        # Cascade: remove child competitions (and their descendants).
        for comp_id in list(self.sport_competitions.pop(sport_id, set())):
            self.remove_competition(comp_id)
        return sport

    def remove_competition(self, comp_id: str) -> Optional[Competition]:
        comp = self.competitions.pop(comp_id, None)
        if comp:
            if comp.topic:
                self.topic_index.pop(comp.topic, None)
            if comp.sport_id:
                s = self.sport_competitions.get(comp.sport_id)
                if s:
                    s.discard(comp_id)
        for ev_id in list(self.comp_events.pop(comp_id, set())):
            self.remove_event(ev_id)
        return comp

    def remove_event(self, event_id: str) -> Optional[Event]:
        ev = self.events.pop(event_id, None)
        if ev:
            if ev.topic:
                self.topic_index.pop(ev.topic, None)
            if ev.competition_id:
                s = self.comp_events.get(ev.competition_id)
                if s:
                    s.discard(event_id)
        for mkt_id in list(self.event_markets.pop(event_id, set())):
            self.remove_market(mkt_id)
        return ev

    def remove_market(self, market_id: str) -> Optional[Market]:
        mkt = self.markets.pop(market_id, None)
        if mkt:
            if mkt.topic:
                self.topic_index.pop(mkt.topic, None)
            if mkt.event_id:
                s = self.event_markets.get(mkt.event_id)
                if s:
                    s.discard(market_id)
        for sel_id in list(self.market_selections.pop(market_id, set())):
            self.remove_selection(sel_id)
        return mkt

    def remove_selection(self, sel_id: str) -> Optional[Participant]:
        sel = self.selections.pop(sel_id, None)
        if sel:
            if sel.topic:
                self.topic_index.pop(sel.topic, None)
            if sel.market_id:
                s = self.market_selections.get(sel.market_id)
                if s:
                    s.discard(sel_id)
        return sel

    def remove_by_path(self, path: str) -> List[ChangeEvent]:
        """
        Handle a delete-path like
        ``OVInPlay_3_0/OV_1_3_0/OVCHINA-DIV2C1_3_0/OV191553085C1A_3_0``

        Each segment is a topic.  We try to match the *deepest* segment first;
        if found, we remove that entity and its children.
        """
        changes: List[ChangeEvent] = []
        segments = path.split("/")
        for segment in reversed(segments):
            entry = self.topic_index.get(segment)
            if entry is None:
                continue
            etype, eid = entry
            removed: Any = None
            ctype: Optional[ChangeType] = None
            if etype == "CL":
                removed = self.remove_sport(eid)
                ctype = ChangeType.SPORT_REMOVED
            elif etype == "CT":
                removed = self.remove_competition(eid)
                ctype = ChangeType.COMPETITION_REMOVED
            elif etype == "EV":
                removed = self.remove_event(eid)
                ctype = ChangeType.EVENT_REMOVED
            elif etype == "MA":
                removed = self.remove_market(eid)
                ctype = ChangeType.MARKET_REMOVED
            elif etype == "PA":
                removed = self.remove_selection(eid)
                ctype = ChangeType.SELECTION_REMOVED
            if removed and ctype:
                changes.append(ChangeEvent(
                    change_type=ctype,
                    entity_type=etype,
                    entity_id=eid,
                    topic=segment,
                    old_value=removed,
                ))
            # Only delete the deepest match.
            break
        return changes


# ---------------------------------------------------------------------------
# Entity field mapping helpers
# ---------------------------------------------------------------------------

_CL_FIELD_MAP = {"ID": "id", "NA": "name", "CL": "sport_id", "IT": "topic"}
_CT_FIELD_MAP = {"ID": "id", "NA": "name", "L3": "short_name", "OR": "order", "IT": "topic"}
_EV_FIELD_MAP = {
    "ID": "id", "NA": "name", "SS": "score", "TM": "minute", "TS": "seconds",
    "TT": "time_type", "TU": "timestamp", "CL": "sport_id", "C2": "competition_id",
    "C3": "fixture_id", "IT": "topic", "UC": "update_context", "GO": "goals",
}
_MA_FIELD_MAP = {
    "ID": "id", "MA": "market_type", "NA": "name", "FI": "fixture_id",
    "CN": "columns", "SU": "suspended", "IT": "topic",
}
_PA_FIELD_MAP = {
    "ID": "id", "NA": "name", "OD": "odds", "OR": "order", "FI": "fixture_id",
    "HA": "handicap", "HD": "handicap_display", "SU": "suspended", "IT": "topic",
}

_FIELD_MAPS: Dict[str, Dict[str, str]] = {
    "CL": _CL_FIELD_MAP,
    "CT": _CT_FIELD_MAP,
    "EV": _EV_FIELD_MAP,
    "MA": _MA_FIELD_MAP,
    "PA": _PA_FIELD_MAP,
}


def _parse_kv_pairs(segment: str) -> Dict[str, str]:
    """
    Parse ``key=value;key=value;...`` pairs from a pipe-separated entity body.

    bet365 uses semicolons **or** the pattern ``AB=value`` where each
    two-letter key is followed by ``=``.  In practice the fields within an
    entity block are separated by ``;``.
    """
    pairs: Dict[str, str] = {}
    for token in segment.split(";"):
        if "=" in token:
            k, _, v = token.partition("=")
            pairs[k] = v
    return pairs


def _build_entity(entity_type: str, kv: Dict[str, str], parent_id: str = "") -> Any:
    """Instantiate the correct dataclass from *entity_type* and *kv* pairs."""
    cls = _ENTITY_TYPE_MAP.get(entity_type)
    if cls is None:
        return None
    fmap = _FIELD_MAPS.get(entity_type, {})
    kwargs: Dict[str, Any] = {"raw": dict(kv)}
    for zap_key, attr_name in fmap.items():
        if zap_key in kv:
            kwargs[attr_name] = kv[zap_key]

    obj = cls(**kwargs)

    # Attach parent linkage when not explicitly present in the data.
    if entity_type == "CT" and not obj.sport_id and parent_id:  # type: ignore[union-attr]
        obj.sport_id = parent_id  # type: ignore[union-attr]
    if entity_type == "MA" and not obj.event_id and parent_id:  # type: ignore[union-attr]
        obj.event_id = parent_id  # type: ignore[union-attr]
    if entity_type == "PA" and not obj.market_id and parent_id:  # type: ignore[union-attr]
        obj.market_id = parent_id  # type: ignore[union-attr]

    return obj


def _apply_kv_to_entity(entity: Any, entity_type: str, kv: Dict[str, str]) -> Dict[str, Tuple[str, str]]:
    """
    Mutate *entity* in-place with new *kv* pairs.  Returns a dict of
    ``{attr_name: (old_value, new_value)}`` for fields that actually changed.
    """
    fmap = _FIELD_MAPS.get(entity_type, {})
    changed: Dict[str, Tuple[str, str]] = {}
    for zap_key, value in kv.items():
        attr = fmap.get(zap_key)
        if attr:
            old = getattr(entity, attr, None)
            if old != value:
                changed[attr] = (str(old), value)
                setattr(entity, attr, value)
        # Always update raw.
        entity.raw[zap_key] = value
    return changed


# ---------------------------------------------------------------------------
# Payload parsing helpers
# ---------------------------------------------------------------------------

def _parse_entity_block(block: str) -> Tuple[str, Dict[str, str]]:
    """
    Parse a single entity block like ``EV;ID=12345;NA=Foo vs Bar;SS=1-0``.

    Returns ``(entity_type_code, kv_dict)``.
    """
    parts = block.split(";", 1)
    etype = parts[0].strip()
    kv: Dict[str, str] = {}
    if len(parts) > 1:
        kv = _parse_kv_pairs(parts[1])
    return etype, kv


def _parse_entities_from_payload(payload: str) -> List[Tuple[str, Dict[str, str]]]:
    """
    Split an INITIAL_TOPIC_LOAD payload (after ``F|`` or similar) into
    individual entity blocks separated by ``|``.
    """
    result: List[Tuple[str, Dict[str, str]]] = []
    blocks = payload.split("|")
    for blk in blocks:
        blk = blk.strip()
        if not blk:
            continue
        etype, kv = _parse_entity_block(blk)
        if etype in _ENTITY_TYPE_MAP:
            result.append((etype, kv))
    return result


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

# Callback signature:  (change_event: ChangeEvent) -> None
ChangeCallback = Callable[[ChangeEvent], None]


class ZapParser:
    """
    Stateful parser for the bet365 ZAP WebSocket protocol.

    Usage::

        parser = ZapParser()
        parser.on_change(my_callback)

        # Feed raw WebSocket frames (str or bytes):
        parser.feed(raw_frame)

        # Query the live state:
        events = parser.get_all_live_events()
        ev     = parser.get_event("12345")
    """

    def __init__(self) -> None:
        self.state = StateTree()
        self._callbacks: List[ChangeCallback] = []
        self.connection_id: Optional[str] = None
        self.protocol_version: Optional[str] = None
        # Track current "parent" context per topic so we can assign children.
        self._topic_parent: Dict[str, str] = {}

    # -- callback registration -----------------------------------------------

    def on_change(self, callback: ChangeCallback) -> None:
        """Register a callback invoked on every state change."""
        self._callbacks.append(callback)

    def remove_callback(self, callback: ChangeCallback) -> None:
        """Remove a previously registered callback."""
        self._callbacks.remove(callback)

    def _emit(self, change: ChangeEvent) -> None:
        for cb in self._callbacks:
            try:
                cb(change)
            except Exception:
                logger.exception("Error in change callback for %s", change.change_type)

    # -- feed ----------------------------------------------------------------

    def feed(self, data: str | bytes) -> List[ParsedMessage]:
        """
        Parse a raw WebSocket frame.

        *data* may be ``str`` or ``bytes``.  Multiple sub-messages separated
        by ``0x08`` (BS) are handled.  Returns a list of ``ParsedMessage``
        objects, one per sub-frame.
        """
        if isinstance(data, (bytes, bytearray, memoryview)):
            raw = bytes(data)
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                # ZAP protocol may contain Latin-1 encoded fields
                text = raw.decode("latin-1")
        else:
            text = data

        # Fix double-encoding: Playwright may decode binary WS frames as Latin-1
        # but the content is actually UTF-8. Detect and fix Ã-style mojibake.
        if any(c in text for c in "ÃÂ"):
            try:
                fixed = text.encode("latin-1").decode("utf-8")
                text = fixed
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass  # Not double-encoded, keep original

        # Strip trailing NUL terminator(s).
        text = text.rstrip(NUL)

        # Split on BS (0x08) for batched sub-frames.
        sub_frames = text.split(BS)
        results: List[ParsedMessage] = []
        for sf in sub_frames:
            sf = sf.strip()
            if not sf:
                continue
            msg = self._parse_sub_frame(sf)
            if msg:
                results.append(msg)
        return results

    # -- sub-frame dispatch --------------------------------------------------

    def _parse_sub_frame(self, frame: str) -> Optional[ParsedMessage]:
        if not frame:
            return None

        first = ord(frame[0])

        if first == BYTE_TOPIC_STATUS:
            return self._parse_topic_status(frame)
        elif first == BYTE_INITIAL_LOAD:
            return self._parse_initial_load(frame)
        elif first == BYTE_DELTA:
            return self._parse_delta(frame)
        elif first == BYTE_CLIENT_SUB:
            return self._parse_client_subscribe(frame)
        elif first == BYTE_SERVER_ACK:
            return self._parse_server_ack(frame)
        else:
            logger.debug("Unknown message type byte 0x%02X", first)
            return ParsedMessage(msg_type=MessageType.UNKNOWN, raw=frame)

    # -- message-type parsers ------------------------------------------------

    def _parse_topic_status(self, frame: str) -> ParsedMessage:
        """
        Parse ``# <ETX> <topic_count> ...`` handshake / topic status.
        """
        msg = ParsedMessage(msg_type=MessageType.TOPIC_STATUS_NOTIFICATION, raw=frame)
        body = frame[1:]  # skip '#'
        if ETX in body:
            parts = body.split(ETX, 1)
            msg.extra["prefix"] = parts[0]
            msg.extra["topic_count"] = parts[1].strip()
        else:
            msg.extra["body"] = body
        return msg

    def _parse_server_ack(self, frame: str) -> ParsedMessage:
        """
        Parse ``1XX <STX> ConnectionID`` server acknowledgment.
        """
        msg = ParsedMessage(msg_type=MessageType.SERVER_ACK, raw=frame)
        if STX in frame:
            code, _, conn_id = frame.partition(STX)
            msg.protocol_version = code.strip()
            msg.connection_id = conn_id.strip().rstrip(NUL)
            self.protocol_version = msg.protocol_version
            self.connection_id = msg.connection_id
        else:
            msg.extra["body"] = frame
        return msg

    def _parse_client_subscribe(self, frame: str) -> ParsedMessage:
        """Parse a CLIENT_SUBSCRIBE echo (0x16)."""
        msg = ParsedMessage(msg_type=MessageType.CLIENT_SUBSCRIBE, raw=frame)
        body = frame[1:]
        if SOH in body:
            topic, _, payload = body.partition(SOH)
            msg.topic = topic
            msg.extra["payload"] = payload
        else:
            msg.extra["body"] = body
        return msg

    def _parse_initial_load(self, frame: str) -> ParsedMessage:
        """
        Parse INITIAL_TOPIC_LOAD (0x14).

        Format: ``0x14 <topic> SOH F| <entity1> | <entity2> | ...``
        """
        body = frame[1:]
        topic = ""
        payload = ""
        if SOH in body:
            topic, _, payload = body.partition(SOH)
        else:
            payload = body

        entities = _parse_entities_from_payload(payload)

        msg = ParsedMessage(
            msg_type=MessageType.INITIAL_TOPIC_LOAD,
            topic=topic,
            operation=DeltaOp.FULL,
            entities=[kv for _, kv in entities],
            raw=frame,
        )

        # Apply to state tree.
        self._apply_full_load(topic, entities)
        return msg

    def _parse_delta(self, frame: str) -> ParsedMessage:
        """
        Parse DELTA (0x15).

        Format: ``0x15 <topic_IT> SOH <op>| <kv pairs> |``

        When the payload contains no entity-type markers (e.g. ``U|TU=...;|``),
        the *topic* string identifies the target entity via ``topic_index``.
        """
        body = frame[1:]
        topic = ""
        payload = ""
        if SOH in body:
            topic, _, payload = body.partition(SOH)
        else:
            payload = body

        # The first character(s) before '|' are the operation code.
        op_str = ""
        rest = payload
        if "|" in payload:
            op_str, _, rest = payload.partition("|")
            op_str = op_str.strip()

        op = _op_from_str(op_str)

        entities = _parse_entities_from_payload(rest)

        msg = ParsedMessage(
            msg_type=MessageType.DELTA,
            topic=topic,
            operation=op,
            entities=[kv for _, kv in entities],
            raw=frame,
        )

        if op == DeltaOp.FULL:
            self._apply_full_load(topic, entities)
        elif op == DeltaOp.UPDATE:
            if entities:
                self._apply_update(topic, entities)
            else:
                # Bare field update: resolve entity by topic IT.
                self._apply_topic_update(topic, rest)
        elif op == DeltaOp.INSERT:
            self._apply_insert(topic, entities)
        elif op == DeltaOp.DELETE:
            self._apply_delete(topic, entities, rest)

        return msg

    def _apply_topic_update(self, topic: str, raw_payload: str) -> None:
        """
        Apply a bare-field delta update where *topic* identifies the entity.

        The *raw_payload* contains ``key=val;key=val;|`` without entity type
        prefixes.  We look up the entity via ``topic_index`` and apply the
        key-value pairs directly.
        """
        # Parse bare kv pairs from the payload.
        kv: Dict[str, str] = {}
        for segment in raw_payload.split("|"):
            segment = segment.strip()
            if not segment:
                continue
            for token in segment.split(";"):
                if "=" in token:
                    k, _, v = token.partition("=")
                    kv[k] = v

        if not kv:
            return

        # Resolve entity by topic.
        entry = self.state.topic_index.get(topic)
        if entry:
            etype, eid = entry
            self._apply_entity_update(etype, eid, kv, topic)
            return

        # Fallback: try to match topic against all entity IT fields.
        for ev in self.state.events.values():
            if ev.topic == topic:
                old_score = ev.score
                changed = _apply_kv_to_entity(ev, "EV", kv)
                ct = ChangeType.SCORE_CHANGE if "score" in changed else ChangeType.EVENT_UPDATED
                self._emit(ChangeEvent(ct, "EV", ev.id, topic, {"old_score": old_score, **changed}, ev))
                return
        for sel in self.state.selections.values():
            if sel.topic == topic:
                old_odds = sel.odds
                changed = _apply_kv_to_entity(sel, "PA", kv)
                ct = ChangeType.ODDS_CHANGE if "odds" in changed else ChangeType.SELECTION_UPDATED
                self._emit(ChangeEvent(ct, "PA", sel.id, topic, {"old_odds": old_odds, **changed}, sel))
                return
        for mkt in self.state.markets.values():
            if mkt.topic == topic:
                changed = _apply_kv_to_entity(mkt, "MA", kv)
                self._emit(ChangeEvent(ChangeType.MARKET_UPDATED, "MA", mkt.id, topic, changed, mkt))
                return

    # -- state application ---------------------------------------------------

    def _apply_full_load(
        self, topic: str, entities: List[Tuple[str, Dict[str, str]]]
    ) -> None:
        """Apply a full snapshot to the state tree.

        Entity IDs in ZAP data:
        - CL: ``CL`` field (sport ID) is the unique key, ``ID`` is local.
        - CT: ``ID`` is **non-unique** (sequence within block). We use the
          ``IT`` (topic) field as the unique key for competitions.
        - EV: ``ID`` (or ``IT``) is unique.  Events are linked to their parent
          competition by position (all EVs after a CT belong to that CT).
        - MA/PA: ``ID`` is **non-unique** across events (e.g. "1", "2").
          We use IT (topic) as the unique key, or composite parent:ID.
        """
        current_cl: str = ""
        current_ct: str = ""
        current_ev: str = ""
        current_ma: str = ""

        for etype, kv in entities:
            if etype == "CL":
                # Use the CL field (sport number) as the unique sport ID.
                sport_id = kv.get("CL", kv.get("ID", ""))
                existing = self.state.sports.get(sport_id)
                if existing:
                    # Merge new fields into existing sport (don't overwrite name with empty).
                    _apply_kv_to_entity(existing, etype, kv)
                    current_cl = sport_id
                    self._emit(ChangeEvent(ChangeType.EVENT_UPDATED, etype, sport_id, topic, None, existing))
                else:
                    obj = _build_entity(etype, kv)
                    if obj:
                        obj.id = sport_id
                        obj.sport_id = sport_id
                        self.state.upsert_sport(obj)
                        current_cl = sport_id
                        self._emit(ChangeEvent(ChangeType.SPORT_ADDED, etype, sport_id, topic, None, obj))

            elif etype == "CT":
                # CT IDs are non-unique; use IT (topic) as the unique key.
                ct_key = kv.get("IT", kv.get("ID", ""))
                obj = _build_entity(etype, kv, parent_id=current_cl)
                if obj:
                    obj.id = ct_key
                    old = self.state.upsert_competition(obj)
                    current_ct = ct_key
                    ctype = ChangeType.COMPETITION_ADDED if old is None else ChangeType.COMPETITION_UPDATED
                    self._emit(ChangeEvent(ctype, etype, ct_key, topic, old, obj))

            elif etype == "EV":
                eid = kv.get("ID", "")
                obj = _build_entity(etype, kv, parent_id="")
                if obj:
                    # Always link to current positional parent CT.
                    obj.competition_id = current_ct or obj.competition_id
                    if not obj.sport_id:
                        obj.sport_id = current_cl
                    old = self.state.upsert_event(obj)
                    current_ev = eid
                    ctype = ChangeType.EVENT_ADDED if old is None else ChangeType.EVENT_UPDATED
                    self._emit(ChangeEvent(ctype, etype, eid, topic, old, obj))

            elif etype == "MA":
                raw_id = kv.get("ID", "")
                parent = current_ev or self._infer_parent(topic, "EV")
                # Market IDs are non-unique across events (e.g. "1", "2", "3").
                # Use IT (topic) as unique key if available, else composite parent:ID.
                ma_key = kv.get("IT", "")
                if not ma_key:
                    ma_key = f"{parent}:{raw_id}" if parent else raw_id
                obj = _build_entity(etype, kv, parent_id=parent)
                if obj:
                    obj.id = ma_key
                    old = self.state.upsert_market(obj)
                    current_ma = ma_key
                    ctype = ChangeType.MARKET_ADDED if old is None else ChangeType.MARKET_UPDATED
                    self._emit(ChangeEvent(ctype, etype, ma_key, topic, old, obj))

            elif etype == "PA":
                raw_id = kv.get("ID", "")
                parent = current_ma or self._infer_parent(topic, "MA")
                # Selection IDs may also collide; use IT or composite key.
                pa_key = kv.get("IT", "")
                if not pa_key:
                    pa_key = f"{parent}:{raw_id}" if parent else raw_id
                obj = _build_entity(etype, kv, parent_id=parent)
                if obj:
                    obj.id = pa_key
                if obj:
                    old = self.state.upsert_selection(obj)
                    ctype = ChangeType.SELECTION_ADDED if old is None else ChangeType.SELECTION_UPDATED
                    self._emit(ChangeEvent(ctype, etype, eid, topic, old, obj))

        # Remember the topic's relationship to the last-seen entity for
        # future delta resolution.
        if topic:
            if current_ev:
                self._topic_parent[topic] = current_ev
            elif current_ct:
                self._topic_parent[topic] = current_ct
            elif current_cl:
                self._topic_parent[topic] = current_cl

    def _apply_update(
        self, topic: str, entities: List[Tuple[str, Dict[str, str]]]
    ) -> None:
        """Apply incremental updates (U delta).

        If *entities* is empty the payload contained only bare ``key=value``
        pairs without an entity-type prefix.  In that case we resolve the
        target entity via the *topic* string (which is the entity's IT field)
        using the ``topic_index``.
        """
        if entities:
            for etype, kv in entities:
                eid = kv.get("ID", "")
                self._apply_entity_update(etype, eid, kv, topic)
        else:
            # Bare field update addressed by topic IT.
            self._apply_bare_update(topic)

    def _apply_bare_update(self, topic: str) -> None:
        """Apply bare kv fields from a delta where the topic IT identifies the entity."""
        # The kv pairs were not extracted by _parse_entities_from_payload
        # because there was no entity type marker.  Re-parse from the raw
        # delta frame stored during _parse_delta.
        pass  # handled by _parse_delta rewrite below

    def _resolve_entity_id(self, etype: str, raw_id: str, kv: Dict[str, str]) -> str:
        """Resolve a raw entity ID to the composite key used in the state tree."""
        if etype in ("MA", "PA"):
            # Try IT (topic) first — it's always unique.
            it = kv.get("IT", "")
            if it and it in (self.state.markets if etype == "MA" else self.state.selections):
                return it
            # Try the raw ID directly (might already be a composite key).
            store = self.state.markets if etype == "MA" else self.state.selections
            if raw_id in store:
                return raw_id
            # Search for composite keys ending with :raw_id
            for key in store:
                if key.endswith(f":{raw_id}"):
                    return key
        return raw_id

    def _apply_entity_update(
        self, etype: str, eid: str, kv: Dict[str, str], topic: str
    ) -> None:
        # Resolve composite key for MA/PA types
        resolved_id = self._resolve_entity_id(etype, eid, kv)

        if etype == "CL":
            existing = self.state.sports.get(resolved_id)
            if existing:
                changed = _apply_kv_to_entity(existing, etype, kv)
                self._emit(ChangeEvent(ChangeType.EVENT_UPDATED, etype, resolved_id, topic, changed, existing))
        elif etype == "CT":
            existing = self.state.competitions.get(resolved_id)
            if existing:
                changed = _apply_kv_to_entity(existing, etype, kv)
                self._emit(ChangeEvent(ChangeType.COMPETITION_UPDATED, etype, resolved_id, topic, changed, existing))
        elif etype == "EV":
            existing = self.state.events.get(resolved_id)
            if existing:
                old_score = existing.score
                changed = _apply_kv_to_entity(existing, etype, kv)
                ct = ChangeType.SCORE_CHANGE if "score" in changed else ChangeType.EVENT_UPDATED
                self._emit(ChangeEvent(ct, etype, resolved_id, topic, {"old_score": old_score, **changed}, existing))
        elif etype == "MA":
            existing = self.state.markets.get(resolved_id)
            if existing:
                changed = _apply_kv_to_entity(existing, etype, kv)
                self._emit(ChangeEvent(ChangeType.MARKET_UPDATED, etype, resolved_id, topic, changed, existing))
        elif etype == "PA":
            existing = self.state.selections.get(resolved_id)
            if existing:
                old_odds = existing.odds
                changed = _apply_kv_to_entity(existing, etype, kv)
                ct = ChangeType.ODDS_CHANGE if "odds" in changed else ChangeType.SELECTION_UPDATED
                self._emit(ChangeEvent(ct, etype, resolved_id, topic, {"old_odds": old_odds, **changed}, existing))

    def _apply_insert(
        self, topic: str, entities: List[Tuple[str, Dict[str, str]]]
    ) -> None:
        """Apply insert delta -- same logic as full load for new entities."""
        self._apply_full_load(topic, entities)

    def _apply_delete(
        self, topic: str, entities: List[Tuple[str, Dict[str, str]]], raw_rest: str
    ) -> None:
        """
        Apply delete delta.

        Deletes may reference entities by ID or by a full topic path.
        """
        if entities:
            for etype, kv in entities:
                raw_id = kv.get("ID", "")
                if not raw_id:
                    continue
                # Resolve composite key for MA/PA
                eid = self._resolve_entity_id(etype, raw_id, kv)
                removed: Any = None
                ctype: Optional[ChangeType] = None
                if etype == "CL":
                    removed = self.state.remove_sport(eid)
                    ctype = ChangeType.SPORT_REMOVED
                elif etype == "CT":
                    removed = self.state.remove_competition(eid)
                    ctype = ChangeType.COMPETITION_REMOVED
                elif etype == "EV":
                    removed = self.state.remove_event(eid)
                    ctype = ChangeType.EVENT_REMOVED
                elif etype == "MA":
                    removed = self.state.remove_market(eid)
                    ctype = ChangeType.MARKET_REMOVED
                elif etype == "PA":
                    removed = self.state.remove_selection(eid)
                    ctype = ChangeType.SELECTION_REMOVED
                if removed and ctype:
                    self._emit(ChangeEvent(ctype, etype, eid, topic, removed))
        else:
            # The raw_rest may be a topic delete-path.
            path = raw_rest.strip().rstrip("|").strip()
            if path:
                changes = self.state.remove_by_path(path)
                for ch in changes:
                    self._emit(ch)

    def _infer_parent(self, topic: str, parent_type: str) -> str:
        """Try to look up a parent entity ID for *topic* from previous loads."""
        return self._topic_parent.get(topic, "")

    # -- query methods -------------------------------------------------------

    @staticmethod
    def _decimal_odds(frac: str) -> str:
        """Convert fractional odds '5/6' to decimal '1.83'. Pass-through if already decimal."""
        if not frac or "/" not in frac:
            return frac
        try:
            num, den = frac.split("/", 1)
            return str(round(float(num) / float(den) + 1, 2))
        except (ValueError, ZeroDivisionError):
            return frac

    def get_all_live_events(self) -> List[Dict[str, Any]]:
        """
        Return all live events with scores, time info, and top-level odds.

        Each item is a dict with event fields plus a ``selections`` list
        containing each selection's odds.
        """
        results: List[Dict[str, Any]] = []
        for ev in self.state.events.values():
            entry: Dict[str, Any] = {
                "id": ev.id,
                "name": ev.name,
                "score": ev.score,
                "minute": ev.minute,
                "seconds": ev.seconds,
                "time_type": ev.time_type,
                "timestamp": ev.timestamp,
                "sport_id": ev.sport_id,
                "competition_id": ev.competition_id,
                "fixture_id": ev.fixture_id,
                "topic": ev.topic,
                "goals": ev.goals,
                "markets": [],
            }
            market_ids = self.state.event_markets.get(ev.id, set())
            for mid in market_ids:
                mkt = self.state.markets.get(mid)
                if mkt is None:
                    continue
                mkt_dict: Dict[str, Any] = {
                    "id": mkt.id,
                    "name": mkt.name,
                    "market_type": mkt.market_type,
                    "suspended": mkt.suspended,
                    "selections": [],
                }
                sel_ids = self.state.market_selections.get(mid, set())
                for sid in sel_ids:
                    sel = self.state.selections.get(sid)
                    if sel:
                        mkt_dict["selections"].append({
                            "id": sel.id,
                            "name": sel.name,
                            "odds": self._decimal_odds(sel.odds),
                            "order": sel.order,
                            "handicap": sel.handicap,
                            "suspended": sel.suspended,
                        })
                entry["markets"].append(mkt_dict)
            results.append(entry)
        return results

    def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Return detailed info for a single event including all markets and
        selections.  Returns ``None`` if the event is not in the state tree.
        """
        ev = self.state.events.get(event_id)
        if ev is None:
            return None

        result: Dict[str, Any] = {
            "id": ev.id,
            "name": ev.name,
            "score": ev.score,
            "minute": ev.minute,
            "seconds": ev.seconds,
            "time_type": ev.time_type,
            "timestamp": ev.timestamp,
            "sport_id": ev.sport_id,
            "competition_id": ev.competition_id,
            "fixture_id": ev.fixture_id,
            "topic": ev.topic,
            "update_context": ev.update_context,
            "goals": ev.goals,
            "raw": ev.raw,
            "markets": [],
        }

        # Attach competition name if available.
        comp = self.state.competitions.get(ev.competition_id)
        if comp:
            result["competition_name"] = comp.name

        # Attach sport name if available.
        sport = self.state.sports.get(ev.sport_id)
        if sport:
            result["sport_name"] = sport.name

        for mid in sorted(self.state.event_markets.get(ev.id, set())):
            mkt = self.state.markets.get(mid)
            if mkt is None:
                continue
            mkt_dict: Dict[str, Any] = {
                "id": mkt.id,
                "name": mkt.name,
                "market_type": mkt.market_type,
                "columns": mkt.columns,
                "suspended": mkt.suspended,
                "raw": mkt.raw,
                "selections": [],
            }
            for sid in sorted(self.state.market_selections.get(mid, set())):
                sel = self.state.selections.get(sid)
                if sel:
                    mkt_dict["selections"].append({
                        "id": sel.id,
                        "name": sel.name,
                        "odds": self._decimal_odds(sel.odds),
                        "order": sel.order,
                        "handicap": sel.handicap,
                        "handicap_display": sel.handicap_display,
                        "suspended": sel.suspended,
                        "raw": sel.raw,
                    })
            result["markets"].append(mkt_dict)
        return result

    def get_sport_events(self, sport_id: str) -> List[Dict[str, Any]]:
        """
        Return all events for a given sport, with basic market/selection data.
        """
        results: List[Dict[str, Any]] = []
        # Collect all competition IDs for this sport.
        comp_ids = self.state.sport_competitions.get(sport_id, set())
        target_event_ids: set = set()
        for cid in comp_ids:
            target_event_ids.update(self.state.comp_events.get(cid, set()))

        # Also include events that reference this sport directly but may not
        # have a mapped competition.
        for ev in self.state.events.values():
            if ev.sport_id == sport_id:
                target_event_ids.add(ev.id)

        for eid in sorted(target_event_ids):
            detail = self.get_event(eid)
            if detail:
                results.append(detail)
        return results

    # -- convenience / debug -------------------------------------------------

    def summary(self) -> Dict[str, int]:
        """Return counts of each entity type in the state tree."""
        return {
            "sports": len(self.state.sports),
            "competitions": len(self.state.competitions),
            "events": len(self.state.events),
            "markets": len(self.state.markets),
            "selections": len(self.state.selections),
        }

    def __repr__(self) -> str:
        s = self.summary()
        return (
            f"<ZapParser sports={s['sports']} comps={s['competitions']} "
            f"events={s['events']} markets={s['markets']} sels={s['selections']}>"
        )


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _op_from_str(s: str) -> DeltaOp:
    s = s.strip().upper()
    if s == "F":
        return DeltaOp.FULL
    elif s == "U":
        return DeltaOp.UPDATE
    elif s == "I":
        return DeltaOp.INSERT
    elif s == "D":
        return DeltaOp.DELETE
    # Default to FULL for unrecognised codes.
    return DeltaOp.FULL
