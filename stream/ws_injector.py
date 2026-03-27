"""
mitmproxy addon: Inject 6V subscribe messages into bet365 premws WebSocket.

This runs as part of mitmproxy. When it detects a premws WebSocket connection,
it waits for the ZAP handshake to complete, captures the auth token from
outgoing messages, then injects 6V detail subscribe messages.

The injected messages use the same format as the browser's own subscribes:
  0x16 0x00 topic1,topic2,...,A_<token> 0x01
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from mitmproxy import ctx, http, websocket

log = logging.getLogger("ws_injector")

# Will be set by the main script via shared state
shared_state: dict[str, Any] = {
    "token": "",
    "topics_to_subscribe": [],  # list of 6V topic strings
    "subscribed_topics": set(),
    "premws_flow": None,
    "ready": False,
    "inject_queue": [],  # list of subscribe payloads to inject
}


class Bet365WSInjector:
    """mitmproxy addon that injects WebSocket frames into premws connections."""

    def __init__(self, state: dict[str, Any] | None = None):
        self.state = state or shared_state
        self._premws_flows: list[http.HTTPFlow] = []
        self._token = ""
        self._handshake_done = False

    def websocket_start(self, flow: http.HTTPFlow):
        """Called when a WebSocket connection is established."""
        if "premws" in flow.request.url:
            log.info("premws WebSocket detected: %s", flow.request.url[:80])
            self._premws_flows.append(flow)
            self.state["premws_flow"] = flow
            self._handshake_done = False

    def websocket_message(self, flow: http.HTTPFlow):
        """Called for every WebSocket message (both directions)."""
        if "premws" not in flow.request.url:
            return
        assert flow.websocket is not None
        msg = flow.websocket.messages[-1]

        # Server -> Client: check for ACK (0x31)
        if msg.from_client is False:
            content = msg.content
            if isinstance(content, bytes) and len(content) > 0 and content[0] == 0x31:
                self._handshake_done = True
                self.state["ready"] = True
                log.info("ZAP handshake complete on premws")

        # Client -> Server: extract auth token
        if msg.from_client is True:
            content = msg.content
            text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
            idx = text.find(",A_")
            if idx >= 0:
                start = idx + 3
                end = start
                chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
                while end < len(text) and text[end] in chars:
                    end += 1
                if end - start > 20:
                    self._token = text[start:end]
                    self.state["token"] = self._token

        # Process inject queue
        if self._handshake_done and self._token and self.state.get("inject_queue"):
            self._process_queue(flow)

    def _process_queue(self, flow: http.HTTPFlow):
        """Inject queued subscribe messages."""
        queue = self.state.get("inject_queue", [])
        while queue:
            topics_csv = queue.pop(0)
            payload = (
                chr(0x16) + chr(0x00)
                + topics_csv + ",A_" + self._token
                + chr(0x01)
            )
            ctx.master.commands.call(
                "inject.websocket", flow, False, payload.encode("utf-8")
            )
            log.info("Injected subscribe: %d chars", len(topics_csv))

    def websocket_end(self, flow: http.HTTPFlow):
        """Called when a WebSocket connection closes."""
        if "premws" in flow.request.url:
            log.info("premws WebSocket closed")
            if flow in self._premws_flows:
                self._premws_flows.remove(flow)
            if self.state.get("premws_flow") is flow:
                self.state["premws_flow"] = None
                self.state["ready"] = False


def create_addon(state: dict[str, Any] | None = None) -> Bet365WSInjector:
    return Bet365WSInjector(state)
