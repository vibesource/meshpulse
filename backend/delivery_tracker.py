"""Correlate opt-in text sends with Meshtastic routing packets.

This module is a development candidate; it is not wired into MeshPulse yet.
It uses no radio interface and never transmits or retries a packet.
"""
import threading
import time
from collections import deque


class DeliveryTracker:
    def __init__(self, max_early=128, early_ttl=30, clock=time.monotonic, pending_ttl=3600):
        self._lock = threading.Lock()
        self._pending = {}
        self._early = deque(maxlen=max_early)
        self._sending = 0
        self._early_ttl = early_ttl
        self._clock = clock
        self._pending_ttl = pending_ttl

    def _prune(self):
        now = self._clock()
        for packet_id, item in list(self._pending.items()):
            if now - item['created'] > self._pending_ttl:
                del self._pending[packet_id]

    def begin_send(self):
        """Allow brief buffering until the library returns the sent packet ID."""
        with self._lock:
            self._prune()
            self._sending += 1

    def end_send(self, packet_id=None, destination=None, local_node=None, context=None):
        """Register a sent packet and return any events received during send."""
        with self._lock:
            if self._sending <= 0:
                raise RuntimeError('Unbalanced send registration')
            self._sending -= 1
            events = []
            if packet_id is not None:
                if packet_id in self._pending:
                    raise ValueError('Duplicate packet ID')
                self._pending[packet_id] = {'destination': destination, 'local': local_node,
                                            'context': context, 'state': 'submitted',
                                            'created': self._clock()}
                for when, packet in self._early:
                    if self._clock() - when <= self._early_ttl:
                        event = self._apply(packet)
                        if event:
                            events.append(event)
            if not self._sending:
                self._early.clear()
            return events

    def receive(self, packet):
        """Return an update for a correlated routing packet, if it proves one."""
        decoded = packet.get('decoded') or {}
        if decoded.get('portnum') not in ('ROUTING_APP', 5):
            return None
        request_id = decoded.get('requestId')
        if not isinstance(request_id, int) or not request_id:
            return None
        with self._lock:
            self._prune()
            if request_id not in self._pending:
                if self._sending:
                    self._early.append((self._clock(), packet))
                return None
            return self._apply(packet)

    def _apply(self, packet):
        decoded = packet.get('decoded') or {}
        item = self._pending.get(decoded.get('requestId'))
        if not item:
            return None
        routing = decoded.get('routing')
        if not isinstance(routing, dict) or 'errorReason' not in routing:
            return None
        reason = routing['errorReason']
        sender = packet.get('from')
        destination, local = item['destination'], item['local']
        if packet.get('to') != local:
            return None
        if reason not in ('NONE', 0):
            if sender != local and (destination is None or sender != destination):
                return None
            state = 'failed'
        elif destination is not None and sender == destination:
            state = 'delivered_recipient'
        elif sender == local:
            state = 'delivered_mesh' if destination is None else 'relayed'
        else:
            return None
        if item['state'] in ('unknown', 'unconfirmed') and state == 'relayed':
            state = 'unconfirmed'
        if item['state'] in ('delivered_recipient', 'delivered_mesh'):
            return None
        # A late recipient acknowledgment is stronger evidence than an earlier
        # radio failure, which cannot prove the message was never delivered.
        if item['state'] == 'failed' and state not in ('delivered_recipient', 'delivered_mesh'):
            return None
        if item['state'] == state:
            return None
        item['state'] = state
        if isinstance(item['context'], dict):
            item['context']['delivery_status'] = state
            item['context']['delivery_revision'] = item['context'].get('delivery_revision', 0) + 1
            item['context']['delivery_error'] = reason if state == 'failed' else None
        return {'packet_id': decoded['requestId'], 'context': item['context'],
                'state': state, 'reason': reason if state == 'failed' else None}

    def forget(self, packet_id):
        with self._lock:
            self._pending.pop(packet_id, None)

    def mark_unconfirmed(self, packet_id):
        """Stop implying a reply is imminent, without claiming non-delivery."""
        with self._lock:
            item = self._pending.get(packet_id)
            if not item or item['state'] not in ('submitted', 'relayed'):
                return None
            state = 'unconfirmed' if item['state'] == 'relayed' else 'unknown'
            item['state'] = state
            message = item['context']
            if isinstance(message, dict):
                message['delivery_status'] = state
                message['delivery_revision'] = message.get('delivery_revision', 0) + 1
                message['delivery_error'] = None
            return message
