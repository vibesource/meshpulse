import ast
import asyncio
import importlib.util
import json
import sys
import threading
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch

root = Path(__file__).resolve().parents[1] / 'backend'
spec = importlib.util.spec_from_file_location('delivery_tracker', root / 'delivery_tracker.py')
tracker_module = importlib.util.module_from_spec(spec)
sys.modules['delivery_tracker'] = tracker_module
spec.loader.exec_module(tracker_module)
tree = ast.parse((root / 'meshpulse.py').read_text())
klass = next(x for x in tree.body if isinstance(x, ast.ClassDef) and x.name == 'ListenBasedMapper')
send_method = next(x for x in klass.body if isinstance(x, ast.FunctionDef) and x.name == 'send_tracked_text_via_listener')
run_method = next(x for x in tree.body if isinstance(x, ast.AsyncFunctionDef) and x.name == 'run_send_message')
ns = {'asyncio':asyncio, 'json':json, 'time':time}
exec(compile(ast.fix_missing_locations(ast.Module(body=[send_method, run_method], type_ignores=[])), '<candidate>', 'exec'), ns)


class Interface:
    def __init__(self, mapper):
        self.mapper = mapper
        self.isConnected = threading.Event()
        self.isConnected.set()
        self.calls = []
        self.early = False
    def sendText(self, text, **kwargs):
        self.calls.append((text, kwargs))
        if self.early:
            self.mapper._delivery_tracker.receive({'from': 2, 'to': 1, 'decoded': {
                'portnum':'ROUTING_APP', 'requestId':99, 'routing':{'errorReason':'NONE'}}})
        return types.SimpleNamespace(id=99)


class Mapper:
    send_tracked_text_via_listener = ns['send_tracked_text_via_listener']
    def __init__(self):
        self._text_send_lock = threading.Lock()
        self._port_hold = threading.Event()
        self._delivery_tracker = tracker_module.DeliveryTracker()
        self.iface = Interface(self)
        self.local_node_id = '!00000001'
        self.connection_type = 'tcp'
        self.messages = {}
        self.broadcasts = []
        self.legacy = []
    def _mesh_iface(self): return self.iface
    def send_text_via_listener(self, *args): self.legacy.append(args)
    async def broadcast_message(self, message): self.broadcasts.append(message.copy())


class Socket:
    def __init__(self): self.events = []
    async def send(self, data): self.events.append(json.loads(data))


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mapper = Mapper()
        ns['mapper'] = self.mapper
        self.socket = Socket()
    async def test_tracked_send_preserves_legacy_result_and_early_ack(self):
        self.mapper.iface.early = True
        await ns['run_send_message']('hello', 0, '!00000002', self.socket, 'test-id')
        self.assertTrue(next(x for x in self.socket.events if x['type'] == 'send_result')['success'])
        self.assertEqual(self.mapper.iface.calls[0][1]['wantAck'], True)
        self.assertEqual(self.mapper.broadcasts[0]['delivery_status'], 'delivered_recipient')
        self.assertEqual(self.mapper.broadcasts[0]['message_id'], 'test-id')
    async def test_legacy_send_does_not_request_acknowledgment(self):
        await ns['run_send_message']('hello', 1, None, self.socket)
        self.assertEqual(self.mapper.legacy, [('hello', None, 1)])
        self.assertNotIn('delivery_status', self.mapper.broadcasts[0])
    async def test_request_id_is_echoed_for_opted_in_send(self):
        await ns['run_send_message']('hello', 0, '!00000002', self.socket, 'test-id', 'request-42')
        result = next(x for x in self.socket.events if x['type'] == 'send_result')
        self.assertTrue(result['success'])
        self.assertEqual(result['request_id'], 'request-42')

    async def test_disconnected_send_reports_failure(self):
        self.mapper.iface.isConnected.clear()
        await ns['run_send_message']('hello', 0, '!00000002', self.socket, 'test-id')
        self.assertFalse(next(x for x in self.socket.events if x['type'] == 'send_result')['success'])
        self.assertFalse(self.mapper.broadcasts)

    async def test_silent_dm_becomes_unknown_then_late_ack_updates(self):
        await ns['run_send_message']('hello', 0, '!00000002', self.socket, 'test-id')
        message = self.mapper._delivery_tracker.mark_unconfirmed(99)
        self.assertEqual(message['delivery_status'], 'unknown')
        ack = {'from': 2, 'to': 1, 'decoded': {'portnum':'ROUTING_APP',
               'requestId':99, 'routing':{'errorReason':'NONE'}}}
        self.mapper._delivery_tracker.receive(ack)
        self.assertEqual(message['delivery_status'], 'delivered_recipient')

    async def test_timeout_does_not_discard_late_send(self):
        original_wait = asyncio.wait_for
        async def immediate_timeout(future, timeout):
            raise asyncio.TimeoutError
        with patch.object(asyncio, 'wait_for', immediate_timeout):
            await ns['run_send_message']('hello', 0, '!00000002', self.socket, 'test-id')
        self.assertIn('unknown', self.socket.events[-2]['message'])
        for _ in range(100):
            if self.mapper.broadcasts: break
            await asyncio.sleep(.01)
        self.assertEqual(self.mapper.broadcasts[0]['message_id'], 'test-id')
        self.assertGreaterEqual(self.mapper.broadcasts[0]['delivery_revision'], 2)


if __name__ == '__main__': unittest.main()
