"Messaging through the websocket mux, on one shared kernel: stdin stamping, reply routing, interrupts, the replay guard, binary buffers, key order, and the startup bound."
import os, time, httpx
from contextlib import contextmanager
from websockets.sync.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed
from jupywire.session import Session, to_frame
from helpers import _connect, _exec, _recv_until, _new_kernel


@contextmanager
def client(gateway, kid, key=b'x'):
    "A session and a websocket on kernel `kid`, closed afterwards."
    ses = Session(key=key)
    with _connect(gateway, kid, ses) as ws: yield ses, ws


def test_messaging_story(gateway, shared_kernel):
    "Clients share a kernel through the websocket mux. Each gets its own replies and answers its own `input()`. An interrupt on the control channel stops a cell, a resent frame is dropped as a replay, large binary buffers survive both ways, and messages keep their key order. A new kernel is ready within 3 seconds."
    kid = shared_kernel
    # Two clients' `input()`s wait at once, and each `input_reply`, sent without a parent, answers its own cell.
    with client(gateway, kid, b'a') as (sa, wa), client(gateway, kid, b'b') as (sb, wb):
        ctl = sa.msg('create_subshell_request', {})
        ctl['channel'] = 'control'
        wa.send(to_frame(ctl))
        sub = _recv_until(wa, lambda m: m['header']['msg_type']=='create_subshell_reply')['content']['subshell_id']
        ma = _exec(sa, "ans_a = input('A?')")
        ma['header']['subshell_id'] = sub
        wa.send(to_frame(ma))
        for ws in (wa, wb):
            m = _recv_until(ws, lambda m: m['parent_header'].get('msg_id')==ma['header']['msg_id'])
            assert (m['channel'], m['header']['msg_type'], m['content']) == ('iopub', 'execute_queued', {})
            assert m['parent_header']['subshell_id'] == sub
        _recv_until(wa, lambda m: m['header']['msg_type']=='input_request')
        wb.send(to_frame(_exec(sb, "ans_b = input('B?')")))
        _recv_until(wb, lambda m: m['header']['msg_type']=='input_request')
        for ses, ws, val in ((sa, wa, 'alpha'), (sb, wb, 'beta')):
            r = ses.msg('input_reply', dict(value=val))
            r['channel'] = 'stdin'   # no parent_header: the gateway must stamp it
            ws.send(to_frame(r))
        _recv_until(wa, lambda m: m['channel']=='shell')
        _recv_until(wb, lambda m: m['channel']=='shell')
        wb.send(to_frame(_exec(sb, "print(ans_a, ans_b)")))
        assert _recv_until(wb, lambda m: m['header']['msg_type']=='stream')['content']['text'].strip() == 'alpha beta'
    # Interleaved executes from two clients: every reply reaches its sender only.
    with client(gateway, kid, b'a') as (sa, wa), client(gateway, kid, b'b') as (sb, wb):
        for fa, fb in zip([to_frame(_exec(sa, f"{i}+{i}")) for i in range(5)], [to_frame(_exec(sb, f"{i}*{i}")) for i in range(5)]):
            wa.send(fa)
            wb.send(fb)
        for ws, ses in ((wa, sa), (wb, sb)):
            for _ in range(5): assert _recv_until(ws, lambda m: m['channel']=='shell')['parent_header']['session'] == ses.session

    # An `interrupt_request` on the control channel, through the mux, stops a running cell.
    with client(gateway, kid) as (ses, ws):
        ws.send(to_frame(_exec(ses, "import time\nwhile True: time.sleep(0.05)")))
        _recv_until(ws, lambda m: m['header']['msg_type']=='status' and m['content'].get('execution_state')=='busy')
        imsg = ses.msg('interrupt_request', {})
        imsg['channel'] = 'control'
        ws.send(to_frame(imsg))
        _recv_until(ws, lambda m: m['header']['msg_type']=='interrupt_reply')
        assert _recv_until(ws, lambda m: m['channel']=='shell')['content']['ename'] == 'KeyboardInterrupt'
    # Deterministic HMAC signing means a byte-identical resend is dropped by the kernel as a replay.
    with client(gateway, kid) as (ses, ws):
        msg, sentinel = _exec(ses, "1+1"), _exec(ses, "2+2")
        frame = to_frame(msg)
        ws.send(frame)
        ws.send(frame)
        ws.send(to_frame(sentinel))
        m = _recv_until(ws, lambda m: m['channel']=='shell')
        assert m['content']['status'] == 'ok' and m['parent_header']['msg_id'] == msg['header']['msg_id']
        m = _recv_until(ws, lambda m: m['channel']=='shell')
        assert m['parent_header']['msg_id'] == sentinel['header']['msg_id'], 'the duplicate produced a second reply'
    # A comm message with several large binary buffers survives both directions intact.
    with client(gateway, kid) as (ses, ws):
        setup = '''from comm import create_comm
def _echo(comm, open_msg):
    @comm.on_msg
    def _(msg): comm.send(data=msg['content']['data'], buffers=msg['buffers'])
get_ipython().kernel.comm_manager.register_target('echo', _echo)'''
        ws.send(to_frame(_exec(ses, setup)))
        _recv_until(ws, lambda m: m['channel']=='shell')
        op = ses.msg('comm_open', dict(comm_id='c1', target_name='echo', data={}))
        op['channel'] = 'shell'
        ws.send(to_frame(op))
        big = os.urandom(2*2**20)
        cm = ses.msg('comm_msg', dict(comm_id='c1', data=dict(n=2)))
        cm['channel'] = 'shell'
        cm['buffers'] = [big, b'tail']
        ws.send(to_frame(cm))
        assert [bytes(b) for b in _recv_until(ws, lambda m: m['header']['msg_type']=='comm_msg')['buffers']] == [big, b'tail']
    # Messages keep their key insertion order, so user_expressions run in order.
    with client(gateway, kid) as (ses, ws):
        exprs = {'__res': 'v', '__del': "globals().pop('v', None)"}
        msg = ses.msg('execute_request', dict(code='v = 42', silent=False, store_history=False,
            user_expressions=exprs, allow_stdin=False, stop_on_error=False))
        msg['channel'] = 'shell'
        ws.send(to_frame(msg))
        ue = _recv_until(ws, lambda m: m['header']['msg_type'] == 'execute_reply')['content']['user_expressions']
        assert ue['__res']['status'] == 'ok' and ue['__res']['data']['text/plain'] == '42', ue
    # Omitted `?session_id=` binds once to wire `header.session` and gets a single IOPub copy;
    # explicit `?session_id=` mismatch, headerless frames, and cross-origin WS upgrades are rejected.
    ws_url = gateway.replace('http', 'ws', 1)
    assert httpx.get(f'{gateway}/api/kernels/{kid}/channels', headers={'Origin': 'http://attacker.test',
        'Connection': 'Upgrade', 'Upgrade': 'websocket', 'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
        'Sec-WebSocket-Version': '13'}, timeout=5).status_code == 403
    ses = Session(key=b'm')
    with ws_connect(f'{ws_url}/api/kernels/{kid}/channels', max_size=64*2**20, open_timeout=5, close_timeout=2) as ws:
        ws.send('{"channel":"shell","content":{}}')  # headerless frame dropped
        msg = _exec(ses, 'print("bound-ok")')
        ws.send(to_frame(msg))
        streams, replied, idle = [], False, False
        while not (replied and idle):
            m = _recv_until(ws, lambda m: m['parent_header'].get('msg_id') == msg['header']['msg_id'])
            if m['header']['msg_type'] == 'stream': streams.append(m['content']['text'])
            elif m['channel'] == 'shell' and m['header']['msg_type'] == 'execute_reply':
                assert m['content']['status'] == 'ok'; replied = True
            elif m['header']['msg_type'] == 'status' and m['content'].get('execution_state') == 'idle': idle = True
        assert ''.join(streams) == 'bound-ok\n', streams
    ses = Session(key=b'd')
    with ws_connect(f'{ws_url}/api/kernels/{kid}/channels?session_id={ses.session}', open_timeout=5, close_timeout=2) as old_ws:
        with ws_connect(f'{ws_url}/api/kernels/{kid}/channels', open_timeout=5, close_timeout=2) as new_ws:
            new_ws.send(to_frame(_exec(ses, '1+1')))
            assert _recv_until(new_ws, lambda m: m['channel'] == 'shell')['content']['status'] == 'ok'
            try:
                while True: old_ws.recv(timeout=2)
            except ConnectionClosed: pass
            # Explicit mismatched `?session_id=` or later conflicting `header.session` is dropped.
            other = Session(key=b'o')
            mis_ses = Session(key=b'm2', session='explicit-other')
            with ws_connect(f'{ws_url}/api/kernels/{kid}/channels?session_id=explicit-other', open_timeout=5, close_timeout=2) as mis_ws:
                mis_ws.send(to_frame(_exec(other, 'conflict_ran = 1')))
                sync = _exec(mis_ses, '1+1')
                mis_ws.send(to_frame(sync))
                assert _recv_until(mis_ws, lambda m: m['channel'] == 'shell' and m['parent_header'].get('msg_id') == sync['header']['msg_id'])['content']['status'] == 'ok'
            new_ws.send(to_frame(_exec(other, 'conflict_ran = 2')))
            follow = _exec(ses, 'globals().get("conflict_ran", 18)')
            new_ws.send(to_frame(follow))
            res = _recv_until(new_ws, lambda m: m['header']['msg_type'] == 'execute_result' and m['parent_header'].get('msg_id') == follow['header']['msg_id'])
            assert res['content']['data']['text/plain'] == '18'

    # A new kernel is ready within 3 seconds.
    t0 = time.monotonic()
    new = _new_kernel(gateway)
    elapsed = time.monotonic() - t0
    httpx.delete(f'{gateway}/api/kernels/{new}', timeout=30)
    assert elapsed < 3, f'kernel took {elapsed:.2f}s to become ready'
