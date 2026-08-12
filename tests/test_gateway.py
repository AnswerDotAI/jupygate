"The load/race/edge tests deferred from the notebooks; see index + meta design notes for the list's rationale."
import os, signal, time
import pytest, httpx
from websockets.sync.client import connect as ws_connect
from jupyter_client.session import Session
from jupygate.core import to_frame, from_frame
from .conftest import in_proc

timeout = 30


def _connect(base, kid, ses):
    url = base.replace('http', 'ws', 1)
    return ws_connect(f"{url}/api/kernels/{kid}/channels?session_id={ses.session}", max_size=64*2**20)

def _exec(ses, code, **kw):
    msg = ses.msg('execute_request', dict(code=code, silent=False, store_history=True,
        user_expressions={}, allow_stdin=True, stop_on_error=False, **kw))
    msg['channel'] = 'shell'
    return msg

def _recv_until(ws, pred, tmax=timeout):
    end = time.monotonic() + tmax
    while time.monotonic() < end:
        m = from_frame(ws.recv(timeout=max(0.1, end - time.monotonic())))
        if pred(m): return m
    raise TimeoutError('condition not met')

def _new_kernel(base):
    r = httpx.post(f'{base}/api/kernels', timeout=90)
    assert r.status_code == 201
    return r.json()['id']


def test_two_client_stdin_stamping(gateway):
    "Concurrent pending input()s from two clients, parentless replies: each cell gets its own answer."
    kid = _new_kernel(gateway)
    sa, sb = Session(key=b'a'), Session(key=b'b')
    wa, wb = _connect(gateway, kid, sa), _connect(gateway, kid, sb)
    try:
        # ipymini is FIFO per subshell, so run A's input() on a subshell to get truly concurrent prompts
        ctl = sa.msg('create_subshell_request', {})
        ctl['channel'] = 'control'
        wa.send(to_frame(ctl))
        sub = _recv_until(wa, lambda m: m['header']['msg_type']=='create_subshell_reply')['content']['subshell_id']
        ma = _exec(sa, "ans_a = input('A?')")
        ma['header']['subshell_id'] = sub
        wa.send(to_frame(ma))
        _recv_until(wa, lambda m: m['header']['msg_type']=='input_request')
        wb.send(to_frame(_exec(sb, "ans_b = input('B?')")))
        _recv_until(wb, lambda m: m['header']['msg_type']=='input_request')
        for ses, ws, val in ((sa, wa, 'alpha'), (sb, wb, 'beta')):
            r = ses.msg('input_reply', dict(value=val))
            r['channel'] = 'stdin'   # deliberately no parent_header: the gateway must stamp it
            ws.send(to_frame(r))
        _recv_until(wa, lambda m: m['channel']=='shell')
        _recv_until(wb, lambda m: m['channel']=='shell')
        wb.send(to_frame(_exec(sb, "print(ans_a, ans_b)")))
        m = _recv_until(wb, lambda m: m['header']['msg_type']=='stream')
        assert m['content']['text'].strip() == 'alpha beta'
    finally:
        wa.close()
        wb.close()


def test_flood_keeps_status_truthful(gateway):
    "A client that reads slowly during an output flood still sees busy and idle."
    kid = _new_kernel(gateway)
    ses = Session(key=b'x')
    ws = _connect(gateway, kid, ses)
    try:
        ws.send(to_frame(_exec(ses, "for i in range(20000): print('x'*100)")))
        time.sleep(3)  # let the flood outrun us: gateway queue bound sheds while we sleep
        states = set()
        def done(m):
            if m['header']['msg_type']=='status': states.add(m['content']['execution_state'])
            return 'idle' in states
        _recv_until(ws, done, tmax=60)
        assert {'busy','idle'} <= states
    finally: ws.close()


def test_concurrent_reply_routing(gateway):
    "Interleaved executes from two clients: every reply reaches its sender only."
    kid = _new_kernel(gateway)
    sa, sb = Session(key=b'a'), Session(key=b'b')
    wa, wb = _connect(gateway, kid, sa), _connect(gateway, kid, sb)
    try:
        sent_a = [to_frame(_exec(sa, f"{i}+{i}")) for i in range(5)]
        sent_b = [to_frame(_exec(sb, f"{i}*{i}")) for i in range(5)]
        for fa, fb in zip(sent_a, sent_b):
            wa.send(fa)
            wb.send(fb)
        for ws_, ses in ((wa, sa), (wb, sb)):
            for _ in range(5):
                m = _recv_until(ws_, lambda m: m['channel']=='shell')
                assert m['parent_header']['session'] == ses.session
    finally:
        wa.close()
        wb.close()


def test_inband_interrupt_via_gateway(gateway):
    "interrupt_request on the control channel, through the ws mux (the nb's HTTP-path demo covers SIGINT)."
    kid = _new_kernel(gateway)
    ses = Session(key=b'x')
    ws = _connect(gateway, kid, ses)
    try:
        ws.send(to_frame(_exec(ses, "import time\nwhile True: time.sleep(0.05)")))
        time.sleep(0.5)
        imsg = ses.msg('interrupt_request', {})
        imsg['channel'] = 'control'
        ws.send(to_frame(imsg))
        _recv_until(ws, lambda m: m['header']['msg_type']=='interrupt_reply')
        m = _recv_until(ws, lambda m: m['channel']=='shell')
        assert m['content']['ename'] == 'KeyboardInterrupt'
    finally: ws.close()


def test_kernel_death_synthesizes_dead(gateway):
    kid = _new_kernel(gateway)
    ses = Session(key=b'x')
    ws = _connect(gateway, kid, ses)
    try:
        ws.send(to_frame(_exec(ses, "import os; os._exit(9)")))
        m = _recv_until(ws, lambda m: m['header']['msg_type']=='status' and m['content']['execution_state']=='dead')
        assert m['channel'] == 'iopub'
        assert httpx.get(f'{gateway}/api/kernels/{kid}', timeout=10).json()['execution_state'] == 'dead'
    finally: ws.close()


def test_duplicate_frames_hit_replay_guard(gateway):
    "The gateway re-signs deterministically, so a byte-identical resend reaches ipymini with the same HMAC and is dropped as a replay: exactly one reply."
    kid = _new_kernel(gateway)
    ses = Session(key=b'x')
    ws = _connect(gateway, kid, ses)
    try:
        frame = to_frame(_exec(ses, "1+1"))
        ws.send(frame)
        ws.send(frame)
        m = _recv_until(ws, lambda m: m['channel']=='shell')
        assert m['content']['status'] == 'ok'
        with pytest.raises(TimeoutError): _recv_until(ws, lambda m: m['channel']=='shell', tmax=2)
    finally: ws.close()


def test_big_binary_buffers_roundtrip(gateway):
    "Large multi-buffer comm messages survive both directions intact."
    kid = _new_kernel(gateway)
    ses = Session(key=b'x')
    ws = _connect(gateway, kid, ses)
    try:
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
        m = _recv_until(ws, lambda m: m['header']['msg_type']=='comm_msg')
        assert [bytes(b) for b in m['buffers']] == [big, b'tail']
    finally: ws.close()




@in_proc
def test_shutdown_reaps_kernels():
    "Gateway lifespan shutdown terminates every kernel process; nothing survives the gateway."
    from starlette.testclient import TestClient
    from jupygate.core import create_app
    app = create_app()
    with TestClient(app) as tc:
        kid = tc.post('/api/kernels').json()['id']
        pid = app.state.kernels[kid].proc.pid
    # exiting the TestClient context runs lifespan shutdown
    end = time.monotonic() + 10
    while time.monotonic() < end:
        try: os.kill(pid, 0)
        except ProcessLookupError: break
        time.sleep(0.1)
    with pytest.raises(ProcessLookupError): os.kill(pid, 0)


def _wait(pred, tmax=timeout):
    end = time.monotonic() + tmax
    while time.monotonic() < end:
        if pred(): return
        time.sleep(0.05)
    raise TimeoutError('condition not met')


def test_reconnect_replays_and_reports_drops(gateway):
    "Kill a ws mid-flood, reconnect with the same session: ordered replay, the newest window, the drop warning, a current status, and the cell's own reply."
    kid = _new_kernel(gateway)
    ses = Session(key=b'x')
    ws = _connect(gateway, kid, ses)
    seen = []
    def grab(m):
        if m['header']['msg_type']=='stream': seen.extend(int(x) for x in m['content']['text'].split())
        return len(seen) >= 50
    ws.send(to_frame(_exec(ses, "import time\nfor i in range(3000): print(i); time.sleep(0.001)")))
    _recv_until(ws, grab)
    ws.close()      # mid-flood: the gateway detaches the queue and keeps routing into the ring
    time.sleep(5)   # the flood keeps running and finishes while we are away; the ring sheds down to the newest window
    ws = _connect(gateway, kid, ses)
    try:
        got, state = [], dict(warned=None, reply=None)
        def done(m):
            if m['channel']=='shell': state['reply'] = m
            if m['header']['msg_type']=='stream':
                t = m['content']['text']
                if '[jupygate]' in t: state['warned'] = int(t.split()[1])
                else: got.extend(int(x) for x in t.split())
            idle = m['header']['msg_type']=='status' and m['content']['execution_state']=='idle'
            return idle and state['warned'] is not None and state['reply'] is not None
        _recv_until(ws, done)
        nums = seen + got
        assert all(a <= b for a,b in zip(nums, nums[1:]))  # order preserved end to end (== allows the one at-least-once resend)
        assert nums[-1] == 2999                            # the newest window survived to the very end
        assert 3000 - len(set(nums)) > 0                   # the shed was real...
        assert state['warned'] > 0                         # ...and reported
        assert state['reply']['parent_header']['session'] == ses.session
    finally: ws.close()


@in_proc
def test_buffering_policy_and_ttl():
    "A gateway-generated session id is discarded on disconnect; a client-supplied one is parked, then reaped after buffer_secs."
    from jupygate.core import create_app, serve
    app = create_app(buffer_secs=2)
    server = serve(app, port=0, in_thread=True)
    try:
        kid = _new_kernel(server.url)
        mux = app.state.kernels[kid].mux
        url = server.url.replace('http', 'ws', 1)
        w = ws_connect(f"{url}/api/kernels/{kid}/channels", max_size=64*2**20)  # no session_id
        _wait(lambda: len(mux.clients)==1)
        w.close()
        _wait(lambda: not mux.clients)  # generated id: nothing to buffer for, discarded at once
        ses = Session(key=b'x')
        w = _connect(server.url, kid, ses)
        _wait(lambda: len(mux.clients)==1)
        w.close()
        _wait(lambda: (cq := mux.clients.get(ses.session)) is not None and cq.detached)  # supplied id: parked
        _wait(lambda: not mux.clients, tmax=10)  # ...until the reaper's TTL collects it
    finally:
        server.should_exit = True
        server.thread.join(timeout=10)


def _model_until(base, kid, pred, tmax=timeout):
    end = time.monotonic() + tmax
    while time.monotonic() < end:
        m = httpx.get(f'{base}/api/kernels/{kid}', timeout=10).json()
        if pred(m): return m
        time.sleep(0.25)
    raise TimeoutError(f'condition not met; last model: {m}')

def test_heartbeat_marks_unresponsive(gateway):
    "A stopped kernel process stops echoing heartbeats: the model says so, and recovery is observed too."
    kid = _new_kernel(gateway)
    m = _model_until(gateway, kid, lambda m: m['last_heartbeat'] is not None)
    assert m['execution_state'] == 'alive'
    assert time.time() - m['last_heartbeat'] < timeout
    os.kill(m['pid'], signal.SIGSTOP)
    try: _model_until(gateway, kid, lambda m: m['execution_state'] == 'unresponsive')
    finally: os.kill(m['pid'], signal.SIGCONT)
    m2 = _model_until(gateway, kid, lambda m: m['execution_state'] == 'alive')
    assert m2['last_heartbeat'] > m['last_heartbeat']
    httpx.delete(f'{gateway}/api/kernels/{kid}', timeout=30)
