"Shared client helpers for the gateway suite; ws timeouts are tight so a stalled open fails loudly and a stalled close costs 2s."
import time, httpx
from contextlib import asynccontextmanager
from websockets.sync.client import connect as ws_connect
from jupywire.session import from_frame
from jupyasyncclient import JupyAsyncKernelClient

timeout = 10


def _connect(base, kid, ses):
    url = base.replace('http', 'ws', 1)
    return ws_connect(f"{url}/api/kernels/{kid}/channels?session_id={ses.session}", max_size=64*2**20, open_timeout=5, close_timeout=2)


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


@asynccontextmanager
async def client(base, kid=None, **kw):
    "A jupyasyncclient client on kernel `kid`, or on a new kernel created with `kw`, whose requests and queue waits time out after `timeout` seconds."
    async with await JupyAsyncKernelClient.connect(base, kernel=kid, default_timeout=timeout, **kw) as kc: yield kc
