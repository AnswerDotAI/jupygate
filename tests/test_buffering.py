"Live delivery and reconnect without retained history."
import httpx
from jupywire.session import Session, to_frame
from helpers import _connect, _exec, _recv_until, _new_kernel


def test_reconnect_starts_fresh(gateway):
    kid = _new_kernel(gateway)
    ses = Session(key=b'x')
    idle = lambda m: m['header']['msg_type']=='status' and m['content']['execution_state']=='idle'
    try:
        with _connect(gateway, kid, Session(key=b'obs')) as obs:
            with _connect(gateway, kid, ses) as ws:
                ws.send(to_frame(_exec(ses, "import time; time.sleep(0.2); print('while away')")))
                _recv_until(ws, lambda m: m['header']['msg_type']=='status' and m['content']['execution_state']=='busy')
            _recv_until(obs, idle)
            with _connect(gateway, kid, ses) as ws:
                ws.send(to_frame(_exec(ses, "print('back')")))
                texts = []
                def done(m):
                    if m['header']['msg_type']=='stream': texts.append(m['content']['text'])
                    return idle(m)
                _recv_until(ws, done)
                assert ''.join(texts) == 'back\n'
    finally: httpx.delete(f'{gateway}/api/kernels/{kid}', timeout=30)
