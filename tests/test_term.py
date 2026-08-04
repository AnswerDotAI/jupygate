"Terminal edge cases: byte fidelity through the pty and the websocket. The literate coverage lives in nbs/01_term.ipynb."
import json, httpx
from websockets.sync.client import connect as ws_connect


def test_bytes_verbatim(gateway):
    "Non-UTF8 and control bytes cross the pty, the replay buffer, and the binary ws frames unmangled."
    http = httpx.Client(base_url=gateway, timeout=30)
    model = http.post('/api/terminals', json=dict(argv=['printf', r'\377\200A\033]x'])).json()
    ws = ws_connect(f"{gateway.replace('http', 'ws')}/api/terminals/{model['name']}/channel")
    assert json.loads(ws.recv(timeout=10))['type'] == 'setup'
    buf, want = b'', b'\xff\x80A\x1b]x'
    while want not in buf:
        frame = ws.recv(timeout=10)
        if isinstance(frame, bytes): buf += frame
        elif json.loads(frame)['type'] == 'eof': break
    assert want in buf
    ws.close()
    http.delete(f"/api/terminals/{model['name']}")
    http.close()
