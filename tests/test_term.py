"Terminal edge cases: incremental UTF-8 decoding, replacement of invalid bytes, and escape preservation. The literate coverage lives in nbs/01_term.ipynb."
import json, httpx
from websockets.sync.client import connect as ws_connect


def test_utf8_and_escape_fidelity(gateway):
    "Split multi-byte UTF-8 and escape sequences cross unmangled; invalid bytes become U+FFFD."
    with httpx.Client(base_url=gateway, timeout=30) as http:
        cmd = r"printf '\342\202'; sleep 0.05; printf '\254\377\200A\033]x'"
        model = http.post('/api/terminals', json=dict(argv=['sh', '-c', cmd])).json()
        with ws_connect(f"{gateway.replace('http', 'ws')}/terminals/websocket/{model['name']}") as ws:
            assert json.loads(ws.recv(timeout=10))[0] == 'setup'
            buf, want = '', '\u20ac\ufffd\ufffdA\x1b]x'
            while want not in buf:
                msg = json.loads(ws.recv(timeout=10))
                if msg[0] == 'stdout': buf += msg[1]
                elif msg[0] == 'disconnect': break
            assert want in buf
        http.delete(f"/api/terminals/{model['name']}")
