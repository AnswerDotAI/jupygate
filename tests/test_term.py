"Terminal sessions: UTF-8 framing, path bindings, scrollback/resize, and process lifetime."
import json, time, httpx
from pathlib import Path
from websockets.sync.client import connect as ws_connect


def test_utf8_and_framing(gateway):
    "Split UTF-8 sequences reassemble cleanly, invalid bytes become U+FFFD, escape sequences stay intact, and exit sends disconnect."
    with httpx.Client(base_url=gateway, timeout=30) as http:
        cmd = r"read line; printf '\342\202'; sleep 0.05; printf '\254\377\200A\033]x'"
        model = http.post('/api/terminals', json=dict(argv=['sh', '-c', cmd])).json()
        assert http.get(f"/terminals/websocket/{model['name']}", headers={'Origin': 'http://attacker.test',
            'Connection': 'Upgrade', 'Upgrade': 'websocket', 'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
            'Sec-WebSocket-Version': '13'}).status_code == 403
        with ws_connect(f"{gateway.replace('http', 'ws')}/terminals/websocket/{model['name']}") as ws:
            assert json.loads(ws.recv(timeout=10)) == ['setup', {'name': model['name']}]
            ws.send(json.dumps(['stdin', '\n']))
            buf, disc = '', None
            while disc is None:
                msg = json.loads(ws.recv(timeout=10))
                if msg[0] == 'stdout': buf += msg[1]
                elif msg[0] == 'disconnect': disc = msg
            assert '€\ufffd\ufffdA\x1b]x' in buf and disc == ['disconnect', 1, 0]


def _terminal_has(gateway, name, command, expected):
    "Run a shell command and wait for its output."
    with ws_connect(f"{gateway.replace('http', 'ws')}/terminals/websocket/{name}") as ws:
        ws.recv(timeout=10)
        ws.send(json.dumps(['stdin', command + '\n']))
        buf = ''
        while expected not in buf:
            msg = json.loads(ws.recv(timeout=10))
            if msg[0] == 'stdout': buf += msg[1]


def _exited(http, name):
    "Wait for terminal `name` to be removed after exit."
    for _ in range(100):
        if http.get(f'/api/terminals/{name}').status_code == 404: return
        time.sleep(0.01)
    raise AssertionError(f'terminal {name} is still registered')


def test_terminal_lifecycle(gateway, files_root):
    "Named terminals reuse by name; free terminals get unique names, support cwd, scrollback replay, resize, rc cleanup, and disappear on exit without reusing auto names."
    (files_root/'proj').mkdir(exist_ok=True)
    (files_root/'other').mkdir(exist_ok=True)
    with httpx.Client(base_url=gateway, timeout=30) as http:
        a = http.post('/api/terminals', json=dict(name='projterm', cwd='proj')).json()
        free = [http.post('/api/terminals', json=dict(cwd='other')).json() for _ in range(2)]
        try:
            _terminal_has(gateway, a['name'], 'pwd', str((files_root/'proj').resolve()))
            assert http.post('/api/terminals', json=dict(name='projterm', cwd='proj')).json()['name'] == a['name']
            assert free[0]['name'] != free[1]['name'] and 'path' not in free[0]
            _terminal_has(gateway, free[0]['name'], 'pwd', str((files_root/'other').resolve()))
            # Scrollback replay and set_size resize.
            _terminal_has(gateway, free[0]['name'], 'echo SCROLLBACK_TOKEN', 'SCROLLBACK_TOKEN')
            with ws_connect(f"{gateway.replace('http', 'ws')}/terminals/websocket/{free[0]['name']}") as ws:
                assert json.loads(ws.recv(timeout=10)) == ['setup', {'name': free[0]['name']}]
                ws.send(json.dumps(['set_size', 33, 99, 0, 0]))
                ws.send(json.dumps(['stdin', 'stty size\n']))
                buf = ''
                while '33 99' not in buf:
                    msg = json.loads(ws.recv(timeout=10))
                    if msg[0] == 'stdout': buf += msg[1]
                assert 'SCROLLBACK_TOKEN' in buf
            done = http.post('/api/terminals', json=dict(argv=['true'])).json()
            _exited(http, done['name'])
            later = http.post('/api/terminals', json=dict(argv=['true'])).json()
            assert later['name'] != done['name'] and http.get(f"/api/terminals/{done['name']}").status_code == 404
            _exited(http, later['name'])
            # Deleting a kernel in standalone jupygate does not kill terminals.
            kid = http.post('/api/kernels', json=dict(path='proj/d.ipynb'), timeout=90).json()['id']
            assert http.delete(f'/api/kernels/{kid}', timeout=30).status_code == 204
            assert http.get(f"/api/terminals/{a['name']}").status_code == 200
            # An rc temp dir is removed automatically once the terminal exits.
            rct = http.post('/api/terminals', json=dict(argv=['sh', '-c', 'read line; printf "RC=%s\n" "{rcdir}"'], rc='# test rc\n')).json()
            with ws_connect(f"{gateway.replace('http', 'ws')}/terminals/websocket/{rct['name']}") as ws:
                ws.recv(timeout=10)
                ws.send(json.dumps(['stdin', '\n']))
                buf = ''
                while True:
                    msg = json.loads(ws.recv(timeout=10))
                    if msg[0] == 'stdout': buf += msg[1]
                    elif msg[0] == 'disconnect': break
            rcdir = next(l.split('RC=', 1)[1].strip() for l in buf.splitlines() if 'RC=' in l)
            _exited(http, rct['name'])
            assert not Path(rcdir).exists(), f'rc temp dir leaked: {rcdir}'
        finally:
            http.delete(f"/api/terminals/{a['name']}")
            for m in free: http.delete(f"/api/terminals/{m['name']}")
