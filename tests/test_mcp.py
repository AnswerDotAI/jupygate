"MCP's protocol and lifecycle stories: session-local selection, execution, kernel lifecycles, startup, and creation provenance."
import ast, concurrent.futures, time

import httpx
from jupywire.route import JmsgQueues

from helpers import client

_version = '2025-11-25'


def _post(base, msg, sid=None):
    headers = {'Accept': 'application/json, text/event-stream'}
    if sid: headers['Mcp-Session-Id'] = sid
    if msg['method'] != 'initialize': headers['MCP-Protocol-Version'] = _version
    return httpx.post(f'{base}/mcp', json=msg, headers=headers, timeout=90)


def _rpc(base, method, params=None, sid=None, id=1):
    msg = dict(jsonrpc='2.0', id=id, method=method)
    if params is not None: msg['params'] = params
    r = _post(base, msg, sid)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['id'] == id
    assert 'error' not in body, body
    return r, body['result']


def _session(base, defaults=None):
    params = dict(protocolVersion=_version, capabilities={}, clientInfo=dict(name='test', version='1'))
    if defaults is not None: params['jupygate'] = defaults
    r, res = _rpc(base, 'initialize', params)
    assert res.get('jupygate') is True
    sid = r.headers.get('mcp-session-id')
    assert _post(base, dict(jsonrpc='2.0', method='notifications/initialized'), sid).status_code == 202
    return sid


def _call(base, sid, name, error=False, **arguments):
    _, result = _rpc(base, 'tools/call', dict(name=name, arguments=arguments), sid)
    assert result['isError'] == error, result
    return ''.join(c.get('text', '') for c in result['content'] if c['type'] == 'text')


def _close(base, sid): assert httpx.delete(f'{base}/mcp', headers={'Mcp-Session-Id': sid}, timeout=30).status_code == 204


def _kernels(base): return {m['id'] for m in httpx.get(f'{base}/api/kernels', timeout=10).json()}


async def test_mcp_session_story(gateway):
    "MCP refuses malformed requests, tracks per-session selection independently, supports use_kernel by ID/prefix, and sets JUPYGATE_LAUNCHER only on MCP-created kernels."
    assert httpx.post(f'{gateway}/mcp', json=[], timeout=10).status_code == 400
    s1 = _session(gateway)
    req = dict(jsonrpc='2.0', id=1, method='tools/list')
    assert httpx.post(f'{gateway}/mcp', json=req, timeout=10).status_code == 400
    assert httpx.post(f'{gateway}/mcp', json=req, headers={'Mcp-Session-Id': 'unknown'}, timeout=10).status_code == 404
    assert httpx.post(f'{gateway}/mcp', json=req, headers={'Mcp-Session-Id': s1, 'MCP-Protocol-Version': '2025-03-26'}, timeout=10).status_code == 400
    init = dict(jsonrpc='2.0', id=1, method='initialize', params=dict(protocolVersion=_version))
    assert httpx.post(f'{gateway}/mcp', json=init, headers={'Origin': 'http://attacker.test'}, timeout=10).status_code == 403
    bad_init = dict(jsonrpc='2.0', id=1, method='initialize', params=dict(protocolVersion=_version, rustygate={}))
    assert 'jupygate' in httpx.post(f'{gateway}/mcp', json=bad_init, timeout=10).json()['error']['message']

    s2, s3, existing = None, None, None
    try:
        _, listed = _rpc(gateway, 'tools/list', sid=s1)
        assert [t['name'] for t in listed['tools']] == ['list_kernels', 'create', 'use_kernel', 'delete_kernel', 'exec', 'restart', 'interrupt']
        create_props = next(t for t in listed['tools'] if t['name'] == 'create')['inputSchema']['properties']
        exec_props = next(t for t in listed['tools'] if t['name'] == 'exec')['inputSchema']['properties']
        assert 'dlgname' not in create_props and 'dlgname' not in exec_props

        created = _call(gateway, s1, 'create', kernel='py')
        assert created.startswith('created kernel ')
        kid_a = created.split()[2]
        assert httpx.get(f'{gateway}/api/kernels/{kid_a}', timeout=10).json()['connections'] == 1
        out = _call(gateway, s1, 'exec', code="x = 40\nprint('hello')\nx + 2")
        assert '<stdout>\nhello\n</stdout>' in out and '<execute_result>\n42\n</execute_result>' in out
        assert _call(gateway, s1, 'exec', code='%%bash\nprintf shell') == 'shell'
        assert _call(gateway, s1, 'exec', code="'a\\nb'") == 'a\nb'
        assert _call(gateway, s1, 'exec', code="from IPython.display import Markdown\nMarkdown('**md**')") == '**md**'
        out = _call(gateway, s1, 'exec', code="from IPython.display import display\ndisplay({'text/plain': 'full-doc'}, raw=True, metadata={'full': True})")
        assert out == '𝍁full-doc𝍁'
        png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII='
        _, result = _rpc(gateway, 'tools/call', dict(name='exec', arguments=dict(code=f"from IPython.display import display\ndisplay({{'image/png': {png!r}}}, raw=True)")), s1)
        assert [c['mimeType'] for c in result['content'] if c['type'] == 'image'] == ['image/png']
        # Initializing a known session again keeps its current kernel.
        r, _ = _rpc(gateway, 'initialize', dict(protocolVersion=_version, capabilities={}, clientInfo=dict(name='test', version='2')), s1)
        assert r.headers['mcp-session-id'] == s1
        assert _call(gateway, s1, 'exec', code='x') == '40', 'reinitializing a known MCP session lost its current kernel'

        s2 = _session(gateway)
        assert _call(gateway, s2, 'use_kernel', kernel=kid_a[:12]).startswith(f'current kernel {kid_a}')
        line = next(l for l in _call(gateway, s1, 'list_kernels').splitlines() if l.startswith(kid_a))
        assert 'idle' in line and 'connections=2' in line
        assert _call(gateway, s2, 'exec', code='x') == '40'
        kid_b = _call(gateway, s2, 'create', kernel='py').split()[2]
        models = {m['id']: m for m in httpx.get(f'{gateway}/api/kernels', timeout=10).json()}
        assert (models[kid_a]['connections'], models[kid_b]['connections']) == (1, 1)
        assert _call(gateway, s2, 'exec', code='x = 99\nx') == '99'
        assert _call(gateway, s1, 'exec', code='x') == '40', 'the other MCP session changed this session current pointer'
        async with client(gateway, kid_a): assert httpx.get(f'{gateway}/api/kernels/{kid_a}', timeout=10).json()['connections'] == 2

        existing = httpx.post(f'{gateway}/api/kernels', json={}, timeout=90).json()['id']
        s3 = _session(gateway, defaults=dict(startup='boot = 41'))
        assert _call(gateway, s3, 'use_kernel', kernel=existing).startswith(f'current kernel {existing}')
        code = "import os\nprint(os.environ.get('JUPYGATE_LAUNCHER', 'unset'))"
        assert _call(gateway, s3, 'exec', code=code) == 'unset\n'
        # Restarting an attached kernel does not inject this MCP session's startup.py.
        assert _call(gateway, s3, 'restart').startswith(f'restarted kernel {existing}')
        assert 'NameError' in _call(gateway, s3, 'exec', code='boot')
        assert _call(gateway, s3, 'create', kernel='py').startswith('created kernel ')
        assert _call(gateway, s3, 'exec', code=code) == 'mcp\n'
        assert _call(gateway, s3, 'restart').startswith('restarted kernel ')
        assert _call(gateway, s3, 'exec', code=code) == 'mcp\n'
        assert _call(gateway, s3, 'exec', code='boot') == '41'
    finally:
        for sid in (s1, s2, s3):
            if sid: httpx.delete(f'{gateway}/mcp', headers={'Mcp-Session-Id': sid}, timeout=30)
        if existing: httpx.delete(f'{gateway}/api/kernels/{existing}', timeout=30)


def test_mcp_kernel_lifecycle_story(gateway, tmp_path):
    "`exec` needs a current kernel. Session defaults apply to kernels created by the session; MCP restart reruns startup on owned kernels while plain REST restart does not; autoclose stops only owned kernels."
    kids = []
    startup = "import os\nboot = 41\nprint('booted')"
    try:
        s1 = _session(gateway)
        for name, args in [('exec', dict(code='1')), ('create', {})]: _call(gateway, s1, name, error=True, **args)
        out = _call(gateway, s1, 'create', kernel='py')
        assert out.startswith('created kernel ') and 'kernel=py language=python' in out
        kid = out.split()[2]
        kids.append(kid)
        assert _call(gateway, s1, 'exec', code='x = 7; x') == '7'
        assert _call(gateway, s1, 'exec', code='x') == '7'
        line = next(l for l in _call(gateway, s1, 'list_kernels').splitlines() if l.startswith(kid))
        assert line.endswith('<- current')
        _close(gateway, s1)
        assert kid not in _kernels(gateway), 'kernel outlived its session'
        s2, s3 = _session(gateway), _session(gateway)
        kid = _call(gateway, s2, 'create', kernel='py').split()[2]
        kids.append(kid)
        _call(gateway, s3, 'use_kernel', kernel=kid)
        _close(gateway, s2)
        assert kid not in _kernels(gateway), 'a selected kernel outlived its creating session'
        _close(gateway, s3)

        work = str(tmp_path.resolve())
        sid = _session(gateway, defaults=dict(cwd=work, env={'JG_PROBE': 'hi'}, startup=startup))
        out = _call(gateway, sid, 'create', kernel='py')
        assert out.startswith('created kernel ') and '\nbooted' in out, 'create header and startup output share a line'
        assert _call(gateway, sid, 'exec', code='boot + 1') == '42'
        auto_kid = out.split()[2]
        assert ast.literal_eval(_call(gateway, sid, 'exec', code="os.getcwd(), os.environ['JG_PROBE']")) == (work, 'hi')
        _call(gateway, sid, 'exec', code='y = 5')
        assert '\nbooted' in _call(gateway, sid, 'restart'), 'restart header and startup output share a line'
        assert _call(gateway, sid, 'exec', code='boot') == '41', 'MCP restart did not re-run startup'
        assert 'NameError' in _call(gateway, sid, 'exec', code='y')

        kept = _call(gateway, sid, 'create', kernel='py', autoclose=False).split()[2]
        kids.append(kept)
        # Plain REST restart is stock Jupyter (does not replay MCP startup).
        assert httpx.post(f'{gateway}/api/kernels/{kept}/restart', timeout=30).status_code == 200
        assert 'NameError' in _call(gateway, sid, 'exec', code='boot')
        s_att = _session(gateway)
        _call(gateway, s_att, 'use_kernel', kernel=auto_kid)
        _close(gateway, s_att)
        assert _kernels(gateway) >= {auto_kid, kept}, 'closing an attaching session stopped a kernel it did not make'

        before = _kernels(gateway)
        s_bad = _session(gateway, defaults=dict(startup='raise ValueError("boom")'))
        assert 'boom' in _call(gateway, s_bad, 'create', error=True, kernel='py')
        assert _kernels(gateway) == before, 'a failed startup left its kernel behind'
        _close(gateway, s_bad)

        s4 = _session(gateway, defaults=dict(startup=startup, quiet=True))
        out = _call(gateway, s4, 'create', kernel='py')
        assert out.startswith('created kernel ') and 'booted' not in out
        assert _call(gateway, s4, 'exec', code='boot') == '41'
        _close(gateway, sid)
        _close(gateway, s4)
        alive = _kernels(gateway)
        assert kept in alive, 'autoclose=False kernel was stopped'
        assert auto_kid not in alive, 'an autoclose kernel outlived its session'
    finally:
        for kid in kids: httpx.delete(f'{gateway}/api/kernels/{kid}', timeout=30)


def test_mcp_large_output(gateway):
    "A full result over the default 64 MiB frame cap and streaming stdout spam both stay within the 1 MiB text ceiling while keeping the tail."
    sid = _session(gateway)
    try:
        _call(gateway, sid, 'create', kernel='py')
        out = _call(gateway, sid, 'exec', code="from IPython.display import display\ndisplay({'text/plain': 'x'*((64<<20)+1)}, raw=True, metadata={'full': True})")
        assert out.startswith('𝍁[') and out.endswith('𝍁') and 'x'*1024 in out and len(out.encode()) < (1<<20)+100
        stream_out = _call(gateway, sid, 'exec', code="for i in range(4000): print(f'{i:04d} ' + 'y'*512)")
        assert stream_out.startswith('[') and 'bytes of output removed]' in stream_out and '3999 ' in stream_out and len(stream_out.encode()) <= (1<<20)
    finally: _close(gateway, sid)


def test_mcp_agent_story(gateway):
    "A tool call's `agent` argument gives that subagent its own session within the transport session, sets `JUPYGATE_AGENT` across restarts, and stops with the transport session."
    new = 'New kernel for this subagent'
    sid = _session(gateway, defaults=dict(startup="from IPython.display import display\nboot = 41\ndisplay({'text/plain': 'agent-doc'}, raw=True, metadata={'full': True})"))
    try:
        main = _call(gateway, sid, 'create', kernel='py').split()[2]
        _call(gateway, sid, 'exec', code='x = 1')
        out = _call(gateway, sid, 'exec', code='boot + 1', agent='a')
        assert out.startswith('𝍁'+new) and '\ncreated kernel ' in out and out.endswith('42𝍁'), out
        a = out.split('created kernel ')[1].split()[0]
        assert a != main and 'NameError' in _call(gateway, sid, 'exec', code='x', agent='a')
        assert _call(gateway, sid, 'exec', code='x = 2; x', agent='a') == '2'
        assert _call(gateway, sid, 'exec', code='x') == '1', 'an agent changed the transport session kernel'
        b = _call(gateway, sid, 'exec', code='1', agent='b').split('created kernel ')[1].split()[0]
        lines = {l.split()[0]: l for l in _call(gateway, sid, 'list_kernels', agent='a').splitlines()}
        assert lines[a].endswith('agent=a  <- current') and lines[b].endswith('agent=b') and 'agent=' not in lines[main]
        env = "import os; print(os.environ.get('JUPYGATE_AGENT', 'unset'))"
        assert _call(gateway, sid, 'exec', code=env, agent='a') == 'a\n' and _call(gateway, sid, 'exec', code=env) == 'unset\n'
        assert _call(gateway, sid, 'restart', agent='a').startswith(f'𝍁restarted kernel {a}')
        assert _call(gateway, sid, 'exec', code=env, agent='a') == 'a\n', 'a restart dropped the agent'
        _call(gateway, sid, 'delete_kernel', agent='a')
        out = _call(gateway, sid, 'exec', code='boot', agent='a')
        assert out.startswith('𝍁'+new) and out.endswith('41𝍁'), out
        a2 = out.split('created kernel ')[1].split()[0]
        httpx.delete(f'{gateway}/api/kernels/{a2}', timeout=30).raise_for_status()
        out = _call(gateway, sid, 'exec', code='boot', agent='a')
        assert out.startswith('𝍁'+new) and out.endswith('41𝍁'), out
        a2 = out.split('created kernel ')[1].split()[0]
        assert _call(gateway, sid, 'exec', code='x') == '1', "an agent's restart or deletion reached the transport session kernel"
        _close(gateway, sid)
        assert not _kernels(gateway) & {main, b, a2}, "a kernel outlived its transport session"
    finally: httpx.delete(f'{gateway}/mcp', headers={'Mcp-Session-Id': sid}, timeout=30)


async def test_mcp_cancellation_and_interrupt(gateway):
    "Cancelling a queued request leaves the running cell alone. Cancelling the running request, or calling `interrupt`, stops its cell. The kernel keeps its state."
    loop = 'import asyncio\nwhile True: await asyncio.sleep(0.02)'
    sid = _session(gateway)
    try:
        kid = _call(gateway, sid, 'create', kernel='py').split()[2]
        _call(gateway, sid, 'exec', code='saved = 42')
        async with client(gateway, kid) as kc:
            iopub = JmsgQueues(kc, queues=('iopub',))
            def started(): return iopub.jmsg_for('execute_input', pred=lambda m: m['content']['code'] == loop, queue='iopub')
            req = dict(jsonrpc='2.0', id='loop', method='tools/call', params=dict(name='exec', arguments=dict(code=loop)))
            with concurrent.futures.ThreadPoolExecutor() as pool:
                pending = pool.submit(_post, gateway, req, sid)
                await started()
                queued = pool.submit(_post, gateway, dict(req, id='queued', params=dict(name='exec', arguments=dict(code='saved = 0'))), sid)
                time.sleep(0.05)
                assert not queued.done(), 'same-kernel MCP executions were not serialized'
                cancel = dict(jsonrpc='2.0', method='notifications/cancelled', params=dict(requestId='queued'))
                assert _post(gateway, cancel, sid).status_code == 202
                assert queued.result(timeout=5).status_code == 202
                assert not pending.done(), 'cancelling a queued request interrupted the active request'
                cancel['params']['requestId'] = 'loop'
                assert _post(gateway, cancel, sid).status_code == 202
                assert pending.result(timeout=5).status_code == 202
                assert _call(gateway, sid, 'exec', code='saved') == '42'
                pending = pool.submit(_call, gateway, sid, 'exec', code=loop)
                await started()
                _call(gateway, sid, 'interrupt')
                assert 'KeyboardInterrupt' in pending.result(timeout=5)
                # Cancelling an MCP request while a WebSocket cell runs must not interrupt the WebSocket cell.
                ws_code = "import asyncio\nawait asyncio.sleep(0.35)\nws_done = 99"
                ws_mid = kc.execute(ws_code)
                await iopub.jmsg_for('status', pred=lambda m: m['parent_header'].get('msg_id') == ws_mid and m['content'].get('execution_state') == 'busy', queue='iopub')
                queued_ws = pool.submit(_post, gateway, dict(req, id='behind-ws', params=dict(name='exec', arguments=dict(code='saved = -1'))), sid)
                time.sleep(0.05)
                assert _post(gateway, dict(jsonrpc='2.0', method='notifications/cancelled', params=dict(requestId='behind-ws')), sid).status_code == 202
                assert queued_ws.result(timeout=5).status_code == 202
                await iopub.jmsg_for('status', pred=lambda m: m['parent_header'].get('msg_id') == ws_mid and m['content'].get('execution_state') == 'idle', queue='iopub')
        assert _call(gateway, sid, 'exec', code='(ws_done, saved)') == '(99, 42)'
    finally: _close(gateway, sid)


def test_mcp_cwd_override(gateway, tmp_path):
    "An unlisted `cwd` argument overrides the session default `cwd` for kernels created by that call and survives restart, without altering session defaults."
    d_default = (tmp_path / 'default').resolve()
    d_create = (tmp_path / 'create').resolve()
    d_agent = (tmp_path / 'agent').resolve()
    for d in (d_default, d_create, d_agent): d.mkdir()
    sid = _session(gateway, defaults=dict(cwd=str(d_default)))
    try:
        _call(gateway, sid, 'create', kernel='py', cwd=str(d_create))
        assert _call(gateway, sid, 'exec', code='import os; os.getcwd()') == str(d_create)
        assert _call(gateway, sid, 'restart', cwd=str(d_agent)).startswith('restarted kernel ')
        assert _call(gateway, sid, 'exec', code='import os; os.getcwd()') == str(d_create)
        _call(gateway, sid, 'create', kernel='py', cwd='')
        assert _call(gateway, sid, 'exec', code='import os; os.getcwd()') == str(d_default)
        assert _call(gateway, sid, 'list_kernels', cwd=str(d_create))
        out = _call(gateway, sid, 'exec', code='import os; os.getcwd()', agent='sub', cwd=str(d_agent))
        assert out.endswith(str(d_agent)), out
        assert 'cwd must be a string' in _call(gateway, sid, 'create', error=True, kernel='py', cwd=123)
    finally: _close(gateway, sid)
