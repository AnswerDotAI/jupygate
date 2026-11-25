"Kernel lifecycle as whole-life stories: restarts (fast, ws-preserving, repeatable, failed, and cancellable on delete), env vars, and independent direct creates."
import os, stat, tempfile, time
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import httpx, pytest
from jupywire.session import Session, to_frame
from jupywire.route import JmsgQueues
from helpers import _connect, _exec, _recv_until, client


def test_restart_story(gateway, files_root):
    "Restarting a busy kernel is fast and id-preserving, the same ws still drives the new process, a second restart works too, and a restart whose respawn fails removes the kernel entry."
    script = Path(tempfile.mkdtemp())/'k.sh'
    script.write_text('#!/bin/sh\nset -e\n'
        "python -c 'import os, urllib.request; urllib.request.urlopen(os.environ[\"JUPYGATE_URL\"] + \"/api/kernels\", timeout=3)'\n"
        'exec python -m ipymini -f "$1"\n')
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    (files_root/'proj').mkdir()
    (files_root/'proj/restart.ipynb').write_text('{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}')
    (files_root/'proj/deep').mkdir()
    (files_root/'proj/alias').symlink_to(files_root/'proj/deep', target_is_directory=True)
    assert httpx.post(f'{gateway}/api/kernels', json=dict(path='proj/alias/another.ipynb')).status_code == 400
    r = httpx.post(f'{gateway}/api/kernels', json=dict(path='proj/restart.ipynb', argv=[str(script), '{connection_file}']), timeout=90)
    assert r.status_code == 201
    kid = r.json()['id']
    ses = Session(key=b'x')
    try:
        with _connect(gateway, kid, ses) as ws:
            ws.send(to_frame(_exec(ses, 'import os; (os.getcwd(), os.environ["JUPYGATE_ROOT"], os.environ["JUPYGATE_KERNEL_ID"], os.environ["JUPYGATE_KERNEL_PATH"])')))
            m = _recv_until(ws, lambda m: m['header']['msg_type'] == 'execute_result')
            assert literal_eval(m['content']['data']['text/plain']) == (str((files_root/'proj').resolve()), str(files_root), kid, 'proj/restart.ipynb')
            ws.send(to_frame(_exec(ses, 'import time\nwhile True: time.sleep(0.05)')))
            _recv_until(ws, lambda m: m['header']['msg_type'] == 'status' and m['content'].get('execution_state') == 'busy')
            t0 = time.monotonic()
            with ThreadPoolExecutor() as pool:
                restarting = pool.submit(httpx.post, f'{gateway}/api/kernels/{kid}/restart', timeout=30)
                _recv_until(ws, lambda m: m['header']['msg_type'] == 'status' and m['content']['execution_state'] == 'restarting')
                mid = _exec(ses, '6*7')
                ws.send(to_frame(mid))
                rej = _recv_until(ws, lambda m: m['channel'] == 'shell' and m['parent_header'].get('msg_id') == mid['header']['msg_id'])
                assert rej['content']['status'] == 'error' and 'restarting' in rej['content']['evalue'].lower()
                r = restarting.result()
            elapsed = time.monotonic() - t0
            assert r.status_code == 200 and r.json()['id'] == kid
            assert r.json()['execution_state'] == 'idle' and r.json()['unresponsive'] is False and r.json()['execution_lost'] is False
            assert elapsed < 4, f'restart took {elapsed:.1f}s: the graceful path was missed'
            ws.send(to_frame(_exec(ses, '6*7')))
            m = _recv_until(ws, lambda m: m['header']['msg_type'] == 'execute_result')
            assert m['content']['data']['text/plain'] == '42'
            ws.send(to_frame(_exec(ses, 'import os; (os.getcwd(), os.environ["JUPYGATE_ROOT"], os.environ["JUPYGATE_KERNEL_ID"], os.environ["JUPYGATE_KERNEL_PATH"])')))
            m = _recv_until(ws, lambda m: m['header']['msg_type'] == 'execute_result')
            assert literal_eval(m['content']['data']['text/plain']) == (str((files_root/'proj').resolve()), str(files_root), kid, 'proj/restart.ipynb')
            ws.send(to_frame(_exec(ses, 'import subprocess, sys; subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"]).pid')))
            child_pid = int(_recv_until(ws, lambda m: m['header']['msg_type'] == 'execute_result')['content']['data']['text/plain'])
            assert httpx.post(f'{gateway}/api/kernels/{kid}/restart', timeout=30).status_code == 200
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try: os.kill(child_pid, 0)
                except ProcessLookupError: break
                time.sleep(0.05)
            else: raise AssertionError(f'kernel child process {child_pid} survived restart')
            script.unlink()  # the respawn inside the next restart now fails
            r2 = httpx.post(f'{gateway}/api/kernels/{kid}/restart', timeout=30)
            assert r2.status_code == 500, f'expected a restart failure, got {r2.status_code}'
            assert httpx.get(f'{gateway}/api/kernels/{kid}', timeout=10).status_code == 404
    finally:
        httpx.delete(f'{gateway}/api/kernels/{kid}', timeout=30)


async def test_independent_creates_death_and_delete_during_restart(gateway, files_root):
    "Two direct POST /api/kernels with the same path create independent kernels; death is broadcast and shows in the model; DELETE cancels a hung restart promptly without waiting for startup timeout."
    # Kernel launchers can outlive their sockets while cleaning up.
    launch = dict(path='pf.ipynb', argv=['sh', '-c', 'python -m ipymini -f "$1"; sleep 3', 'sh', '{connection_file}'])
    r1, r2 = [httpx.post(f'{gateway}/api/kernels', json=launch, timeout=90) for _ in range(2)]
    assert r1.status_code == 201 and r2.status_code == 201
    kid1, kid2 = r1.json()['id'], r2.json()['id']
    assert kid1 != kid2, 'direct kernel creates must be independent'
    assert httpx.post(f'{gateway}/api/kernels', json=dict(path='./pf.ipynb'), timeout=90).status_code == 400
    assert httpx.post(f'{gateway}/api/kernels', json=dict(cull_idle=-1), timeout=10).status_code == 400
    kid3 = None
    try:
        async with client(gateway, kid1) as kc:
            iopub = JmsgQueues(kc, queues=('iopub',))
            await kc.shutdown()
            await iopub.jmsg_for('execution_lost', queue='iopub')
            lost = httpx.get(f'{gateway}/api/kernels/{kid1}').json()
            assert lost['execution_lost'] and not lost['unresponsive'] and lost['execution_state'] == 'idle'
            await iopub.jmsg_for('status', pred=lambda m: m['content']['execution_state'] == 'dead', queue='iopub')
            assert httpx.get(f'{gateway}/api/kernels/{kid1}', timeout=10).json()['execution_state'] == 'dead'
            with pytest.raises(ProcessLookupError): os.kill(lost['pid'], 0)
        assert httpx.get(f'{gateway}/api/kernels/{kid2}', timeout=10).json()['execution_state'] == 'idle'

        # Delete cancels a hung restart immediately instead of blocking on START_TIMEOUT.
        flag = Path(tempfile.mkdtemp())/'hang'
        script = flag.parent/'slow_restart.sh'
        script.write_text(f'#!/bin/sh\nif [ -f "{flag}" ]; then sleep 60; fi\nexec python -m ipymini -f "$1"\n')
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        r3 = httpx.post(f'{gateway}/api/kernels', json=dict(argv=[str(script), '{connection_file}']), timeout=90)
        assert r3.status_code == 201
        kid3 = r3.json()['id']
        flag.write_text('1')
        with ThreadPoolExecutor() as pool:
            restarting = pool.submit(httpx.post, f'{gateway}/api/kernels/{kid3}/restart', timeout=30)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if httpx.get(f'{gateway}/api/kernels/{kid3}', timeout=10).json()['execution_state'] == 'restarting': break
                time.sleep(0.05)
            t0 = time.monotonic()
            assert httpx.delete(f'{gateway}/api/kernels/{kid3}', timeout=10).status_code == 204
            assert time.monotonic() - t0 < 5, 'delete blocked on hung restart'
            assert restarting.result(timeout=5).status_code == 404
            assert httpx.get(f'{gateway}/api/kernels/{kid3}', timeout=10).status_code == 404
        kid3 = None
    finally:
        for k in (kid1, kid2, kid3):
            if k: httpx.delete(f'{gateway}/api/kernels/{k}', timeout=30)
