"Wheel-level smoke and edge/security checks for /api/contents, /api/sessions, and directory-path kernel cwd (full stock parity lives in jupyasyncclient)."
import httpx
from helpers import client


async def test_contents_sessions_edges_and_dir_cwd(gateway, files_root):
    assert httpx.put(f'{gateway}/api/contents/work', json=dict(type='directory'), timeout=10).status_code == 201
    assert httpx.put(f'{gateway}/api/contents/work/demo.ipynb', json=dict(type='notebook'), timeout=10).status_code == 201

    # Security and conflict edges owned by jupygate.
    assert httpx.put(f'{gateway}/api/contents/work', json=dict(type='file', format='text', content='x'), timeout=10).status_code == 409
    assert httpx.post(f'{gateway}/api/contents/work', json=dict(copy_from='work'), timeout=10).status_code == 400
    assert httpx.post(f'{gateway}/api/contents/work', json=dict(type='file', ext='/../../escape.txt'), timeout=10).status_code == 400
    assert httpx.post(f'{gateway}/api/contents/work', json=dict(type='file', ext='..'), timeout=10).status_code == 400
    assert httpx.post(f'{gateway}/api/contents/work', json=dict(type='file', ext='a/b.txt'), timeout=10).status_code == 400
    (files_root/'work/escape').symlink_to('/etc')
    assert httpx.get(f'{gateway}/api/contents/work/escape', timeout=10).status_code == 400
    assert httpx.delete(f'{gateway}/api/contents/work/escape', timeout=10).status_code == 204
    assert httpx.post(f'{gateway}/api/sessions', json=dict(path='../escape.ipynb', type='notebook'), timeout=10).status_code == 400

    # Directory path on POST /api/kernels sets cwd to that directory (matching stock jupyter_server).
    k_dir = httpx.post(f'{gateway}/api/kernels', json=dict(path='work'), timeout=90)
    assert k_dir.status_code == 201
    dir_kid = k_dir.json()['id']

    # Session reuse by path, conflict on PATCH to another session's path, and DELETE cleanup.
    s1 = httpx.post(f'{gateway}/api/sessions', json=dict(path='work/demo.ipynb', type='notebook'), timeout=90)
    assert s1.status_code == 201
    sid1, kid1 = s1.json()['id'], s1.json()['kernel']['id']
    sid2 = None
    try:
        async with client(gateway, dir_kid) as kc:
            assert await kc.eval_expr('__import__("os").getcwd()') == str((files_root/'work').resolve())
        s_again = httpx.post(f'{gateway}/api/sessions', json=dict(path='work/demo.ipynb', type='notebook'), timeout=30)
        assert s_again.status_code == 201 and s_again.json()['id'] == sid1 and s_again.json()['kernel']['id'] == kid1
        s2 = httpx.post(f'{gateway}/api/sessions', json=dict(path='work/other.ipynb', type='notebook'), timeout=90)
        assert s2.status_code == 201
        sid2 = s2.json()['id']
        assert httpx.patch(f'{gateway}/api/sessions/{sid2}', json=dict(path='work/demo.ipynb'), timeout=10).status_code == 409
        assert httpx.delete(f'{gateway}/api/sessions/{sid1}', timeout=30).status_code == 204
        assert httpx.get(f'{gateway}/api/kernels/{kid1}', timeout=10).status_code == 404
        sid1 = None
    finally:
        httpx.delete(f'{gateway}/api/kernels/{dir_kid}', timeout=30)
        for sid in (sid1, sid2):
            if sid: httpx.delete(f'{gateway}/api/sessions/{sid}', timeout=30)
        for p in ('work/demo.ipynb', 'work'):
            httpx.delete(f'{gateway}/api/contents/{p}', timeout=10)
