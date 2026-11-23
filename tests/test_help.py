"Gateway status and the public OpenAPI document."
from pathlib import Path
import os, tomllib, httpx

_version = tomllib.loads((Path(__file__).parents[1]/'Cargo.toml').read_text())['package']['version']


def test_status(gateway, files_root):
    r = httpx.get(f'{gateway}/')
    assert r.status_code == 200 and r.json() == dict(version=_version, cwd=str(files_root.resolve()))


def test_openapi(gateway):
    doc = httpx.get(f'{gateway}/openapi.json').json()
    assert doc['openapi'].startswith('3.1')
    assert set(doc['paths']) == {
        '/api/contents', '/api/contents/{path}',
        '/api/sessions', '/api/sessions/{sid}',
        '/api/kernels', '/api/kernels/{kid}', '/api/kernels/{kid}/interrupt', '/api/kernels/{kid}/restart', '/api/kernels/{kid}/channels',
        '/api/kernelspecs', '/api/kernelspecs/{name}', '/api/terminals', '/api/terminals/{name}', '/terminals/websocket/{name}'}
    ids = [op.get('operationId') for ms in doc['paths'].values() for op in ms.values()]
    assert len(ids) == len(set(ids)), 'operationIds are unique'
    get_kernels = doc['paths']['/api/kernels']['get']
    assert get_kernels['operationId'] == 'kernels.list_kernels'
    assert 'parameters' not in get_kernels
    kid = [p for p in doc['paths']['/api/kernels/{kid}']['get']['parameters'] if p['in'] == 'path']
    assert kid[0]['name'] == 'kid' and kid[0]['required']
    ok = get_kernels['responses']['2XX']['content']['application/json']['schema']
    assert ok['items']['$ref'] == '#/components/schemas/KernelModel'
    km = doc['components']['schemas']['KernelModel']['properties']
    assert {'id', 'name', 'language', 'execution_state', 'unresponsive', 'execution_lost', 'connections', 'pid', 'path'} <= set(km)
    assert 'summary' not in get_kernels and 'description' not in km['id']
    err = get_kernels['responses']['default']['content']['application/json']['schema']
    assert err['$ref'] == '#/components/schemas/ErrMsg'
    body = doc['paths']['/api/kernels']['post']['requestBody']['content']['application/json']['schema']
    assert body['$ref'] == '#/components/schemas/CreateKernelBody'
    assert 'username' not in doc['components']['schemas']['CreateKernelBody']['properties']
    assert 'username' not in doc['components']['schemas']['TermCreateBody']['properties']
    assert {'name', 'alive', 'last_activity'} <= set(doc['components']['schemas']['TermModel']['properties'])
    assert 'path' not in doc['components']['schemas']['TermModel']['properties']


def test_port_file(tmp_path):
    "`--port 0 --port-file PATH` binds an ephemeral port and writes it atomically."
    import subprocess, time
    pf = tmp_path/'port'
    proc = subprocess.Popen(['jupygate', '--port', '0', '--port-file', str(pf), '--root', str(tmp_path)])
    try:
        deadline = time.monotonic() + 5
        while not pf.exists() and time.monotonic() < deadline: time.sleep(0.02)
        port = int(pf.read_text().strip())
        assert port > 0
        assert httpx.get(f'http://127.0.0.1:{port}/', timeout=5).status_code == 200
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    bad = subprocess.run(['jupygate', '--port', '0', '--root', str(tmp_path)],
        env={**os.environ, 'JUPYGATE_TOKEN': 'retired'}, capture_output=True, text=True, timeout=10)
    assert bad.returncode != 0 and '--token-file' in bad.stderr
