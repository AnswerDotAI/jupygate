"The gateway finds the kernelspecs Jupyter finds, and launches them by name."
import json, httpx
from jupyter_client.kernelspec import KernelSpecManager
from jupyasyncclient import start_gateway
from helpers import client


async def test_kernelspecs_match_jupyter(gateway, kernels_dir, tmp_path):
    "A spec installed while the gateway runs is listed, resolved as jupyter_client resolves it, and launchable by name."
    spec = dict(argv=['python', '-m', 'ipykernel', '-f', '{connection_file}'], display_name='Scratch', language='python')
    (kernels_dir/'Scratch').mkdir(parents=True)
    (kernels_dir/'Scratch/kernel.json').write_text(json.dumps(spec))
    listed = httpx.get(f'{gateway}/api/kernelspecs', timeout=10).json()
    assert listed['default'] == 'py'

    ksm = KernelSpecManager(ensure_native_kernel=False)
    ours = {k: v['spec']['argv'] for k,v in listed['kernelspecs'].items()}
    assert ours == {k: ksm.get_kernel_spec(k).argv for k in ksm.find_kernel_specs()}
    assert {'py', 'scratch'} <= set(ours)

    assert httpx.get(f'{gateway}/api/kernelspecs/scratch', timeout=10).json()['spec'] == spec
    assert httpx.get(f'{gateway}/api/kernelspecs/missing', timeout=10).status_code == 404
    r = httpx.post(f'{gateway}/api/kernels', json=dict(name='scratch', appendenv=dict(IPYTHONDIR=str(tmp_path/'ipython'))), timeout=90)
    assert r.status_code == 201
    kernel = r.json()
    async with client(gateway, kernel['id']) as kc:
        outputs = await kc.exec_outs('print(6 * 7)')
        assert any(o['output_type'] == 'stream' and o['text'].strip() == '42' for o in outputs)
    httpx.delete(f"{gateway}/api/kernels/{kernel['id']}", timeout=30)
    assert kernel['name'] == 'scratch' and kernel['language'] == 'python'

    # Duplicate names resolve by search-path order, then fall back to the user directory.
    roots = {name: tmp_path/name for name in ('first', 'second', 'user')}
    for name, root in roots.items():
        target = root/'kernels/priority'
        target.mkdir(parents=True)
        (target/'kernel.json').write_text(json.dumps(spec | dict(display_name=name)))
    for order in [('first', 'second'), ('second', 'first'), ()]:
        dirs = [roots[name] for name in order]
        args = ['env', f'JUPYTER_PATH={":".join(map(str, dirs))}', f'JUPYTER_DATA_DIR={roots["user"]}', 'jupygate']
        g = start_gateway(args)
        try:
            ksm = KernelSpecManager(kernel_dirs=[str(p/'kernels') for p in [*dirs, roots['user']]], ensure_native_kernel=False)
            found = httpx.get(f'{g.url}/api/kernelspecs/priority', timeout=10).json()['spec']
            assert found['display_name'] == ksm.get_kernel_spec('priority').display_name == (order[0] if order else 'user')
        finally: g.stop()
