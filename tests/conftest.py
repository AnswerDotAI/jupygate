import os

import pytest, httpx
from helpers import _new_kernel
from jupyasyncclient import start_gateway

_root = None


@pytest.fixture(scope='session', autouse=True)
def jupyter_path(tmp_path_factory):
    "A scratch Jupyter data directory, first on the kernelspec search path of every gateway the suite starts."
    jupyter = tmp_path_factory.mktemp('jupyter')
    os.environ['JUPYTER_PATH'] = str(jupyter)
    return jupyter


@pytest.fixture(scope='session')
def gateway(tmp_path_factory):
    "A live gateway's base URL: JUPYGATE_TEST_URL if set, else the installed `jupygate` serving a scratch files root."
    global _root
    url = os.environ.get('JUPYGATE_TEST_URL')
    if url:
        yield url.rstrip('/')
        return
    _root = tmp_path_factory.mktemp('files')
    g = start_gateway(['jupygate', '--root', str(_root)])
    yield g.url
    g.stop()


@pytest.fixture
def files_root(gateway):
    "The gateway's scratch files root; unknowable for an external gateway."
    if _root is None: pytest.skip('external gateway: files root unknown')
    return _root


@pytest.fixture
def kernels_dir(gateway, jupyter_path):
    "A scratch kernels directory, first on the gateway's kernelspec search path. Skips for an external gateway."
    if _root is None: pytest.skip('external gateway: kernelspec path unknown')
    return jupyter_path/'kernels'


@pytest.fixture(scope='session')
def shared_kernel(gateway):
    "One kernel shared by the non-destructive messaging tests: the spawn paid once per worker, teardown owned here."
    kid = _new_kernel(gateway)
    yield kid
    httpx.delete(f'{gateway}/api/kernels/{kid}', timeout=30)
