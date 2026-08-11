import os
import pytest
from jupygate.core import create_app, serve

@pytest.fixture
def gateway():
    "A live gateway's base URL: JUPYGATE_TEST_URL if set (an external gateway under conformance test), else a fresh in-thread server."
    url = os.environ.get('JUPYGATE_TEST_URL')
    if url:
        yield url.rstrip('/')
        return
    server = serve(create_app(), port=0, in_thread=True)
    yield server.url
    server.should_exit = True
    server.thread.join(timeout=10)

in_proc = pytest.mark.skipif(bool(os.environ.get('JUPYGATE_TEST_URL')),
    reason='inspects in-process gateway internals; not a conformance test')
