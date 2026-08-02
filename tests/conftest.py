import pytest
from jupygate.core import create_app, serve

@pytest.fixture
def gateway():
    "A live gateway on a random port; yields its base URL, shuts everything down after."
    server = serve(create_app(), port=0, in_thread=True)
    yield server.url
    server.should_exit = True
    server.thread.join(timeout=10)
