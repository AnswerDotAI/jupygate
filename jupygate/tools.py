"Start and stop gateway binaries in the background, for tests and ad-hoc kernel use."
import atexit, signal, socket, subprocess, tempfile, time
from pathlib import Path

_running = []

class Gateway:
    "A background gateway process and its base URL."
    def __init__(self, proc, url, out, err): self.proc,self.url,self.out,self.err = proc,url,out,err

    def stop(
        self,
        timeout=10, # Seconds to allow graceful shutdown before SIGKILL
    ):
        "SIGINT (so the gateway reaps its kernels), then SIGKILL after `timeout`."
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try: self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout)
        if self in _running: _running.remove(self)

    def __repr__(self):
        state = 'up' if self.proc.poll() is None else f'exited {self.proc.returncode}'
        return f'<Gateway {self.url} pid={self.proc.pid} {state}>'

def _free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def start_gateway(
    argv=('rustygate',), # Gateway command; e.g. ('rustygate',) or ('jupygate',)
    port=0,              # Port to listen on; 0 picks a free one
    token=None,          # Auth token passed as --token
    timeout=10,          # Seconds to wait for the port to accept connections
):
    "Spawn a gateway binary in the background, returning a `Gateway` once it is listening."
    port = port or _free_port()
    cmd = [*argv, '--port', str(port), *(['--token', token] if token else [])]
    d = Path(tempfile.mkdtemp(prefix='gateway-'))
    out,err = d/'stdout.txt', d/'stderr.txt'
    proc = subprocess.Popen(cmd, stdout=open(out, 'w'), stderr=open(err, 'w'))
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            raise RuntimeError(f'gateway exited with {proc.returncode}; stderr:\n{err.read_text()}')
        try:
            socket.create_connection(('127.0.0.1', port), 0.2).close()
            break
        except OSError: time.sleep(0.05)
    else:
        proc.kill()
        raise TimeoutError(f'gateway not listening after {timeout}s; stderr:\n{err.read_text()}')
    g = Gateway(proc, f'http://127.0.0.1:{port}', out, err)
    _running.append(g)
    return g

def stop_all():
    "Stop every gateway this module started."
    for g in list(_running): g.stop()

atexit.register(stop_all)
