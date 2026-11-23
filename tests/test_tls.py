"TLS: --tls serves https and wss from a self-signed in-memory certificate."
import httpx
from jupyasyncclient import start_gateway
from helpers import client


async def test_tls(tmp_path):
    "One kernel story over https and wss, verification skipped client-side."
    g = start_gateway(['jupygate', '--root', str(tmp_path)], tls=True)
    try:
        assert g.url.startswith('https://')
        with httpx.Client(base_url=g.url, verify=False, timeout=90) as http:
            assert http.get('/api/kernels').json() == []
            kid = http.post('/api/kernels').json()['id']
            async with client(g.url, kid, verify=False) as kc: await kc.kernel_info()  # over wss
            assert http.delete(f'/api/kernels/{kid}').status_code == 204
    finally: g.stop()
