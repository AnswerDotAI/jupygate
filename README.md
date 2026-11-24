# jupygate

A standalone Jupyter server (contents, sessions, kernels, kernelspecs, terminals) with a thin MCP layer, distributed as a single static binary and reusable Rust library crate.

## Installation

Prebuilt wheels cover Linux x86-64, Linux ARM64, and Apple Silicon macOS:

```bash
pip install jupygate
```

Installing from the source distribution on other platforms requires a Rust toolchain.

## Running

```bash
jupygate [--host H] [--port N] [--port-file PATH] [--token T | --token-file PATH] [--trust-loopback] [--root DIR] [--tls] [--cull-idle SECONDS] [--exit-with-parent]
```

Defaults are `127.0.0.1`, port `8787`, no authentication, and the current working directory as `--root`. Pass `--port 0 --port-file PATH` to bind an ephemeral port and write the bound port number atomically to `PATH` once listening. When a token is configured, clients authenticate with `Authorization: Bearer T`, `Authorization: token T`, or `?token=T`. `--trust-loopback` lets loopback TCP peers omit the token. `--tls` serves HTTPS and WSS with a self-signed certificate generated at startup. `--exit-with-parent` shuts the gateway down once its parent process exits.

## Running as a service

`jupygate service` installs and controls a user service via launchd on macOS and systemd on Linux:

```bash
jupygate service install --root ~/git --workdir ~/git
jupygate service status
jupygate service restart
jupygate service stop
jupygate service start
jupygate service uninstall
```

## APIs

- Contents: `GET/POST /api/contents`, `GET/PUT/POST/PATCH/DELETE /api/contents/{path}` (stock Jupyter file, notebook, and directory models).
- Sessions: `GET/POST /api/sessions`, `GET/PATCH/DELETE /api/sessions/{sid}` (standard Jupyter path-to-kernel session reuse and rename tracking).
- Kernels: `GET/POST /api/kernels`, `GET/DELETE /api/kernels/{kid}`, `POST /api/kernels/{kid}/interrupt`, `POST /api/kernels/{kid}/restart`, `WS /api/kernels/{kid}/channels`. Direct kernel creates are always independent; an optional `path` sets the default working directory. A WebSocket reconnect starts a fresh event stream with no detached-session replay buffer. Internal kernel-channel loss requires a restart.
- Kernelspecs: `GET /api/kernelspecs`, `GET /api/kernelspecs/{name}` (matching `jupyter kernelspec list` search order).
- Terminals: `GET/POST /api/terminals`, `GET/DELETE /api/terminals/{name}`, `WS /terminals/websocket/{name}` (Jupyter terminado JSON-array framing). Terminal lifetimes are independent of kernel deletions.
- MCP: `POST /mcp` and `DELETE /mcp` (Streamable HTTP JSON-RPC with `list_kernels`, `create`, `use_kernel`, `delete_kernel`, `exec`, `restart`, `interrupt`, `/params/jupygate` session defaults, and `"jupygate": true` in the `initialize` result).
- Contract: `GET /` returns `{"version", "cwd"}` and `GET /openapi.json` returns the public OpenAPI 3.1 schema.

