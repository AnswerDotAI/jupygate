//! jupygate: standalone open-source Jupyter server (contents, sessions, kernels,
//! kernelspecs, terminals) with a thin MCP layer, structured as reusable building blocks
//! plus a single static CLI binary.

pub mod api;
pub mod channels;
pub mod contents;
pub mod gateway;
pub mod kernelspec;
pub mod mcp;
pub mod mux_core;
pub mod openapi;
pub mod paths;
pub mod proc;
pub mod server;
pub mod service;
pub mod session;
pub mod sessions;
pub mod term;

#[doc(hidden)]
pub use axum as __axum;

pub use api::err;
pub use server::{cull_cutoff, should_cull};

use api::{Body, Qry};
use axum::Router;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use gateway::{GatewayKernel, KernelModel, Kernels, LaunchInput};
use mcp::{Mcp, McpBackend, McpCreate, McpHost, McpKernel};
use serde_json::Value;
use sessions::Sessions;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::{env, process, slice};
use term::Terminals;
use tokio::time;

const USAGE: &str = "usage: jupygate [--host H] [--port N] [--port-file PATH] [--token T | --token-file PATH] [--trust-loopback] [--root DIR] [--tls] [--cull-idle SECONDS] [--exit-with-parent]";

/// The server's command-line options, read by `main` and checked by `jupygate service install`.
struct Opts {
    host: String, port: u16, port_file: Option<PathBuf>, token: Option<String>, token_file: Option<String>, trust_loopback: bool, tls: bool, root: PathBuf,
    cull_idle: f64, exit_with_parent: bool
}

impl Opts {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let root = env::current_dir().map_err(|e| format!("cannot read working directory: {e}"))?;
        let mut o = Opts { host: "127.0.0.1".into(), port: 8787, port_file: None, token: None, token_file: None, trust_loopback: false, tls: false, root,
            cull_idle: 0.0, exit_with_parent: false };
        let mut args = args.into_iter();
        while let Some(a) = args.next() {
            let mut val = || args.next().ok_or_else(|| format!("{a} needs a value\n{USAGE}"));
            match a.as_str() {
                "--host" => o.host = val()?, "--port" => o.port = val()?.parse().map_err(|_| "--port must be a number".to_string())?,
                "--port-file" => o.port_file = Some(val()?.into()), "--token" => o.token = Some(val()?), "--token-file" => o.token_file = Some(val()?),
                "--trust-loopback" => o.trust_loopback = true, "--tls" => o.tls = true, "--root" => o.root = val()?.into(),
                "--cull-idle" => o.cull_idle = val()?.parse().map_err(|_| "--cull-idle must be a number of seconds".to_string())?,
                "--exit-with-parent" => o.exit_with_parent = true, "--help" | "-h" => { println!("{USAGE}"); process::exit(0) }
                _ => return Err(format!("unknown option {a}\n{USAGE}"))
            }
        }
        if o.token.is_some() && o.token_file.is_some() { return Err(format!("--token and --token-file are mutually exclusive\n{USAGE}")); }
        Ok(o)
    }
}

/// How long a subagent kernel may stay idle before culling stops it.
pub const AGENT_IDLE: f64 = 1800.0;

async fn cull(app: AppState, secs: f64) {
    loop {
        time::sleep(Duration::from_secs(60)).await;
        for kid in app.kernels.idle(secs, |gk| gk.lifecycle.launch.agent.is_some().then_some(AGENT_IDLE)).await { app.delete_kernel(&kid).await; }
    }
}

/// The gateway's own base URL, for its children: set once at bind, read at kernel spawn.
pub static SELF_URL: OnceLock<String> = OnceLock::new();

#[derive(Clone)]
pub struct AppState {
    pub kernels: Arc<Kernels>, pub sessions: Arc<Sessions>, pub terminals: Arc<Terminals>, token: Option<String>, trust_loopback: bool, pub root: Arc<PathBuf>,
    pub mcp: Arc<Mcp>
}

impl McpHost for AppState { fn mcp(&self) -> &Mcp { &self.mcp } }

fn to_mcp_kernel(m: KernelModel, agent: Option<String>) -> McpKernel {
    McpKernel { id: m.id, name: m.name, language: m.language, execution_state: m.execution_state, connections: m.connections, path: m.path, agent }
}

impl AppState {
    async fn add_mcp_connections(&self, models: &mut [KernelModel]) {
        let counts = self.mcp.connection_counts().await;
        for m in models { m.connections += counts.get(&m.id).copied().unwrap_or(0); }
    }

    async fn kernel_models(&self) -> Vec<KernelModel> {
        let mut ms = self.kernels.models().await;
        self.add_mcp_connections(&mut ms).await;
        ms
    }

    pub(crate) async fn kernel_model(&self, kid: &str) -> Option<KernelModel> {
        let mut m = self.kernels.model(kid).await?;
        self.add_mcp_connections(slice::from_mut(&mut m)).await;
        Some(m)
    }

    pub(crate) async fn delete_kernel(&self, kid: &str) -> bool {
        let ok = self.kernels.delete(kid).await;
        if ok {
            self.mcp.clear_kernel(kid).await;
            self.sessions.remove_by_kernel(kid).await;
        }
        ok
    }

    pub(crate) async fn restart_kernel(&self, kid: &str) -> Option<Result<KernelModel, gateway::RestartError>> {
        let res = self.kernels.restart(kid).await?;
        if res.is_err() {
            self.mcp.clear_kernel(kid).await;
            self.sessions.remove_by_kernel(kid).await;
        }
        Some(res)
    }

    async fn mcp_kernel_of(&self, gk: &GatewayKernel) -> McpKernel {
        let mut m = gk.model().await;
        self.add_mcp_connections(slice::from_mut(&mut m)).await;
        to_mcp_kernel(m, gk.lifecycle.launch.agent.clone())
    }
}

impl McpBackend for AppState {
    type Caller = ();

    async fn list_kernels(&self) -> Vec<McpKernel> {
        let mut rows = self.kernels.models_with_agents().await;
        let counts = self.mcp.connection_counts().await;
        for (m, _) in &mut rows { m.connections += counts.get(&m.id).copied().unwrap_or(0); }
        rows.into_iter().map(|(m, agent)| to_mcp_kernel(m, agent)).collect()
    }

    async fn get_kernel(&self, kid: &str) -> Option<McpKernel> { let gk = self.kernels.get(kid).await?; Some(self.mcp_kernel_of(&gk).await) }

    async fn kernel_state(&self, kid: &str) -> Option<(String, String)> {
        let gk = self.kernels.get(kid).await?;
        Some((gk.lifecycle.launch.name.clone(), gk.lifecycle.execution_state().await.to_string()))
    }

    async fn create_kernel(&self, req: McpCreate, _caller: &Self::Caller) -> Result<McpKernel, String> {
        let specs = kernelspec::specs().await;
        if !specs.contains_key(&req.kernel) { return Err(kernelspec::unknown(&req.kernel, &specs)); }
        let agent = req.agent.clone();
        let input = LaunchInput { name: Some(req.kernel), appendenv: req.appendenv, cwd: req.cwd, launcher: Some(req.launcher.into()), agent: req.agent,
            ..LaunchInput::default() };
        let m = self.kernels.create(input, None).await?;
        Ok(to_mcp_kernel(m, agent))
    }

    async fn execute(
        &self, kid: &str, code: &str, _caller: &Self::Caller, cancel: &mut tokio::sync::watch::Receiver<bool>) -> Result<Vec<session::Msg>, String> {
        let gk = self.kernels.get(kid).await.ok_or("kernel disappeared")?;
        let mux = &gk.mux;
        gk.lifecycle.mcp_execute(code, cancel, |sid, msg| async move { mux.handle_message(&sid, msg).await }, self.kernels.interrupt(kid)).await
    }

    async fn delete_kernel(&self, kid: &str) -> bool { self.delete_kernel(kid).await }

    async fn restart_kernel(&self, kid: &str, _caller: &Self::Caller) -> Option<Result<McpKernel, String>> {
        let res = self.restart_kernel(kid).await?;
        match res {
            Ok(m) => {
                let ag = self.kernels.get(&m.id).await.and_then(|gk| gk.lifecycle.launch.agent.clone());
                Some(Ok(to_mcp_kernel(m, ag)))
            }
            Err(e) => Some(Err(e.into_message())),
        }
    }

    async fn interrupt_kernel(&self, kid: &str, _caller: &Self::Caller) -> Option<Result<(), String>> { self.kernels.interrupt(kid).await }
}

#[derive(serde::Serialize, schemars::JsonSchema)]
struct Status { version: &'static str, cwd: String }

async fn status(State(app): State<AppState>) -> Json<Status> { Json(Status { version: env!("CARGO_PKG_VERSION"), cwd: app.root.display().to_string() }) }

async fn list_kernels(State(app): State<AppState>) -> Json<Vec<KernelModel>> { Json(app.kernel_models().await) }

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct CreateKernelBody {
    name: Option<String>, path: Option<String>, argv: Option<Vec<String>>, env: Option<HashMap<String, String>>, appendenv: Option<HashMap<String, String>>,
    cwd: Option<String>, cull_idle: Option<f64>
}

async fn create_kernel(State(app): State<AppState>, Body(b): Body<CreateKernelBody>) -> Response {
    if let Some(s) = b.cull_idle && (!s.is_finite() || s < 0.0) { return err(StatusCode::BAD_REQUEST, "cull_idle must be a non-negative number of seconds"); }
    if let Some(p) = b.path.as_deref() && let Err(e) = paths::canonical_rel(&app.root, p) { return err(StatusCode::BAD_REQUEST, &e); }
    let input = LaunchInput { name: b.name, argv: b.argv, env: b.env, appendenv: b.appendenv, cwd: b.cwd, cull_idle: b.cull_idle, ..LaunchInput::default() };
    match app.kernels.create(input, b.path).await {
        Ok(m) => (StatusCode::CREATED, Json(m)).into_response(),
        Err(e) => {
            let code = if e.starts_with("unknown kernel") || e.contains("cannot be combined with argv") { StatusCode::BAD_REQUEST }
            else { StatusCode::INTERNAL_SERVER_ERROR };
            err(code, &format!("kernel failed to start: {e}"))
        }
    }
}

async fn get_kernel(State(app): State<AppState>, Path(kid): Path<String>) -> Response {
    match app.kernel_model(&kid).await { Some(m) => Json(m).into_response(), None => err(StatusCode::NOT_FOUND, "no such kernel") }
}

async fn delete_kernel(State(app): State<AppState>, Path(kid): Path<String>) -> Response {
    match app.delete_kernel(&kid).await { true => StatusCode::NO_CONTENT.into_response(), false => err(StatusCode::NOT_FOUND, "no such kernel") }
}

async fn interrupt(State(app): State<AppState>, Path(kid): Path<String>) -> Response {
    match app.kernels.interrupt(&kid).await {
        Some(Ok(())) => StatusCode::NO_CONTENT.into_response(), Some(Err(e)) => err(StatusCode::INTERNAL_SERVER_ERROR, &e),
        None => err(StatusCode::NOT_FOUND, "no such kernel")
    }
}

async fn restart(State(app): State<AppState>, Path(kid): Path<String>) -> Response {
    match app.restart_kernel(&kid).await {
        Some(Ok(m)) => Json(m).into_response(), Some(Err(e)) => err(StatusCode::INTERNAL_SERVER_ERROR, e.message()),
        None => err(StatusCode::NOT_FOUND, "no such kernel")
    }
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct ChannelsQ { session_id: Option<String> }

async fn channels_ws(ws: WebSocketUpgrade, headers: HeaderMap, State(app): State<AppState>, Path(kid): Path<String>, Qry(q): Qry<ChannelsQ>) -> Response {
    if !server::origin_ok(&headers) { return err(StatusCode::FORBIDDEN, "Origin does not match Host"); }
    let Some(gk) = app.kernels.get(&kid).await else { return err(StatusCode::NOT_FOUND, "no such kernel"); };
    let sid = q.session_id;
    ws.on_upgrade(move |socket| kernel_client(socket, app.kernels, gk, sid))
}

async fn kernel_client(socket: WebSocket, kernels: Arc<Kernels>, gk: Arc<GatewayKernel>, sid: Option<String>) {
    let kid = gk.mux.kid.clone();
    mux_core::serve_ws(socket, gk.mux.core.clone(), sid, move |session_id, msg| {
        let (kernels, kid) = (kernels.clone(), kid.clone());
        async move { kernels.handle_msg(&kid, &session_id, msg).await }
    }).await;
}

crate::routes! {
    get "/" => status ret(Status),
    pub get "/api/kernels" => list_kernels ret(Vec<KernelModel>),
    pub post "/api/kernels" => create_kernel body(CreateKernelBody) ret(KernelModel),
    pub get "/api/kernels/{kid}" => get_kernel ret(KernelModel),
    pub delete "/api/kernels/{kid}" => delete_kernel,
    pub post "/api/kernels/{kid}/interrupt" => interrupt,
    pub post "/api/kernels/{kid}/restart" => restart ret(KernelModel),
    pub get "/api/kernels/{kid}/channels" => channels_ws query(ChannelsQ),
}

fn base_routes() -> openapi::RouteSet<AppState> {
    routes().merge(contents::routes()).merge(sessions::routes()).merge(kernelspec::routes()).merge(mcp::routes()).merge(term::routes())
}

fn all_routes() -> openapi::RouteSet<AppState> {
    let base = base_routes();
    let public_doc = openapi::spec("jupygate", env!("CARGO_PKG_VERSION"), &base.routes, false);
    base.merge(openapi::openapi_routes(public_doc))
}

/// The OpenAPI document: with `full`, every route; otherwise the public ones only.
pub fn spec(full: bool) -> Value { let all = all_routes(); openapi::spec("jupygate", env!("CARGO_PKG_VERSION"), &all.routes, full) }

fn router(app: AppState) -> Router {
    let inner = all_routes().router.layer(axum::extract::DefaultBodyLimit::disable()).with_state(app.clone());
    server::with_auth(inner, app.token.clone(), app.trust_loopback)
}

#[tokio::main]
pub async fn main() {
    let mut args = env::args();
    let program = args.next().expect("argv[0]");
    let mut args = args.peekable();
    if args.peek().is_some_and(|a| a == "service") {
        args.next();
        let args = args.collect::<Vec<_>>();
        match service::run(&service::JUPYGATE, &program, &args, |a| Opts::parse(a).map(|_| ())) {
            Ok(code) => process::exit(code),
            Err(e) => {
                eprintln!("{e}");
                process::exit(2);
            }
        }
    }
    let o = Opts::parse(args).unwrap_or_else(|e| { eprintln!("{e}"); process::exit(2) });
    let token = server::load_token(o.token.as_deref(), o.token_file.as_deref()).unwrap_or_else(|e| {
        eprintln!("{e}");
        process::exit(2)
    });
    let root = std::path::absolute(&o.root).expect("absolute served root");
    let kernels = Arc::new(Kernels::new(root.clone()));
    let sessions = Arc::new(Sessions::new());
    let terminals = Arc::new(Terminals::new(root.clone()));
    let app = AppState { kernels, sessions, terminals, mcp: Arc::new(Mcp::default()), token, trust_loopback: o.trust_loopback, root: Arc::new(root) };
    let (std_listener, addr) = server::bind_listener(&o.host, o.port).expect("bind failed");
    if let Some(pf) = &o.port_file { server::write_port_file(pf, addr.port()).expect("write port file"); }
    let scheme = if o.tls { "https" } else { "http" };
    SELF_URL.set(format!("{scheme}://127.0.0.1:{}", addr.port())).expect("set once");
    tokio::spawn(cull(app.clone(), o.cull_idle));
    let mcp = app.mcp.clone();
    let signal = async move {
        server::shutdown_signal(o.exit_with_parent).await;
        mcp.cancel_all().await;
    };
    server::serve_router(std_listener, o.tls, &o.host, router(app.clone()), signal).await;
    app.kernels.shutdown().await;
    app.terminals.shutdown().await;
}
