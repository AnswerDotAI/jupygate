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
use std::process;
use std::sync::{Arc, OnceLock};
use term::Terminals;

const USAGE: &str = "usage: jupygate [--host H] [--port N] [--port-file PATH] [--token T | --token-file PATH] [--trust-loopback] [--root DIR] [--tls] [--cull-idle SECONDS] [--exit-with-parent]";

/// The gateway's own base URL, for its children: set once at bind, read at kernel spawn.
pub static SELF_URL: OnceLock<String> = OnceLock::new();

#[derive(Clone)]
pub struct AppState {
    pub kernels: Arc<Kernels>, pub sessions: Arc<Sessions>, pub terminals: Arc<Terminals>, token: Option<String>, trust_loopback: bool, pub root: Arc<PathBuf>,
    pub mcp: Arc<Mcp>
}

impl McpHost for AppState { fn mcp(&self) -> &Mcp { &self.mcp } }

impl AppState {
    async fn kernel_models(&self) -> Vec<KernelModel> {
        let mut ms = self.kernels.models().await;
        self.mcp.add_connections(&mut ms).await;
        ms
    }

    pub(crate) async fn kernel_model(&self, kid: &str) -> Option<KernelModel> {
        let mut m = self.kernels.model(kid).await?;
        self.mcp.add_connections([&mut m]).await;
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
        let mut res = self.kernels.restart(kid).await?;
        match &mut res {
            Ok(m) => self.mcp.add_connections([m]).await,
            Err(_) => {
                self.mcp.clear_kernel(kid).await;
                self.sessions.remove_by_kernel(kid).await;
            }
        }
        Some(res)
    }
}

impl McpBackend for AppState {
    type Caller = ();

    async fn list_kernels(&self) -> Vec<McpKernel> { self.kernel_models().await.into_iter().map(Into::into).collect() }

    async fn get_kernel(&self, kid: &str) -> Option<McpKernel> { self.kernel_model(kid).await.map(Into::into) }

    async fn kernel_state(&self, kid: &str) -> Option<(String, String)> {
        let gk = self.kernels.get(kid).await?;
        Some((gk.lifecycle.launch.name.clone(), gk.lifecycle.execution_state().await.to_string()))
    }

    async fn create_kernel(&self, req: McpCreate, _caller: &Self::Caller) -> Result<McpKernel, String> {
        Ok(self.kernels.create(req.into_launch_input().await?, None).await?.into())
    }

    async fn execute(
        &self, kid: &str, code: &str, _caller: &Self::Caller, cancel: &mut tokio::sync::watch::Receiver<bool>) -> Result<Vec<session::Msg>, String> {
        let gk = self.kernels.get(kid).await.ok_or("kernel disappeared")?;
        let core = gk.core();
        gk.lifecycle.mcp_execute(code, cancel, |sid, msg| async move { core.handle_message(&sid, msg).await }, self.kernels.interrupt(kid)).await
    }

    async fn delete_kernel(&self, kid: &str) -> bool { self.delete_kernel(kid).await }

    async fn restart_kernel(&self, kid: &str, _caller: &Self::Caller) -> Option<Result<McpKernel, String>> {
        Some(self.restart_kernel(kid).await?.map(Into::into).map_err(gateway::RestartError::into_message))
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
    let kid = gk.kid().to_string();
    mux_core::serve_ws(socket, gk.core().clone(), sid, move |session_id, msg| {
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
    let args = server::cli_args(&service::JUPYGATE, |a| server::ServerOpts::parse(a.iter().cloned(), USAGE).map(|_| ()));
    let o = server::ServerOpts::parse(args, USAGE).unwrap_or_else(|e| { eprintln!("{e}"); process::exit(2) });
    let token = o.load_token().unwrap_or_else(|e| { eprintln!("{e}"); process::exit(2) });
    let root = std::path::absolute(&o.root).expect("absolute served root");
    let kernels = Arc::new(Kernels::new(root.clone()));
    let sessions = Arc::new(Sessions::new());
    let terminals = Arc::new(Terminals::new(root.clone()));
    let app = AppState { kernels, sessions, terminals, mcp: Arc::new(Mcp::default()), token, trust_loopback: o.trust_loopback, root: Arc::new(root) };
    let (std_listener, _addr, self_url) = o.bind().expect("bind failed");
    SELF_URL.set(self_url).expect("set once");
    let cull_idle = o.cull_idle;
    server::spawn_cull(app.clone(), move |a| async move { for kid in a.kernels.idle(cull_idle).await { a.delete_kernel(&kid).await; } });
    let signal = server::shutdown_with_mcp(o.exit_with_parent, app.mcp.clone());
    server::serve_router(std_listener, o.tls, &o.host, router(app.clone()), signal).await;
    app.kernels.shutdown().await;
    app.terminals.shutdown().await;
}
