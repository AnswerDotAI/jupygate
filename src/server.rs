//! Shared HTTP/TLS listener, authentication middleware, culling math, and shutdown signals.

use axum::Router;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::response::Response;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs, io::Write, process, time::SystemTime, time::UNIX_EPOCH};
use tokio::time;
use uuid::Uuid;

use crate::api::err;
use crate::mcp::Mcp;
use crate::service::{self, ServiceMeta};

/// Common command-line options shared by `jupygate` and downstream gateways.
pub struct ServerOpts {
    pub host: String, pub port: u16, pub port_file: Option<PathBuf>, pub token: Option<String>, pub token_file: Option<String>, pub trust_loopback: bool,
    pub tls: bool, pub root: PathBuf, pub cull_idle: f64, pub exit_with_parent: bool
}

impl ServerOpts {
    pub fn parse(args: impl IntoIterator<Item = String>, usage: &str) -> Result<Self, String> { Self::parse_with(args, usage, |_, _| Ok(false)) }

    pub fn parse_with(
        args: impl IntoIterator<Item = String>, usage: &str, mut extra: impl FnMut(&str, &mut dyn Iterator<Item = String>) -> Result<bool, String>
    ) -> Result<Self, String> {
        let root = env::current_dir().map_err(|e| format!("cannot read working directory: {e}"))?;
        let mut o = Self { host: "127.0.0.1".into(), port: 8787, port_file: None, token: None, token_file: None, trust_loopback: false, tls: false, root,
            cull_idle: 0.0, exit_with_parent: false };
        let mut args = args.into_iter();
        while let Some(a) = args.next() {
            if extra(&a, &mut args)? { continue; }
            let mut val = || args.next().ok_or_else(|| format!("{a} needs a value\n{usage}"));
            match a.as_str() {
                "--host" => o.host = val()?, "--port" => o.port = val()?.parse().map_err(|_| "--port must be a number".to_string())?,
                "--port-file" => o.port_file = Some(val()?.into()), "--token" => o.token = Some(val()?), "--token-file" => o.token_file = Some(val()?),
                "--trust-loopback" => o.trust_loopback = true, "--tls" => o.tls = true, "--root" => o.root = val()?.into(),
                "--cull-idle" => o.cull_idle = val()?.parse().map_err(|_| "--cull-idle must be a number of seconds".to_string())?,
                "--exit-with-parent" => o.exit_with_parent = true, "--help" | "-h" => { println!("{usage}"); process::exit(0) }
                _ => return Err(format!("unknown option {a}\n{usage}"))
            }
        }
        if o.token.is_some() && o.token_file.is_some() { return Err(format!("--token and --token-file are mutually exclusive\n{usage}")); }
        Ok(o)
    }

    pub fn load_token(&self) -> Result<Option<String>, String> { load_token(self.token.as_deref(), self.token_file.as_deref()) }

    pub fn bind(&self) -> std::io::Result<(std::net::TcpListener, SocketAddr, String)> {
        let (listener, addr) = bind_listener(&self.host, self.port)?;
        if let Some(pf) = &self.port_file { write_port_file(pf, addr.port())?; }
        let url = format!("{}://127.0.0.1:{}", if self.tls { "https" } else { "http" }, addr.port());
        Ok((listener, addr, url))
    }
}

/// Handle `service ...` subcommands immediately, or return the remaining CLI arguments for server startup.
pub fn cli_args(meta: &ServiceMeta, validate: impl Fn(&[String]) -> Result<(), String>) -> Vec<String> {
    let mut args = env::args();
    let program = args.next().expect("argv[0]");
    let mut args = args.peekable();
    if args.peek().is_some_and(|a| a == "service") {
        args.next();
        let rest: Vec<String> = args.collect();
        match service::run(meta, &program, &rest, |a| validate(&a)) {
            Ok(code) => process::exit(code),
            Err(e) => {
                eprintln!("{e}");
                process::exit(2);
            }
        }
    }
    args.collect()
}

/// Run an idle-culling tick once per minute on a background task.
pub fn spawn_cull<S: Clone + Send + 'static, Fut: Future<Output = ()> + Send + 'static>(state: S, mut tick: impl FnMut(S) -> Fut + Send + 'static) {
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(60)).await;
            tick(state.clone()).await;
        }
    });
}

/// Wait for the shutdown signal, then cancel any in-flight MCP requests before draining the server.
pub async fn shutdown_with_mcp(exit_with_parent: bool, mcp: Arc<Mcp>) {
    shutdown_signal(exit_with_parent).await;
    mcp.cancel_all().await;
}

pub fn now() -> f64 { SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after 1970").as_secs_f64() }

pub fn cull_cutoff(now_secs: f64, global: f64, per_kernel: Option<f64>, extra: Option<f64>) -> Option<f64> {
    [Some(global), per_kernel, extra].into_iter().flatten().filter(|&s| s.is_finite() && s > 0.0).reduce(f64::min).map(|s| now_secs - s)
}

pub fn should_cull(last_activity: f64, cutoff: f64, busy: bool, protected: bool) -> bool { !busy && !protected && last_activity < cutoff }

pub fn load_token(token: Option<&str>, token_file: Option<&str>) -> Result<Option<String>, String> {
    if std::env::var_os("JUPYGATE_TOKEN").is_some() {
        return Err("JUPYGATE_TOKEN is no longer supported; pass --token-file PATH (or --token T) instead and unset JUPYGATE_TOKEN".into());
    }
    match token_file {
        Some(path) => {
            let value = fs::read_to_string(path).map_err(|e| format!("cannot read token file {path}: {e}"))?;
            let value = value.trim_end_matches(&['\r', '\n'][..]);
            if value.is_empty() { return Err(format!("token file {path} is empty")); }
            Ok(Some(value.to_string()))
        }
        None => Ok(token.map(String::from)),
    }
}

pub fn is_loopback(ip: IpAddr) -> bool { ip.is_loopback() || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback())) }

pub fn origin_ok(headers: &axum::http::HeaderMap) -> bool {
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else { return true; };
    let Some(host) = headers.get("host").and_then(|v| v.to_str().ok()) else { return false; };
    let Some((scheme, rest)) = origin.split_once("://") else { return false; };
    matches!(scheme, "http" | "https") && rest.split('/').next().is_some_and(|authority| authority.eq_ignore_ascii_case(host))
}

pub fn authorized(token: Option<&str>, trust_loopback: bool, peer: IpAddr, authorization: &str, query: &str) -> bool {
    if trust_loopback && is_loopback(peer) { return true; }
    match token {
        None => true,
        Some(t) => {
            let hdr_ok = authorization.split_once(' ').is_some_and(|(scheme, credential)| {
                (scheme.eq_ignore_ascii_case("token") || scheme.eq_ignore_ascii_case("bearer")) && credential.trim_start() == t
            });
            hdr_ok || query.split('&').any(|kv| kv == format!("token={t}"))
        }
    }
}

#[derive(Clone)]
pub struct AuthConfig { pub token: Option<String>, pub trust_loopback: bool }

async fn auth_mw(
    State(auth): State<Arc<AuthConfig>>, ConnectInfo(peer): ConnectInfo<SocketAddr>, req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let authorization = req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
    if !authorized(auth.token.as_deref(), auth.trust_loopback, peer.ip(), authorization, req.uri().query().unwrap_or("")) {
        return err(StatusCode::FORBIDDEN, "forbidden");
    }
    next.run(req).await
}

pub fn with_auth(router: Router, token: Option<String>, trust_loopback: bool) -> Router {
    let auth = Arc::new(AuthConfig { token, trust_loopback });
    router.layer(axum::middleware::from_fn_with_state(auth, auth_mw))
}

/// Bind a blocking std TCP listener so both plain Axum and rustls branches share actual-port discovery.
pub fn bind_listener(host: &str, port: u16) -> std::io::Result<(std::net::TcpListener, SocketAddr)> {
    let listener = std::net::TcpListener::bind((host, port))?;
    let addr = listener.local_addr()?;
    Ok((listener, addr))
}

/// Atomically write the bound TCP port (`"{port}\n"`) to `path` via sibling temp file + rename.
pub fn write_port_file(path: &Path, port: u16) -> std::io::Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("port");
    let tmp = parent.join(format!(".{name}.{}.{}.tmp", process::id(), Uuid::new_v4().simple()));
    let write_res = (|| -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        writeln!(f, "{port}")?;
        f.sync_all()?;
        fs::rename(&tmp, path)
    })();
    if write_res.is_err() { let _ = fs::remove_file(&tmp); }
    write_res
}

/// Serve `router` on an already-bound listener (with optional self-signed TLS) until `signal` resolves,
/// with a bounded graceful drain on both plain HTTP and TLS so child cleanup always runs.
pub async fn serve_router(std_listener: std::net::TcpListener, tls: bool, host: &str, router: Router, signal: impl Future<Output = ()> + Send + 'static) {
    let handle = axum_server::Handle::new();
    let h = handle.clone();
    tokio::spawn(async move {
        signal.await;
        h.graceful_shutdown(Some(Duration::from_secs(5)));
    });
    if tls {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let ck = rcgen::generate_simple_self_signed(vec![host.to_string(), "localhost".into()]).expect("self-signed certificate");
        let cfg =
            axum_server::tls_rustls::RustlsConfig::from_pem(ck.cert.pem().into_bytes(), ck.key_pair.serialize_pem().into_bytes()).await.expect("tls config");
        axum_server::from_tcp_rustls(std_listener, cfg).handle(handle).serve(router.into_make_service_with_connect_info::<SocketAddr>()).await
            .expect("server failed");
    } else {
        axum_server::from_tcp(std_listener).handle(handle).serve(router.into_make_service_with_connect_info::<SocketAddr>()).await.expect("server failed");
    }
}

/// SIGINT or SIGTERM, or with `exit_with_parent` the exit of the process that started the gateway.
pub async fn shutdown_signal(exit_with_parent: bool) {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    let parent = std::os::unix::process::parent_id();
    let orphaned = async { while std::os::unix::process::parent_id() == parent { time::sleep(Duration::from_secs(1)).await } };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate.recv() => {},
        _ = orphaned, if exit_with_parent => {},
    }
}

/// Result of claiming an in-flight start slot for a binding path.
pub enum StartSlot<T> { Leader(StartGuard<T>), Wait(tokio::sync::watch::Receiver<Option<T>>) }

/// Outcome of `StartTable::get_or_start`.
pub enum StartOutcome<T> { Created(T), Reused(T) }

/// Holds a per-path in-flight start reservation until `finish` publishes an outcome or `Drop` clears it.
pub struct StartGuard<T> {
    starting: Arc<std::sync::Mutex<std::collections::HashMap<String, tokio::sync::watch::Receiver<Option<T>>>>>, path: String,
    tx: Option<tokio::sync::watch::Sender<Option<T>>>
}

impl<T> StartGuard<T> {
    pub fn finish(mut self, res: T) { self.starting.lock().unwrap().remove(&self.path); if let Some(tx) = self.tx.take() { let _ = tx.send(Some(res)); } }
}

impl<T> Drop for StartGuard<T> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.starting.lock().unwrap().remove(&self.path);
            drop(tx);
        }
    }
}

/// Coordinates concurrent path-bound kernel starts so one caller creates while waiters receive its outcome.
pub struct StartTable<T> { starting: Arc<std::sync::Mutex<std::collections::HashMap<String, tokio::sync::watch::Receiver<Option<T>>>>> }

impl<T> Default for StartTable<T> { fn default() -> Self { Self { starting: Default::default() } } }

impl<T: Clone> StartTable<T> {
    pub fn new() -> Self { Self::default() }

    pub fn slot(&self, path: &str) -> StartSlot<T> {
        let mut map = self.starting.lock().unwrap();
        if let Some(rx) = map.get(path) { return StartSlot::Wait(rx.clone()); }
        let (tx, rx) = tokio::sync::watch::channel(None);
        map.insert(path.to_string(), rx);
        StartSlot::Leader(StartGuard { starting: self.starting.clone(), path: path.to_string(), tx: Some(tx) })
    }

    pub async fn wait(mut rx: tokio::sync::watch::Receiver<Option<T>>) -> Option<T> {
        loop {
            if let Some(res) = rx.borrow().clone() { return Some(res); }
            if rx.changed().await.is_err() { return rx.borrow().clone(); }
        }
    }
}

impl<T: Clone, E: Clone> StartTable<Result<T, E>> {
    /// Atomically reuse an existing live value for `key` or run `create` as the single leader.
    pub async fn get_or_start<FExist, FutExist, FCreate, FutCreate>(&self, key: &str, mut existing: FExist, create: FCreate) -> Result<StartOutcome<T>, E> where
        FExist: FnMut() -> FutExist, FutExist: Future<Output = Option<T>>, FCreate: FnOnce() -> FutCreate, FutCreate: Future<Output = Result<T, E>> {
        loop {
            match self.slot(key) {
                StartSlot::Wait(rx) => match Self::wait(rx).await {
                    Some(Ok(v)) => return Ok(StartOutcome::Reused(v)), Some(Err(e)) => return Err(e), None => continue
                },
                StartSlot::Leader(guard) => {
                    if let Some(v) = existing().await { guard.finish(Ok(v.clone())); return Ok(StartOutcome::Reused(v)); }
                    let res = create().await;
                    guard.finish(res.clone());
                    return res.map(StartOutcome::Created);
                }
            }
        }
    }
}
