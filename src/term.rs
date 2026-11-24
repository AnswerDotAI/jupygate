//! Terminals: gateway-hosted ptys, siblings of kernels. REST + a terminado-compatible websocket
//! at `/terminals/websocket/{name}`, over ptymini's Rust core.
//! Text frames carry JSON arrays: `setup`, `stdout`, `gap`, `disconnect` out; `stdin`, `set_size` in.

use crate::api::Body;
use crate::paths;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use ptymini::PtyCore;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use std::{env, fs};
use tokio::sync::watch;

#[derive(Clone, serde::Serialize, schemars::JsonSchema)]
pub struct TermModel { pub name: String, pub alive: bool, pub last_activity: f64 }

/// Optional host binding attached to a live terminal entry (e.g. Solveit dialog path and caller scope).
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct TermBinding { pub path: Option<String>, pub scope: Option<String> }

/// One pty session: the core's change hook wakes a tokio `watch` channel, which is the
/// whole asyncio bridge - no event loop machinery crosses the thread boundary.
pub struct TermSession {
    pub name: String,
    pub core: PtyCore,
    pub binding: TermBinding,
    changed: watch::Sender<()>,
    /// How many websocket clients are attached
    attached: AtomicUsize,
}

impl TermSession {
    fn new(
        name: &str, argv: &[String], cwd: Option<&str>, env: HashMap<String, String>, rows: u16, cols: u16, binding: TermBinding
    ) -> std::io::Result<Arc<TermSession>> {
        let core = PtyCore::spawn(argv, cwd, Some(&env), rows, cols, 1_000_000)?;
        let (tx, _) = watch::channel(());
        let hook = tx.clone();
        core.set_callback(Arc::new(move || { let _ = hook.send(()); }));
        Ok(Arc::new(TermSession { name: name.into(), core, binding, changed: tx, attached: AtomicUsize::new(0) }))
    }

    pub fn subscribe(&self) -> watch::Receiver<()> { self.changed.subscribe() }
    pub fn attached(&self) -> usize { self.attached.load(Ordering::Relaxed) }
    pub fn alive(&self) -> bool { self.core.alive() }
    pub fn last_activity(&self) -> f64 { self.core.last_activity() }

    pub fn model(&self) -> TermModel { TermModel { name: self.name.clone(), alive: self.core.alive(), last_activity: self.core.last_activity() } }

    /// Escalate SIGHUP/SIGCONT/SIGINT/SIGTERM, then SIGKILL if `force`; true once reaped (`!alive()`).
    pub async fn terminate(self: &Arc<Self>, force: bool) -> bool {
        let s = self.clone();
        tokio::task::spawn_blocking(move || s.core.terminate(force)).await.unwrap_or(false)
    }
}

struct RemoveDirOnDrop(Option<PathBuf>);
impl Drop for RemoveDirOnDrop { fn drop(&mut self) { if let Some(dir) = self.0.take() { let _ = fs::remove_dir_all(dir); } } }

/// Named-session registry: get-or-create by name or binding, list, terminate.
pub struct Terminals { terms: Arc<Mutex<HashMap<String, Arc<TermSession>>>>, argv: Vec<String>, root: PathBuf, next: AtomicUsize }

fn default_argv() -> Vec<String> { vec![std::env::var("SHELL").unwrap_or_else(|_| "bash".into()), "-i".into()] }

impl Terminals {
    pub fn new(root: PathBuf) -> Terminals { Terminals { terms: Arc::new(Mutex::new(HashMap::new())), argv: default_argv(), root, next: AtomicUsize::new(1) } }

    fn spawn_locked(&self, terms: &mut HashMap<String, Arc<TermSession>>, body: TermCreateBody, binding: TermBinding) -> std::io::Result<Arc<TermSession>> {
        let name = body.name.unwrap_or_else(|| {
            loop { let n = self.next.fetch_add(1, Ordering::Relaxed).to_string(); if !terms.contains_key(&n) { break n; } }
        });
        let mut argv = body.argv.unwrap_or_else(|| self.argv.clone());
        let (mut env, mut appendenv) = (body.env, body.appendenv);
        let mut rc_guard = RemoveDirOnDrop(None);
        if let Some(rc) = &body.rc {
            let rcdir = env::temp_dir().join(format!("jupygate-rc-{}", uuid::Uuid::new_v4().simple()));
            fs::create_dir_all(&rcdir)?;
            rc_guard.0 = Some(rcdir.clone());
            let rcfile = rcdir.join(".zshrc"); // one name serves bash (--rcfile {rcfile}) and zsh (ZDOTDIR={rcdir})
            fs::write(&rcfile, rc)?;
            let subst = |s: &str| s.replace("{rcfile}", &rcfile.to_string_lossy()).replace("{rcdir}", &rcdir.to_string_lossy());
            argv = argv.iter().map(|a| subst(a)).collect();
            for m in [&mut env, &mut appendenv].into_iter().flatten() { for v in m.values_mut() { *v = subst(v) } }
        }
        // `env` replaces the inherited environment; `appendenv` overlays the base.
        let mut full_env: HashMap<String, String> = env.unwrap_or_else(|| env::vars().collect());
        full_env.extend(appendenv.unwrap_or_default());
        let cwd = paths::process_cwd(&self.root, body.cwd.as_deref(), None)?;
        let t = TermSession::new(&name, &argv, cwd.as_deref().and_then(|p| p.to_str()), full_env, body.rows.unwrap_or(24), body.cols.unwrap_or(80), binding)?;
        if let Some(rcdir) = rc_guard.0.take() { t.core.set_cleanup_dir(rcdir); }
        terms.insert(name, t.clone());
        let (registry, session) = (self.terms.clone(), t.clone());
        let mut changed = t.subscribe();
        tokio::spawn(async move {
            while session.core.alive() && changed.changed().await.is_ok() {}
            registry.lock().unwrap().retain(|_, t| t.core.alive());
        });
        Ok(t)
    }

    fn lock_live(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<TermSession>>> {
        let mut terms = self.terms.lock().unwrap();
        terms.retain(|_, t| t.core.alive());
        terms
    }

    /// Get a live session by `name`, or start a new one with the supplied `env`, `appendenv`, and `cwd`.
    /// Exited sessions are removed automatically. Auto names count up and are never reused.
    pub fn create(&self, body: TermCreateBody) -> std::io::Result<Arc<TermSession>> {
        let mut terms = self.lock_live();
        if let Some(name) = body.name.as_deref() && let Some(t) = terms.get(name) { return Ok(t.clone()); }
        self.spawn_locked(&mut terms, body, TermBinding::default())
    }

    /// Atomically reuse a live terminal matching `(binding.path, binding.scope)` or `(name, binding.scope)`,
    /// reject a cross-scope name collision with 409, or invoke `build` and spawn a bound terminal.
    pub fn create_bound(
        &self, name: Option<&str>, binding: TermBinding, build: impl FnOnce() -> Result<TermCreateBody, (StatusCode, String)>
    ) -> Result<Arc<TermSession>, (StatusCode, String)> {
        let mut terms = self.lock_live();
        if let Some(p) = binding.path.as_deref()
            && let Some(t) = terms.values().find(|t| t.binding.path.as_deref() == Some(p) && t.binding.scope == binding.scope) { return Ok(t.clone()); }
        if let Some(n) = name && let Some(t) = terms.get(n) {
            if t.binding.scope != binding.scope { return Err((StatusCode::CONFLICT, "terminal name belongs to another caller".into())); }
            return Ok(t.clone());
        }
        let body = build()?;
        self.spawn_locked(&mut terms, body, binding).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("terminal failed to start: {e}")))
    }

    pub fn get(&self, name: &str) -> Option<Arc<TermSession>> { self.lock_live().get(name).cloned() }
    pub fn list(&self) -> Vec<Arc<TermSession>> { self.list_matching(|_| true) }
    pub fn list_matching(&self, pred: impl Fn(&TermSession) -> bool) -> Vec<Arc<TermSession>> {
        self.lock_live().values().filter(|t| pred(t)).cloned().collect()
    }
    pub fn any_alive(&self, pred: impl Fn(&TermSession) -> bool) -> bool { self.lock_live().values().any(|t| pred(t)) }
    pub fn models(&self) -> Vec<TermModel> { self.list().into_iter().map(|t| t.model()).collect() }
    pub async fn delete(&self, name: &str) -> bool { self.delete_matching(|t| t.name == name).await > 0 }

    pub async fn delete_matching(&self, pred: impl Fn(&TermSession) -> bool) -> usize {
        let victims: Vec<Arc<TermSession>> = {
            let mut terms = self.terms.lock().unwrap();
            let names: Vec<String> = terms.iter().filter(|(_, t)| pred(t)).map(|(n, _)| n.clone()).collect();
            names.into_iter().filter_map(|n| terms.remove(&n)).collect()
        };
        let n = victims.len();
        for t in victims { t.terminate(true).await; }
        n
    }

    /// Reap every terminal; nothing survives the registry.
    pub async fn shutdown(&self) { self.delete_matching(|_| true).await; }
}

#[derive(Default, serde::Deserialize, schemars::JsonSchema)]
pub struct TermCreateBody {
    pub name: Option<String>, pub argv: Option<Vec<String>>, pub env: Option<HashMap<String, String>>, pub appendenv: Option<HashMap<String, String>>,
    pub cwd: Option<String>, pub rows: Option<u16>, pub cols: Option<u16>, pub rc: Option<String>
}

async fn list_terms(State(app): State<crate::AppState>) -> Json<Vec<TermModel>> { Json(app.terminals.models()) }

async fn create_term(State(app): State<crate::AppState>, Body(b): Body<TermCreateBody>) -> Response {
    match app.terminals.create(b) {
        Ok(t) => (StatusCode::CREATED, Json(t.model())).into_response(),
        Err(e) => crate::api::err(StatusCode::INTERNAL_SERVER_ERROR, &format!("terminal failed to start: {e}")),
    }
}

async fn get_term(State(app): State<crate::AppState>, Path(name): Path<String>) -> Response {
    match app.terminals.get(&name) { Some(t) => Json(t.model()).into_response(), None => crate::api::err(StatusCode::NOT_FOUND, "no such terminal") }
}

async fn delete_term(State(app): State<crate::AppState>, Path(name): Path<String>) -> Response {
    match app.terminals.delete(&name).await {
        true => StatusCode::NO_CONTENT.into_response(), false => crate::api::err(StatusCode::NOT_FOUND, "no such terminal")
    }
}

async fn channel(ws: WebSocketUpgrade, headers: HeaderMap, State(app): State<crate::AppState>, Path(name): Path<String>) -> Response {
    if !crate::server::origin_ok(&headers) { return crate::api::err(StatusCode::FORBIDDEN, "Origin does not match Host"); }
    let Some(t) = app.terminals.get(&name) else { return crate::api::err(StatusCode::NOT_FOUND, "no such terminal"); };
    ws.on_upgrade(move |socket| client_session(socket, t))
}

/// Incrementally decode UTF-8 bytes, holding an incomplete trailing sequence in `carry` until more
/// bytes arrive (or replacing it with U+FFFD when `flush` is true).
fn decode_utf8(carry: &mut Vec<u8>, chunk: &[u8], flush: bool) -> String {
    carry.extend_from_slice(chunk);
    let mut out = String::new();
    let mut i = 0;
    while i < carry.len() {
        match std::str::from_utf8(&carry[i..]) {
            Ok(s) => {
                out.push_str(s);
                i = carry.len();
            }
            Err(e) => {
                out.push_str(std::str::from_utf8(&carry[i..i + e.valid_up_to()]).unwrap());
                i += e.valid_up_to();
                match e.error_len() {
                    Some(n) => {
                        out.push('\u{FFFD}');
                        i += n;
                    }
                    None if flush => {
                        out.push('\u{FFFD}');
                        i = carry.len();
                    }
                    None => break,
                }
            }
        }
    }
    carry.drain(..i);
    out
}

pub async fn client_session(socket: WebSocket, t: Arc<TermSession>) {
    use futures_util::{SinkExt, StreamExt};
    let (mut sink, mut stream) = socket.split();
    let text = |v: Value| Message::Text(v.to_string().into());
    if sink.send(text(json!(["setup", {"name": t.name}]))).await.is_err() { return; }
    t.attached.fetch_add(1, Ordering::Relaxed);
    let pt = t.clone();
    let pump = tokio::spawn(async move {
        let mut rx = pt.subscribe();
        let mut off = pt.core.start();
        let mut carry = Vec::new();
        loop {
            rx.mark_unchanged();
            // Read `alive` before the ring: the reader marks the core dead only after its last append
            let alive = pt.core.alive();
            let (data, new_off, dropped) = pt.core.read_from(off, None);
            off = new_off;
            if dropped > 0 {
                let tail = decode_utf8(&mut carry, &[], true);
                if !tail.is_empty() && sink.send(text(json!(["stdout", tail]))).await.is_err() { return; }
                if sink.send(text(json!(["gap", dropped]))).await.is_err() { return; }
            }
            if !data.is_empty() {
                let s = decode_utf8(&mut carry, &data, false);
                if !s.is_empty() && sink.send(text(json!(["stdout", s]))).await.is_err() { return; }
            } else if !alive {
                let tail = decode_utf8(&mut carry, &[], true);
                if !tail.is_empty() { let _ = sink.send(text(json!(["stdout", tail]))).await; }
                let _ = sink.send(text(json!(["disconnect", 1, pt.core.exit_code()]))).await;
                let _ = sink.send(Message::Close(None)).await;
                return;
            } else { let _ = rx.changed().await; }
        }
    });
    while let Some(Ok(m)) = stream.next().await {
        match m {
            Message::Text(s) => {
                let Ok(Value::Array(a)) = serde_json::from_str::<Value>(&s) else { continue; };
                match a.first().and_then(Value::as_str) {
                    Some("stdin") => { if let Some(d) = a.get(1).and_then(Value::as_str) { let _ = t.core.write(d.as_bytes()); } }
                    Some("set_size") => {
                        if let (Some(r), Some(co)) = (a.get(1).and_then(Value::as_u64), a.get(2).and_then(Value::as_u64)) {
                            let _ = t.core.resize(r as u16, co as u16);
                        }
                    }
                    _ => {}
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    pump.abort();
    t.attached.fetch_sub(1, Ordering::Relaxed);
}

crate::routes! {
    pub get "/api/terminals" => list_terms ret(Vec<TermModel>),
    pub post "/api/terminals" => create_term body(TermCreateBody) ret(TermModel),
    pub get "/api/terminals/{name}" => get_term ret(TermModel),
    pub delete "/api/terminals/{name}" => delete_term,
    pub get "/terminals/websocket/{name}" => channel,
}
