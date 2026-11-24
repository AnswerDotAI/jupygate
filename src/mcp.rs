//! Thin stateful workbench MCP frontend for kernel lifecycle and execution.

use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::future::Future;
use tokio::sync::{RwLock, watch};
use uuid::Uuid;

use crate::api::Body;
use crate::kernelspec;
use crate::server::origin_ok;
use crate::session::{Msg, iopub};
use aihist::output as render;

const PROTOCOLS: [&str; 2] = ["2025-11-25", "2025-06-18"];

/// The subset of a kernel model that MCP exposes and tracks.
#[derive(Clone, Debug)]
pub struct McpKernel { pub id: String, pub name: String, pub language: String, pub execution_state: String, pub connections: usize, pub path: Option<String> }

impl From<crate::gateway::KernelModel> for McpKernel {
    fn from(m: crate::gateway::KernelModel) -> Self {
        Self { id: m.id, name: m.name, language: m.language, execution_state: m.execution_state, connections: m.connections, path: m.path }
    }
}

/// Parameters MCP passes to the gateway backend when creating a kernel.
pub struct McpCreate { pub kernel: String, pub cwd: Option<String>, pub appendenv: Option<HashMap<String, String>>, pub launcher: &'static str }

impl McpCreate {
    pub async fn into_launch_input(self) -> Result<crate::gateway::LaunchInput, String> {
        let specs = kernelspec::specs().await;
        if !specs.contains_key(&self.kernel) { return Err(kernelspec::unknown(&self.kernel, &specs)); }
        Ok(crate::gateway::LaunchInput { name: Some(self.kernel), appendenv: self.appendenv, cwd: self.cwd, launcher: Some(self.launcher.into()),
            ..Default::default() })
    }
}

/// Minimal gateway kernel operations required by `/mcp` (shared by `jupygate` and `rustygate`).
pub trait McpBackend: Send + Sync {
    type Caller: Clone + Send + Sync;

    fn list_kernels(&self) -> impl Future<Output = Vec<McpKernel>> + Send;
    fn get_kernel(&self, kid: &str) -> impl Future<Output = Option<McpKernel>> + Send;

    fn kernel_state(&self, kid: &str) -> impl Future<Output = Option<(String, String)>> + Send {
        async move { self.get_kernel(kid).await.map(|m| (m.name, m.execution_state)) }
    }

    fn create_kernel(&self, req: McpCreate, caller: &Self::Caller) -> impl Future<Output = Result<McpKernel, String>> + Send;

    /// Serialize per-kernel MCP execution, honor `cancel` before submission (without interrupting)
    /// and during execution (interrupting before releasing the execution lock).
    fn execute(
        &self, kid: &str, code: &str, caller: &Self::Caller, cancel: &mut watch::Receiver<bool>) -> impl Future<Output = Result<Vec<Msg>, String>> + Send;

    fn delete_kernel(&self, kid: &str) -> impl Future<Output = bool> + Send;
    fn restart_kernel(&self, kid: &str, caller: &Self::Caller) -> impl Future<Output = Option<Result<McpKernel, String>>> + Send;
    fn interrupt_kernel(&self, kid: &str, caller: &Self::Caller) -> impl Future<Output = Option<Result<(), String>>> + Send;
}

struct McpSession {
    defaults: Defaults, protocol: String, inflight: HashMap<String, watch::Sender<bool>>, session_cancel: watch::Sender<bool>, current: Option<String>,
    made: HashMap<String, bool>
}

impl McpSession {
    fn new(defaults: Defaults, protocol: &str) -> Self {
        let (session_cancel, _) = watch::channel(false);
        Self { defaults, protocol: protocol.to_string(), inflight: HashMap::new(), session_cancel, current: None, made: HashMap::new() }
    }
}

/// Per-session kernel-creation defaults, sent by the client as a `jupygate` extension field
/// in the initialize params. `env` merges over the kernel's inherited environment.
#[derive(Default, Clone)]
struct Defaults { cwd: Option<String>, env: Option<HashMap<String, String>>, startup: Option<String>, quiet: bool }

fn defaults_of(v: &Value) -> Defaults {
    let env = v["env"].as_object().map(|m| m.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string())).collect());
    Defaults { cwd: v["cwd"].as_str().map(String::from), env, startup: v["startup"].as_str().map(String::from), quiet: v["quiet"].as_bool().unwrap_or(false) }
}

/// MCP transport sessions. The current-kernel pointer lives here, never in model-visible tool arguments.
pub struct Mcp { sessions: RwLock<HashMap<String, McpSession>>, server_name: &'static str, server_version: &'static str }

impl Default for Mcp { fn default() -> Self { Self::new("jupygate", env!("CARGO_PKG_VERSION")) } }
fn rpc_result(id: Value, result: Value) -> Value { json!({"jsonrpc": "2.0", "id": id, "result": result}) }

fn rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message.into()}})
}

fn json_response(status: StatusCode, body: Value, sid: Option<&str>) -> Response {
    let mut resp = (status, Json(body)).into_response();
    if let Some(sid) = sid { resp.headers_mut().insert("mcp-session-id", HeaderValue::from_str(sid).expect("UUID is a header value")); }
    resp
}

fn envelope<'a>(headers: &HeaderMap, msg: &'a Value) -> Result<(Value, &'a str), Response> {
    if !msg.is_object() {
        return Err(json_response(StatusCode::BAD_REQUEST, rpc_error(Value::Null, -32600, "batch requests are not supported"), None));
    }
    let id = msg.get("id").cloned().unwrap_or(Value::Null);
    if !origin_ok(headers) { return Err(json_response(StatusCode::FORBIDDEN, rpc_error(id, -32000, "Origin does not match Host"), None)); }
    Ok((id, msg.get("method").and_then(Value::as_str).unwrap_or("")))
}

fn negotiated_protocol(msg: &Value) -> &'static str {
    let proposed = msg.pointer("/params/protocolVersion").and_then(Value::as_str).unwrap_or(PROTOCOLS[0]);
    PROTOCOLS.iter().copied().find(|p| *p == proposed).unwrap_or(PROTOCOLS[0])
}

fn request_key(id: &Value) -> Option<String> { matches!(id, Value::String(_) | Value::Number(_)).then(|| id.to_string()) }

fn text_block(text: impl Into<String>) -> Value { json!({"type": "text", "text": text.into()}) }

fn tool_result(content: Vec<Value>, is_error: bool) -> Value { json!({"content": content, "isError": is_error}) }

fn text_result(text: impl Into<String>) -> Value { tool_result(vec![text_block(text)], false) }

fn error_result(text: impl Into<String>) -> Value { tool_result(vec![text_block(text)], true) }

fn empty_schema() -> Value { json!({"type": "object", "properties": {}, "additionalProperties": false}) }

fn tools(kernels: &[String]) -> Vec<Value> {
    vec![
        json!({
            "name": "list_kernels",
            "description": "List gateway kernels and which kernel is current for this MCP session.",
            "inputSchema": empty_schema(),
        }), json!({
            "name": "create",
            "description": "Create a new kernel and make it current for this MCP session. Returns kernel-specific usage guidance. Kernels this session creates stop at session end unless autoclose=false.",
            "inputSchema": {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "kernel": {"type": "string", "enum": kernels, "description": "Kernel implementation"},
                    "autoclose": {"type": "boolean", "description": "Stop the new kernel at session end (default true)"},
                },
                "required": ["kernel"],
            },
        }), json!({
            "name": "use_kernel",
            "description": "Select an already-running kernel as current for this MCP session, without changing or restarting it.",
            "inputSchema": {
                "type": "object", "additionalProperties": false,
                "properties": {"kernel": {"type": "string", "description": "Kernel id or unique id prefix"}},
                "required": ["kernel"],
            },
        }), json!({
            "name": "delete_kernel",
            "description": "Stop a kernel permanently and forget it. With no argument, stops this MCP session's current kernel.",
            "inputSchema": {
                "type": "object", "additionalProperties": false,
                "properties": {"kernel": {"type": "string", "description": "Kernel id or unique id prefix; omit for current"}},
            },
        }), json!({
            "name": "exec",
            "description": "Execute code exactly as written in the current kernel, preserving state across calls. Call create or use_kernel first; execution never creates or switches kernels (except subagent sessions, which auto-create a kernel of the parent session's implementation on first exec).",
            "inputSchema": {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "code": {"type": "string", "description": "Code to execute"},
                },
                "required": ["code"],
            },
        }),
        lifecycle_tool("restart", "Restart a kernel under the same id with a fresh interpreter. Python startup reruns on kernels this MCP session created."),
        lifecycle_tool("interrupt", "Interrupt a kernel, normally to interrupt a concurrently running exec call."),
    ]
}

fn lifecycle_tool(name: &str, description: &str) -> Value {
    json!({
        "name": name, "description": description,
        "inputSchema": {
            "type": "object", "additionalProperties": false,
            "properties": {"kernel": {"type": "string", "description": "Kernel id or unique id prefix; omit for current"}},
        },
    })
}

fn arg<'a>(args: &'a Map<String, Value>, name: &str) -> Result<Option<&'a str>, String> {
    match args.get(name) { None | Some(Value::Null) => Ok(None), Some(Value::String(v)) => Ok(Some(v)), Some(_) => Err(format!("{name} must be a string")) }
}

fn bool_arg(args: &Map<String, Value>, name: &str) -> Result<Option<bool>, String> {
    match args.get(name) { None | Some(Value::Null) => Ok(None), Some(Value::Bool(v)) => Ok(Some(*v)), Some(_) => Err(format!("{name} must be a boolean")) }
}

fn required_arg<'a>(args: &'a Map<String, Value>, name: &str) -> Result<&'a str, String> {
    let v = arg(args, name)?.ok_or_else(|| format!("missing required argument {name}"))?;
    if v.is_empty() { Err(format!("{name} must not be empty")) } else { Ok(v) }
}

fn live_state(state: &str) -> bool { state != "dead" && state != "restarting" }

fn kernel_reply(action: &str, model: &McpKernel) -> String {
    let path = model.path.as_deref().map(|p| format!(" for {p}")).unwrap_or_default();
    let guide = match model.language.as_str() {
        "python" => "\nExec runs Python/IPython cells; magics such as %%bash work as written.",
        "bpl" => "\nExec runs BPL. Names persist across calls; separate statements with ⋄ or newlines. Python startup/inspectors do not run.", _ => ""
    };
    format!("{action} kernel {}{path} kernel={} language={}{guide}", model.id, model.name, model.language)
}

fn full_body(text: &str) -> Option<&str> { text.strip_prefix('𝍁')?.strip_suffix('𝍁') }

/// Reply blocks in order. Text blocks end their own line. Marked text protects the combined reply, including headers.
fn stacked(blocks: impl IntoIterator<Item = Value>) -> Vec<Value> {
    let mut out: Vec<Value> = blocks.into_iter().collect();
    let last = out.len().saturating_sub(1);
    for b in &mut out[..last] {
        if let Some(t) = b["text"].as_str().filter(|t| !t.is_empty() && !t.ends_with('\n')).map(|t| format!("{t}\n")) { b["text"] = t.into(); }
    }
    if out.iter().any(|b| b["text"].as_str().is_some_and(|t| full_body(t.trim_end_matches('\n')).is_some())) {
        let mut text = String::new();
        out.retain(|b| {
            let Some(t) = b["text"].as_str() else { return true };
            let body = t.trim_end_matches('\n');
            text.push_str(full_body(body).unwrap_or(body));
            text.push_str(&t[body.len()..]);
            false
        });
        out.insert(0, text_block(format!("𝍁{text}𝍁")));
    }
    out
}

async fn resolve_kernel<B: McpBackend>(backend: &B, selector: &str, require_live: bool) -> Result<McpKernel, String> {
    let models = backend.list_kernels().await;
    let mut found: Vec<_> = models.iter().filter(|m| m.id == selector).cloned().collect();
    if found.is_empty() { found = models.iter().filter(|m| m.id.starts_with(selector)).cloned().collect(); }
    let m = match found.len() {
        0 => return Err(format!("no kernel matching {selector:?}")), 1 => found.pop().unwrap(),
        _ => return Err(format!("kernel selector {selector:?} is ambiguous"))
    };
    if require_live && !live_state(&m.execution_state) { return Err(format!("kernel {} is {}", m.id, m.execution_state)); }
    Ok(m)
}

/// Format a startup failure error if any message in `msgs` is a kernel `error`.
pub fn startup_error(msgs: &[Msg]) -> Option<String> {
    if !msgs.iter().any(|m| m.msg_type() == "error") { return None; }
    let text = render_output(msgs).into_iter().filter_map(|b| b["text"].as_str().map(String::from)).collect::<Vec<_>>().join("\n");
    Some(format!("startup failed; the kernel was stopped:\n{text}"))
}

const NO_CURRENT: &str = "no current kernel: call create or use_kernel first";

impl Mcp {
    pub fn new(server_name: &'static str, server_version: &'static str) -> Self { Self { sessions: RwLock::new(HashMap::new()), server_name, server_version } }

    pub async fn connection_counts(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::<String, usize>::new();
        for s in self.sessions.read().await.values() { if let Some(kid) = s.current.as_deref() { *counts.entry(kid.to_string()).or_default() += 1; } }
        counts
    }

    pub async fn add_connections<'a>(&self, models: impl IntoIterator<Item = &'a mut crate::gateway::KernelModel>) {
        let counts = self.connection_counts().await;
        for m in models { m.connections += counts.get(&m.id).copied().unwrap_or(0); }
    }

    async fn initialize_session(&self, known: Option<&str>, protocol: &str, defaults: Defaults) -> String {
        let mut sessions = self.sessions.write().await;
        if let Some(sid) = known && let Some(session) = sessions.get_mut(sid) {
            session.protocol = protocol.to_string();
            session.defaults = defaults;
            return sid.to_string();
        }
        let sid = Uuid::new_v4().simple().to_string();
        sessions.insert(sid.clone(), McpSession::new(defaults, protocol));
        sid
    }

    /// Forget transport session `sid`. Returns the kernels it created with autoclose, or `None` if unknown.
    async fn end_session(&self, sid: &str) -> Option<Vec<String>> {
        let s = self.sessions.write().await.remove(sid)?;
        let _ = s.session_cancel.send(true);
        for (_, tx) in s.inflight { let _ = tx.send(true); }
        Some(s.made.into_iter().filter_map(|(kid, auto)| auto.then_some(kid)).collect())
    }

    async fn protocol(&self, sid: &str) -> Option<String> { self.sessions.read().await.get(sid).map(|s| s.protocol.clone()) }

    async fn session_cancel(&self, sid: &str) -> Option<watch::Receiver<bool>> { self.sessions.read().await.get(sid).map(|s| s.session_cancel.subscribe()) }

    async fn is_made(&self, sid: &str, kid: &str) -> bool { self.sessions.read().await.get(sid).is_some_and(|s| s.made.contains_key(kid)) }

    async fn begin_request(&self, sid: &str, request: &str) -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);
        if let Some(session) = self.sessions.write().await.get_mut(sid) { session.inflight.insert(request.to_string(), tx); }
        rx
    }

    async fn cancel_request(&self, sid: &str, request: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        let Some(tx) = sessions.get_mut(sid).and_then(|s| s.inflight.get_mut(request)) else { return false; };
        let _ = tx.send(true);
        true
    }

    async fn end_request(&self, sid: &str, request: &str) {
        if let Some(session) = self.sessions.write().await.get_mut(sid) { session.inflight.remove(request); }
    }

    /// Signal cancellation to every in-flight MCP request so gateway shutdown drains immediately.
    pub async fn cancel_all(&self) {
        for session in self.sessions.write().await.values_mut() {
            let _ = session.session_cancel.send(true);
            for (_, tx) in session.inflight.drain() { let _ = tx.send(true); }
        }
    }

    async fn current(&self, sid: &str) -> Option<String> { self.sessions.read().await.get(sid).and_then(|s| s.current.clone()) }

    async fn select(&self, sid: &str, kid: &str) { if let Some(s) = self.sessions.write().await.get_mut(sid) { s.current = Some(kid.to_string()); } }

    pub async fn clear_kernel(&self, kid: &str) {
        for s in self.sessions.write().await.values_mut() {
            if s.current.as_deref() == Some(kid) { s.current = None; }
            s.made.remove(kid);
        }
    }

    async fn defaults(&self, sid: &str) -> Defaults { self.sessions.read().await.get(sid).map(|s| s.defaults.clone()).unwrap_or_default() }

    /// Record kernel ownership if the session is still active; returns `false` if `DELETE /mcp` ended it concurrently.
    async fn record_made(&self, sid: &str, kid: &str, autoclose: bool) -> bool {
        let mut sessions = self.sessions.write().await;
        let Some(s) = sessions.get_mut(sid) else { return false; };
        s.made.insert(kid.to_string(), autoclose);
        true
    }

    /// Resolve a lifecycle tool's target kernel ID: explicit selectors list & match; omitted
    /// selectors read the session's current ID directly without listing unrelated kernels.
    async fn target_id<B: McpBackend>(&self, backend: &B, sid: &str, selector: Option<&str>) -> Result<String, String> {
        if let Some(sel) = selector { return resolve_kernel(backend, sel, false).await.map(|m| m.id); }
        let kid = self.current(sid).await.ok_or(NO_CURRENT)?;
        if backend.kernel_state(&kid).await.is_none() { self.clear_kernel(&kid).await; return Err(NO_CURRENT.into()); }
        Ok(kid)
    }

    async fn run_startup<B: McpBackend>(&self, backend: &B, sid: &str, m: &McpKernel, caller: &B::Caller) -> Result<Vec<Msg>, String> {
        let d = self.defaults(sid).await;
        let Some(src) = d.startup.as_deref() else { return Ok(vec![]); };
        if m.language != "python" { return Ok(vec![]); }
        let mut cancel = self.session_cancel(sid).await.ok_or("MCP session ended")?;
        let msgs = backend.execute(&m.id, src, caller, &mut cancel).await?;
        match startup_error(&msgs) { Some(err) => Err(err), None => Ok(msgs) }
    }

    /// Create a kernel for `sid`, run MCP session Python startup if configured, record it
    /// for autoclose, then select it. Returns `(kid, rendered_blocks)`.
    async fn open<B: McpBackend>(
        &self, backend: &B, sid: &str, kernel: &str, autoclose: bool, cwd: Option<&str>, caller: &B::Caller) -> Result<(String, Vec<Value>), String> {
        let d = self.defaults(sid).await;
        let req = McpCreate { kernel: kernel.to_string(), cwd: cwd.map(String::from).or_else(|| d.cwd.clone()), appendenv: d.env.clone(), launcher: "mcp" };
        let m = backend.create_kernel(req, caller).await?;
        let startup_msgs = match self.run_startup(backend, sid, &m, caller).await {
            Ok(msgs) => msgs, Err(e) => { backend.delete_kernel(&m.id).await; return Err(e); }
        };
        if !self.record_made(sid, &m.id, autoclose).await { if autoclose { backend.delete_kernel(&m.id).await; } return Err("MCP session ended".into()); }
        self.select(sid, &m.id).await;
        let startup = if d.quiet { vec![] } else { render_output(&startup_msgs) };
        let blocks = stacked([vec![text_block(kernel_reply("created", &m))], startup].concat());
        Ok((m.id, blocks))
    }

    /// Run `code` directly on the session's current kernel ID.
    async fn execute<B: McpBackend>(
        &self, backend: &B, sid: &str, args: &Map<String, Value>, cancel: &mut watch::Receiver<bool>, caller: &B::Caller) -> Result<Value, String> {
        let code = arg(args, "code")?.ok_or("missing required argument code")?;
        let kid = self.current(sid).await.ok_or(NO_CURRENT)?;
        match backend.kernel_state(&kid).await {
            Some((_, state)) if !live_state(&state) => return Err(format!("kernel {kid} is {state}")),
            None => { self.clear_kernel(&kid).await; return Err(NO_CURRENT.into()); } Some(_) => {}
        }
        let output = backend.execute(&kid, code, caller, cancel).await?;
        Ok(tool_result(render_output(&output), false))
    }

    async fn call_tool<B: McpBackend>(
        &self, backend: &B, sid: &str, name: &str, args: &Map<String, Value>, cwd: Option<&str>, cancel: Option<&mut watch::Receiver<bool>>, caller: &B::Caller
    ) -> Result<Value, String> {
        match name {
            "list_kernels" => {
                let current = self.current(sid).await;
                let mut ms = backend.list_kernels().await;
                ms.sort_by(|a, b| a.id.cmp(&b.id));
                if ms.is_empty() { return Ok(text_result("no kernels")); }
                let lines = ms.into_iter().map(|m| {
                    let path = m.path.as_deref().map(|p| format!("  path={p}")).unwrap_or_default();
                    let cur = if current.as_deref() == Some(&m.id) { "  <- current" } else { "" };
                    format!("{}  {}  kernel={}  language={}  connections={}{path}{cur}", m.id, m.execution_state, m.name, m.language, m.connections)
                }).collect::<Vec<_>>().join(
                        "
",
                    );
                Ok(text_result(lines))
            }
            "create" => {
                let kernel = required_arg(args, "kernel")?;
                let autoclose = bool_arg(args, "autoclose")?.unwrap_or(true);
                let (_, blocks) = self.open(backend, sid, kernel, autoclose, cwd, caller).await?;
                Ok(tool_result(blocks, false))
            }
            "use_kernel" => {
                let m = resolve_kernel(backend, required_arg(args, "kernel")?, true).await?;
                self.select(sid, &m.id).await;
                Ok(text_result(kernel_reply("current", &m)))
            }
            "delete_kernel" => {
                let kid = self.target_id(backend, sid, arg(args, "kernel")?).await?;
                if !backend.delete_kernel(&kid).await { return Err("kernel disappeared".into()); }
                self.clear_kernel(&kid).await;
                Ok(text_result(format!("deleted kernel {kid}")))
            }
            "exec" => self.execute(backend, sid, args, cancel.expect("execution calls register cancellation"), caller).await,
            "restart" => {
                let kid = self.target_id(backend, sid, arg(args, "kernel")?).await?;
                let quiet = self.defaults(sid).await.quiet;
                let owned = self.is_made(sid, &kid).await;
                match backend.restart_kernel(&kid, caller).await {
                    Some(Ok(m)) => {
                        let startup_msgs = if owned { self.run_startup(backend, sid, &m, caller).await? } else { vec![] };
                        let startup = if quiet { vec![] } else { render_output(&startup_msgs) };
                        Ok(tool_result(stacked([vec![text_block(format!("restarted kernel {kid}"))], startup].concat()), false))
                    }
                    Some(Err(e)) => { if backend.kernel_state(&kid).await.is_none() { self.clear_kernel(&kid).await; } Err(e) }
                    None => Err("kernel disappeared".into()),
                }
            }
            "interrupt" => {
                let kid = self.target_id(backend, sid, arg(args, "kernel")?).await?;
                backend.interrupt_kernel(&kid, caller).await.ok_or("kernel disappeared")??;
                Ok(text_result(format!("interrupt sent to kernel {kid}")))
            }
            _ => Err(format!("unknown tool {name:?}")),
        }
    }

    pub async fn handle_post<B: McpBackend>(&self, backend: &B, headers: HeaderMap, caller: B::Caller, msg: Value) -> Response {
        let (id, method) = match envelope(&headers, &msg) { Ok(v) => v, Err(e) => return e };
        if method == "initialize" {
            if msg.pointer("/params/rustygate").is_some() {
                return json_response(StatusCode::OK, rpc_error(id, -32602, "params.rustygate was renamed to params.jupygate; upgrade clikernel"), None);
            }
            let protocol = negotiated_protocol(&msg);
            let known = headers.get("mcp-session-id").and_then(|v| v.to_str().ok()).filter(|v| !v.is_empty());
            let defaults = msg.pointer("/params/jupygate").map(defaults_of).unwrap_or_default();
            let sid = self.initialize_session(known, protocol, defaults).await;
            let result = json!({
                "protocolVersion": protocol,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": self.server_name, "version": self.server_version},
                "jupygate": true,
                "instructions": "Select a kernel with create(kernel=...) or use_kernel(kernel=...), then run code with exec(code=...). Kernel implementations are listed in create's schema; creation and selection return usage guidance. New kernels require an explicit implementation; exec never creates or switches kernels. Kernels this session creates autoclose by default. Python startup/inspectors run only in Python kernels this session creates.",
            });
            return json_response(StatusCode::OK, rpc_result(id, result), Some(&sid));
        }
        let sid = headers.get("mcp-session-id").and_then(|v| v.to_str().ok()).unwrap_or("");
        if sid.is_empty() { return json_response(StatusCode::BAD_REQUEST, rpc_error(id, -32000, "missing Mcp-Session-Id"), None); }
        let Some(protocol) = self.protocol(sid).await else {
            return json_response(StatusCode::NOT_FOUND, rpc_error(id, -32000, "unknown Mcp-Session-Id"), None);
        };
        if let Some(sent) = headers.get("mcp-protocol-version").and_then(|v| v.to_str().ok()) && sent != protocol {
            return json_response(StatusCode::BAD_REQUEST, rpc_error(id, -32000, format!("expected MCP-Protocol-Version {protocol}")), None);
        }
        if method == "notifications/cancelled" {
            if let Some(request) = msg.pointer("/params/requestId").and_then(request_key) && self.cancel_request(sid, &request).await {
                eprintln!("MCP session {sid} cancelling request {request}");
            }
            return StatusCode::ACCEPTED.into_response();
        }
        if msg.get("id").is_none() { return StatusCode::ACCEPTED.into_response(); }
        let result = match method {
            "ping" => rpc_result(id, json!({})),
            "tools/list" => rpc_result(id, json!({"tools": tools(&kernelspec::names().await)})),
            "tools/call" => {
                let name = msg.pointer("/params/name").and_then(Value::as_str).unwrap_or("");
                let mut args = msg.pointer("/params/arguments").and_then(Value::as_object).cloned().unwrap_or_default();
                let cwd = match arg(&args, "cwd") {
                    Ok(c) => c.filter(|c| !c.is_empty()).map(String::from),
                    Err(e) => return json_response(StatusCode::OK, rpc_result(id, error_result(e)), None),
                };
                args.remove("cwd");
                let request = request_key(&id);
                let mut cancel = match (name == "exec", request.as_deref()) {
                    (true, Some(request)) => Some(self.begin_request(sid, request).await),
                    (true, None) => return json_response(StatusCode::BAD_REQUEST, rpc_error(id, -32600, "tools/call needs a string or number id"), None),
                    (false, _) => None,
                };
                let result = self.call_tool(backend, sid, name, &args, cwd.as_deref(), cancel.as_mut(), &caller).await;
                if let Some(request) = &request && cancel.is_some() { self.end_request(sid, request).await; }
                if cancel.as_ref().is_some_and(|rx| *rx.borrow()) { return StatusCode::ACCEPTED.into_response(); }
                let result = match result { Ok(v) => v, Err(e) => error_result(e) };
                rpc_result(id, result)
            }
            _ => rpc_error(id, -32601, format!("method not found: {method}")),
        };
        json_response(StatusCode::OK, result, None)
    }

    pub async fn handle_delete<B: McpBackend>(&self, backend: &B, headers: HeaderMap) -> Response {
        if !origin_ok(&headers) { return StatusCode::FORBIDDEN.into_response(); }
        let sid = headers.get("mcp-session-id").and_then(|v| v.to_str().ok()).unwrap_or("");
        let Some(kids) = self.end_session(sid).await else { return StatusCode::NOT_FOUND.into_response(); };
        for kid in kids { if backend.delete_kernel(&kid).await { self.clear_kernel(&kid).await; } }
        StatusCode::NO_CONTENT.into_response()
    }
}

/// MCP content for an execution's iopub messages: the text, then one image per display.
fn render_output(msgs: &[Msg]) -> Vec<Value> {
    let r = render::render(iopub(msgs), true, true);
    let text = (!r.text.is_empty() || r.images.is_empty()).then(|| text_block(if r.full { format!("𝍁{}𝍁", r.text) } else { r.text }));
    text.into_iter().chain(r.images.into_iter().map(|(mime, data)| json!({"type": "image", "data": data, "mimeType": mime}))).collect()
}

/// Host hook allowing both `jupygate` and `rustygate` to mount the shared `/mcp` routes.
pub trait McpHost: McpBackend + Clone + 'static
where
    Self::Caller: axum::extract::FromRequestParts<Self>,
{ fn mcp(&self) -> &Mcp; }

async fn post<S: McpHost>(State(app): State<S>, headers: HeaderMap, caller: S::Caller, Body(msg): Body<Value>) -> Response where
    S::Caller: axum::extract::FromRequestParts<S> { app.mcp().handle_post(&app, headers, caller, msg).await }

async fn delete_session<S: McpHost>(State(app): State<S>, headers: HeaderMap) -> Response where S::Caller: axum::extract::FromRequestParts<S> {
    app.mcp().handle_delete(&app, headers).await
}

pub fn routes<S: McpHost>() -> crate::openapi::RouteSet<S> where S::Caller: axum::extract::FromRequestParts<S> {
    use crate::openapi::{Route, RouteSet};
    RouteSet { router: axum::Router::new().route("/mcp", axum::routing::post(post::<S>).delete(delete_session::<S>)), routes: vec![
        Route { method: "post", path: "/mcp", handler: concat!(module_path!(), "::post"), public: false, query: None, body: None, ret: None },
        Route { method: "delete", path: "/mcp", handler: concat!(module_path!(), "::delete_session"), public: false, query: None, body: None, ret: None },
    ] }
}
