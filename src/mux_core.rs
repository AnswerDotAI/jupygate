//! Shared kernel mux mechanics: single `ChannelState` machine, typed `ExecError`,
//! bounded client queues, single-socket session binding, collector registration,
//! stdin parent repair, and legacy WebSocket framing.

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::mem;
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::channels::{ChannelRx, ChannelTx, KernelChannels};
use crate::server::now;
use crate::session::{Msg, Session, from_frame, to_frame};

/// Why a WebSocket client frame could not resolve a session routing key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionError { Disconnected, ConflictingSession, MissingSession }

/// Typed kernel execution failure returned as data instead of English error-string matching.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecError { Aborted, Restarting, Dead, ChannelLost(String), SendFailed(String) }

impl ExecError {
    pub fn ename(&self) -> &'static str {
        match self {
            Self::Restarting => "KernelRestarting", Self::Dead => "KernelDead", Self::Aborted | Self::ChannelLost(_) | Self::SendFailed(_) => "KernelError"
        }
    }
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aborted => f.write_str("execution aborted"), Self::Restarting => f.write_str("kernel restarting"),
            Self::Dead => f.write_str("kernel is dead"), Self::ChannelLost(s) | Self::SendFailed(s) => f.write_str(s)
        }
    }
}

impl std::error::Error for ExecError {}

/// Single authoritative channel and admission state for one kernel.
#[derive(Clone)]
pub enum ChannelState {
    /// Starting or restarting; `Some(ch)` allows internal initialization while rejecting user executes.
    Initializing(Option<Arc<KernelChannels>>),
    /// Live channels admitting user executions.
    Ready(Arc<KernelChannels>),
    /// Required internal channel (shell or IOPub) failed; terminal until restart.
    ChannelLost { reason: String },
    /// Kernel process exited or shut down.
    Dead,
}

/// A bounded live connection. Overflow ends the connection, never a partial event stream.
struct ClientQueue { tx: mpsc::Sender<Message>, closed: Notify }

impl ClientQueue {
    fn put_frame(&self, frame: Message) { if self.tx.try_send(frame).is_err() { self.close(); } }
    fn close(&self) { self.closed.notify_one(); }
}

/// Physical socket registry: exactly one queue per physical socket (so broadcasts and `n_attached`
/// never double-count), with each socket owning at most one session routing key.
#[derive(Default)]
struct Clients { sockets: HashMap<u64, Arc<ClientQueue>>, by_session: HashMap<String, u64>, session_of: HashMap<u64, String>, next_id: u64 }

impl Clients {
    fn displace(&mut self, conn_id: u64, pending_stdin: &Mutex<HashMap<String, VecDeque<Map<String, Value>>>>) {
        if let Some(old) = self.sockets.remove(&conn_id) { old.close(); }
        if let Some(sid) = self.session_of.remove(&conn_id) {
            if self.by_session.get(&sid) == Some(&conn_id) {
                self.by_session.remove(&sid);
                pending_stdin.lock().unwrap().remove(&sid);
            }
        }
    }
}

/// A request in flight. Yields the request's iopub output and its `input_request`s until both
/// its parented shell `execute_reply` and parented `idle` status arrive.
pub struct Running {
    pub msg_id: String, rx: mpsc::UnboundedReceiver<Result<Msg, ExecError>>, core: Arc<MuxCore>, session_id: String, idle: bool, replied: bool, done: bool
}

impl Drop for Running { fn drop(&mut self) { self.core.collectors.lock().unwrap().remove(&self.session_id); } }

impl Running {
    pub fn session_id(&self) -> &str { &self.session_id }

    /// The next output or `input_request`, `None` after completion, or `Err` on abort/failure.
    pub async fn next(&mut self) -> Option<Result<Msg, ExecError>> {
        while !self.done {
            let msg = match self.rx.recv().await {
                Some(Ok(msg)) => msg, Some(Err(e)) => { self.done = true; return Some(Err(e)); }
                None => { self.done = true; return Some(Err(ExecError::ChannelLost("kernel channels closed during execution".into()))); }
            };
            let state = msg.content["execution_state"].as_str();
            if msg.msg_type() == "status" {
                match state {
                    Some("dead") => { self.done = true; return Some(Err(ExecError::Dead)); }
                    Some("restarting") => { self.done = true; return Some(Err(ExecError::Restarting)); } _ => {}
                }
            }
            if msg.channel == "shell" {
                if msg.msg_type() == "execute_reply" && msg.parent_id() == Some(&self.msg_id) {
                    if msg.content["status"].as_str() == Some("aborted") { self.done = true; return Some(Err(ExecError::Aborted)); }
                    self.replied = true;
                    if self.idle { self.done = true; return None; }
                }
                continue;
            }
            if msg.channel == "iopub" && msg.parent_id() != Some(&self.msg_id) { continue; }
            if msg.msg_type() == "status" { if state == Some("idle") { self.idle = true; if self.replied { self.done = true; return None; } } continue; }
            return Some(Ok(msg));
        }
        None
    }

    /// Collect messages accepted by `keep` until execution completes, bounding retained text to
    /// `aihist::output::TEXT_MAX` while continuing to drain messages to completion.
    pub async fn collect(&mut self, mut keep: impl FnMut(&Msg) -> bool) -> Result<Vec<Msg>, ExecError> {
        let mut buf = BoundedOutputs::default();
        while let Some(msg) = self.next().await { let msg = msg?; if keep(&msg) { buf.push(msg); } }
        Ok(buf.finish())
    }

    /// Answer a pending `input_request` directly on `core`'s live channels.
    pub async fn input_reply(&self, value: &str) -> Result<(), ExecError> {
        let ch = self.core.channels().ok_or(ExecError::Dead)?;
        let mut msg = Msg::new(&self.core.session, "stdin", "input_reply", json!({"value": value}));
        msg.header.insert("session".into(), self.session_id.clone().into());
        self.core.send(&ch, self.session_id(), &mut msg).await.map_err(ExecError::SendFailed)
    }
}

fn value_text_len(v: &Value) -> usize {
    match v { Value::String(s) => s.len(), Value::Array(a) => a.iter().filter_map(Value::as_str).map(str::len).sum(), _ => 0 }
}

fn msg_text_len(m: &Msg) -> usize {
    match m.msg_type() {
        "stream" => value_text_len(&m.content["text"]),
        "error" => m.content["traceback"].as_array().map(|a| a.iter().map(value_text_len).sum())
            .unwrap_or_else(|| value_text_len(&m.content["ename"]) + value_text_len(&m.content["evalue"])),
        "display_data" | "execute_result" => {
            m.content["data"].as_object().map(|d| aihist::output::TEXT_MIMES.iter().filter_map(|k| d.get(*k)).map(value_text_len).sum()).unwrap_or(0)
        }
        _ => 0,
    }
}

/// Trim `s` from the front so `[<dropped> bytes of output removed]\n<tail>` fits within `max` bytes.
fn trim_tail_fit(s: &mut String, max: usize, prior_dropped: usize) {
    if prior_dropped == 0 && s.len() <= max { return; }
    let mut b = 0usize;
    let mut marker = String::new();
    for _ in 0..3 {
        let total = prior_dropped + b;
        marker = format!("[{total} bytes of output removed]\n");
        let keep = max.saturating_sub(marker.len());
        let next_b = s.ceil_char_boundary(s.len().saturating_sub(keep));
        if next_b == b { break; }
        b = next_b;
    }
    let tail = s.split_off(b);
    s.clear();
    s.reserve(marker.len() + tail.len());
    s.push_str(&marker);
    s.push_str(&tail);
}

struct KeptEntry { msg: Msg, text_bytes: usize, dropped: usize }

#[derive(Default)]
struct BoundedOutputs { entries: VecDeque<KeptEntry>, total_text: usize, evicted_text: usize }

impl BoundedOutputs {
    fn push(&mut self, mut msg: Msg) {
        let max = aihist::output::TEXT_MAX;
        let slack = max + (max / 4);
        match msg.msg_type() {
            "stream" => {
                let text = aihist::output::text_str(&msg.content["text"]);
                let name = msg.content["name"].as_str().unwrap_or("stdout");
                if let Some(last) = self.entries.back_mut() && last.msg.msg_type() == "stream" && last.msg.content["name"].as_str().unwrap_or("stdout") == name
                {
                    if let Some(Value::String(buf)) = last.msg.content.get_mut("text") {
                        buf.push_str(&text);
                        last.text_bytes += text.len();
                        self.total_text += text.len();
                        if buf.len() > slack {
                            let cut = buf.ceil_char_boundary(buf.len() - max);
                            buf.drain(..cut);
                            last.dropped += cut;
                            last.text_bytes -= cut;
                            self.total_text -= cut;
                        }
                        self.evict_front(max);
                        return;
                    }
                }
                let mut text = text;
                let mut dropped = 0;
                if text.len() > max {
                    let cut = text.ceil_char_boundary(text.len() - max);
                    text.drain(..cut);
                    dropped = cut;
                }
                let len = text.len();
                msg.content["text"] = Value::String(text);
                self.entries.push_back(KeptEntry { msg, text_bytes: len, dropped });
                self.total_text += len;
            }
            "display_data" | "execute_result" => {
                if let Some(data) = msg.content.get_mut("data").and_then(Value::as_object_mut) {
                    for mime in aihist::output::TEXT_MIMES {
                        if let Some(val) = data.get_mut(mime) && value_text_len(val) > max {
                            let mut s = aihist::output::text_str(val);
                            trim_tail_fit(&mut s, max, 0);
                            *val = Value::String(s);
                        }
                    }
                }
                let len = msg_text_len(&msg);
                self.entries.push_back(KeptEntry { msg, text_bytes: len, dropped: 0 });
                self.total_text += len;
            }
            _ => {
                let len = msg_text_len(&msg);
                self.entries.push_back(KeptEntry { msg, text_bytes: len, dropped: 0 });
                self.total_text += len;
            }
        }
        self.evict_front(max);
    }

    fn evict_front(&mut self, max: usize) {
        while self.total_text > max && self.entries.len() > 1 {
            let Some(idx) = self.entries.iter().take(self.entries.len() - 1).position(|e| e.text_bytes > 0) else { break; };
            let removed = self.entries.remove(idx).expect("valid index");
            self.total_text -= removed.text_bytes;
            self.evicted_text += removed.text_bytes + removed.dropped;
        }
    }

    fn finish(mut self) -> Vec<Msg> {
        let max = aihist::output::TEXT_MAX;
        if self.evicted_text > 0 {
            let extra = self.evicted_text;
            if let Some(e) = self.entries.iter_mut().find(|e| e.msg.msg_type() == "stream") {
                e.dropped += extra;
            } else if let Some(e) = self.entries.iter_mut().find(|e| e.text_bytes > 0) {
                if let Some(data) = e.msg.content.get_mut("data").and_then(Value::as_object_mut) {
                    for mime in aihist::output::TEXT_MIMES {
                        if let Some(val) = data.get_mut(mime) {
                            let mut s = aihist::output::text_str(val);
                            trim_tail_fit(&mut s, max, extra);
                            *val = Value::String(s);
                            break;
                        }
                    }
                }
            }
        }
        self.entries.into_iter().map(|mut e| {
            if e.msg.msg_type() == "stream" && (e.dropped > 0 || e.text_bytes > max) {
                if let Some(Value::String(buf)) = e.msg.content.get_mut("text") { trim_tail_fit(buf, max, e.dropped); }
            }
            e.msg
        }).collect()
    }
}

/// Shared kernel communication mechanics: single `ChannelState`, bounded client queues, collectors,
/// stdin repair, active execution tracking, kernel_info metadata, activity timestamp, and pump tasks.
pub struct MuxCore {
    pub kid: String, pub session: Session, qmax: usize, state: Mutex<ChannelState>, clients: Mutex<Clients>,
    pending_stdin: Mutex<HashMap<String, VecDeque<Map<String, Value>>>>, collectors: Mutex<HashMap<String, mpsc::UnboundedSender<Result<Msg, ExecError>>>>,
    active: Mutex<HashMap<String, bool>>, idle_notify: Notify, info: Mutex<Value>, activity: Mutex<f64>, tasks: Mutex<Vec<JoinHandle<()>>>
}

impl MuxCore {
    pub fn new(kid: String, qmax: usize, username: &str) -> Arc<Self> {
        Arc::new(Self { kid, session: Session::new(b"", username), qmax, state: Mutex::new(ChannelState::Initializing(None)),
            clients: Mutex::new(Clients::default()), pending_stdin: Mutex::new(HashMap::new()), collectors: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()), idle_notify: Notify::new(), info: Mutex::new(Value::Null), activity: Mutex::new(now()),
            tasks: Mutex::new(vec![]) })
    }

    pub fn set_info(&self, info: Value) { *self.info.lock().unwrap() = info; }
    pub fn language(&self) -> String { self.info.lock().unwrap()["language_info"]["name"].as_str().unwrap_or("unknown").to_lowercase() }
    pub fn language_version(&self) -> String { self.info.lock().unwrap()["language_info"]["version"].as_str().unwrap_or_default().into() }
    pub fn supports(&self, feature: &str) -> bool { self.info.lock().unwrap()["supported_features"].as_array().is_some_and(|f| f.iter().any(|f| f == feature)) }

    pub fn touch(&self) { *self.activity.lock().unwrap() = now() }
    pub fn last_activity(&self) -> f64 { *self.activity.lock().unwrap() }

    fn mark_queued(&self, msg_id: String) { self.active.lock().unwrap().entry(msg_id).or_insert(false); }
    pub fn mark_busy(&self, msg_id: String) { self.active.lock().unwrap().insert(msg_id, true); }
    fn mark_idle(&self, msg_id: &str) {
        let notify = {
            let mut active = self.active.lock().unwrap();
            active.remove(msg_id);
            !active.values().any(|&running| running)
        };
        if notify { self.idle_notify.notify_waiters(); }
    }
    pub fn clear_active(&self) {
        self.active.lock().unwrap().clear();
        self.idle_notify.notify_waiters();
    }
    pub fn active_snapshot(&self) -> HashMap<String, bool> { self.active.lock().unwrap().clone() }
    pub fn is_active_running(&self, msg_id: &str) -> bool { self.active.lock().unwrap().get(msg_id) == Some(&true) }
    pub fn is_running(&self) -> bool { self.active.lock().unwrap().values().any(|&running| running) }
    pub fn notified_idle(&self) -> tokio::sync::futures::Notified<'_> { self.idle_notify.notified() }
    pub fn busy(&self) -> bool { !self.active.lock().unwrap().is_empty() }

    /// Single transition point for channel and admission state. Clears active executions, fails
    /// in-flight collectors, and closes displaced channels when they become unavailable.
    ///
    /// Returns `false` if:
    /// - `ChannelLost` is ignored because the kernel is not currently `Ready` or `Initializing(Some)`, or
    /// - `Ready(ch)` does not match the current `Initializing(Some(cur))` channel set (preventing
    ///   startup from erasing an early `ChannelLost` or `Dead` transition).
    pub fn transition(&self, next: ChannelState) -> bool {
        let (fail_err, close_ch) = {
            let mut st = self.state.lock().unwrap();
            let prev_ch = match &*st { ChannelState::Ready(ch) | ChannelState::Initializing(Some(ch)) => Some(ch.clone()), _ => None };
            match &next {
                ChannelState::ChannelLost { reason } => {
                    if !matches!(*st, ChannelState::Ready(_) | ChannelState::Initializing(Some(_))) { return false; }
                    let err = ExecError::ChannelLost(reason.clone());
                    *st = next;
                    (Some(err), prev_ch)
                }
                ChannelState::Initializing(None) => { *st = next; (Some(ExecError::Restarting), prev_ch) }
                ChannelState::Dead => { *st = next; (Some(ExecError::Dead), prev_ch) }
                ChannelState::Ready(ch) => {
                    let matches = matches!(&*st, ChannelState::Initializing(Some(cur)) if Arc::ptr_eq(cur, ch));
                    if !matches { return false; }
                    *st = next;
                    (None, None)
                }
                ChannelState::Initializing(Some(ch)) => {
                    let close = prev_ch.filter(|cur| !Arc::ptr_eq(cur, ch));
                    *st = next;
                    (None, close)
                }
            }
        };
        if let Some(ch) = close_ch { ch.close(); }
        if let Some(err) = fail_err {
            self.clear_active();
            self.fail_collectors(&err);
        }
        true
    }

    pub fn channel_state(&self) -> ChannelState { self.state.lock().unwrap().clone() }

    /// Live channels for internal or ready use (`Initializing(Some)` or `Ready`).
    pub fn channels(&self) -> Option<Arc<KernelChannels>> {
        match &*self.state.lock().unwrap() {
            ChannelState::Ready(ch) | ChannelState::Initializing(Some(ch)) => Some(ch.clone()),
            ChannelState::Initializing(None) | ChannelState::ChannelLost { .. } | ChannelState::Dead => None,
        }
    }

    /// Live channels for user-admitted executions (`Ready` only), or the typed rejection reason.
    pub fn ready_channels(&self) -> Result<Arc<KernelChannels>, ExecError> {
        match &*self.state.lock().unwrap() {
            ChannelState::Ready(ch) => Ok(ch.clone()), ChannelState::Initializing(_) => Err(ExecError::Restarting),
            ChannelState::ChannelLost { reason } => Err(ExecError::ChannelLost(reason.clone())), ChannelState::Dead => Err(ExecError::Dead)
        }
    }

    pub fn check_admission(&self) -> Result<(), ExecError> { self.ready_channels().map(|_| ()) }
    pub fn is_dead(&self) -> bool { matches!(*self.state.lock().unwrap(), ChannelState::Dead) }
    pub fn is_channel_lost(&self) -> bool { matches!(*self.state.lock().unwrap(), ChannelState::ChannelLost { .. }) }
    pub fn channel_lost(&self) -> Option<String> {
        match &*self.state.lock().unwrap() { ChannelState::ChannelLost { reason } => Some(reason.clone()), _ => None }
    }

    pub fn spawn_task(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|t| !t.is_finished());
        tasks.push(tokio::spawn(task));
    }

    /// Synchronously abort all pump tasks (usable from `Drop` guards on cancelled creation futures).
    pub fn abort_tasks(&self) { for t in mem::take(&mut *self.tasks.lock().unwrap()) { t.abort(); } }

    pub async fn stop_channels(&self) {
        if let Some(ch) = self.channels() { ch.close(); }
        let tasks = mem::take(&mut *self.tasks.lock().unwrap());
        for t in &tasks { t.abort(); }
        for t in tasks { let _ = t.await; }
        self.clear_active();
        self.pending_stdin.lock().unwrap().clear();
    }

    /// Drive one kernel channel until it closes. A lost shell or IOPub channel requires a restart.
    pub async fn pump(&self, mut rx: ChannelRx, mut receive: impl FnMut(Msg)) {
        let error = loop { match rx.recv().await { Ok(msg) => receive(msg), Err(error) => break error } };
        let reason = match rx.name {
            "shell" => "The kernel's execution channel failed. Restart the kernel to resume.",
            "iopub" => "The kernel's output channel failed and this execution's output was lost. Restart the kernel to resume.",
            _ => return,
        };
        if self.transition(ChannelState::ChannelLost { reason: reason.into() }) {
            eprintln!("kernel {} {} failed: {error}", self.kid, rx.name);
            self.publish(&Map::new(), "execution_lost", json!({"error": reason}));
        }
    }

    /// Deliver an accepted kernel message. A notebook host calls this under its own lock after applying the output.
    pub fn deliver(&self, msg: Msg) {
        if msg.channel == "iopub" {
            self.touch();
            if msg.msg_type() == "status" {
                match (msg.content["execution_state"].as_str(), msg.parent_id()) {
                    (Some("busy"), Some(pid)) => self.mark_busy(pid.into()),
                    (Some("idle"), Some(pid)) => self.mark_idle(pid),
                    (Some("dead"), _) => { self.transition(ChannelState::Dead); }
                    _ => {}
                }
            }
            self.relay_iopub(&msg);
        } else if let Some(sid) = msg.parent_session().map(String::from) {
            if msg.channel == "stdin" && msg.msg_type() == "input_request" { self.record_stdin(&sid, msg.header.clone()); }
            self.relay_reply(&sid, msg);
        }
    }

    pub fn n_attached(&self) -> usize { self.clients.lock().unwrap().sockets.len() }

    /// Register one physical WebSocket connection. When `query_sid` is present, it binds the
    /// connection's session immediately (displacing any prior socket using that same session id).
    fn add_client(&self, query_sid: Option<&str>) -> (u64, Arc<ClientQueue>, mpsc::Receiver<Message>) {
        let (tx, rx) = mpsc::channel(self.qmax);
        let cq = Arc::new(ClientQueue { tx, closed: Notify::new() });
        let mut c = self.clients.lock().unwrap();
        c.next_id += 1;
        let conn_id = c.next_id;
        if let Some(sid) = query_sid.filter(|s| !s.is_empty()) {
            if let Some(old_id) = c.by_session.get(sid).copied() { c.displace(old_id, &self.pending_stdin); }
            c.by_session.insert(sid.to_string(), conn_id);
            c.session_of.insert(conn_id, sid.to_string());
        }
        c.sockets.insert(conn_id, cq.clone());
        (conn_id, cq, rx)
    }

    /// Verify `conn_id` is still live and resolve its session routing key.
    /// Rejects any non-empty `wire_sid` conflicting with an already-bound session (whether from
    /// `?session_id=` or an earlier frame), and rejects frames with neither query nor header session.
    fn resolve_client_session(&self, conn_id: u64, wire_sid: Option<&str>) -> Result<String, SessionError> {
        let mut c = self.clients.lock().unwrap();
        if !c.sockets.contains_key(&conn_id) { return Err(SessionError::Disconnected); }
        let wire = wire_sid.filter(|s| !s.is_empty());
        if let Some(existing) = c.session_of.get(&conn_id) {
            if let Some(wsid) = wire && wsid != existing { return Err(SessionError::ConflictingSession); }
            return Ok(existing.clone());
        }
        let Some(wsid) = wire else { return Err(SessionError::MissingSession); };
        if let Some(old_id) = c.by_session.get(wsid).copied() && old_id != conn_id { c.displace(old_id, &self.pending_stdin); }
        c.by_session.insert(wsid.to_string(), conn_id);
        c.session_of.insert(conn_id, wsid.to_string());
        Ok(wsid.to_string())
    }

    fn drop_client(&self, conn_id: u64) {
        let mut c = self.clients.lock().unwrap();
        c.displace(conn_id, &self.pending_stdin);
    }

    pub fn execute_request(&self, code: &str, store_history: bool, allow_stdin: bool) -> Msg {
        Msg::new(&self.session, "shell", "execute_request", json!({
            "code": code, "silent": false, "store_history": store_history, "user_expressions": {}, "allow_stdin": allow_stdin, "stop_on_error": false,
        }))
    }

    /// Register an in-process collector before sending a request; dropping `Running` removes it.
    pub fn begin_collector(self: &Arc<Self>, msg: &mut Msg) -> Running {
        let sid = format!("internal-{}", uuid::Uuid::new_v4().simple());
        let (tx, rx) = mpsc::unbounded_channel();
        self.collectors.lock().unwrap().insert(sid.clone(), tx);
        msg.header.insert("session".into(), sid.clone().into());
        Running { msg_id: msg.msg_id().into(), rx, core: self.clone(), session_id: sid, idle: false, replied: false, done: false }
    }

    fn fail_collectors(&self, error: &ExecError) { for (_, tx) in self.collectors.lock().unwrap().drain() { let _ = tx.send(Err(error.clone())); } }

    pub fn broadcast(&self, msg: &Msg) {
        let frame = to_frame(msg);
        for cq in self.clients.lock().unwrap().sockets.values() { cq.put_frame(frame.clone()); }
    }

    /// Route parented iopub directly to its owning collector (and broadcast unparented/global
    /// status to all collectors), while always broadcasting to attached WebSocket frontends.
    pub fn relay_iopub(&self, msg: &Msg) {
        if let Some(sid) = msg.parent_session() {
            let mut cols = self.collectors.lock().unwrap();
            if let Some(tx) = cols.get(sid) && tx.send(Ok(msg.clone())).is_err() { cols.remove(sid); }
        } else { self.collectors.lock().unwrap().retain(|_, tx| tx.send(Ok(msg.clone())).is_ok()); }
        self.broadcast(msg);
    }

    pub fn iopub_msg(&self, parent: &Map<String, Value>, kind: &str, content: Value) -> Msg {
        let mut msg = Msg::new(&self.session, "iopub", kind, content);
        msg.parent_header = parent.clone();
        msg
    }

    pub fn publish(&self, parent: &Map<String, Value>, kind: &str, content: Value) { self.relay_iopub(&self.iopub_msg(parent, kind, content)); }

    pub fn synthesize_status(&self, state: &str) {
        if state == "dead" { self.transition(ChannelState::Dead); }
        self.publish(&Map::new(), "status", json!({"execution_state": state}));
    }

    pub fn send_client(&self, session_id: &str, msg: &Msg) {
        let c = self.clients.lock().unwrap();
        if let Some(cq) = c.by_session.get(session_id).and_then(|id| c.sockets.get(id)) { cq.put_frame(to_frame(msg)); }
    }

    pub fn relay_reply(&self, session_id: &str, msg: Msg) {
        if let Some(tx) = self.collectors.lock().unwrap().get(session_id) { let _ = tx.send(Ok(msg)); return; }
        self.send_client(session_id, &msg);
    }

    /// Reject an `execute_request` during restart or unavailability with a standard Jupyter
    /// `execute_reply` error and parented `idle` completion status.
    pub fn reject_execute(&self, session_id: &str, request: &Msg, err: &ExecError) {
        self.error_reply(session_id, request, err.ename(), &err.to_string());
        self.publish(&request.header, "status", json!({"execution_state": "idle"}));
    }

    pub fn error_reply(&self, session_id: &str, request: &Msg, ename: &str, error: &str) {
        let mut reply = Msg::new(&self.session, "shell", "execute_reply", json!({"status": "error", "ename": ename, "evalue": error, "traceback": []}));
        reply.parent_header = request.header.clone();
        self.relay_reply(session_id, reply);
    }

    /// Record a pending `input_request` header for `session_id` (FIFO queue supports concurrent subshells).
    pub fn record_stdin(&self, session_id: &str, header: Map<String, Value>) {
        self.pending_stdin.lock().unwrap().entry(session_id.to_string()).or_default().push_back(header);
    }

    pub async fn send(&self, ch: &KernelChannels, session_id: &str, msg: &mut Msg) -> Result<(), String> {
        self.send_reserved(ch.sender(&msg.channel).await?, session_id, msg, |_| {}).await
    }

    /// Once the writer is reserved, publish queued and stage the request without a cancellable wait between them.
    pub async fn send_reserved(&self, tx: ChannelTx<'_>, session_id: &str, msg: &mut Msg, annotate_queued: impl FnOnce(&mut Msg)) -> Result<(), String> {
        let execute = msg.is_execute();
        if execute {
            self.mark_queued(msg.msg_id().into());
            let mut queued = self.iopub_msg(&msg.header, "execute_queued", json!({}));
            annotate_queued(&mut queued);
            self.relay_iopub(&queued);
        }
        self.repair_stdin(session_id, msg);
        let result = tx.send(msg).await;
        if execute && result.is_err() { self.mark_idle(msg.msg_id()); }
        result
    }

    /// Repair a parentless `input_reply` from `pending_stdin`, or consume a matching pending entry when
    /// the client already supplied `parent_header.msg_id`.
    fn repair_stdin(&self, session_id: &str, msg: &mut Msg) {
        if msg.channel != "stdin" { return; }
        let mut map = self.pending_stdin.lock().unwrap();
        let Some(q) = map.get_mut(session_id) else { return; };
        if msg.parent_header.is_empty() {
            if let Some(hdr) = q.pop_front() { msg.parent_header = hdr; }
        } else if let Some(pid) = msg.parent_id() {
            if let Some(pos) = q.iter().position(|h| h.get("msg_id").and_then(Value::as_str) == Some(pid)) { q.remove(pos); }
        }
        if q.is_empty() { map.remove(session_id); }
    }

    pub fn interrupt_reply(&self, session_id: &str, request: &Msg, result: &Result<(), String>) {
        let content = match result { Ok(()) => json!({"status": "ok"}), Err(e) => json!({"status": "error", "ename": "InterruptError", "evalue": e}) };
        let mut reply = Msg::new(&self.session, "control", "interrupt_reply", content);
        reply.parent_header = request.header.clone();
        self.relay_reply(session_id, reply);
    }

    pub async fn control(&self, msg_type: &str, content: Value) -> Result<(), String> {
        let ch = self.channels().ok_or("kernel has no live channels")?;
        ch.send(&Msg::new(&self.session, "control", msg_type, content)).await
    }

    pub async fn delete_subshell(&self, id: &str) -> Result<(), String> {
        if !self.supports("kernel subshells") { return Ok(()); }
        self.control("delete_subshell_request", json!({"subshell_id": id})).await
    }

    pub async fn interrupt(&self) -> Result<(), String> { self.control("interrupt_request", json!({})).await }

    pub async fn close(&self) {
        self.transition(ChannelState::Dead);
        self.stop_channels().await;
        let mut c = self.clients.lock().unwrap();
        for cq in c.sockets.values() { cq.close(); }
        c.sockets.clear();
        c.by_session.clear();
        c.session_of.clear();
        self.collectors.lock().unwrap().clear();
    }
}

/// Run a WebSocket connection against `core`, registering its outgoing queue before invoking
/// `on_connected`, decoding legacy frames, enforcing session binding, and invoking `on_msg`.
pub async fn serve_ws_with<F, Fut>(socket: WebSocket, core: Arc<MuxCore>, query_sid: Option<String>, on_connected: impl FnOnce(), mut on_msg: F) where
    F: FnMut(String, Msg) -> Fut, Fut: Future<Output = Result<(), String>> {
    let (mut sink, mut stream) = socket.split();
    let (conn_id, cq, mut rx) = core.add_client(query_sid.as_deref());
    on_connected();
    let writer = async { while let Some(frame) = rx.recv().await { if sink.send(frame).await.is_err() { break; } } };
    let reader = async {
        while let Some(Ok(m)) = stream.next().await {
            match m {
                Message::Text(_) | Message::Binary(_) => {
                    let mut msg = match from_frame(&m) { Ok(msg) => msg, Err(e) => { eprintln!("dropped malformed frame: {e}"); continue; } };
                    let wire_sid = msg.header.get("session").and_then(Value::as_str).map(String::from);
                    let sid = match core.resolve_client_session(conn_id, wire_sid.as_deref()) {
                        Ok(sid) => sid, Err(SessionError::Disconnected) => break,
                        Err(SessionError::ConflictingSession) => { eprintln!("dropped frame with conflicting session on conn {conn_id}"); continue; }
                        Err(SessionError::MissingSession) => { eprintln!("dropped frame without session id on conn {conn_id}"); continue; }
                    };
                    if wire_sid.as_deref().is_none_or(str::is_empty) { msg.header.insert("session".into(), sid.clone().into()); }
                    if let Err(e) = on_msg(sid.clone(), msg).await { eprintln!("dropped frame from {sid}: {e}"); }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    };
    tokio::select! { _ = writer => {}, _ = reader => {}, _ = cq.closed.notified() => {} }
    core.drop_client(conn_id);
}

pub async fn serve_ws<F, Fut>(socket: WebSocket, core: Arc<MuxCore>, query_sid: Option<String>, on_msg: F) where F: FnMut(String, Msg) -> Fut,
    Fut: Future<Output = Result<(), String>> { serve_ws_with(socket, core, query_sid, || {}, on_msg).await; }
