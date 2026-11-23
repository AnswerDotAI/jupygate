//! Reusable kernel lifecycle primitives and standalone kernel mux/registry for `jupygate`.

use axum::http::StatusCode;
use kernmini::ConnectionInfo;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::{self, JoinHandle};
use tokio::time;
pub use tokio_util::sync::CancellationToken;
pub use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::channels::{ChannelRx, KernelChannels};
use crate::kernelspec;
use crate::mux_core::{ChannelState, MuxCore};
use crate::paths;
use crate::proc::{KernelProc, ReapOnDrop, SpawnOpts};
use crate::server::{cull_cutoff, now, should_cull};
use crate::session::Msg;

pub const START_TIMEOUT: Duration = Duration::from_secs(60);

pub type StartResult = Result<KernelModel, (StatusCode, String)>;

/// Typed inputs to resolve a kernel launch recipe (shared by REST, MCP, and downstream hosts).
#[derive(Clone, Default)]
pub struct LaunchInput {
    pub name: Option<String>, pub argv: Option<Vec<String>>, pub env: Option<HashMap<String, String>>, pub appendenv: Option<HashMap<String, String>>,
    pub cwd: Option<String>, pub cull_idle: Option<f64>, pub launcher: Option<String>, pub agent: Option<String>
}

/// Resolved kernel launch recipe: fixed at creation and replayed on every restart.
#[derive(Clone)]
pub struct Launch {
    pub name: String,
    pub argv: Vec<String>,
    /// The kernelspec's `env`, merged before caller secrets and `appendenv`.
    pub ks_env: HashMap<String, String>,
    /// Explicit base environment replacement (`env`), if requested.
    pub env: Option<HashMap<String, String>>,
    /// Explicit environment overlay (`appendenv`), winning over kernelspec env and caller secrets.
    pub appendenv: HashMap<String, String>,
    /// Explicit working directory override (`cwd`); `None` derives from path or served root.
    pub cwd: Option<String>,
    pub message_interrupt: bool,
    pub cull_idle: Option<f64>,
    pub launcher: Option<String>,
    pub agent: Option<String>,
}

impl Launch {
    pub fn resolve(input: LaunchInput) -> std::io::Result<Self> {
        let (name, argv, ks_env, message_interrupt) = if let Some(argv) = input.argv {
            if let Some(name) = input.name { return Err(std::io::Error::other(format!("name={name} cannot be combined with argv"))); }
            (String::new(), argv, HashMap::new(), false)
        } else {
            let name = input.name.unwrap_or_else(|| kernelspec::DEFAULT.into());
            let mut specs = kernelspec::load();
            let Some(ks) = specs.remove(&name) else { return Err(std::io::Error::other(kernelspec::unknown(&name, &specs))); };
            let ks_env = ks.env().into_iter().map(|(k, v)| (k, v.as_str().unwrap_or_default().to_string())).collect();
            (name, ks.argv(), ks_env, ks.message_interrupt())
        };
        if argv.is_empty() { return Err(std::io::Error::other("kernel argv is empty")); }
        let cull_idle = input.cull_idle.filter(|&s| s.is_finite() && s > 0.0);
        Ok(Self { name, argv, ks_env, env: input.env, appendenv: input.appendenv.unwrap_or_default(), cwd: input.cwd, message_interrupt, cull_idle,
            launcher: input.launcher, agent: input.agent })
    }

    /// Spawn a process from this recipe with per-spawn path, gateway URL, and optional caller secrets.
    /// Precedence: base (`env` or inherited) -> `ks_env` -> `caller_secrets` -> `appendenv` -> `JUPYGATE_*`.
    pub fn spawn(
        &self, root: &Path, path: Option<&str>, kid: &str, self_url: Option<&str>, caller_secrets: Option<&HashMap<String, String>>, conn_prefix: &str
    ) -> std::io::Result<KernelProc> {
        let cwd = paths::process_cwd(root, self.cwd.as_deref(), path)?;
        let mut appendenv = self.ks_env.clone();
        if let Some(secrets) = caller_secrets { appendenv.extend(secrets.iter().map(|(k, v)| (k.clone(), v.clone()))); }
        appendenv.extend(self.appendenv.iter().map(|(k, v)| (k.clone(), v.clone())));
        let url = self_url.or_else(|| crate::SELF_URL.get().map(String::as_str));
        let extra = [
            ("JUPYGATE_URL", url), ("JUPYGATE_KERNEL_ID", Some(kid)), ("JUPYGATE_KERNEL_PATH", path), ("JUPYGATE_LAUNCHER", self.launcher.as_deref()),
            ("JUPYGATE_AGENT", self.agent.as_deref()),
        ];
        KernelProc::spawn(&SpawnOpts { argv: &self.argv, env: self.env.as_ref(), appendenv: Some(&appendenv), extra_env: &extra,
            cwd: cwd.as_deref().and_then(|p| p.to_str()), conn_prefix })
    }
}

/// Coarse process lifecycle phase; combined with `MuxCore::busy()` to yield standard Jupyter `execution_state`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecyclePhase { Starting, Running, Restarting, Dead }

/// Phase-specific restart failure.
#[derive(Clone, Debug)]
pub enum RestartError { Respawn(String), Ready(String) }

impl RestartError {
    pub fn message(&self) -> &str { match self { Self::Respawn(s) | Self::Ready(s) => s } }
    pub fn into_message(self) -> String { match self { Self::Respawn(s) | Self::Ready(s) => s } }
}

/// Reusable concrete kernel process lifecycle: launch recipe, optional process handle, phase,
/// heartbeat health, cancellable deletion token, MCP execution lock, restart lock, and clean teardown.
pub struct KernelLifecycle {
    pub launch: Launch, proc_: Mutex<Option<KernelProc>>, phase: Mutex<LifecyclePhase>, unresponsive: AtomicBool, deleted: CancellationToken,
    last_beat: Mutex<Option<f64>>, watcher: Mutex<Option<JoinHandle<()>>>, mcp_exec: Mutex<()>, pub restart_lock: Mutex<()>, pub cull_idle: Option<f64>,
    pub core: Arc<MuxCore>
}

impl KernelLifecycle {
    pub fn new(launch: Launch, proc_: KernelProc, core: Arc<MuxCore>) -> Arc<Self> {
        let cull_idle = launch.cull_idle;
        Arc::new(Self { launch, proc_: Mutex::new(Some(proc_)), phase: Mutex::new(LifecyclePhase::Running), unresponsive: AtomicBool::new(false),
            deleted: CancellationToken::new(), last_beat: Mutex::new(None), watcher: Mutex::new(None), mcp_exec: Mutex::new(()), restart_lock: Mutex::new(()),
            cull_idle, core })
    }

    pub async fn phase(&self) -> LifecyclePhase { *self.phase.lock().await }

    pub async fn set_phase(&self, phase: LifecyclePhase) {
        *self.phase.lock().await = phase;
        if phase != LifecyclePhase::Running { self.unresponsive.store(false, Ordering::Relaxed); }
    }

    pub async fn is_dead(&self) -> bool { *self.phase.lock().await == LifecyclePhase::Dead }

    pub fn unresponsive(&self) -> bool { self.unresponsive.load(Ordering::Relaxed) }

    pub fn execution_lost(&self) -> bool { self.core.is_channel_lost() }

    pub fn mark_deleted(&self) { self.deleted.cancel(); }
    pub fn is_deleted(&self) -> bool { self.deleted.is_cancelled() }
    pub fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> { self.deleted.cancelled() }

    /// Standard Jupyter `execution_state`: `starting`, `idle`, `busy`, `restarting`, or `dead`.
    pub async fn execution_state(&self) -> &'static str {
        match *self.phase.lock().await {
            LifecyclePhase::Starting => "starting", LifecyclePhase::Restarting => "restarting", LifecyclePhase::Dead => "dead",
            LifecyclePhase::Running => { if self.core.busy() { "busy" } else { "idle" } }
        }
    }

    /// Serialize MCP calls, cancelling a waiting submit without interrupting another client's execution.
    pub async fn mcp_execute<F: Future<Output = Result<(), String>>>(
        &self, code: &str, cancel: &mut tokio::sync::watch::Receiver<bool>, send: impl FnOnce(String, Msg) -> F,
        interrupt: impl Future<Output = Option<Result<(), String>>>,
    ) -> Result<Vec<Msg>, String> {
        let _exec = tokio::select! {
            biased;
            _ = cancel.wait_for(|v| *v) => return Err("request cancelled".into()),
            guard = self.mcp_exec.lock() => guard,
        };
        let mut msg = self.core.execute_request(code, true, false);
        let mid = msg.msg_id().to_string();
        let execute = async {
            loop {
                self.core.check_admission().map_err(|e| e.to_string())?;
                let idle = self.core.notified_idle();
                if !self.core.is_running() { break; }
                idle.await;
            }
            let mut running = self.core.begin_collector(&mut msg);
            send(running.session_id().into(), msg).await?;
            running.collect(|m| matches!(m.msg_type(), "stream" | "display_data" | "execute_result" | "error")).await.map_err(|e| e.to_string())
        };
        tokio::select! {
            biased;
            _ = cancel.wait_for(|v| *v) => (),
            output = execute => return output,
        }
        if self.core.is_active_running(&mid) && let Some(Err(e)) = interrupt.await { eprintln!("MCP interrupt failed for kernel {}: {e}", self.core.kid); }
        Err("request cancelled".into())
    }

    pub async fn pid(&self) -> u32 { self.proc_.lock().await.as_ref().map_or(0, KernelProc::pid) }
    pub async fn last_heartbeat(&self) -> Option<f64> { *self.last_beat.lock().await }

    /// Mark the kernel `dead` and broadcast the synthesized `dead` status.
    pub async fn mark_dead(&self) {
        *self.phase.lock().await = LifecyclePhase::Dead;
        self.unresponsive.store(false, Ordering::Relaxed);
        self.core.synthesize_status("dead");
    }

    pub async fn start_watcher(self: &Arc<Self>) {
        let mut guard = self.watcher.lock().await;
        if let Some(w) = guard.take() { w.abort(); }
        *guard = Some(tokio::spawn(watch_lifecycle(self.clone())));
    }

    pub async fn abort_watcher(&self) { if let Some(w) = self.watcher.lock().await.take() { w.abort(); } }

    pub async fn interrupt(&self) -> Result<(), String> {
        if self.launch.message_interrupt { self.core.interrupt().await } else {
            let guard = self.proc_.lock().await;
            let Some(p) = guard.as_ref() else { return Err("kernel is dead".into()); };
            p.interrupt();
            Ok(())
        }
    }

    /// Replace the owned process handle (terminating any prior one first).
    pub async fn replace_proc(&self, next: KernelProc) {
        let old = self.proc_.lock().await.replace(next);
        if let Some(mut p) = old { task::block_in_place(|| p.terminate()); }
    }

    pub async fn terminate_proc(&self) { let old = self.proc_.lock().await.take(); if let Some(mut p) = old { task::block_in_place(|| p.terminate()); } }

    /// Shut down channels, watcher, and process, invoking `on_close` before terminating the child.
    pub async fn shutdown_with(&self, on_close: impl std::future::Future<Output = ()>) {
        self.mark_deleted();
        self.core.transition(ChannelState::Dead);
        self.abort_watcher().await;
        self.mark_dead().await;
        on_close.await;
        self.terminate_proc().await;
    }

    pub async fn shutdown(&self) { self.shutdown_with(self.core.close()).await; }
}

/// The beat loop: process death broadcasts `dead`; three missed beats set `unresponsive = true`,
/// the next echo clears it. Observational only - a kernel becomes dead only when its process exits.
async fn watch_lifecycle(lc: Arc<KernelLifecycle>) {
    let mut misses = 0;
    loop {
        let alive = lc.proc_.lock().await.as_mut().is_some_and(KernelProc::alive);
        if !alive { break; }
        let Some(ch) = lc.core.channels() else { return };
        if ch.beat(Duration::from_secs(1)).await {
            *lc.last_beat.lock().await = Some(now());
            misses = 0;
            if lc.unresponsive.swap(false, Ordering::Relaxed) { eprintln!("kernel {}: responsive again", lc.core.kid); }
        } else {
            misses += 1;
            if misses >= 3 && *lc.phase.lock().await == LifecyclePhase::Running && !lc.unresponsive.swap(true, Ordering::Relaxed) {
                eprintln!("kernel {}: unresponsive, 3 heartbeats missed", lc.core.kid);
            }
        }
        time::sleep(Duration::from_secs(1)).await;
    }
    if *lc.phase.lock().await == LifecyclePhase::Running { lc.mark_dead().await; }
}

/// A standalone gateway kernel with standard Jupyter message delivery.
pub struct KernelMux { pub core: Arc<MuxCore>, pub kid: String, path: std::sync::Mutex<Option<String>> }

impl KernelMux {
    pub fn new(qmax: usize, path: Option<String>, kid: String) -> Arc<Self> {
        let core = MuxCore::new(kid.clone(), qmax, "jupygate");
        Arc::new(Self { core, kid, path: std::sync::Mutex::new(path) })
    }

    pub fn path(&self) -> Option<String> { self.path.lock().unwrap().clone() }

    fn start(self: &Arc<Self>, channels: Vec<ChannelRx>, info: Value) {
        self.core.set_info(info);
        for rx in channels {
            let core = self.core.clone();
            self.core.spawn_task(async move { core.pump(rx, |msg| core.deliver(msg)).await });
        }
    }

    pub async fn handle_message(&self, session_id: &str, mut msg: Msg) -> Result<(), String> {
        let ch = self.core.ready_channels().map_err(|err| {
            if msg.is_execute() { self.core.reject_execute(session_id, &msg, &err); }
            err.to_string()
        })?;
        self.core.send(&ch, session_id, &mut msg).await
    }
}

async fn start_channels(conn: &ConnectionInfo, mux: &Arc<KernelMux>) -> Result<Arc<KernelChannels>, String> {
    let (ch, rx, info) = KernelChannels::connect(conn, "jupygate", START_TIMEOUT).await?;
    mux.core.transition(ChannelState::Initializing(Some(ch.clone())));
    mux.start(rx, info);
    Ok(ch)
}

/// A kernel process plus its channel set and mux, as one lifecycle.
pub struct GatewayKernel { pub lifecycle: Arc<KernelLifecycle>, pub mux: Arc<KernelMux> }

#[derive(Clone, serde::Serialize, schemars::JsonSchema)]
pub struct KernelModel {
    pub id: String, pub name: String, pub language: String, pub execution_state: String, pub unresponsive: bool, pub execution_lost: bool,
    pub connections: usize, pub pid: u32, pub last_heartbeat: Option<f64>, pub last_activity: f64, pub path: Option<String>
}

impl GatewayKernel {
    pub async fn model(&self) -> KernelModel {
        KernelModel { id: self.mux.kid.clone(), name: self.lifecycle.launch.name.clone(), language: self.mux.core.language(),
            execution_state: self.lifecycle.execution_state().await.to_string(), unresponsive: self.lifecycle.unresponsive(),
            execution_lost: self.lifecycle.execution_lost(), connections: self.mux.core.n_attached(), pid: self.lifecycle.pid().await,
            last_heartbeat: self.lifecycle.last_heartbeat().await, last_activity: self.mux.core.last_activity(), path: self.mux.path() }
    }
}

/// Abort pump tasks and close channels if an async `create` future is dropped before publication.
struct CloseMuxOnDrop(Option<Arc<MuxCore>>);
impl CloseMuxOnDrop { fn disarm(&mut self) { self.0 = None; } }
impl Drop for CloseMuxOnDrop {
    fn drop(&mut self) {
        if let Some(core) = self.0.take() {
            core.transition(ChannelState::Dead);
            core.abort_tasks();
        }
    }
}

/// Explicitly terminate and reap a candidate kernel process before returning from error/cancel paths.
async fn reap_proc(mut proc_guard: ReapOnDrop) {
    let mut proc_ = proc_guard.take();
    let _ = task::spawn_blocking(move || proc_.terminate()).await;
}
/// All live kernels, keyed solely by kernel ID. Path-based reuse belongs in `/api/sessions`.
pub struct Kernels { kernels: RwLock<HashMap<String, Arc<GatewayKernel>>>, teardowns: TaskTracker, shutdown: CancellationToken, root: PathBuf, qmax: usize }

impl Kernels {
    pub fn new(root: PathBuf) -> Self {
        Self { kernels: RwLock::new(HashMap::new()), teardowns: TaskTracker::new(), shutdown: CancellationToken::new(), root, qmax: 1000 }
    }

    /// Spawn a new independent kernel, wait until ready, and publish it under a fresh ID.
    pub async fn create(&self, input: LaunchInput, path: Option<String>) -> Result<KernelModel, String> {
        if self.shutdown.is_cancelled() { return Err("gateway shutting down".into()); }
        let id = Uuid::new_v4().simple().to_string();
        let mux = KernelMux::new(self.qmax, path.clone(), id.clone());
        let mut mux_guard = CloseMuxOnDrop(Some(mux.core.clone()));
        let (root, kid, path_c) = (self.root.clone(), id.clone(), path.clone());
        let (proc_guard, launch) = task::spawn_blocking(move || {
            let launch = Launch::resolve(input)?;
            let proc_ = launch.spawn(&root, path_c.as_deref(), &kid, None, None, "jupygate")?;
            Ok::<_, std::io::Error>((ReapOnDrop::new(proc_), launch))
        }).await.expect("spawn task panicked").map_err(|e| e.to_string())?;
        let conn_info = proc_guard.as_ref().unwrap().info().clone();
        let ch = tokio::select! {
            res = start_channels(&conn_info, &mux) => match res {
                Ok(chans) => chans,
                Err(e) => {
                    mux_guard.disarm();
                    mux.core.close().await;
                    reap_proc(proc_guard).await;
                    return Err(e);
                }
            },
            _ = self.shutdown.cancelled() => {
                mux_guard.disarm();
                mux.core.close().await;
                reap_proc(proc_guard).await;
                return Err("gateway shutting down".into());
            }
        };
        if !mux.core.transition(ChannelState::Ready(ch)) {
            mux_guard.disarm();
            mux.core.close().await;
            reap_proc(proc_guard).await;
            return Err("kernel channels lost during startup".into());
        }
        let mut proc_guard = proc_guard;
        let proc_ = proc_guard.take();
        mux_guard.disarm();
        let lifecycle = KernelLifecycle::new(launch, proc_, mux.core.clone());
        let gk = Arc::new(GatewayKernel { lifecycle: lifecycle.clone(), mux });
        {
            let mut map = self.kernels.write().await;
            if self.shutdown.is_cancelled() {
                drop(map);
                gk.lifecycle.shutdown().await;
                return Err("gateway shutting down".into());
            }
            map.insert(id, gk.clone());
        }
        lifecycle.start_watcher().await;
        Ok(gk.model().await)
    }

    pub async fn get(&self, kid: &str) -> Option<Arc<GatewayKernel>> { self.kernels.read().await.get(kid).cloned() }

    pub async fn model(&self, kid: &str) -> Option<KernelModel> { match self.get(kid).await { Some(gk) => Some(gk.model().await), None => None } }

    pub async fn models(&self) -> Vec<KernelModel> {
        let gks: Vec<_> = self.kernels.read().await.values().cloned().collect();
        let mut out = Vec::with_capacity(gks.len());
        for gk in gks { out.push(gk.model().await); }
        out
    }

    pub async fn models_with_agents(&self) -> Vec<(KernelModel, Option<String>)> {
        let gks: Vec<_> = self.kernels.read().await.values().cloned().collect();
        let mut out = Vec::with_capacity(gks.len());
        for gk in gks { out.push((gk.model().await, gk.lifecycle.launch.agent.clone())); }
        out
    }

    pub async fn idle(&self, secs: f64, extra: impl Fn(&GatewayKernel) -> Option<f64>) -> Vec<String> {
        let t = now();
        self.kernels.read().await.values().filter(|gk| {
            let Some(cutoff) = cull_cutoff(t, secs, gk.lifecycle.cull_idle, extra(gk)) else { return false };
            should_cull(gk.mux.core.last_activity(), cutoff, gk.mux.core.busy(), false)
        }).map(|gk| gk.mux.kid.clone()).collect()
    }

    /// Delete `kid`: cancels any in-flight restart immediately so deletion never blocks on startup.
    pub async fn delete(&self, kid: &str) -> bool {
        let gk = {
            let mut map = self.kernels.write().await;
            let Some(gk) = map.remove(kid) else { return false; };
            gk.lifecycle.mark_deleted();
            gk
        };
        let lc = gk.lifecycle.clone();
        let _ = self.teardowns.spawn(async move {
            let _guard = lc.restart_lock.lock().await;
            lc.shutdown().await;
        }).await;
        true
    }

    pub async fn interrupt(self: &Arc<Self>, kid: &str) -> Option<Result<(), String>> {
        let gk = self.get(kid).await?;
        if gk.mux.core.supports("interrupt requires restart") { return self.restart(kid).await.map(|r| r.map(|_| ()).map_err(RestartError::into_message)); }
        Some(gk.lifecycle.interrupt().await)
    }

    pub async fn handle_msg(self: &Arc<Self>, kid: &str, session_id: &str, msg: Msg) -> Result<(), String> {
        let gk = self.get(kid).await.ok_or("no such kernel")?;
        if msg.channel == "control" && msg.msg_type() == "interrupt_request" && gk.mux.core.supports("interrupt requires restart") {
            let result = self.interrupt(kid).await.ok_or("no such kernel")?;
            gk.mux.core.interrupt_reply(session_id, &msg, &result);
            result
        } else { gk.mux.handle_message(session_id, msg).await }
    }

    async fn remove_failed_restart(&self, kid: &str, gk: &Arc<GatewayKernel>) {
        let mut map = self.kernels.write().await;
        if map.get(kid).is_some_and(|cur| Arc::ptr_eq(cur, gk)) { map.remove(kid); }
        drop(map);
        gk.lifecycle.shutdown().await;
    }

    async fn run_restart(&self, kid: &str) -> Option<Result<KernelModel, RestartError>> {
        let gk = self.get(kid).await?;
        let _restarting = gk.lifecycle.restart_lock.lock().await;
        if gk.lifecycle.is_deleted() || self.get(kid).await.is_none() { return None; }
        let path = gk.mux.path();
        gk.mux.core.transition(ChannelState::Initializing(None));
        gk.lifecycle.set_phase(LifecyclePhase::Restarting).await;
        gk.mux.core.synthesize_status("restarting");
        gk.mux.core.stop_channels().await;
        gk.lifecycle.abort_watcher().await;
        gk.lifecycle.terminate_proc().await;
        if gk.lifecycle.is_deleted() { return None; }

        let (launch, root, kid_c, path_c) = (gk.lifecycle.launch.clone(), self.root.clone(), gk.mux.kid.clone(), path.clone());
        let spawn_res = task::spawn_blocking(move || launch.spawn(&root, path_c.as_deref(), &kid_c, None, None, "jupygate").map(ReapOnDrop::new)).await
            .expect("spawn task panicked");
        let mut proc_guard = match spawn_res {
            Ok(g) => g, Err(e) => { self.remove_failed_restart(kid, &gk).await; return Some(Err(RestartError::Respawn(format!("respawn failed: {e}")))); }
        };
        if gk.lifecycle.is_deleted() { reap_proc(proc_guard).await; return None; }
        let info = proc_guard.as_ref().unwrap().info().clone();
        let ch = tokio::select! {
            res = start_channels(&info, &gk.mux) => match res {
                Ok(ch) => ch,
                Err(e) => {
                    gk.mux.core.stop_channels().await;
                    reap_proc(proc_guard).await;
                    self.remove_failed_restart(kid, &gk).await;
                    return Some(Err(RestartError::Ready(format!("kernel not ready after respawn: {e}"))));
                }
            },
            _ = gk.lifecycle.cancelled() => {
                gk.mux.core.stop_channels().await;
                reap_proc(proc_guard).await;
                return None;
            }
        };
        {
            let map = self.kernels.read().await;
            if gk.lifecycle.is_deleted() || !map.contains_key(kid) {
                drop(map);
                gk.mux.core.stop_channels().await;
                reap_proc(proc_guard).await;
                return None;
            }
            if !gk.mux.core.transition(ChannelState::Ready(ch)) {
                drop(map);
                gk.mux.core.stop_channels().await;
                reap_proc(proc_guard).await;
                self.remove_failed_restart(kid, &gk).await;
                return Some(Err(RestartError::Ready("kernel channels lost during restart".into())));
            }
            gk.lifecycle.replace_proc(proc_guard.take()).await;
            gk.lifecycle.set_phase(LifecyclePhase::Running).await;
        }
        gk.mux.core.synthesize_status("starting");
        gk.lifecycle.start_watcher().await;
        Some(Ok(gk.model().await))
    }

    /// Plain stock Jupyter restart: terminate and respawn under the same ID while keeping client queues.
    pub async fn restart(self: &Arc<Self>, kid: &str) -> Option<Result<KernelModel, RestartError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let this = self.clone();
        let kid = kid.to_string();
        self.teardowns.spawn(async move {
            let res = this.run_restart(&kid).await;
            let _ = tx.send(res);
        });
        rx.await.ok().flatten()
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        let gks: Vec<_> = { let mut map = self.kernels.write().await; map.drain().map(|(_, gk)| { gk.lifecycle.mark_deleted(); gk }).collect() };
        for gk in gks {
            let lc = gk.lifecycle.clone();
            self.teardowns.spawn(async move {
                let _guard = lc.restart_lock.lock().await;
                lc.shutdown().await;
            });
        }
        self.teardowns.close();
        self.teardowns.wait().await;
    }
}
