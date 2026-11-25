//! Reusable kernel lifecycle primitives and standalone kernel mux/registry for `jupygate`.

use axum::http::StatusCode;
use kernmini::ConnectionInfo;
use std::collections::HashMap;
use std::future::Future;
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

use crate::channels::KernelChannels;
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
    pub cwd: Option<String>, pub cull_idle: Option<f64>, pub launcher: Option<String>
}

/// Typed per-spawn inputs supplied by the host on initial start and every restart.
pub struct SpawnContext {
    pub path: Option<String>, pub self_url: Option<String>, pub caller_secrets: Option<HashMap<String, String>>, pub conn_prefix: &'static str
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
            launcher: input.launcher })
    }

    /// Spawn a process from this recipe with per-spawn path, gateway URL, and optional caller secrets.
    /// Precedence: inherited -> `ks_env` -> `caller_secrets` -> `env` -> `appendenv` -> `JUPYGATE_*`.
    pub fn spawn_with(&self, root: &Path, kid: &str, ctx: &SpawnContext) -> std::io::Result<KernelProc> {
        let env_cwd = self.env.as_ref().and_then(|e| e.get("KERNEL_WORKING_DIR")).map(String::as_str).filter(|s| !s.is_empty());
        let cwd = paths::process_cwd(root, self.cwd.as_deref().or(env_cwd), ctx.path.as_deref())?;
        let mut appendenv = self.ks_env.clone();
        if let Some(secrets) = ctx.caller_secrets.as_ref() { appendenv.extend(secrets.iter().map(|(k, v)| (k.clone(), v.clone()))); }
        if let Some(env) = self.env.as_ref() { appendenv.extend(env.iter().map(|(k, v)| (k.clone(), v.clone()))); }
        appendenv.extend(self.appendenv.iter().map(|(k, v)| (k.clone(), v.clone())));
        let url = ctx.self_url.as_deref().or_else(|| crate::SELF_URL.get().map(String::as_str));
        let extra = [
            ("JUPYGATE_URL", url), ("JUPYGATE_ROOT", root.to_str()), ("JUPYGATE_KERNEL_ID", Some(kid)),
            ("JUPYGATE_KERNEL_PATH", ctx.path.as_deref()), ("JUPYGATE_LAUNCHER", self.launcher.as_deref()),
        ];
        KernelProc::spawn(&SpawnOpts { argv: &self.argv, appendenv: Some(&appendenv), extra_env: &extra, cwd: cwd.as_deref().and_then(|p| p.to_str()),
            conn_prefix: ctx.conn_prefix })
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
        interrupt: impl Future<Output = Option<Result<(), String>>>) -> Result<Vec<Msg>, String> {
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

    /// Build the base Jupyter `KernelModel` snapshot for this kernel.
    pub async fn model(&self, path: Option<String>) -> KernelModel {
        KernelModel { id: self.core.kid.clone(), name: self.launch.name.clone(), language: self.core.language(),
            execution_state: self.execution_state().await.to_string(), unresponsive: self.unresponsive(), execution_lost: self.execution_lost(),
            connections: self.core.n_attached(), pid: self.pid().await, last_heartbeat: self.last_heartbeat().await,
            last_activity: crate::contents::rfc3339_secs(self.core.last_activity()), path }
    }

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
        if let Some(ch) = lc.core.channels() {
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
        }
        time::sleep(Duration::from_secs(1)).await;
    }
    if *lc.phase.lock().await == LifecyclePhase::Running { lc.mark_dead().await; }
}

/// Product-specific attachment owned alongside a `KernelLifecycle` inside `KernelPool`.
pub trait KernelEntry: Send + Sync + Sized + 'static {
    fn core(&self) -> &Arc<MuxCore>;
    fn spawn_context(&self) -> std::io::Result<SpawnContext>;
    fn on_message(self: &Arc<Self>, _ch: &Arc<KernelChannels>, msg: Msg) { self.core().deliver(msg); }
    fn init(self: &Arc<Self>, _ch: Arc<KernelChannels>) -> impl Future<Output = Result<(), String>> + Send { async { Ok(()) } }
    fn on_stop_channels(&self) {}
    fn on_close(self: &Arc<Self>) -> impl Future<Output = ()> + Send { async {} }
}

/// A live kernel process plus its `KernelLifecycle` and host-specific entry.
pub struct ManagedKernel<E> { pub lifecycle: Arc<KernelLifecycle>, pub entry: Arc<E> }

impl<E> ManagedKernel<E> {
    pub fn kid(&self) -> &str { &self.lifecycle.core.kid }
    pub fn core(&self) -> &Arc<MuxCore> { &self.lifecycle.core }
}

#[derive(Clone, serde::Serialize, schemars::JsonSchema)]
pub struct KernelModel {
    pub id: String, pub name: String, pub language: String, pub execution_state: String, pub unresponsive: bool, pub execution_lost: bool,
    pub connections: usize, pub pid: u32, pub last_heartbeat: Option<f64>, pub last_activity: String, pub path: Option<String>
}

/// Abort pump tasks and run close cleanup if an async `create` future is dropped before publication.
struct AbortCreateGuard<E: KernelEntry> { teardowns: TaskTracker, entry: Option<Arc<E>> }
impl<E: KernelEntry> AbortCreateGuard<E> { fn disarm(&mut self) { self.entry = None; } }
impl<E: KernelEntry> Drop for AbortCreateGuard<E> {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            entry.core().transition(ChannelState::Dead);
            entry.core().abort_tasks();
            self.teardowns.spawn(async move { close_entry(&entry).await; });
        }
    }
}

/// Explicitly terminate and reap a candidate kernel process before returning from error/cancel paths.
async fn reap_proc(mut proc_guard: ReapOnDrop) {
    let mut proc_ = proc_guard.take();
    let _ = task::spawn_blocking(move || proc_.terminate()).await;
}

async fn stop_entry_channels<E: KernelEntry>(entry: &Arc<E>) {
    entry.core().stop_channels().await;
    entry.on_stop_channels();
}

async fn close_entry<E: KernelEntry>(entry: &Arc<E>) {
    entry.core().close().await;
    entry.on_close().await;
}

async fn shutdown_kernel<E: KernelEntry>(gk: &ManagedKernel<E>) { gk.lifecycle.shutdown_with(close_entry(&gk.entry)).await; }

enum BringUpError { Cancelled, Respawn(String), Ready(String) }

async fn connect_and_init<E: KernelEntry>(conn: &ConnectionInfo, username: &str, entry: &Arc<E>) -> Result<Arc<KernelChannels>, String> {
    let ready = async {
        let (ch, rx, info) = KernelChannels::connect(conn, username, START_TIMEOUT).await?;
        entry.core().transition(ChannelState::Initializing(Some(ch.clone())));
        let e = entry.clone();
        entry.core().start_pumps(ch.clone(), rx, info, move |ch, msg| e.on_message(ch, msg));
        entry.init(ch.clone()).await?;
        Ok(ch)
    };
    time::timeout(START_TIMEOUT, ready).await.map_err(|_| "kernel startup timed out".to_string())?
}

/// Spawn one kernel generation, connect/init channels under `START_TIMEOUT`, and reap synchronously on error or cancellation.
async fn bring_up<E: KernelEntry>(
    root: &Path, kid: &str, username: &str, launch: &Launch, entry: &Arc<E>, cancel: &CancellationToken
) -> Result<(ReapOnDrop, Arc<KernelChannels>, Option<String>), BringUpError> {
    let (entry_c, launch_c, root, kid_c) = (entry.clone(), launch.clone(), root.to_path_buf(), kid.to_string());
    let spawn_res = task::spawn_blocking(move || {
        let ctx = entry_c.spawn_context()?;
        let proc_ = launch_c.spawn_with(&root, &kid_c, &ctx)?;
        Ok::<_, std::io::Error>((ReapOnDrop::new(proc_), ctx.path))
    }).await.expect("spawn task panicked");
    let (proc_guard, spawned_path) = spawn_res.map_err(|e| BringUpError::Respawn(e.to_string()))?;
    if cancel.is_cancelled() { reap_proc(proc_guard).await; return Err(BringUpError::Cancelled); }
    let conn_info = proc_guard.as_ref().unwrap().info().clone();
    let ch = tokio::select! {
        res = connect_and_init(&conn_info, username, entry) => match res {
            Ok(ch) => ch,
            Err(e) => {
                stop_entry_channels(entry).await;
                reap_proc(proc_guard).await;
                return Err(BringUpError::Ready(e));
            }
        },
        _ = cancel.cancelled() => {
            entry.core().transition(ChannelState::Dead);
            stop_entry_channels(entry).await;
            reap_proc(proc_guard).await;
            return Err(BringUpError::Cancelled);
        }
    };
    Ok((proc_guard, ch, spawned_path))
}

/// Concrete owner of live kernel entries, process lifecycle, cancellation, and teardown.
pub struct KernelPool<E: KernelEntry> {
    kernels: RwLock<HashMap<String, Arc<ManagedKernel<E>>>>, teardowns: TaskTracker, shutdown: CancellationToken, root: PathBuf, qmax: usize,
    username: &'static str
}

impl<E: KernelEntry> KernelPool<E> {
    pub fn new(root: PathBuf, username: &'static str) -> Self {
        Self { kernels: RwLock::new(HashMap::new()), teardowns: TaskTracker::new(), shutdown: CancellationToken::new(), root, qmax: 1000, username }
    }

    pub fn root(&self) -> &Path { &self.root }
    pub fn qmax(&self) -> usize { self.qmax }
    pub fn is_shutting_down(&self) -> bool { self.shutdown.is_cancelled() }

    pub async fn get(&self, kid: &str) -> Option<Arc<ManagedKernel<E>>> { self.kernels.read().await.get(kid).cloned() }

    pub async fn list(&self) -> Vec<Arc<ManagedKernel<E>>> { self.kernels.read().await.values().cloned().collect() }

    pub async fn find(&self, pred: impl Fn(&ManagedKernel<E>) -> bool) -> Vec<Arc<ManagedKernel<E>>> {
        self.kernels.read().await.values().filter(|gk| pred(gk)).cloned().collect()
    }

    pub async fn idle(&self, secs: f64, protected: impl Fn(&ManagedKernel<E>, f64) -> bool) -> Vec<String> {
        let t = now();
        self.kernels.read().await.values().filter(|gk| {
            let Some(cutoff) = cull_cutoff(t, secs, gk.lifecycle.cull_idle, None) else { return false };
            should_cull(gk.core().last_activity(), cutoff, gk.core().busy(), protected(gk, cutoff))
        }).map(|gk| gk.kid().to_string()).collect()
    }

    async fn teardown(gk: Arc<ManagedKernel<E>>) {
        let _guard = gk.lifecycle.restart_lock.lock().await;
        shutdown_kernel(&gk).await;
    }

    async fn remove_failed_restart(&self, kid: &str, gk: &Arc<ManagedKernel<E>>) {
        let mut map = self.kernels.write().await;
        if map.get(kid).is_some_and(|cur| Arc::ptr_eq(cur, gk)) { map.remove(kid); }
        drop(map);
        shutdown_kernel(gk).await;
    }

    /// Spawn, connect/initialize, transition to `Ready`, publish into the pool, and start heartbeat watching.
    pub async fn create(&self, id: String, launch: Launch, entry: Arc<E>) -> Result<Arc<ManagedKernel<E>>, String> {
        if self.shutdown.is_cancelled() { close_entry(&entry).await; return Err("gateway shutting down".into()); }
        let mut guard = AbortCreateGuard { teardowns: self.teardowns.clone(), entry: Some(entry.clone()) };
        let (mut proc_guard, ch, _) = match bring_up(&self.root, &id, self.username, &launch, &entry, &self.shutdown).await {
            Ok(ok) => ok,
            Err(e) => {
                guard.disarm();
                close_entry(&entry).await;
                return Err(match e { BringUpError::Cancelled => "gateway shutting down".into(), BringUpError::Respawn(s) | BringUpError::Ready(s) => s });
            }
        };
        if !entry.core().transition(ChannelState::Ready(ch)) {
            guard.disarm();
            close_entry(&entry).await;
            reap_proc(proc_guard).await;
            return Err("kernel channels lost during startup".into());
        }
        let proc_ = proc_guard.take();
        let lifecycle = KernelLifecycle::new(launch, proc_, entry.core().clone());
        let gk = Arc::new(ManagedKernel { lifecycle: lifecycle.clone(), entry });
        {
            let mut map = self.kernels.write().await;
            if self.shutdown.is_cancelled() {
                drop(map);
                guard.disarm();
                shutdown_kernel(&gk).await;
                return Err("gateway shutting down".into());
            }
            guard.disarm();
            map.insert(id, gk.clone());
        }
        lifecycle.start_watcher().await;
        Ok(gk)
    }

    /// Remove `kid`, cancel any in-flight restart, and shut down its process/channels/entry.
    pub async fn delete(&self, kid: &str) -> Option<Arc<ManagedKernel<E>>> {
        let gk = {
            let mut map = self.kernels.write().await;
            let gk = map.remove(kid)?;
            gk.lifecycle.mark_deleted();
            gk
        };
        let _ = self.teardowns.spawn(Self::teardown(gk.clone())).await;
        Some(gk)
    }

    /// Remove and tear down any dead kernels matching `pred`.
    pub async fn remove_dead(&self, pred: impl Fn(&ManagedKernel<E>) -> bool) {
        let candidates = self.find(pred).await;
        let mut dead = vec![];
        for gk in candidates { if gk.lifecycle.is_dead().await { dead.push(gk.kid().to_string()); } }
        for id in dead {
            let gk = {
                let mut map = self.kernels.write().await;
                let Some(gk) = map.get(&id).cloned() else { continue; };
                if !gk.lifecycle.is_dead().await { continue; }
                map.remove(&id);
                gk.lifecycle.mark_deleted();
                gk
            };
            let _ = self.teardowns.spawn(Self::teardown(gk)).await;
        }
    }

    async fn run_restart<S, FBefore, FutBefore, FAfter, FutAfter>(
        &self, gk: Arc<ManagedKernel<E>>, kid: &str, before_respawn: FBefore, after_ready: FAfter) -> Option<Result<Arc<ManagedKernel<E>>, RestartError>> where
        FBefore: FnOnce(&Arc<ManagedKernel<E>>) -> FutBefore, FutBefore: Future<Output = S>,
        FAfter: FnOnce(&Arc<ManagedKernel<E>>, Option<String>, S) -> FutAfter, FutAfter: Future<Output = ()> {
        let _restarting = gk.lifecycle.restart_lock.lock().await;
        if gk.lifecycle.is_deleted() || self.get(kid).await.is_none() { return None; }
        gk.core().transition(ChannelState::Initializing(None));
        gk.lifecycle.set_phase(LifecyclePhase::Restarting).await;
        gk.core().synthesize_status("restarting");
        stop_entry_channels(&gk.entry).await;
        let state = before_respawn(&gk).await;
        gk.lifecycle.abort_watcher().await;
        gk.lifecycle.terminate_proc().await;
        if gk.lifecycle.is_deleted() { return None; }
        let (mut proc_guard, ch, spawned_path) = match bring_up(&self.root, kid, self.username, &gk.lifecycle.launch, &gk.entry, &gk.lifecycle.deleted).await {
            Ok(ok) => ok,
            Err(BringUpError::Cancelled) => return None,
            Err(BringUpError::Respawn(e)) => {
                self.remove_failed_restart(kid, &gk).await;
                return Some(Err(RestartError::Respawn(format!("respawn failed: {e}"))));
            }
            Err(BringUpError::Ready(e)) => {
                self.remove_failed_restart(kid, &gk).await;
                return Some(Err(RestartError::Ready(format!("kernel not ready after respawn: {e}"))));
            }
        };
        {
            let map = self.kernels.read().await;
            if gk.lifecycle.is_deleted() || !map.contains_key(kid) {
                drop(map);
                gk.core().transition(ChannelState::Dead);
                stop_entry_channels(&gk.entry).await;
                reap_proc(proc_guard).await;
                return None;
            }
            if !gk.core().transition(ChannelState::Ready(ch)) {
                drop(map);
                stop_entry_channels(&gk.entry).await;
                reap_proc(proc_guard).await;
                self.remove_failed_restart(kid, &gk).await;
                return Some(Err(RestartError::Ready("kernel channels lost during restart".into())));
            }
            gk.lifecycle.replace_proc(proc_guard.take()).await;
            gk.lifecycle.set_phase(LifecyclePhase::Running).await;
        }
        gk.core().synthesize_status("starting");
        gk.lifecycle.start_watcher().await;
        after_ready(&gk, spawned_path, state).await;
        Some(Ok(gk.clone()))
    }

    /// Terminate and respawn on the pool's teardown tracker so caller disconnect cannot abandon the transition.
    pub async fn restart_with<S, FBefore, FutBefore, FAfter, FutAfter>(
        self: &Arc<Self>, kid: &str, before_respawn: FBefore, after_ready: FAfter) -> Option<Result<Arc<ManagedKernel<E>>, RestartError>> where
        S: Send + 'static, FBefore: FnOnce(&Arc<ManagedKernel<E>>) -> FutBefore + Send + 'static, FutBefore: Future<Output = S> + Send,
        FAfter: FnOnce(&Arc<ManagedKernel<E>>, Option<String>, S) -> FutAfter + Send + 'static, FutAfter: Future<Output = ()> + Send {
        let gk = self.get(kid).await?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (this, kid) = (self.clone(), kid.to_string());
        self.teardowns.spawn(async move {
            let res = this.run_restart(gk, &kid, before_respawn, after_ready).await;
            let _ = tx.send(res);
        });
        rx.await.ok().flatten()
    }

    pub async fn restart(self: &Arc<Self>, kid: &str) -> Option<Result<Arc<ManagedKernel<E>>, RestartError>> {
        self.restart_with(kid, |_| async {}, |_, _, ()| async {}).await
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        let gks: Vec<_> = { let mut map = self.kernels.write().await; map.drain().map(|(_, gk)| { gk.lifecycle.mark_deleted(); gk }).collect() };
        for gk in gks { self.teardowns.spawn(Self::teardown(gk)); }
        self.teardowns.close();
        self.teardowns.wait().await;
    }
}

/// Standalone gateway kernel entry with standard Jupyter message delivery.
pub struct KernelMux { pub core: Arc<MuxCore>, path: std::sync::Mutex<Option<String>> }

impl KernelMux {
    pub fn new(qmax: usize, path: Option<String>, kid: String) -> Arc<Self> {
        Arc::new(Self { core: MuxCore::new(kid, qmax, "jupygate"), path: std::sync::Mutex::new(path) })
    }

    pub fn path(&self) -> Option<String> { self.path.lock().unwrap().clone() }
}

impl KernelEntry for KernelMux {
    fn core(&self) -> &Arc<MuxCore> { &self.core }
    fn spawn_context(&self) -> std::io::Result<SpawnContext> {
        Ok(SpawnContext { path: self.path(), self_url: None, caller_secrets: None, conn_prefix: "jupygate" })
    }
}

pub type GatewayKernel = ManagedKernel<KernelMux>;

impl GatewayKernel { pub async fn model(&self) -> KernelModel { self.lifecycle.model(self.entry.path()).await } }

/// All live standalone kernels, keyed solely by kernel ID. Path-based reuse belongs in `/api/sessions`.
pub struct Kernels { pool: Arc<KernelPool<KernelMux>> }

impl Kernels {
    pub fn new(root: PathBuf) -> Self { Self { pool: Arc::new(KernelPool::new(root, "jupygate")) } }

    /// Spawn a new independent kernel, wait until ready, and publish it under a fresh ID.
    pub async fn create(&self, input: LaunchInput, path: Option<String>) -> Result<KernelModel, String> {
        if self.pool.is_shutting_down() { return Err("gateway shutting down".into()); }
        let launch = task::spawn_blocking(move || Launch::resolve(input)).await.expect("resolve task panicked").map_err(|e| e.to_string())?;
        let id = Uuid::new_v4().to_string();
        let entry = KernelMux::new(self.pool.qmax(), path, id.clone());
        Ok(self.pool.create(id, launch, entry).await?.model().await)
    }

    pub async fn get(&self, kid: &str) -> Option<Arc<GatewayKernel>> { self.pool.get(kid).await }

    pub async fn model(&self, kid: &str) -> Option<KernelModel> { match self.get(kid).await { Some(gk) => Some(gk.model().await), None => None } }

    pub async fn models(&self) -> Vec<KernelModel> {
        let gks = self.pool.list().await;
        let mut out = Vec::with_capacity(gks.len());
        for gk in gks { out.push(gk.model().await); }
        out
    }

    pub async fn idle(&self, secs: f64) -> Vec<String> { self.pool.idle(secs, |_, _| false).await }

    pub async fn delete(&self, kid: &str) -> bool { self.pool.delete(kid).await.is_some() }

    pub async fn interrupt(self: &Arc<Self>, kid: &str) -> Option<Result<(), String>> {
        let gk = self.get(kid).await?;
        if gk.core().supports("interrupt requires restart") { return self.restart(kid).await.map(|r| r.map(|_| ()).map_err(RestartError::into_message)); }
        Some(gk.lifecycle.interrupt().await)
    }

    pub async fn handle_msg(self: &Arc<Self>, kid: &str, session_id: &str, msg: Msg) -> Result<(), String> {
        let gk = self.get(kid).await.ok_or("no such kernel")?;
        if msg.channel == "control" && msg.msg_type() == "interrupt_request" && gk.core().supports("interrupt requires restart") {
            let result = self.interrupt(kid).await.ok_or("no such kernel")?;
            gk.core().interrupt_reply(session_id, &msg, &result);
            result
        } else { gk.core().handle_message(session_id, msg).await }
    }

    pub async fn restart(self: &Arc<Self>, kid: &str) -> Option<Result<KernelModel, RestartError>> {
        match self.pool.restart(kid).await? { Ok(gk) => Some(Ok(gk.model().await)), Err(e) => Some(Err(e)) }
    }

    pub async fn shutdown(&self) { self.pool.shutdown().await; }
}
