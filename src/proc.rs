//! Kernel processes: connection files, spawn from an argv template, watch, terminate.

use kernmini::{ConnectionFile, ConnectionInfo};
use nix::sys::signal::Signal;
use std::collections::HashMap;
use std::fs;
use std::process::{self, Child, Command};
use std::time::{Duration, Instant};

/// Parameters for spawning a kernel process.
pub struct SpawnOpts<'a> {
    pub argv: &'a [String],
    pub appendenv: Option<&'a HashMap<String, String>>,
    /// Gateway-injected variables applied last: `Some(v)` sets, `None` removes from inherited env.
    pub extra_env: &'a [(&'a str, Option<&'a str>)],
    pub cwd: Option<&'a str>,
    pub conn_prefix: &'a str,
}

/// One kernel process: spawned from an argv template with a fresh connection file.
pub struct KernelProc { child: Child, conn_file: ConnectionFile, terminated: bool }

impl KernelProc {
    /// Write a connection file with 5 distinct local ports and a fresh key, then spawn.
    pub fn spawn(opts: &SpawnOpts<'_>) -> std::io::Result<KernelProc> {
        let conn_file = ConnectionFile::create_local(opts.conn_prefix).map_err(std::io::Error::other)?;
        let conn_path = conn_file.path().to_string_lossy().into_owned();
        let cmd: Vec<String> = opts.argv.iter().map(|a| a.replace("{connection_file}", &conn_path)).collect();
        let mut c = Command::new(&cmd[0]);
        c.args(&cmd[1..]);
        if let Some(e) = opts.appendenv { for (k, v) in e { c.env(k, v); } }
        if let Some(d) = opts.cwd { c.current_dir(d); }
        c.env("JPY_PARENT_PID", process::id().to_string());
        for &(k, v) in opts.extra_env { match v { Some(val) => { c.env(k, val); } None => { c.env_remove(k); } } }
        std::os::unix::process::CommandExt::process_group(&mut c, 0);
        c.stdin(process::Stdio::null());
        let child = c.spawn()?;
        Ok(KernelProc { child, conn_file, terminated: false })
    }

    pub fn info(&self) -> &ConnectionInfo { self.conn_file.info() }

    pub fn pid(&self) -> u32 { self.child.id() }

    pub fn alive(&mut self) -> bool { matches!(self.child.try_wait(), Ok(None)) }

    fn signal(&self, sig: Signal) { let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(self.child.id() as i32), sig); }

    fn signal_group(&self, sig: Signal) { let _ = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(self.child.id() as i32), sig); }

    fn group_alive(&self) -> bool { nix::sys::signal::killpg(nix::unistd::Pid::from_raw(self.child.id() as i32), None).is_ok() }

    pub fn interrupt(&self) { self.signal(Signal::SIGINT) }

    /// SIGTERM then SIGKILL the kernel's process group; reaps the leader and removes the connection file. Blocking.
    pub fn terminate(&mut self) {
        if !self.terminated {
            self.terminated = true;
            if self.alive() || self.group_alive() {
                self.signal_group(Signal::SIGTERM);
                let end = Instant::now() + Duration::from_secs(5);
                while (self.alive() || self.group_alive()) && Instant::now() < end { std::thread::sleep(Duration::from_millis(50)); }
                if self.alive() || self.group_alive() {
                    self.signal_group(Signal::SIGKILL);
                    if self.alive() {
                        let _ = self.child.kill();
                        let _ = self.child.wait();
                    }
                }
            }
        }
        let _ = fs::remove_file(self.conn_file.path());
    }
}

/// Ensures a spawned process is terminated even if an async creation future is cancelled.
pub struct ReapOnDrop(pub Option<KernelProc>);

impl ReapOnDrop {
    pub fn new(proc_: KernelProc) -> Self { Self(Some(proc_)) }
    pub fn as_ref(&self) -> Option<&KernelProc> { self.0.as_ref() }
    pub fn take(&mut self) -> KernelProc { self.0.take().expect("proc still held") }
}

impl Drop for ReapOnDrop { fn drop(&mut self) { if let Some(mut proc_) = self.0.take() { std::thread::spawn(move || proc_.terminate()); } } }
