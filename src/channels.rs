//! Jupyter kernel channels. Each reader yields decoded, channel-tagged messages directly from its socket.
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use zmtpmini::{ClientReader, Dealer, DealerWriter, Sub};

use crate::session::{Msg, Session};

/// The largest frame a kernel may send; a single kernel must not fill the gateway's memory.
const MAX_FRAME: usize = 1 << 30;
const CLOSED: &str = "kernel channels closed";

pub struct KernelChannels {
    session: Arc<Session>, outs: HashMap<&'static str, DealerWriter>, closed: CancellationToken,
    hb_addr: String, hb: Mutex<Option<Dealer>>,
}

/// A reserved kernel channel. Its next message is staged before the first write can suspend.
pub struct ChannelTx<'a> { channels: &'a KernelChannels, writer: zmtpmini::DealerSend<'a> }

impl ChannelTx<'_> {
    pub async fn send(mut self, msg: &kernmini::Message) -> Result<(), String> {
        let parts = self.channels.session.encode(msg).map_err(|e| e.to_string())?;
        self.channels.closed.run_until_cancelled(self.writer.send(parts)).await.ok_or(CLOSED)?.map_err(|e| e.to_string())
    }
}

/// A single kernel channel, first used to establish readiness, then handed to a mux pump.
pub struct ChannelRx { pub name: &'static str, socket: ClientReader, session: Arc<Session>, closed: CancellationToken }

impl ChannelRx {
    pub async fn recv(&mut self) -> Result<Msg, String> {
        let read = async {
            loop {
                let parts = self.socket.recv().await.map_err(|e| e.to_string())?;
                if let Ok(message) = self.session.decode(parts) { return Ok(Msg { message, channel: self.name.into() }); }
            }
        };
        self.closed.run_until_cancelled(read).await.ok_or(CLOSED)?
    }

    async fn recv_for(&mut self, limit: Duration) -> Result<Option<Msg>, String> { time::timeout(limit, self.recv()).await.ok().transpose() }
}

// The kernel binds each endpoint as it boots. Retry transient connects, including a TCP handshake that stalls.
async fn connect_retry<T, F: Future<Output = zmtpmini::Result<T>>>(deadline: Instant, open: impl Fn() -> F) -> Result<T, String> {
    loop {
        let result = time::timeout(Duration::from_secs(5), open()).await.map_err(|e| e.to_string()).and_then(|r| r.map_err(|e| e.to_string()));
        if result.is_ok() || Instant::now() >= deadline { return result; }
        time::sleep(Duration::from_millis(100)).await;
    }
}

impl KernelChannels {
    /// Connect the four channels, subscribe to IOPub, and wait for the kernel's info reply and live output subscription.
    pub async fn connect(info: &kernmini::ConnectionInfo, username: &str, timeout: Duration) -> Result<(Arc<Self>, Vec<ChannelRx>, Value), String> {
        let ready = async {
            let session = Arc::new(Session::new(info.key.as_bytes(), username));
            let closed = CancellationToken::new();
            let addr = |port| info.address(port).map_err(|e| e.to_string());
            let deadline = Instant::now() + Duration::from_secs(30);
            let reader = |name, socket| ChannelRx { name, socket, session: session.clone(), closed: closed.clone() };
            let dealer = async |name, port| {
                let addr = addr(port)?;
                let d = connect_retry(deadline, || Dealer::connect(&addr, Some(session.id().as_bytes()))).await?;
                let (rx, tx) = d.with_max_frame(MAX_FRAME).split();
                Ok::<_, String>((reader(name, rx), tx))
            };
            let (mut shell, shell_tx) = dealer("shell", info.shell_port).await?;
            let (control, control_tx) = dealer("control", info.control_port).await?;
            let (stdin, stdin_tx) = dealer("stdin", info.stdin_port).await?;
            let ep = addr(info.iopub_port)?;
            let mut sub = connect_retry(deadline, || Sub::connect(&ep)).await?.with_max_frame(MAX_FRAME);
            sub.subscribe(b"").await.map_err(|e| e.to_string())?;
            let mut iopub = reader("iopub", sub.into_reader());
            let outs = HashMap::from([(shell.name, shell_tx), (control.name, control_tx), (stdin.name, stdin_tx)]);
            let ch = Arc::new(Self { session, outs, closed, hb_addr: addr(info.hb_port)?, hb: Mutex::new(None) });
            let info = wait_ready(&ch, &mut shell, &mut iopub).await?;
            Ok((ch, vec![shell, control, stdin, iopub], info))
        };
        time::timeout(timeout, ready).await.map_err(|_| "kernel startup timed out".to_string())?
    }

    /// Stop pending reads and writes from this channel generation, including handles retained by a running request.
    pub fn close(&self) { self.closed.cancel(); }

    /// Wait for the channel's writer before making an execution visible to other tasks.
    pub async fn sender(&self, channel: &str) -> Result<ChannelTx<'_>, String> {
        let writer = self.outs.get(channel).ok_or_else(|| format!("no such channel {channel}"))?;
        let writer = self.closed.run_until_cancelled(writer.reserve()).await.ok_or(CLOSED)?;
        Ok(ChannelTx { channels: self, writer })
    }

    /// Sign and send the message on its own channel.
    pub async fn send(&self, msg: &Msg) -> Result<(), String> { self.sender(&msg.channel).await?.send(msg).await }

    /// One heartbeat ping; true if the kernel echoed in time. The timeout covers the whole
    /// round trip, connect included: a stopped kernel still completes TCP connects from its
    /// listen queue and then never speaks, so the greeting can hang, not just the echo.
    /// Any failure resets the socket.
    pub async fn beat(&self, timeout: Duration) -> bool {
        match time::timeout(timeout, self.beat_once()).await { Ok(true) => true, _ => { *self.hb.lock().await = None; false } }
    }

    async fn beat_once(&self) -> bool {
        let mut guard = self.hb.lock().await;
        if guard.is_none() { match Dealer::connect(&*self.hb_addr, None).await { Ok(d) => *guard = Some(d), Err(_) => return false } }
        let d = guard.as_mut().expect("just set");
        if d.send([&b""[..], b"ping"]).await.is_err() { return false; }
        d.recv().await.is_ok()
    }
}

/// Prove IOPub is live and consume the startup probes before handing the readers to the mux.
async fn wait_ready(ch: &KernelChannels, shell: &mut ChannelRx, iopub: &mut ChannelRx) -> Result<Value, String> {
    let probe = async || {
        let msg = Msg::new(&ch.session, "shell", "kernel_info_request", serde_json::json!({}));
        ch.send(&msg).await?;
        Ok::<_, String>(msg.msg_id().to_string())
    };
    let mut probes = vec![];
    let first = loop {
        probes.push(probe().await?);
        if let Some(msg) = iopub.recv_for(Duration::from_millis(500)).await? { break msg; }
    };
    if first.msg_type() == "iopub_welcome" {
        // Replies are FIFO: a final probe's reply and idle mark the end of all preceding probes on both channels.
        let id = probe().await?;
        let reply = loop {
            let msg = shell.recv().await?;
            if msg.parent_id() == Some(&id) { break msg; }
        };
        loop {
            let msg = iopub.recv().await?;
            if msg.parent_id() == Some(&id) && msg.msg_type() == "status" && msg.content["execution_state"] == "idle" { break; }
        }
        return Ok(reply.message.content);
    }
    let reply = loop {
        let msg = shell.recv().await?;
        if msg.msg_type() == "kernel_info_reply" { break msg; }
    };
    let mut outstanding = probes.len() - 1;
    while outstanding > 0 {
        let Some(msg) = shell.recv_for(Duration::from_secs(1)).await? else { break; };
        if msg.parent_id().is_some_and(|id| probes.iter().any(|p| p == id)) { outstanding -= 1; }
    }
    while iopub.recv_for(Duration::from_millis(200)).await?.is_some() {}
    Ok(reply.message.content)
}
