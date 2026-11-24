//! Gateway channel envelope and websocket framing. Jupyter wire encoding is kernmini's.

use bytes::Bytes;
pub use kernmini::Session;
use serde_json::{Value, json};

/// The shared Jupyter message plus its gateway channel tag.
#[derive(Clone, Debug)]
pub struct Msg { pub message: kernmini::Message, pub channel: String }

impl Msg {
    pub fn new(session: &Session, channel: &str, msg_type: &str, content: Value) -> Self {
        Self { message: session.message(msg_type, content, None), channel: channel.into() }
    }
    pub fn parent_id(&self) -> Option<&str> { self.parent_header.get("msg_id").and_then(Value::as_str) }
    pub fn parent_session(&self) -> Option<&str> { self.parent_header.get("session").and_then(Value::as_str) }
    pub fn is_execute(&self) -> bool { self.channel == "shell" && self.msg_type() == "execute_request" }
}

impl std::ops::Deref for Msg {
    type Target = kernmini::Message;
    fn deref(&self) -> &Self::Target { &self.message }
}
impl std::ops::DerefMut for Msg { fn deref_mut(&mut self) -> &mut Self::Target { &mut self.message } }

/// An execution's iopub messages as outputs for `aihist::output::render`: each message's type and content
pub fn iopub(msgs: &[Msg]) -> impl Iterator<Item = (&str, &Value)> { msgs.iter().map(|m| (m.msg_type(), &m.content)) }

/// Encode for the websocket: JSON text frame, or the legacy binary frame if buffers are present.
pub(crate) fn to_frame(msg: &Msg) -> axum::extract::ws::Message {
    use axum::extract::ws::Message;
    let mut v = json!({
        "header": msg.header, "parent_header": msg.parent_header, "metadata": msg.metadata,
        "content": msg.content, "channel": msg.channel,
    });
    if msg.buffers.is_empty() { return Message::Text(v.to_string().into()); }
    v["buffers"] = json!([]);
    let body = v.to_string().into_bytes();
    let n = 1 + msg.buffers.len();
    let mut out = Vec::with_capacity(4 * (n + 1) + body.len() + msg.buffers.iter().map(|b| b.len()).sum::<usize>());
    out.extend((n as u32).to_be_bytes());
    let mut off = 4 * (n + 1) as u32;
    for len in std::iter::once(body.len()).chain(msg.buffers.iter().map(|b| b.len())).take(n) {
        out.extend(off.to_be_bytes());
        off += len as u32;
    }
    out.extend(&body);
    for b in &msg.buffers { out.extend(b.as_ref()) }
    axum::extract::ws::Message::Binary(out.into())
}

/// Decode a websocket frame (text or binary) into a `Msg`.
pub(crate) fn from_frame(m: &axum::extract::ws::Message) -> Result<Msg, String> {
    use axum::extract::ws::Message;
    match m {
        Message::Text(t) => from_parts(t.as_bytes(), vec![]),
        Message::Binary(b) => {
            if b.len() < 4 { return Err("binary frame too short".into()); }
            let n = u32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
            if b.len() < 4 * (n + 1) { return Err("binary frame truncated".into()); }
            let mut offs: Vec<usize> = (0..n).map(|i| u32::from_be_bytes(b[4 * (i + 1)..4 * (i + 2)].try_into().unwrap()) as usize).collect();
            offs.push(b.len());
            if n == 0 || offs.windows(2).any(|w| w[0] > w[1]) || offs[0] < 4 * (n + 1) { return Err("bad offsets".into()); }
            from_parts(&b[offs[0]..offs[1]], offs[1..].windows(2).map(|w| b.slice(w[0]..w[1])).collect())
        }
        _ => Err("unexpected frame type".into()),
    }
}

fn from_parts(body: &[u8], buffers: Vec<Bytes>) -> Result<Msg, String> {
    let mut v: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let channel = v["channel"].as_str().unwrap_or("shell").to_string();
    let take = |v: &mut Value, k: &str| v.get_mut(k).map(Value::take).unwrap_or(json!({}));
    let header: serde_json::Map<String, Value> = serde_json::from_value(take(&mut v, "header")).map_err(|e| e.to_string())?;
    if header.get("msg_type").and_then(Value::as_str).is_none_or(str::is_empty) { return Err("missing header.msg_type".into()); }
    Ok(Msg { message: kernmini::Message { identities: vec![], header,
            parent_header: serde_json::from_value(take(&mut v, "parent_header")).map_err(|e| e.to_string())?,
            metadata: serde_json::from_value(take(&mut v, "metadata")).map_err(|e| e.to_string())?, content: take(&mut v, "content"), buffers }, channel })
}
