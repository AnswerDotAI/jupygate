//! Stock Jupyter `/api/sessions` registry: maps notebook/file paths to kernels.

use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::AppState;
use crate::api::{Body, err};
use crate::gateway::{KernelModel, LaunchInput};
use crate::server::{StartOutcome, StartTable};

#[derive(Clone, Serialize, JsonSchema)]
pub struct SessionModel { pub id: String, pub path: String, pub name: String, #[serde(rename = "type")] pub type_: String, pub kernel: KernelModel }

#[derive(Clone)]
struct SessionRecord { id: String, path: String, name: String, type_: String, kernel_id: String }

#[derive(Default)]
pub struct Sessions { by_id: RwLock<HashMap<String, SessionRecord>>, starting: StartTable<Result<SessionModel, (StatusCode, String)>> }

impl Sessions {
    pub fn new() -> Self { Self::default() }

    async fn resolve(&self, app: &AppState, rec: &SessionRecord) -> Option<SessionModel> {
        let kernel = app.kernel_model(&rec.kernel_id).await?;
        Some(SessionModel { id: rec.id.clone(), path: rec.path.clone(), name: rec.name.clone(), type_: rec.type_.clone(), kernel })
    }

    pub async fn list(&self, app: &AppState) -> Vec<SessionModel> {
        let recs: Vec<_> = self.by_id.read().await.values().cloned().collect();
        let mut out = Vec::with_capacity(recs.len());
        let mut stale = Vec::new();
        for rec in recs { match self.resolve(app, &rec).await { Some(m) => out.push(m), None => stale.push(rec.id) } }
        if !stale.is_empty() { let mut map = self.by_id.write().await; for id in stale { map.remove(&id); } }
        out
    }

    pub async fn get(&self, app: &AppState, sid: &str) -> Option<SessionModel> {
        let rec = self.by_id.read().await.get(sid).cloned()?;
        match self.resolve(app, &rec).await { Some(m) => Some(m), None => { self.by_id.write().await.remove(sid); None } }
    }

    pub async fn by_path(&self, app: &AppState, path: &str) -> Option<SessionModel> {
        let rec = self.by_id.read().await.values().find(|r| r.path == path).cloned()?;
        match self.resolve(app, &rec).await { Some(m) => Some(m), None => { self.by_id.write().await.remove(&rec.id); None } }
    }

    pub async fn remove_by_kernel(&self, kid: &str) { self.by_id.write().await.retain(|_, r| r.kernel_id != kid); }
}

#[derive(Deserialize, JsonSchema, Default)]
struct KernelSpecRef { id: Option<String>, name: Option<String> }

#[derive(Deserialize, JsonSchema)]
struct LegacyNotebook { path: Option<String>, name: Option<String> }

#[derive(Deserialize, JsonSchema)]
struct CreateSessionBody {
    path: Option<String>, name: Option<String>, #[serde(rename = "type")] type_: Option<String>, kernel: Option<KernelSpecRef>, notebook: Option<LegacyNotebook>
}

#[derive(Deserialize, JsonSchema)]
struct PatchSessionBody {
    path: Option<String>, name: Option<String>, #[serde(rename = "type")] type_: Option<String>, kernel: Option<KernelSpecRef>, notebook: Option<LegacyNotebook>
}

async fn start_kernel_for(app: &AppState, kernel_name: Option<String>, path: &str) -> Result<KernelModel, (StatusCode, String)> {
    let input = LaunchInput { name: kernel_name, ..LaunchInput::default() };
    app.kernels.create(input, Some(path.to_string())).await.map_err(|e| {
        let code = if e.starts_with("unknown kernel") { StatusCode::BAD_REQUEST } else { StatusCode::INTERNAL_SERVER_ERROR };
        (code, format!("kernel failed to start: {e}"))
    })
}

async fn list_sessions(State(app): State<AppState>) -> Json<Vec<SessionModel>> { Json(app.sessions.list(&app).await) }

async fn create_session(State(app): State<AppState>, Body(b): Body<CreateSessionBody>) -> Response {
    let (path, type_) = if let Some(nb) = b.notebook { let p = nb.name.or(nb.path).or(b.path); (p, Some("notebook".to_string())) } else { (b.path, b.type_) };
    let Some(path) = path else { return err(StatusCode::BAD_REQUEST, "Missing field in JSON data: path"); };
    let path = path.trim_matches('/').to_string();
    if let Err(e) = crate::paths::resolve(&app.root, &path) { return err(StatusCode::BAD_REQUEST, &e); }
    let type_ = type_.unwrap_or_else(|| "notebook".into());
    let name = b.name.unwrap_or_else(|| std::path::Path::new(&path).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default());
    let kref = b.kernel.unwrap_or_default();

    let outcome = app.sessions.starting.get_or_start(&path, || app.sessions.by_path(&app, &path), || async {
        let kernel = if let Some(kid) = kref.id.as_deref().filter(|s| !s.is_empty()) {
            match app.kernel_model(kid).await { Some(m) => m, None => start_kernel_for(&app, kref.name.clone(), &path).await? }
        } else { start_kernel_for(&app, kref.name.clone(), &path).await? };
        let id = Uuid::new_v4().to_string();
        let rec = SessionRecord { id: id.clone(), path: path.clone(), name: name.clone(), type_: type_.clone(), kernel_id: kernel.id.clone() };
        app.sessions.by_id.write().await.insert(id.clone(), rec);
        Ok(SessionModel { id, path: path.clone(), name: name.clone(), type_: type_.clone(), kernel })
    }).await;

    let model = match outcome { Ok(StartOutcome::Created(m) | StartOutcome::Reused(m)) => m, Err((code, msg)) => return err(code, &msg) };
    let mut resp = (StatusCode::CREATED, Json(&model)).into_response();
    if let Ok(loc) = HeaderValue::from_str(&format!("/api/sessions/{}", model.id)) { resp.headers_mut().insert(header::LOCATION, loc); }
    resp
}

async fn get_session(State(app): State<AppState>, Path(sid): Path<String>) -> Response {
    match app.sessions.get(&app, &sid).await { Some(m) => Json(m).into_response(), None => err(StatusCode::NOT_FOUND, "no such session") }
}

async fn patch_session(State(app): State<AppState>, Path(sid): Path<String>, Body(b): Body<PatchSessionBody>) -> Response {
    let Some(before) = app.sessions.by_id.read().await.get(&sid).cloned() else { return err(StatusCode::NOT_FOUND, "no such session"); };
    let mut new_path = b.notebook.and_then(|nb| nb.path).or(b.path).map(|p| p.trim_matches('/').to_string());
    if new_path.is_none() { new_path = Some(before.path.clone()); }
    let target_path = new_path.unwrap();
    if let Err(e) = crate::paths::resolve(&app.root, &target_path) { return err(StatusCode::BAD_REQUEST, &e); }
    let new_name = b.name.unwrap_or_else(|| before.name.clone());
    let new_type = b.type_.unwrap_or_else(|| before.type_.clone());

    let mut old_kernel_to_stop = None;
    let mut spawned_new = false;
    let new_kernel_id = if let Some(kref) = b.kernel {
        if let Some(kid) = kref.id.as_deref().filter(|s| !s.is_empty()) {
            if app.kernels.get(kid).await.is_none() { return err(StatusCode::BAD_REQUEST, &format!("No such kernel: {kid}")); }
            if kid != before.kernel_id { old_kernel_to_stop = Some(before.kernel_id.clone()); }
            kid.to_string()
        } else if let Some(kname) = kref.name.filter(|s| !s.is_empty()) {
            match start_kernel_for(&app, Some(kname), &target_path).await {
                Ok(m) => {
                    old_kernel_to_stop = Some(before.kernel_id.clone());
                    spawned_new = true;
                    m.id
                }
                Err((code, msg)) => return err(code, &msg),
            }
        } else { before.kernel_id.clone() }
    } else { before.kernel_id.clone() };

    let stop_old = {
        let mut map = app.sessions.by_id.write().await;
        if map.iter().any(|(id, r)| id != &sid && r.path == target_path) {
            drop(map);
            if spawned_new { app.delete_kernel(&new_kernel_id).await; }
            return err(StatusCode::CONFLICT, &format!("session already exists for {target_path:?}"));
        }
        let Some(rec) = map.get_mut(&sid) else {
            drop(map);
            if spawned_new { app.delete_kernel(&new_kernel_id).await; }
            return err(StatusCode::NOT_FOUND, "no such session");
        };
        rec.path = target_path;
        rec.name = new_name;
        rec.type_ = new_type;
        rec.kernel_id = new_kernel_id;
        old_kernel_to_stop.filter(|old_kid| !map.values().any(|r| r.kernel_id == *old_kid))
    };

    if let Some(old_kid) = stop_old {
        app.kernels.delete(&old_kid).await;
        app.mcp.clear_kernel(&old_kid).await;
    }

    match app.sessions.get(&app, &sid).await { Some(m) => Json(m).into_response(), None => err(StatusCode::NOT_FOUND, "no such session") }
}

async fn delete_session(State(app): State<AppState>, Path(sid): Path<String>) -> Response {
    let Some(rec) = app.sessions.by_id.write().await.remove(&sid) else { return err(StatusCode::NOT_FOUND, "no such session"); };
    app.delete_kernel(&rec.kernel_id).await;
    StatusCode::NO_CONTENT.into_response()
}

crate::routes! {
    pub get "/api/sessions" => list_sessions ret(Vec<SessionModel>),
    pub post "/api/sessions" => create_session body(CreateSessionBody) ret(SessionModel),
    pub get "/api/sessions/{sid}" => get_session ret(SessionModel),
    pub patch "/api/sessions/{sid}" => patch_session body(PatchSessionBody) ret(SessionModel),
    pub delete "/api/sessions/{sid}" => delete_session,
}
