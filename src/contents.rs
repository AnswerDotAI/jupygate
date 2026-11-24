//! Pure-disk Jupyter `/api/contents` operations and reusable file helpers.

use axum::body::Bytes;
use axum::extract::{Path as AxPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use base64::Engine;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, io};

use crate::AppState;
use crate::api::{Body, Qry, err};
use crate::paths;

pub fn b64() -> base64::engine::GeneralPurpose { base64::engine::general_purpose::STANDARD }

/// Encode `bytes` as `text` when valid UTF-8, else `base64`.
pub fn encode_bytes(bytes: Vec<u8>) -> (String, &'static str) {
    match String::from_utf8(bytes) { Ok(s) => (s, "text"), Err(e) => (b64().encode(e.into_bytes()), "base64") }
}

/// Decode contents payload according to `format` (`"base64"` or `"text"`/default).
pub fn decode_bytes(content: &str, format: Option<&str>) -> Result<Vec<u8>, String> {
    match format {
        Some("base64") => b64().decode(content).map_err(|_| "invalid base64 content".into()), Some("text") | None => Ok(content.as_bytes().to_vec()),
        Some(other) => Err(format!("unsupported format {other:?}"))
    }
}

/// Write via a hidden sibling temp file, capture its metadata from the open file handle before
/// rename, then atomically replace `path`, creating missing parents. Removes the temp file on failure.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<fs::Metadata> {
    use std::io::Write;
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let tmp = path.with_file_name(format!(".{fname}.{}.tmp", uuid::Uuid::new_v4().simple()));
    let res = (|| -> io::Result<fs::Metadata> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.flush()?;
        let md = f.metadata()?;
        drop(f);
        fs::rename(&tmp, path)?;
        Ok(md)
    })();
    if res.is_err() { let _ = fs::remove_file(&tmp); }
    res
}

/// `abs` itself if free, else the first free `stem_n.ext` (n = 1, 2, ...).
pub fn unq_path(abs: &Path, mut occupied: impl FnMut(&Path) -> bool) -> PathBuf {
    if !occupied(abs) { return abs.to_path_buf(); }
    let stem = abs.file_stem().unwrap_or_default().to_string_lossy();
    let ext = abs.extension().map(|e| format!(".{}", e.to_string_lossy())).unwrap_or_default();
    let parent = abs.parent().unwrap_or(Path::new(""));
    (1..).map(|n| parent.join(format!("{stem}_{n}{ext}"))).find(|p| !occupied(p)).unwrap()
}

/// Empty nbformat 4.5 notebook JSON value.
pub fn empty_notebook() -> Value { json!({"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}) }

/// Format a `SystemTime` as an ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SS.ffffffZ`).
pub fn rfc3339(t: SystemTime) -> String {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let micros = dur.subsec_micros();
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil_from_days algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + i64::from(mo <= 2);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{micros:06}Z")
}

pub fn mtime_secs(md: &fs::Metadata) -> f64 { md.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map(|d| d.as_secs_f64()).unwrap_or(0.0) }

fn guess_mimetype(path: &Path, format: Option<&str>) -> String {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "py" => "text/x-python", "json" => "application/json", "md" => "text/markdown", "html" | "htm" => "text/html", "css" => "text/css",
        "js" | "mjs" => "text/javascript", "txt" | "csv" | "toml" | "yaml" | "yml" | "rs" => "text/plain", "png" => "image/png", "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif", "svg" => "image/svg+xml", "pdf" => "application/pdf",
        _ => { if format == Some("base64") { "application/octet-stream" } else { "text/plain" } }
    }.into()
}

/// Standard Jupyter contents model.
#[derive(Clone, Serialize, JsonSchema)]
pub struct ContentsModel {
    pub name: String, pub path: String, #[serde(rename = "type")] pub type_: String, pub writable: bool, pub created: String, pub last_modified: String,
    pub size: Option<u64>, pub mimetype: Option<String>, pub content: Option<Value>, pub format: Option<String>, pub hash: Option<String>,
    pub hash_algorithm: Option<String>
}

/// Cheap stat-only Jupyter contents model (`content` and `format` are `None`).
pub fn stat_model(root: &Path, abs: &Path) -> Option<ContentsModel> {
    let md = fs::metadata(abs).ok()?;
    let rel = abs.strip_prefix(root).ok()?.to_string_lossy().into_owned();
    let name = if rel.is_empty() { String::new() } else { abs.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default() };
    let typ = if md.is_dir() { "directory" } else if name.ends_with(".ipynb") { "notebook" } else { "file" };
    let modified = md.modified().unwrap_or(UNIX_EPOCH);
    let created = md.created().unwrap_or(modified);
    let mimetype = if typ == "file" { Some(guess_mimetype(abs, None)) } else { None };
    Some(ContentsModel { name, path: rel, type_: typ.into(), writable: !md.permissions().readonly(), created: rfc3339(created),
        last_modified: rfc3339(modified), size: if md.is_dir() { None } else { Some(md.len()) }, mimetype, content: None, format: None, hash: None,
        hash_algorithm: None })
}

fn clean_rel(rel: &str) -> String { rel.trim_matches('/').to_string() }

/// Validate `rel` stays inside canonical `root` (rejecting `..` and symlink escapes).
/// Returns `(logical_abs_under_root, canonical_real_path)`.
fn checked_path(root: &Path, rel: &str) -> Result<(PathBuf, PathBuf), (StatusCode, String)> {
    let abs = paths::resolve(root, rel).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let canon_root = root.canonicalize().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let real = paths::realpath(&canon_root, rel).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    Ok((abs, real))
}

fn entry_exists(p: &Path) -> bool { p.symlink_metadata().is_ok() }

/// Read a Jupyter contents model from disk under `root`.
pub fn get_model(root: &Path, rel: &str, content: bool, req_type: Option<&str>, req_format: Option<&str>) -> Result<ContentsModel, (StatusCode, String)> {
    let rel = clean_rel(rel);
    let (abs, _) = checked_path(root, &rel)?;
    let mut m = stat_model(root, &abs).ok_or_else(|| (StatusCode::NOT_FOUND, format!("file or directory does not exist: {rel:?}")))?;
    if let Some(t) = req_type {
        match (t, m.type_.as_str()) {
            ("directory", "directory") | ("notebook", "notebook") | ("file", "file") => {} ("file", "notebook") => { m.type_ = "file".into(); }
            ("directory" | "notebook" | "file", actual) => return Err((StatusCode::BAD_REQUEST, format!("{rel:?} is a {actual}, not a {t}"))), _ => {}
        }
    }
    if !content { return Ok(m); }
    match m.type_.as_str() {
        "directory" => {
            let mut entries: Vec<ContentsModel> = fs::read_dir(&abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?.flatten()
                .filter_map(|e| stat_model(root, &e.path())).collect();
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            m.content = Some(serde_json::to_value(entries).expect("models serialize"));
            m.format = Some("json".into());
            m.mimetype = None;
        }
        "notebook" => {
            let bytes = fs::read(&abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            let nb: Value = serde_json::from_slice(&bytes).map_err(|e| (StatusCode::BAD_REQUEST, format!("unreadable notebook: {e}")))?;
            m.content = Some(nb);
            m.format = Some("json".into());
            m.mimetype = None;
        }
        _ => {
            let bytes = fs::read(&abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            let (body, fmt) = match req_format {
                Some("base64") => (b64().encode(bytes), "base64"),
                Some("text") => (String::from_utf8(bytes).map_err(|_| (StatusCode::BAD_REQUEST, "file is not UTF-8".into()))?, "text"),
                None => encode_bytes(bytes), Some(other) => return Err((StatusCode::BAD_REQUEST, format!("invalid format {other:?}")))
            };
            m.mimetype = Some(guess_mimetype(&abs, Some(fmt)));
            m.content = Some(Value::String(body));
            m.format = Some(fmt.into());
        }
    }
    Ok(m)
}

#[derive(Deserialize, JsonSchema)]
struct GetQ { #[serde(rename = "type")] type_: Option<String>, format: Option<String>, content: Option<u8> }

#[derive(Deserialize, JsonSchema, Default)]
pub struct PutBody {
    pub content: Option<Value>, pub format: Option<String>, #[serde(rename = "type")] pub type_: Option<String>, pub copy_from: Option<String>
}

#[derive(Deserialize, JsonSchema, Default)]
pub struct PostBody { pub copy_from: Option<String>, pub ext: Option<String>, #[serde(rename = "type")] pub type_: Option<String> }

#[derive(Deserialize, JsonSchema)]
pub struct PatchBody { pub path: String }

/// Save a file, notebook, or directory at `rel` under `root`. Returns `(model, created)`.
pub fn save_model(root: &Path, rel: &str, body: PutBody) -> Result<(ContentsModel, bool), (StatusCode, String)> {
    if body.copy_from.is_some() { return Err((StatusCode::BAD_REQUEST, "Cannot copy with PUT, only POST".into())); }
    let rel = clean_rel(rel);
    let (abs, _) = checked_path(root, &rel)?;
    let existed = entry_exists(&abs);
    let typ = match body.type_.as_deref() {
        Some("") | None => { if rel.ends_with(".ipynb") { "notebook" } else { "file" } } Some(t @ ("directory" | "file" | "notebook")) => t,
        Some(other) => return Err((StatusCode::BAD_REQUEST, format!("invalid type {other:?}")))
    };
    match typ {
        "directory" => {
            if abs.is_file() { return Err((StatusCode::CONFLICT, format!("{rel:?} is an existing file"))); }
            fs::create_dir_all(&abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        "notebook" => {
            if abs.is_dir() { return Err((StatusCode::CONFLICT, format!("{rel:?} is an existing directory"))); }
            let bytes = match body.content {
                Some(Value::String(s)) => decode_bytes(&s, body.format.as_deref()).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
                Some(v @ Value::Object(_)) => {
                    let mut s = serde_json::to_string_pretty(&v).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
                    s.push('\n');
                    s.into_bytes()
                }
                None => {
                    let mut s = serde_json::to_string_pretty(&empty_notebook()).expect("empty notebook serializes");
                    s.push('\n');
                    s.into_bytes()
                }
                Some(_) => return Err((StatusCode::BAD_REQUEST, "notebook content must be a JSON object or string".into())),
            };
            atomic_write(&abs, &bytes).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        _ => {
            if abs.is_dir() { return Err((StatusCode::CONFLICT, format!("{rel:?} is an existing directory"))); }
            let bytes = match body.content {
                Some(Value::String(s)) => decode_bytes(&s, body.format.as_deref()).map_err(|e| (StatusCode::BAD_REQUEST, e))?, None => Vec::new(),
                Some(_) => return Err((StatusCode::BAD_REQUEST, "file content must be a string".into()))
            };
            atomic_write(&abs, &bytes).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
    }
    let m = stat_model(root, &abs).ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "saved file disappeared".into()))?;
    Ok((m, !existed))
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        let target = dst.join(entry.file_name());
        if ft.is_dir() { copy_dir_recursive(&entry.path(), &target)?; } else if ft.is_file() { fs::copy(entry.path(), &target)?; }
    }
    Ok(())
}

fn next_copy_path(dir: &Path, src_name: &str) -> PathBuf {
    let p = dir.join(src_name);
    if !entry_exists(&p) { return p; }
    let (stem, ext) = if let Some(base) = src_name.strip_suffix(".ipynb") { (base, ".ipynb") }
    else if let Some((s, _)) = src_name.rsplit_once('.') { (s, &src_name[s.len()..]) }
    else { (src_name, "") };
    (1..).map(|n| dir.join(format!("{stem}-Copy{n}{ext}"))).find(|cand| !entry_exists(cand)).unwrap()
}

fn next_untitled_path(dir: &Path, base: &str, ext: &str, sep: &str) -> PathBuf {
    let first = dir.join(format!("{base}{ext}"));
    if !entry_exists(&first) { return first; }
    (1..).map(|n| dir.join(format!("{base}{sep}{n}{ext}"))).find(|cand| !entry_exists(cand)).unwrap()
}

fn validate_ext(ext: &str) -> Result<(), (StatusCode, String)> {
    if ext.contains('/') || ext.contains('\\') || ext.contains('\0') || ext.contains("..") {
        return Err((StatusCode::BAD_REQUEST, format!("invalid extension: {ext:?}")));
    }
    Ok(())
}

fn check_dest(root: &Path, dst_abs: &Path) -> Result<(), (StatusCode, String)> {
    let rel = dst_abs.strip_prefix(root).map_err(|_| (StatusCode::BAD_REQUEST, "destination escapes served root".into()))?.to_string_lossy();
    checked_path(root, &rel)?;
    Ok(())
}

/// Handle stock Jupyter `POST /api/contents/{dir}`: copy `copy_from` or create untitled file/notebook/dir.
pub fn post_in_dir(root: &Path, dir_rel: &str, body: PostBody) -> Result<ContentsModel, (StatusCode, String)> {
    let dir_rel = clean_rel(dir_rel);
    let (dir_abs, dir_real) = checked_path(root, &dir_rel)?;
    if dir_abs.is_file() { return Err((StatusCode::BAD_REQUEST, "Cannot POST to files, use PUT instead.".into())); }
    if !dir_abs.is_dir() { return Err((StatusCode::NOT_FOUND, format!("No such directory: {dir_rel:?}"))); }
    if let Some(copy_from) = body.copy_from {
        let src_rel = clean_rel(&copy_from);
        let (src_abs, src_real) = checked_path(root, &src_rel)?;
        if !entry_exists(&src_abs) { return Err((StatusCode::NOT_FOUND, format!("file or directory does not exist: {src_rel:?}"))); }
        let src_name = src_abs.file_name().ok_or_else(|| (StatusCode::BAD_REQUEST, "cannot copy root directory".into()))?.to_string_lossy();
        let dst_abs = next_copy_path(&dir_abs, &src_name);
        check_dest(root, &dst_abs)?;
        if src_abs.is_dir() {
            if dir_real.starts_with(&src_real) { return Err((StatusCode::BAD_REQUEST, "cannot copy a directory into itself".into())); }
            copy_dir_recursive(&src_abs, &dst_abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        } else { fs::copy(&src_abs, &dst_abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?; }
        return stat_model(root, &dst_abs).ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "copied file disappeared".into()));
    }
    let ext = body.ext.unwrap_or_default();
    validate_ext(&ext)?;
    let typ = match body.type_.as_deref() {
        Some("") | None => { if ext == ".ipynb" { "notebook" } else { "file" } } Some(t @ ("directory" | "file" | "notebook")) => t,
        Some(other) => return Err((StatusCode::BAD_REQUEST, format!("Unexpected model type: {other:?}")))
    };
    let target = match typ {
        "directory" => {
            let p = next_untitled_path(&dir_abs, "Untitled Folder", "", " ");
            check_dest(root, &p)?;
            fs::create_dir(&p).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            p
        }
        "notebook" => {
            let p = next_untitled_path(&dir_abs, "Untitled", ".ipynb", "");
            check_dest(root, &p)?;
            let mut s = serde_json::to_string_pretty(&empty_notebook()).expect("empty notebook serializes");
            s.push('\n');
            atomic_write(&p, s.as_bytes()).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            p
        }
        _ => {
            let p = next_untitled_path(&dir_abs, "untitled", &ext, "");
            check_dest(root, &p)?;
            atomic_write(&p, b"").map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            p
        }
    };
    stat_model(root, &target).ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "created file disappeared".into()))
}

/// Rename a file or directory on disk under `root` without overwriting an existing destination.
pub fn rename_disk(root: &Path, old_rel: &str, new_rel: &str) -> Result<ContentsModel, (StatusCode, String)> {
    let old_rel = clean_rel(old_rel);
    let new_rel = clean_rel(new_rel);
    let (old_abs, _) = checked_path(root, &old_rel)?;
    let (new_abs, _) = checked_path(root, &new_rel)?;
    if !entry_exists(&old_abs) { return Err((StatusCode::NOT_FOUND, format!("file or directory does not exist: {old_rel:?}"))); }
    if old_rel != new_rel {
        if entry_exists(&new_abs) { return Err((StatusCode::CONFLICT, format!("File already exists: {new_rel}"))); }
        if let Some(parent) = new_abs.parent() { fs::create_dir_all(parent).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?; }
        fs::rename(&old_abs, &new_abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    stat_model(root, &new_abs).ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "renamed file disappeared".into()))
}

/// Delete a file, symlink, or empty directory on disk under `root`.
/// Validates the parent directory stays inside canonical `root` so symlink entries themselves can be deleted safely.
pub fn delete_disk(root: &Path, rel: &str) -> Result<(), (StatusCode, String)> {
    let rel = clean_rel(rel);
    let abs = paths::resolve(root, &rel).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if abs == root { return Err((StatusCode::BAD_REQUEST, "cannot delete the served root".into())); }
    let parent_rel = Path::new(&rel).parent().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
    checked_path(root, &parent_rel)?;
    let md = fs::symlink_metadata(&abs).map_err(|_| (StatusCode::NOT_FOUND, format!("file or directory does not exist: {rel:?}")))?;
    if md.is_dir() {
        if fs::read_dir(&abs).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?.next().is_some() {
            return Err((StatusCode::BAD_REQUEST, format!("Directory {rel:?} not empty")));
        }
        fs::remove_dir(&abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    } else { fs::remove_file(&abs).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?; }
    Ok(())
}

async fn get_root(State(app): State<AppState>, Qry(q): Qry<GetQ>) -> Response { get_path(State(app), AxPath(String::new()), Qry(q)).await }

async fn get_path(State(app): State<AppState>, AxPath(rel): AxPath<String>, Qry(q): Qry<GetQ>) -> Response {
    let content = match q.content {
        None | Some(1) => true, Some(0) => false, Some(other) => return err(StatusCode::BAD_REQUEST, &format!("Content {other} is invalid"))
    };
    if let Some(fmt) = q.format.as_deref() && !matches!(fmt, "text" | "base64") { return err(StatusCode::BAD_REQUEST, &format!("Format {fmt:?} is invalid")); }
    match get_model(&app.root, &rel, content, q.type_.as_deref(), q.format.as_deref()) { Ok(m) => Json(m).into_response(), Err((code, msg)) => err(code, &msg) }
}

async fn put_path(State(app): State<AppState>, AxPath(rel): AxPath<String>, Body(body): Body<PutBody>) -> Response {
    match save_model(&app.root, &rel, body) {
        Ok((m, created)) => (if created { StatusCode::CREATED } else { StatusCode::OK }, Json(m)).into_response(), Err((code, msg)) => err(code, &msg)
    }
}

fn parse_optional_post(bytes: &Bytes) -> Result<PostBody, Response> {
    if bytes.is_empty() { return Ok(PostBody::default()); }
    serde_json::from_slice(bytes).map_err(|e| err(StatusCode::BAD_REQUEST, &e.to_string()))
}

async fn post_root(State(app): State<AppState>, bytes: Bytes) -> Response { post_path(State(app), AxPath(String::new()), bytes).await }

async fn post_path(State(app): State<AppState>, AxPath(rel): AxPath<String>, bytes: Bytes) -> Response {
    let body = match parse_optional_post(&bytes) { Ok(b) => b, Err(r) => return r };
    match post_in_dir(&app.root, &rel, body) { Ok(m) => (StatusCode::CREATED, Json(m)).into_response(), Err((code, msg)) => err(code, &msg) }
}

async fn patch_path(State(app): State<AppState>, AxPath(rel): AxPath<String>, Body(body): Body<PatchBody>) -> Response {
    match rename_disk(&app.root, &rel, &body.path) { Ok(m) => Json(m).into_response(), Err((code, msg)) => err(code, &msg) }
}

async fn delete_path(State(app): State<AppState>, AxPath(rel): AxPath<String>) -> Response {
    match delete_disk(&app.root, &rel) { Ok(()) => StatusCode::NO_CONTENT.into_response(), Err((code, msg)) => err(code, &msg) }
}

crate::routes! {
    pub get "/api/contents" => get_root query(GetQ) ret(ContentsModel),
    pub post "/api/contents" => post_root body(PostBody) ret(ContentsModel),
    pub get "/api/contents/{*path}" => get_path query(GetQ) ret(ContentsModel),
    pub put "/api/contents/{*path}" => put_path body(PutBody) ret(ContentsModel),
    pub patch "/api/contents/{*path}" => patch_path body(PatchBody) ret(ContentsModel),
    pub post "/api/contents/{*path}" => post_path body(PostBody) ret(ContentsModel),
    pub delete "/api/contents/{*path}" => delete_path,
}
