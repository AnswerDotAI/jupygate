//! Kernelspecs: the `kernels/<name>/kernel.json` directories on Jupyter's search path.
//! Discovery and interpreter resolution are shared with `kernmini`.

use axum::extract::Path as UrlPath;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use tokio::task;

use crate::api::err;

/// The kernel started when a request names neither a kernel nor an argv.
pub const DEFAULT: &str = "py";

#[derive(Clone, Serialize, JsonSchema)]
pub struct KernelSpec { pub name: String, pub spec: Value, pub resources: Map<String, Value>, #[serde(skip)] inner: kernmini::KernelSpec }

impl KernelSpec {
    /// The launch command, with `{resource_dir}` filled in and bare `python`/`python3` resolved.
    pub fn argv(&self) -> Vec<String> { self.inner.argv(None) }

    pub fn env(&self) -> Map<String, Value> { self.inner.env() }

    /// Interrupt by a control-channel `interrupt_request` rather than SIGINT?
    pub fn message_interrupt(&self) -> bool { self.inner.message_interrupt() }
}

/// Every launchable kernel by name: the first spec of each name along Jupyter's search path. Blocking.
pub fn load() -> BTreeMap<String, KernelSpec> {
    kernmini::discover_kernelspecs(None).into_iter()
        .map(|(k, inner)| (k, KernelSpec { name: inner.name.clone(), spec: inner.spec.clone(), resources: Map::new(), inner })).collect()
}

pub async fn specs() -> BTreeMap<String, KernelSpec> { task::spawn_blocking(load).await.expect("kernelspec scan panicked") }

pub async fn names() -> Vec<String> { specs().await.into_keys().collect() }

pub fn unknown(name: &str, specs: &BTreeMap<String, KernelSpec>) -> String {
    format!("unknown kernel {name:?}; available: {}", specs.keys().map(String::as_str).collect::<Vec<_>>().join(", "))
}

#[derive(Serialize, JsonSchema)]
pub struct KernelSpecs { pub default: &'static str, pub kernelspecs: BTreeMap<String, KernelSpec> }

pub async fn list_specs() -> Json<KernelSpecs> { Json(KernelSpecs { default: DEFAULT, kernelspecs: specs().await }) }

pub async fn get_spec(UrlPath(name): UrlPath<String>) -> Response {
    match specs().await.remove(&name) { Some(s) => Json(s).into_response(), None => err(StatusCode::NOT_FOUND, "no such kernelspec") }
}

pub fn routes<S: Clone + Send + Sync + 'static>() -> crate::openapi::RouteSet<S> {
    crate::openapi::RouteSet {
        router: axum::Router::new().route("/api/kernelspecs", axum::routing::get(list_specs)).route("/api/kernelspecs/{name}", axum::routing::get(get_spec)),
        routes: vec![
            crate::openapi::Route {
                method: "get",
                path: "/api/kernelspecs",
                handler: concat!(module_path!(), "::list_specs"),
                public: true,
                query: None,
                body: None,
                ret: Some(crate::openapi::schema_of::<KernelSpecs>),
            }, crate::openapi::Route {
                method: "get",
                path: "/api/kernelspecs/{name}",
                handler: concat!(module_path!(), "::get_spec"),
                public: true,
                query: None,
                body: None,
                ret: Some(crate::openapi::schema_of::<KernelSpec>),
            },
        ] }
}
