//! The OpenAPI 3.1 machine contract, generated from the route table. `GET /openapi.json`
//! serves the public routes, the ones marked `pub` in their route tables. `cargo spec` writes the full document to this repo's `openapi.json`.
//! Schemas come from the `query()`, `body()`, and `ret()` types the route table declares.
use axum::response::Json;
use schemars::generate::SchemaSettings;
use schemars::{Schema, SchemaGenerator};
use serde_json::{Map, Value, json};
use std::collections::HashSet;

/// One mounted route, recorded by `routes!` beside its `.route` call. The schema thunks come from
/// the route table's `query()`, `body()`, and `ret()` annotations; None means the route has none.
pub struct Route {
    pub method: &'static str,
    pub path: &'static str,
    /// Listed by the served spec: marked `pub` in its route table
    pub public: bool,
    /// The handler's `module::name`, used to derive its operation id
    pub handler: &'static str,
    /// The query struct's schema
    pub query: Option<SchemaFn>,
    /// The request body's schema
    pub body: Option<SchemaFn>,
    /// The success payload's schema
    pub ret: Option<SchemaFn>,
}

pub type SchemaFn = fn(&mut SchemaGenerator) -> Schema;

pub fn schema_of<T: schemars::JsonSchema>(g: &mut SchemaGenerator) -> Schema { g.subschema_for::<T>() }

/// A router together with the route metadata declared alongside it.
pub struct RouteSet<S> { pub router: axum::Router<S>, pub routes: Vec<Route> }

impl<S: Clone + Send + Sync + 'static> Default for RouteSet<S> { fn default() -> Self { Self::new() } }

impl<S: Clone + Send + Sync + 'static> RouteSet<S> {
    pub fn new() -> Self { Self { router: axum::Router::new(), routes: Vec::new() } }

    pub fn merge(mut self, other: RouteSet<S>) -> Self {
        self.router = self.router.merge(other.router);
        for r in other.routes {
            assert!(!self.routes.iter().any(|x| x.method == r.method && x.path == r.path), "duplicate route: {} {}", r.method, r.path);
            self.routes.push(r);
        }
        self
    }
}

/// A module's `routes()` from its route table, returning a `RouteSet` carrying both the router and OpenAPI metadata.
/// Methods are axum's routing fn names. `pub` marks a public route, which the served spec lists. A route may declare
/// its request and response types: `pub get "/x" => handler query(XQ) body(XBody) ret(XModel)`.
#[macro_export]
macro_rules! routes {
    ($state:ty; $($v:vis $m:ident $p:literal => $h:ident $(query($q:ty))? $(body($b:ty))? $(ret($r:ty))?),* $(,)?) => {
        pub fn routes() -> $crate::openapi::RouteSet<$state> {
            $crate::openapi::RouteSet {
                router: $crate::__axum::Router::new()$(.route($p, $crate::__axum::routing::$m($h)))*,
                routes: vec![$($crate::openapi::Route {
                    method: stringify!($m), path: $p, handler: concat!(module_path!(), "::", stringify!($h)),
                    public: !stringify!($v).is_empty(),
                    query: $crate::opt_schema!($($q)?), body: $crate::opt_schema!($($b)?), ret: $crate::opt_schema!($($r)?),
                }),*],
            }
        }
    };
    ($($v:vis $m:ident $p:literal => $h:ident $(query($q:ty))? $(body($b:ty))? $(ret($r:ty))?),* $(,)?) => {
        $crate::routes!($crate::AppState; $($v $m $p => $h $(query($q))? $(body($b))? $(ret($r))?),*);
    };
}

/// `Some(schema_of::<T>)` for a present `routes!` annotation, `None` for an absent one.
#[macro_export]
macro_rules! opt_schema {
    () => { None };
    ($t:ty) => {
        Some($crate::openapi::schema_of::<$t>)
    };
}

const METHODS: [&str; 5] = ["get", "post", "put", "patch", "delete"];

/// Every route, or only the public ones unless `full`, sorted by path then method.
fn all(routes: &[Route], full: bool) -> Vec<&Route> {
    let mut v: Vec<_> = routes.iter().filter(|r| full || r.public).collect();
    v.sort_by_key(|r| (r.path, METHODS.iter().position(|m| *m == r.method)));
    v
}

/// The axum template as an OpenAPI path (`{*path}` becomes `{path}`), and its parameter names.
fn oai_path(tmpl: &str) -> (String, Vec<String>) {
    let mut names = vec![];
    let segs: Vec<String> = tmpl.trim_start_matches('/').split('/').map(|s| {
        if let Some(n) = s.strip_prefix("{*").and_then(|s| s.strip_suffix('}')) {
            names.push(n.to_string());
            format!("{{{n}}}")
        } else if let Some(n) = s.strip_prefix('{').and_then(|s| s.strip_suffix('}')) { names.push(n.to_string()); s.to_string() }
        else { s.to_string() }
    }).collect();
    (format!("/{}", segs.join("/")), names)
}

/// The schema `v` refers to when it's a `$ref` into `defs`; `v` otherwise.
fn deref<'a>(v: &'a Value, defs: &'a Map<String, Value>) -> &'a Value {
    v.get("$ref").and_then(Value::as_str).and_then(|r| r.rsplit('/').next()).and_then(|n| defs.get(n)).unwrap_or(v)
}

/// The dotted operation id `group.name`: the first concrete path segment after `api` (`gateway` for the
/// root), then the handler's fn name, a numeric suffix deduplicating a handler mounted at more
/// than one path.
fn op_id(path: &str, handler: &str, seen: &mut HashSet<String>) -> String {
    let seg = path.trim_matches('/').split('/').find(|s| !s.is_empty() && *s != "api" && !s.starts_with('{'));
    let group = seg.map_or("gateway".to_string(), |s| s.replace('.', "_"));
    let name = handler.rsplit("::").next().unwrap_or(handler);
    let mut id = format!("{group}.{name}");
    let mut n = 1;
    while !seen.insert(id.clone()) {
        n += 1;
        id = format!("{group}.{name}_{n}");
    }
    id
}

/// One route as an OpenAPI operation object.
fn operation(id: &str, names: &[String], schemas: &(Option<Value>, Option<Value>, Option<Value>), errs: &Value, defs: &Map<String, Value>) -> Value {
    let (query, body, ret) = schemas;
    let mut op = Map::new();
    op.insert("operationId".into(), id.into());
    let mut params: Vec<_> = names.iter().map(|n| json!({"name": n, "in": "path", "required": true, "schema": {"type": "string"}})).collect();
    if let Some(q) = query {
        let q = deref(q, defs);
        let req: Vec<&str> = q["required"].as_array().map(|a| a.iter().filter_map(Value::as_str).collect()).unwrap_or_default();
        if let Some(props) = q["properties"].as_object() {
            for (name, schema) in props { params.push(json!({"name": name, "in": "query", "required": req.contains(&name.as_str()), "schema": schema})); }
        }
    }
    if !params.is_empty() { op.insert("parameters".into(), params.into()); }
    if let Some(b) = body { op.insert("requestBody".into(), json!({"content": {"application/json": {"schema": b}}})); }
    let mut responses = json!({"default": {"description": "Error", "content": {"application/json": {"schema": errs}}}});
    if let Some(rt) = ret { responses["2XX"] = json!({"description": "Success", "content": {"application/json": {"schema": rt}}}); }
    op.insert("responses".into(), responses);
    op.into()
}

/// The OpenAPI document: with `full`, every route; otherwise the public ones only.
pub fn spec(title: &str, version: &str, routes: &[Route], full: bool) -> Value {
    let mut g = SchemaGenerator::new(SchemaSettings::draft2020_12().with(|s| s.definitions_path = "#/components/schemas/".into()));
    let routes = all(routes, full);
    let schemas: Vec<_> =
        routes.iter().map(|r| (r.query.map(|f| f(&mut g).to_value()), r.body.map(|f| f(&mut g).to_value()), r.ret.map(|f| f(&mut g).to_value()))).collect();
    let errs = g.subschema_for::<crate::api::ErrMsg>().to_value();
    let defs = g.take_definitions(true);
    let mut paths = Map::new();
    let mut seen = HashSet::new();
    for (r, sch) in routes.iter().zip(&schemas) {
        let (p, names) = oai_path(r.path);
        let entry = paths.entry(p).or_insert_with(|| json!({}));
        entry[r.method] = operation(&op_id(r.path, r.handler, &mut seen), &names, sch, &errs, &defs);
    }
    json!({
        "openapi": "3.1.0",
        "info": {"title": title, "version": version},
        "paths": paths,
        "components": {"schemas": defs},
    })
}

/// Mount `GET /openapi.json` serving `doc` and record the route in the returned `RouteSet`.
pub fn openapi_routes<S: Clone + Send + Sync + 'static>(doc: Value) -> RouteSet<S> {
    let router = axum::Router::new().route("/openapi.json", axum::routing::get(move || {
        let doc = doc.clone();
        async move { Json(doc) }
    }));
    RouteSet { router, routes: vec![Route {
        method: "get",
        path: "/openapi.json",
        public: false,
        handler: concat!(module_path!(), "::openapi_json"),
        query: None,
        body: None,
        ret: None,
    }] }
}
