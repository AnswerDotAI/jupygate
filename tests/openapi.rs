use serde_json::Value;
use std::{fs, path::Path};

#[test]
fn spec_is_current() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.json");
    let unversioned = |mut v: Value| { v["info"]["version"].take(); v };
    let want = unversioned(jupygate::spec(true));
    let saved = fs::read_to_string(&p).ok().and_then(|s| serde_json::from_str(&s).ok());
    assert!(saved.map(unversioned) == Some(want), "{} is stale: run `cargo spec`", p.display());
}
