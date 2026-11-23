//! Checked filesystem path helpers under one served root.

use std::path::{Path, PathBuf};

pub use aihist::paths::realpath;

/// Resolve a request path under the served root. Rejection is lexical: any `..` segment fails.
pub fn resolve(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let mut p = root.to_path_buf();
    for seg in rel.split('/').filter(|s| !s.is_empty()) {
        if seg == ".." { return Err("path escapes the served root".into()); }
        p.push(seg);
    }
    Ok(p)
}

/// Rebase `path` below `old` onto `new`, matching complete path components.
pub fn repath(path: &str, old: &str, new: &str) -> Option<String> {
    let suffix = Path::new(path).strip_prefix(old).ok()?;
    Some(if suffix.as_os_str().is_empty() { new.into() } else { Path::new(new).join(suffix).to_string_lossy().into_owned() })
}

/// Notebook identity uses one spelling; aliases must not create another held state or kernel.
pub fn canonical_rel(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let abs = resolve(root, rel)?;
    if !rel.is_empty() && rel.split('/').any(|s| s.is_empty() || s == ".") { return Err("use a canonical root-relative path".into()); }
    let canonical_root = root.canonicalize().map_err(|e| e.to_string())?;
    if realpath(&canonical_root, rel)? != canonical_root.join(rel) { return Err("path aliases are not supported; use the canonical path".into()); }
    Ok(abs)
}

/// The working directory for a process with this binding: explicit `cwd`, else a `.ipynb`
/// binding's parent folder, else `binding` itself if it is a directory (walking up to the
/// nearest existing directory under `root`, matching Jupyter's `cwd_for_path`), else none.
/// Relative directories use the served root.
pub fn process_cwd(root: &Path, cwd: Option<&str>, binding: Option<&str>) -> std::io::Result<Option<PathBuf>> {
    if let Some(cwd) = cwd {
        if Path::new(cwd).is_absolute() { return Ok(Some(cwd.into())); }
        return resolve(root, cwd).map(Some).map_err(std::io::Error::other);
    }
    let Some(path) = binding else { return Ok(None); };
    if path.ends_with(".ipynb") {
        let folder = Path::new(path).parent().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
        return resolve(root, &folder).map(Some).map_err(std::io::Error::other);
    }
    let mut abs = resolve(root, path).map_err(std::io::Error::other)?;
    while !abs.is_dir() && abs != root {
        let Some(parent) = abs.parent() else { break; };
        abs = parent.to_path_buf();
    }
    Ok(Some(abs))
}
