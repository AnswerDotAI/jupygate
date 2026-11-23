use std::{fs, path::Path, process::{Command, Stdio}};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let local_only = std::env::args().any(|a| a == "--local");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = serde_json::to_string_pretty(&jupygate::spec(true))? + "\n";
    fs::write(root.join("openapi.json"), &text)?;
    let jac = root.join("../jupyasyncclient");
    if !local_only && jac.is_dir() {
        let mut child = Command::new("python").arg(jac.join("tools/build.py")).stdin(Stdio::piped()).spawn()?;
        serde_json::to_writer(child.stdin.take().unwrap(), &jupygate::spec(false))?;
        if !child.wait()?.success() { return Err("jupyasyncclient spec generation failed".into()); }
    }
    Ok(())
}
