//! Install and control the gateway as a launchd (macOS) or systemd (Linux) user service.

#[cfg(target_os = "macos")]
use plist::{Dictionary, Value};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(target_os = "macos")]
use std::process::Stdio;

/// Binary and service identifiers parameterized so both `jupygate` and `rustygate` share this module.
pub struct ServiceMeta { pub bin: &'static str, pub label: &'static str, pub unit: &'static str, pub description: &'static str, pub log_name: &'static str }

pub const JUPYGATE: ServiceMeta =
    ServiceMeta { bin: "jupygate", label: "ai.answerdotai.jupygate", unit: "jupygate.service", description: "jupygate Jupyter gateway", log_name: "jupygate" };

type Result<T> = std::result::Result<T, String>;

fn usage(meta: &ServiceMeta) -> String {
    format!(
        "usage: {} service <install|uninstall|start|stop|restart|status> [install options]\n\
         install options: [--workdir DIR] [--env-file PATH], plus any server option",
        meta.bin
    )
}

#[derive(Clone, Copy, PartialEq)]
enum Manager { #[cfg(target_os = "macos")] Launchd, Systemd }

/// What `install` writes: the service's working directory and environment file, and the server's arguments.
pub struct Config { pub workdir: PathBuf, pub env_file: Option<PathBuf>, pub token_file: Option<PathBuf>, pub server: Vec<String> }

fn manager(meta: &ServiceMeta) -> Result<Manager> {
    #[cfg(target_os = "macos")]
    { let _ = meta; Ok(Manager::Launchd) }
    #[cfg(target_os = "linux")]
    { let _ = meta; Ok(Manager::Systemd) }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    { Err(format!("{} service requires launchd or systemd", meta.bin)) }
}

fn home() -> Result<PathBuf> { env::var_os("HOME").map(PathBuf::from).ok_or_else(|| "HOME is not set".into()) }

fn absolute(path: impl AsRef<Path>) -> Result<PathBuf> {
    let path = path.as_ref();
    if path.is_absolute() { Ok(path.to_path_buf()) }
    else { env::current_dir().map(|p| p.join(path)).map_err(|e| format!("cannot read working directory: {e}")) }
}

fn executable(meta: &ServiceMeta, program: &str) -> Result<PathBuf> {
    if program.contains('/') { return absolute(program); }
    let path = env::var_os("PATH").ok_or_else(|| "PATH is not set".to_string())?;
    env::split_paths(&path).map(|dir| dir.join(program)).find(|path| path.is_file())
        .ok_or_else(|| format!("{} executable not found on PATH: {program}", meta.bin))
}

fn existing(path: &str, name: &str, kind: fn(&Path) -> bool) -> Result<PathBuf> {
    let path = absolute(path)?;
    if kind(&path) { fs::canonicalize(&path).map_err(|e| format!("cannot resolve {name} {}: {e}", path.display())) }
    else { Err(format!("{name} not found: {}", path.display())) }
}

/// Take `install`'s own options out of `args`. The rest are the server's, passed through with `--root` (default: the
/// working directory) and `--token-file` made absolute, after `validate` has checked them.
fn parse_install(meta: &ServiceMeta, args: &[String], validate: impl FnOnce(Vec<String>) -> Result<()>) -> Result<Config> {
    let (mut workdir, mut root, mut env_file, mut token_file, mut server) = (".".to_string(), ".".to_string(), None, None, vec![]);
    let mut args = args.iter();
    while let Some(a) = args.next() {
        let mut val = || args.next().cloned().ok_or_else(|| format!("{a} needs a value\n{}", usage(meta)));
        match a.as_str() {
            "--workdir" => workdir = val()?, "--root" => root = val()?, "--env-file" => env_file = Some(existing(&val()?, "environment file", Path::is_file)?),
            "--token-file" => token_file = Some(absolute(val()?)?), _ => server.push(a.clone())
        }
    }
    server.extend(["--root".into(), existing(&root, "files root", Path::is_dir)?.display().to_string()]);
    if let Some(path) = &token_file { server.extend(["--token-file".into(), path.display().to_string()]); }
    validate(server.clone())?;
    Ok(Config { workdir: existing(&workdir, "working directory", Path::is_dir)?, env_file, token_file, server })
}

fn ensure_token(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?; }
    if !path.exists() {
        let mut file = OpenOptions::new().write(true).create_new(true).open(path).map_err(|e| format!("cannot create {}: {e}", path.display()))?;
        writeln!(file, "{}", uuid::Uuid::new_v4().simple()).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    }
    let token = fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    if token.trim().is_empty() { return Err(format!("token file is empty: {}", path.display())); }
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|e| format!("cannot protect {}: {e}", path.display()))
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty() && value.bytes().all(|c| c.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(&c)) { value.into() }
    else { format!("'{}'", value.replace('\'', "'\"'\"'")) }
}

pub fn server_command(binary: &Path, config: &Config) -> Vec<String> {
    let cmd = [binary.display().to_string()].into_iter().chain(config.server.iter().cloned()).collect::<Vec<_>>();
    let Some(path) = &config.env_file else { return cmd };
    let command = format!("set -a; . {}; exec {}", shell_quote(&path.display().to_string()), cmd.iter().map(|s| shell_quote(s)).collect::<Vec<_>>().join(" "));
    vec!["/bin/bash".into(), "-lc".into(), command]
}

/// The service PATH value: the binary's own directory (the venv bin, under a pip install) ahead of `system`,
/// because launchd and systemd give user services a minimal default and kernels must find python.
fn path_env(binary: &Path, system: &str) -> String { format!("{}:{system}", binary.parent().unwrap_or(Path::new("/")).display()) }

/// The virtual environment root holding `binary`, when `binary` lives under `<venv>/bin/`.
fn venv_env(binary: &Path) -> Option<String> {
    let prefix = binary.parent()?.parent()?;
    prefix.join("pyvenv.cfg").exists().then(|| prefix.display().to_string())
}

#[cfg(target_os = "macos")]
pub fn launchd_plist(meta: &ServiceMeta, cmd: &[String], binary: &Path, config: &Config, home: &Path) -> Result<Vec<u8>> {
    let mut keep_alive = Dictionary::new();
    keep_alive.insert("SuccessfulExit".into(), Value::Boolean(false));
    let mut envs = Dictionary::new();
    envs.insert("PATH".into(), Value::String(path_env(binary, "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")));
    if let Some(venv) = venv_env(binary) { envs.insert("VIRTUAL_ENV".into(), Value::String(venv)); }
    let mut dict = Dictionary::new();
    dict.insert("Label".into(), Value::String(meta.label.into()));
    dict.insert("ProgramArguments".into(), Value::Array(cmd.iter().cloned().map(Value::String).collect()));
    dict.insert("WorkingDirectory".into(), Value::String(config.workdir.display().to_string()));
    dict.insert("RunAtLoad".into(), Value::Boolean(true));
    dict.insert("KeepAlive".into(), Value::Dictionary(keep_alive));
    dict.insert("EnvironmentVariables".into(), Value::Dictionary(envs));
    dict.insert("ProcessType".into(), Value::String("Background".into()));
    dict.insert("StandardOutPath".into(), Value::String(home.join(format!("Library/Logs/{}.log", meta.log_name)).display().to_string()));
    dict.insert("StandardErrorPath".into(), Value::String(home.join(format!("Library/Logs/{}.err.log", meta.log_name)).display().to_string()));
    let mut out = Vec::new();
    plist::to_writer_xml(&mut out, &Value::Dictionary(dict)).map_err(|e| format!("cannot build launchd plist: {e}"))?;
    Ok(out)
}

fn systemd_quote(value: &str) -> String { format!("\"{}\"", value.replace('%', "%%").replace('\\', "\\\\").replace('"', "\\\"")) }

fn systemd_path(path: &Path) -> String {
    path.as_os_str().as_encoded_bytes().iter()
        .map(|&c| if c.is_ascii_alphanumeric() || b"/._-".contains(&c) { (c as char).to_string() } else { format!("\\x{c:02x}") }).collect()
}

pub fn systemd_unit(meta: &ServiceMeta, cmd: &[String], binary: &Path, config: &Config) -> String {
    let mut envs =
        format!("Environment={}", systemd_quote(&format!("PATH={}", path_env(binary, "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"))));
    if let Some(venv) = venv_env(binary) { envs.push_str(&format!("\nEnvironment={}", systemd_quote(&format!("VIRTUAL_ENV={venv}")))); }
    format!(
        "[Unit]\nDescription={}\n\n[Service]\nExecStart={}\nWorkingDirectory={}\n{}\nRestart=on-failure\nKillSignal=SIGINT\nKillMode=mixed\n\n[Install]\nWantedBy=default.target\n",
        meta.description, cmd.iter().map(|s| systemd_quote(s)).collect::<Vec<_>>().join(" "), systemd_path(&config.workdir), envs
    )
}

fn service_path(meta: &ServiceMeta, manager: Manager, home: &Path) -> PathBuf {
    match manager {
        #[cfg(target_os = "macos")] Manager::Launchd => home.join("Library/LaunchAgents").join(format!("{}.plist", meta.label)),
        Manager::Systemd => home.join(".config/systemd/user").join(meta.unit)
    }
}

fn write_service(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| format!("service path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    let temp = path.with_extension("tmp");
    fs::write(&temp, contents).map_err(|e| format!("cannot write {}: {e}", temp.display()))?;
    fs::rename(&temp, path).map_err(|e| format!("cannot replace {}: {e}", path.display()))
}

#[cfg(target_os = "macos")]
fn checked(command: &mut Command) -> Result<()> {
    let shown = format!("{command:?}");
    let status = command.status().map_err(|e| format!("cannot run {shown}: {e}"))?;
    if status.success() { Ok(()) } else { Err(format!("{shown} exited with {status}")) }
}

#[cfg(target_os = "macos")]
fn launch_target(meta: &ServiceMeta) -> String { format!("gui/{}/{}", unsafe { libc::getuid() }, meta.label) }

#[cfg(target_os = "macos")]
fn launchd_loaded(meta: &ServiceMeta) -> bool {
    Command::new("launchctl").args(["print", &launch_target(meta)]).stdout(Stdio::null()).stderr(Stdio::null()).status().is_ok_and(|s| s.success())
}

#[cfg(target_os = "macos")]
fn launchd_stop(meta: &ServiceMeta) -> Result<()> {
    if launchd_loaded(meta) { checked(Command::new("launchctl").args(["bootout", &launch_target(meta)])) } else { Ok(()) }
}

#[cfg(target_os = "macos")]
fn launchd_start(meta: &ServiceMeta, path: &Path) -> Result<()> {
    if launchd_loaded(meta) { checked(Command::new("launchctl").args(["kickstart", &launch_target(meta)])) } else {
        checked(Command::new("launchctl").args(["bootstrap", &format!("gui/{}", unsafe { libc::getuid() }), &path.display().to_string()]))
    }
}

fn systemctl(args: &[&str], check: bool) -> Result<i32> {
    let status = Command::new("systemctl").arg("--user").args(args).status().map_err(|e| format!("cannot run systemctl: {e}"))?;
    if check && !status.success() { Err(format!("systemctl {} exited with {status}", args.join(" "))) } else { Ok(status.code().unwrap_or(1)) }
}

fn install(meta: &ServiceMeta, program: &str, args: &[String], validate: impl FnOnce(Vec<String>) -> Result<()>) -> Result<i32> {
    let manager = manager(meta)?;
    let home = home()?;
    let config = parse_install(meta, args, validate)?;
    if let Some(path) = &config.token_file { ensure_token(path)?; }
    let binary = executable(meta, program)?;
    let cmd = server_command(&binary, &config);
    let path = service_path(meta, manager, &home);
    match manager {
        #[cfg(target_os = "macos")]
        Manager::Launchd => {
            launchd_stop(meta)?;
            write_service(&path, &launchd_plist(meta, &cmd, &binary, &config, &home)?)?;
            launchd_start(meta, &path)?;
        }
        Manager::Systemd => {
            write_service(&path, systemd_unit(meta, &cmd, &binary, &config).as_bytes())?;
            systemctl(&["daemon-reload"], true)?;
            systemctl(&["enable", meta.unit], true)?;
            systemctl(&["restart", meta.unit], true)?;
        }
    }
    println!("installed {}", path.display());
    Ok(0)
}

fn uninstall(meta: &ServiceMeta) -> Result<i32> {
    let manager = manager(meta)?;
    let path = service_path(meta, manager, &home()?);
    match manager {
        #[cfg(target_os = "macos")] Manager::Launchd => launchd_stop(meta)?, Manager::Systemd => { systemctl(&["disable", "--now", meta.unit], false)?; }
    }
    if path.exists() { fs::remove_file(&path).map_err(|e| format!("cannot remove {}: {e}", path.display()))?; }
    if manager == Manager::Systemd { systemctl(&["daemon-reload"], true)?; }
    println!("removed {}", path.display());
    Ok(0)
}

fn control(meta: &ServiceMeta, action: &str) -> Result<i32> {
    let manager = manager(meta)?;
    let path = service_path(meta, manager, &home()?);
    if !path.exists() { return Err(format!("{} service is not installed", meta.bin)); }
    match (manager, action) {
        #[cfg(target_os = "macos")] (Manager::Launchd, "start") => launchd_start(meta, &path).map(|_| 0), #[cfg(target_os = "macos")]
        (Manager::Launchd, "stop") => launchd_stop(meta).map(|_| 0), #[cfg(target_os = "macos")]
        (Manager::Launchd, "restart") if launchd_loaded(meta) => checked(Command::new("launchctl").args(["kickstart", "-k", &launch_target(meta)])).map(|_| 0),
        #[cfg(target_os = "macos")] (Manager::Launchd, "restart") => launchd_start(meta, &path).map(|_| 0), #[cfg(target_os = "macos")]
        (Manager::Launchd, "status") => Command::new("launchctl").args(["print", &launch_target(meta)]).status().map(|s| s.code().unwrap_or(1))
            .map_err(|e| format!("cannot run launchctl: {e}")), (Manager::Systemd, action) => systemctl(&[action, meta.unit], action != "status"),
        _ => Err(format!("unknown service command {action}\n{}", usage(meta)))
    }
}

pub fn run(meta: &ServiceMeta, program: &str, args: &[String], validate: impl FnOnce(Vec<String>) -> Result<()>) -> Result<i32> {
    let Some(action) = args.first().map(String::as_str) else { return Err(usage(meta)) };
    match action {
        "install" => install(meta, program, &args[1..], validate), "uninstall" if args.len() == 1 => uninstall(meta),
        "start" | "stop" | "restart" | "status" if args.len() == 1 => control(meta, action), _ => Err(usage(meta))
    }
}
