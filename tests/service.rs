#[cfg(target_os = "macos")]
use jupygate::service::launchd_plist;
use jupygate::service::{Config, JUPYGATE, server_command, systemd_unit};
#[cfg(target_os = "macos")]
use plist::Value;
use std::fs;
#[cfg(target_os = "macos")]
use std::io::Cursor;
use std::path::Path;

#[test]
fn builds_native_service_definitions() {
    let config = Config { workdir: "/Users/a/x mojo".into(), env_file: Some("/Users/a/.secrets".into()),
        token_file: Some("/Users/a/.config/jupygate/token".into()),
        server: ["--host", "0.0.0.0", "--trust-loopback", "--tls", "--root", "/Users/a/My Git"].map(String::from).to_vec() };
    let cmd = server_command(Path::new("/venv/bin/jupygate"), &config);
    assert_eq!(cmd[0..2], ["/bin/bash", "-lc"]);
    assert!(cmd[2].contains("--root '/Users/a/My Git'"));

    let venv = std::env::temp_dir().join(format!("jupygate-venv-test-{}", uuid::Uuid::new_v4().simple()));
    fs::create_dir_all(venv.join("bin")).unwrap();
    fs::write(venv.join("pyvenv.cfg"), "home = /usr/bin\n").unwrap();
    let venv_bin = venv.join("bin/jupygate");

    #[cfg(target_os = "macos")]
    {
        let bytes = launchd_plist(&JUPYGATE, &cmd, Path::new("/venv/bin/jupygate"), &config, Path::new("/Users/a")).unwrap();
        let plist = Value::from_reader_xml(Cursor::new(bytes)).unwrap();
        let dict = plist.as_dictionary().unwrap();
        assert_eq!(dict["Label"].as_string(), Some("ai.answerdotai.jupygate"));
        assert_eq!(dict["ProgramArguments"].as_array().unwrap(), &cmd.iter().cloned().map(Value::String).collect::<Vec<_>>());
        assert_eq!(dict["WorkingDirectory"].as_string(), Some("/Users/a/x mojo"));
        assert_eq!(dict["KeepAlive"].as_dictionary().unwrap()["SuccessfulExit"].as_boolean(), Some(false));
        let envs = dict["EnvironmentVariables"].as_dictionary().unwrap();
        assert_eq!(envs["PATH"].as_string().unwrap(), "/venv/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin");
        assert!(!envs.contains_key("VIRTUAL_ENV"));

        let vbytes = launchd_plist(&JUPYGATE, &cmd, &venv_bin, &config, Path::new("/Users/a")).unwrap();
        let vplist = Value::from_reader_xml(Cursor::new(vbytes)).unwrap();
        let venvs = vplist.as_dictionary().unwrap()["EnvironmentVariables"].as_dictionary().unwrap();
        assert_eq!(venvs["VIRTUAL_ENV"].as_string(), Some(venv.to_str().unwrap()));
    }

    let unit = systemd_unit(&JUPYGATE, &cmd, Path::new("/venv/bin/jupygate"), &config);
    assert!(unit.contains("WorkingDirectory=/Users/a/x\\x20mojo"));
    assert!(unit.contains("Restart=on-failure\nKillSignal=SIGINT\nKillMode=mixed"));
    assert!(unit.contains("\"/bin/bash\" \"-lc\""));
    assert!(unit.contains("Environment=\"PATH=/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\""));
    assert!(!unit.contains("VIRTUAL_ENV="));

    let vunit = systemd_unit(&JUPYGATE, &cmd, &venv_bin, &config);
    assert!(vunit.contains(&format!("Environment=\"VIRTUAL_ENV={}\"", venv.display())));
    let _ = fs::remove_dir_all(&venv);

    let config = Config { env_file: None, ..config };
    assert_eq!(server_command(Path::new("/venv/bin/jupygate"), &config)[1..], config.server);
}
