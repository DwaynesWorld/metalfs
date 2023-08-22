use std::process::Command;

fn set_env(name: &str, cmd: &mut Command) {
    let value = match cmd.output() {
        Ok(output) => String::from_utf8(output.stdout).unwrap(),
        Err(err) => {
            println!("cargo:warning={}", err);
            "".to_string()
        }
    };
    println!("cargo:rustc-env={}={}", name, value);
}

fn set_version() {
    set_env(
        "GIT_BRANCH",
        Command::new("git").args(&["rev-parse", "--abbrev-ref", "HEAD"]),
    );
    set_env(
        "GIT_SHA",
        Command::new("git").args(&["rev-parse", "--short", "HEAD"]),
    );
    set_env(
        "GIT_VERSION",
        Command::new("git").args(&["describe", "--always", "HEAD"]),
    );
    set_env("RUST_VERSION", Command::new("rustc").arg("--version"));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_version();
    tonic_build::compile_protos("../protos/metalfs.proto").unwrap();
    Ok(())
}
