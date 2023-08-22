use crate::GIT_BRANCH;
use crate::GIT_SHA;
use crate::PKG_NAME;
use crate::PKG_VERS;
use crate::RUST_VERS;

pub fn init() -> std::io::Result<()> {
    get_cfg!(target_os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
    get_cfg!(target_arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
    println!("Name: {PKG_NAME}\n Release Version: {PKG_VERS}\n Target Arch: {} - {}\n Rust Version: {RUST_VERS}\n Build: {GIT_BRANCH} - {GIT_SHA}\n", target_os(), target_arch());
    Ok(())
}
