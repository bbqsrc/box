use std::{env, path::PathBuf, process::Command};

#[allow(deprecated)]
fn cargo_install_dir() -> PathBuf {
    std::env::home_dir().unwrap().join(".cargo").join("bin")
}

#[cfg(unix)]
fn bin_name() -> &'static str {
    "selfextract"
}

#[cfg(windows)]
fn bin_name() -> &'static str {
    "selfextract.exe"
}

fn main() {
    if env::var("CARGO_FEATURE_SELFEXTRACT").is_ok() {
        let target = env::var("TARGET").unwrap();

        let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
        let cwd = manifest_dir.join("..").join("selfextract");

        #[cfg(unix)]
        println!(
            "cargo:rerun-if-changed={}",
            cwd.join("src")
                .join("main.rs")
                .canonicalize()
                .unwrap()
                .display()
        );
        let xargo = cargo_install_dir().join("xargo");

        assert!(
            Command::new(xargo)
                .args(["build", "--release", "--target"])
                .arg(&target)
                .current_dir(cwd)
                .status()
                .expect("`xargo` binary was not found. Run: `cargo install xargo`")
                .success()
        );

        let output_dir = manifest_dir
            .join("..")
            .join("selfextract")
            .join("target")
            .join(&target)
            .join("release")
            .join(bin_name())
            .canonicalize()
            .unwrap();

        #[cfg(unix)]
        Command::new("strip").arg(&output_dir).status().unwrap();

        println!("cargo:rerun-if-changed={}", output_dir.display());
        println!("cargo:rustc-env=SELFEXTRACT_PATH={}", output_dir.display());
    }
}
