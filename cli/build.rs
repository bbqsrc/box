use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn calendar_date_from_unix_seconds(seconds: i64) -> String {
    // Howard Hinnant's civil-from-days algorithm. Keeping this here avoids a
    // build-time dependency just to format one UTC date.
    let days = seconds.div_euclid(86_400);
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }

    format!("{year:04}-{month:02}-{day:02}")
}

fn build_date() -> String {
    let seconds = env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .try_into()
                .unwrap_or(i64::MAX)
        });
    calendar_date_from_unix_seconds(seconds)
}

fn short_hash(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() < 7 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(value[..7].to_ascii_lowercase())
}

fn git_hash(manifest_dir: &Path) -> String {
    for variable in ["BOX_GIT_HASH", "GIT_COMMIT", "VERGEN_GIT_SHA"] {
        if let Ok(value) = env::var(variable)
            && let Some(hash) = short_hash(&value)
        {
            return hash;
        }
    }

    Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .current_dir(manifest_dir)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|hash| short_hash(&hash))
        .unwrap_or_else(|| "unknown".to_string())
}

fn git_metadata_path(manifest_dir: &Path, name: &str) -> Option<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-path", name])
        .current_dir(manifest_dir)
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let path = PathBuf::from(String::from_utf8(output.stdout).ok()?.trim());
    let path = if path.is_absolute() {
        path
    } else {
        manifest_dir.join(path)
    };
    path.canonicalize().ok()
}

fn emit_build_info(manifest_dir: &Path) {
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-env-changed=BOX_GIT_HASH");
    println!("cargo:rerun-if-env-changed=GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=VERGEN_GIT_SHA");
    for name in ["HEAD", "logs/HEAD"] {
        if let Some(path) = git_metadata_path(manifest_dir, name) {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
    println!("cargo:rustc-env=BOX_BUILD_DATE={}", build_date());
    println!("cargo:rustc-env=BOX_GIT_HASH={}", git_hash(manifest_dir));
}

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
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    emit_build_info(&manifest_dir);

    if env::var("CARGO_FEATURE_SELFEXTRACT").is_ok() {
        let target = env::var("TARGET").unwrap();

        let cwd = manifest_dir.join("..").join("selfextract");

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
