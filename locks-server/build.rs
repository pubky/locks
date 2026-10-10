use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=LOCKS_GIT_SHA");
    println!("cargo:rerun-if-env-changed=LOCKS_BUILT_AT");
    // The reflog moves on every commit and checkout, so local builds pick up a new HEAD.
    if std::path::Path::new("../.git/logs/HEAD").exists() {
        println!("cargo:rerun-if-changed=../.git/logs/HEAD");
    }

    let commit = env_value("LOCKS_GIT_SHA")
        .or_else(git_head)
        .unwrap_or_else(|| "unknown".to_owned());
    let built_at = env_value("LOCKS_BUILT_AT").unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=LOCKS_GIT_SHA={commit}");
    println!("cargo:rustc-env=LOCKS_BUILT_AT={built_at}");
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn git_head() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!sha.is_empty()).then_some(sha)
}
