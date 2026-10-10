use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=LOCKS_GIT_SHA");
    println!("cargo:rerun-if-env-changed=LOCKS_BUILT_AT");
    // The reflog moves on every commit and checkout, so local builds pick up a new HEAD.
    // Ask git for its path, since a linked worktree keeps it outside `../.git`.
    if let Some(reflog) = git(&["rev-parse", "--git-path", "logs/HEAD"])
        && std::path::Path::new(&reflog).exists()
    {
        println!("cargo:rerun-if-changed={reflog}");
    }

    let commit = env_value("LOCKS_GIT_SHA")
        .or_else(|| git(&["rev-parse", "HEAD"]))
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

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}
