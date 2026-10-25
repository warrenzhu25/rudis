//! Embeds the git commit the binary was built from (`RUDIS_GIT_SHA`,
//! `RUDIS_GIT_DIRTY`) for `--version` and `INFO server`. Builds outside a
//! git checkout (e.g. the Docker image, which copies only the sources) get
//! Redis's placeholder `00000000`.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn main() {
    let sha = git(&["rev-parse", "--short=8", "HEAD"])
        .filter(|s| s.len() == 8)
        .unwrap_or_else(|| "00000000".to_string());
    let dirty =
        git(&["status", "--porcelain", "--untracked-files=no"]).is_some_and(|s| !s.is_empty());
    println!("cargo:rustc-env=RUDIS_GIT_SHA={sha}");
    println!("cargo:rustc-env=RUDIS_GIT_DIRTY={}", u8::from(dirty));
    // Re-run when HEAD moves (a commit, checkout or worktree switch).
    if let Some(dir) = git(&["rev-parse", "--git-dir"]) {
        println!("cargo:rerun-if-changed={dir}/HEAD");
        println!("cargo:rerun-if-changed={dir}/index");
        if let Some(head_ref) = git(&["symbolic-ref", "-q", "HEAD"])
            && let Some(common) = git(&["rev-parse", "--git-common-dir"])
        {
            println!("cargo:rerun-if-changed={common}/{head_ref}");
        }
    }
    println!("cargo:rerun-if-changed=build.rs");
}
