//! Build identity: the crate version and the git commit it was built from
//! (see `build.rs`), for `--version`, `INFO server` and the startup log.

/// The crate version, e.g. `0.1.0`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The 8-hex-digit commit the binary was built from, or `00000000` when it
/// was built outside a git checkout.
pub const GIT_SHA: &str = env!("RUDIS_GIT_SHA");

/// Whether the checkout had uncommitted changes to tracked files.
pub const GIT_DIRTY: bool = matches!(env!("RUDIS_GIT_DIRTY").as_bytes(), b"1");

/// `0.1.0 (1a2b3c4d)`: the version and the commit it was built from.
pub const fn long_version() -> &'static str {
    LONG_VERSION
}

const LONG_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("RUDIS_GIT_SHA"), ")");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_identity_is_well_formed() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
        assert_eq!(GIT_SHA.len(), 8);
        assert!(GIT_SHA.bytes().all(|b| b.is_ascii_hexdigit()), "{GIT_SHA}");
        assert_eq!(long_version(), format!("{VERSION} ({GIT_SHA})"));
    }
}
