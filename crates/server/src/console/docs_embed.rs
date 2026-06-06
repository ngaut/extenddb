// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Runtime documentation loader.
//!
//! Loads HTML fragments and PDF files from a configured directory at request
//! time. The directory is populated by `docs/build-docs.py` and its path is
//! set via the `docs_dir` key in `extenddb.toml`.
//!
//! Previous versions embedded docs at compile time via `include_bytes!` /
//! `include_str!`. This module replaces that approach so docs can be updated
//! without recompilation.

use std::path::{Path, PathBuf};

/// Metadata for a single document (loaded from `manifest.json`).
#[derive(Clone)]
pub struct DocEntry {
    pub slug: String,
    pub title: String,
    pub category: String,
}

/// Runtime documentation store backed by a filesystem directory.
#[derive(Clone)]
pub struct DocsStore {
    dir: PathBuf,
    entries: Vec<DocEntry>,
}

impl DocsStore {
    /// Load the docs manifest from `docs_dir/manifest.json`.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is missing or malformed.
    pub fn load(docs_dir: &Path) -> Result<Self, String> {
        let manifest_path = docs_dir.join("manifest.json");
        let data = std::fs::read_to_string(&manifest_path).map_err(|e| {
            format!(
                "Cannot read docs manifest at {}: {e}",
                manifest_path.display()
            )
        })?;
        let raw: Vec<serde_json::Value> =
            serde_json::from_str(&data).map_err(|e| format!("Invalid docs manifest JSON: {e}"))?;

        let entries: Vec<DocEntry> = raw
            .into_iter()
            .filter_map(|v| {
                Some(DocEntry {
                    slug: v.get("slug")?.as_str()?.to_owned(),
                    title: v.get("title")?.as_str()?.to_owned(),
                    category: v.get("category")?.as_str()?.to_owned(),
                })
            })
            .collect();

        if entries.is_empty() {
            return Err("Docs manifest contains no entries".to_owned());
        }

        Ok(Self {
            dir: docs_dir.to_owned(),
            entries,
        })
    }

    /// All document entries in manifest order.
    #[must_use]
    pub fn entries(&self) -> &[DocEntry] {
        &self.entries
    }

    /// Read the HTML fragment for a document by slug.
    ///
    /// Returns `None` if the slug is invalid, not in the manifest, or the file
    /// is missing. Uses `std::fs` (blocking I/O) — acceptable for small local
    /// files; if `docs_dir` ever points to a network mount, wrap callers in
    /// `spawn_blocking`.
    #[must_use]
    pub fn read_html(&self, slug: &str) -> Option<String> {
        if !Self::is_safe_slug(slug) || self.find(slug).is_none() {
            return None;
        }
        let path = self.dir.join(format!("{slug}.html"));
        std::fs::read_to_string(path).ok()
    }

    /// Read the PDF bytes for a document by slug.
    ///
    /// Returns `None` if the slug is invalid, not in the manifest, or the file
    /// is missing. Uses `std::fs` (blocking I/O) — acceptable for small local
    /// files; if `docs_dir` ever points to a network mount, wrap callers in
    /// `spawn_blocking`.
    #[must_use]
    pub fn read_pdf(&self, slug: &str) -> Option<Vec<u8>> {
        if !Self::is_safe_slug(slug) || self.find(slug).is_none() {
            return None;
        }
        let path = self.dir.join(format!("{slug}.pdf"));
        std::fs::read(path).ok()
    }

    /// Find a document entry by slug.
    #[must_use]
    pub fn find(&self, slug: &str) -> Option<&DocEntry> {
        self.entries.iter().find(|e| e.slug == slug)
    }

    /// Validate that a slug is safe for path construction.
    /// Uses a positive allowlist: ASCII alphanumeric, hyphens, and underscores.
    fn is_safe_slug(slug: &str) -> bool {
        !slug.is_empty()
            && slug
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_from_valid_directory() {
        let dir = std::env::temp_dir().join("extenddb_test_docs_store");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");

        let manifest = r#"[{"slug":"test","title":"Test Doc","category":"reference","number":1}]"#;
        std::fs::write(dir.join("manifest.json"), manifest).expect("write manifest");
        std::fs::write(dir.join("test.html"), "<p>hello</p>").expect("write html");
        std::fs::write(dir.join("test.pdf"), b"%PDF-fake").expect("write pdf");

        let store = DocsStore::load(&dir).expect("load");
        assert_eq!(store.entries().len(), 1);
        assert_eq!(store.entries()[0].slug, "test");
        assert_eq!(store.read_html("test"), Some("<p>hello</p>".to_owned()));
        assert!(store.read_pdf("test").is_some());
        assert!(store.find("test").is_some());
        assert!(store.find("missing").is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_manifest() {
        let dir = std::env::temp_dir().join("extenddb_test_docs_store_missing");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        assert!(DocsStore::load(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_path_traversal_slugs() {
        assert!(!DocsStore::is_safe_slug("../etc/passwd"));
        assert!(!DocsStore::is_safe_slug("foo/bar"));
        assert!(!DocsStore::is_safe_slug("foo\\bar"));
        assert!(!DocsStore::is_safe_slug("foo\0bar"));
        assert!(!DocsStore::is_safe_slug(""));
        assert!(!DocsStore::is_safe_slug("foo bar"));
        assert!(!DocsStore::is_safe_slug("foo.bar"));
        assert!(DocsStore::is_safe_slug("architecture-guide"));
        assert!(DocsStore::is_safe_slug("getting-started"));
        assert!(DocsStore::is_safe_slug("doc_v2"));
    }
}
