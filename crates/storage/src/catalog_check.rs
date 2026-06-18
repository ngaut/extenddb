// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared report types for TiDB catalog/data integrity checks.

/// TiDB catalog/data integrity check result.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct CatalogCheckReport {
    pub sections: Vec<CatalogCheckSection>,
}

impl CatalogCheckReport {
    #[must_use]
    pub fn issue_count(&self) -> usize {
        self.sections
            .iter()
            .map(|section| section.issues.len())
            .sum()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogCheckSection {
    pub title: String,
    pub ok_message: String,
    pub issues: Vec<CatalogCheckIssue>,
}

impl CatalogCheckSection {
    #[must_use]
    pub fn new(
        title: impl Into<String>,
        ok_message: impl Into<String>,
        issues: Vec<CatalogCheckIssue>,
    ) -> Self {
        Self {
            title: title.into(),
            ok_message: ok_message.into(),
            issues,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogCheckIssue {
    pub name: String,
    pub detail: Option<String>,
    pub fix: Option<CatalogCheckFix>,
}

impl CatalogCheckIssue {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            detail: None,
            fix: None,
        }
    }

    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    #[must_use]
    pub fn with_fix(mut self, fix: CatalogCheckFix) -> Self {
        self.fix = Some(fix);
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CatalogCheckFix {
    Applied(String),
    Failed(String),
}
