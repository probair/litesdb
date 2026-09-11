// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{Db, OpenOptions, Result, fsutil::TestDir};

#[test]
fn ordinary_open_creates_no_archive_artifacts() -> Result<()> {
    let root = TestDir::new("archive-default-off");
    let db = Db::open(root.path(), OpenOptions::default())?;
    db.sync()?;
    assert!(!root.path().join("ARCHIVE").exists());
    assert!(!root.path().join("archive").exists());
    Ok(())
}

#[test]
fn archive_feature_layout_evidence() {
    println!(
        "archive={} Db={} Engine={} WalWriter={}",
        cfg!(feature = "archive"),
        std::mem::size_of::<Db>(),
        std::mem::size_of::<crate::db::Engine>(),
        std::mem::size_of::<crate::wal::WalWriter>()
    );
}
