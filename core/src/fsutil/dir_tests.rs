// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, os::unix::fs::symlink};

use super::{Area, DbDir};
use crate::fsutil::testutil::TestDir;

#[test]
fn initialization_creates_exact_canonical_layout() {
    let temporary = TestDir::new("layout");
    let root = temporary.path().join("db");
    let Ok(directory) = DbDir::initialize(&root) else {
        unreachable!("valid local directory rejected");
    };

    assert_eq!(directory.path(Area::Root), root);
    for (area, child) in [
        (Area::Wal, "wal"),
        (Area::Units, "units"),
        (Area::Heads, "heads"),
        (Area::Aggregates, "agg"),
        (Area::Temporary, "tmp"),
    ] {
        assert_eq!(directory.path(area), root.join(child));
        assert!(directory.path(area).is_dir());
    }
}

#[test]
fn temporary_cleanup_is_scoped_and_does_not_follow_symlinks() {
    let temporary = TestDir::new("cleanup");
    let outside = temporary.path().join("outside");
    assert!(fs::create_dir(&outside).is_ok());
    assert!(fs::write(outside.join("keep"), b"authoritative").is_ok());

    let root = temporary.path().join("db");
    let Ok(directory) = DbDir::initialize(&root) else {
        unreachable!();
    };
    let tmp = directory.path(Area::Temporary);
    assert!(fs::write(tmp.join("partial"), b"x").is_ok());
    assert!(fs::create_dir(tmp.join("nested")).is_ok());
    assert!(fs::write(tmp.join("nested/file"), b"x").is_ok());
    assert!(symlink(&outside, tmp.join("outside-link")).is_ok());

    assert!(directory.clear_temporary().is_ok());
    let Ok(mut entries) = fs::read_dir(tmp) else {
        unreachable!();
    };
    assert!(entries.next().is_none());
    assert_eq!(
        fs::read(outside.join("keep")).ok().as_deref(),
        Some(b"authoritative".as_slice())
    );
}

#[test]
fn a_file_cannot_be_initialized_as_a_database_root() {
    let temporary = TestDir::new("not-directory");
    let root = temporary.path().join("file");
    assert!(fs::write(&root, b"not a directory").is_ok());
    assert!(DbDir::initialize(&root).is_err());
}
