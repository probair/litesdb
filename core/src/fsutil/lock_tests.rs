// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::fs;

use super::DbLock;
use crate::{ErrorKind, fsutil::testutil::TestDir};

#[test]
fn lock_is_exclusive_and_drop_releases_it() {
    let temporary = TestDir::new("lock");
    let root = temporary.path();
    let Ok(first) = DbLock::acquire(root) else {
        unreachable!("first lock acquisition failed");
    };

    let Err(error) = DbLock::acquire(root) else {
        unreachable!("second lock acquisition unexpectedly succeeded");
    };
    assert_eq!(error.kind(), ErrorKind::Io);

    drop(first);
    assert!(DbLock::acquire(root).is_ok());
    assert_eq!(
        fs::metadata(root.join("LOCK")).map(|meta| meta.len()).ok(),
        Some(0)
    );
}

#[test]
fn nonempty_lock_file_is_rejected_as_corruption() {
    let temporary = TestDir::new("lock-content");
    assert!(fs::write(temporary.path().join("LOCK"), b"unexpected").is_ok());

    let Err(error) = DbLock::acquire(temporary.path()) else {
        unreachable!();
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn lock_guard_is_sendable_between_owner_threads() {
    fn assert_send<T: Send>() {}
    assert_send::<DbLock>();
}
