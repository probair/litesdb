// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, io};

use super::{PublishStep, publish_atomically, publish_with_hook};
use crate::{
    ErrorKind,
    fsutil::{
        dir::{Area, DbDir},
        testutil::TestDir,
    },
};

fn database(label: &str) -> (TestDir, DbDir) {
    let temporary = TestDir::new(label);
    let Ok(directory) = DbDir::initialize(&temporary.path().join("db")) else {
        unreachable!("test directory initialization failed");
    };
    (temporary, directory)
}

fn fail_at(step: PublishStep) -> impl FnMut(PublishStep) -> io::Result<()> {
    move |current| {
        if current == step {
            Err(io::Error::other("injected publish failure"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn rename_is_the_only_visibility_switch() {
    for step in [
        PublishStep::Write,
        PublishStep::FileSync,
        PublishStep::Rename,
    ] {
        let (_temporary, directory) = database("publish-before-rename");
        assert!(publish_atomically(&directory, Area::Root, "MANIFEST", b"old").is_ok());

        assert!(
            publish_with_hook(&directory, Area::Root, "MANIFEST", b"new", fail_at(step),).is_err()
        );
        assert_eq!(
            fs::read(directory.file(Area::Root, "MANIFEST"))
                .ok()
                .as_deref(),
            Some(b"old".as_slice())
        );
    }
}

#[test]
fn directory_sync_failure_is_an_ambiguous_post_rename_failure() {
    let (_temporary, directory) = database("publish-after-rename");
    assert!(publish_atomically(&directory, Area::Root, "MANIFEST", b"old").is_ok());

    let Err(error) = publish_with_hook(
        &directory,
        Area::Root,
        "MANIFEST",
        b"new",
        fail_at(PublishStep::DirectorySync),
    ) else {
        unreachable!();
    };
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(
        fs::read(directory.file(Area::Root, "MANIFEST"))
            .ok()
            .as_deref(),
        Some(b"new".as_slice())
    );
}

#[test]
fn failed_temporary_files_do_not_block_a_retry() {
    let (_temporary, directory) = database("publish-retry");
    assert!(
        publish_with_hook(
            &directory,
            Area::Units,
            "0000000000000001.lsu",
            b"first",
            fail_at(PublishStep::Rename),
        )
        .is_err()
    );
    assert!(
        publish_atomically(&directory, Area::Units, "0000000000000001.lsu", b"second",).is_ok()
    );
    assert_eq!(
        fs::read(directory.file(Area::Units, "0000000000000001.lsu"))
            .ok()
            .as_deref(),
        Some(b"second".as_slice())
    );
    let Ok(entries) = fs::read_dir(directory.path(Area::Temporary)) else {
        unreachable!();
    };
    assert_eq!(
        entries.count(),
        1,
        "failed temp must remain for reopen cleanup"
    );
}

#[test]
fn target_name_cannot_escape_its_database_area() {
    let (_temporary, directory) = database("publish-name");
    for name in ["", ".", "..", "../MANIFEST", "nested/file"] {
        let Err(error) = publish_atomically(&directory, Area::Root, name, b"x") else {
            unreachable!("invalid name accepted: {name}");
        };
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
    let Ok(mut entries) = fs::read_dir(directory.path(Area::Temporary)) else {
        unreachable!();
    };
    assert!(entries.next().is_none());
}
