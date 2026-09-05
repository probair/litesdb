// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{DAMAGED_DIRECTORY, frozen_boundary, measure, preserve, reclaim};
use crate::{
    ErrorKind,
    fsutil::{Area, DbDir, TestDir},
    wal::segment::{SegmentHeader, segment_name},
};
use std::{fs, os::unix::fs::MetadataExt};

fn fixture(label: &str) -> (TestDir, DbDir, Vec<u8>) {
    let temporary = TestDir::new(label);
    let directory =
        DbDir::initialize(&temporary.path().join("db")).unwrap_or_else(|_| unreachable!());
    let mut original = SegmentHeader::new(1, 0, 0).encode().to_vec();
    original.extend_from_slice(&[7_u8; 100]);
    fs::write(directory.file(Area::Wal, &segment_name(1)), &original)
        .unwrap_or_else(|_| unreachable!());
    (temporary, directory, original)
}

#[test]
fn retained_original_is_immutable_and_counted_once() {
    let (_temporary, directory, original) = fixture("wal-storage-links");
    let wal = directory.path(Area::Wal);
    assert_eq!(preserve(&wal, 1, 32, 1).ok(), Some(true));
    assert_eq!(preserve(&wal, 1, 32, 1).ok(), Some(false));
    assert_eq!(measure(&wal).ok(), Some(132));
    let retained = fs::read_dir(wal.join(DAMAGED_DIRECTORY))
        .unwrap_or_else(|_| unreachable!())
        .next()
        .unwrap_or_else(|| unreachable!())
        .unwrap_or_else(|_| unreachable!())
        .path();
    assert_eq!(fs::read(&retained).ok(), Some(original));
    let active = fs::metadata(wal.join(segment_name(1))).unwrap_or_else(|_| unreachable!());
    let archived = fs::metadata(&retained).unwrap_or_else(|_| unreachable!());
    assert_eq!(
        (active.dev(), active.ino()),
        (archived.dev(), archived.ino())
    );
    assert_eq!(
        frozen_boundary(&wal, 1)
            .ok()
            .flatten()
            .map(|b| (b.offset, b.next_seq)),
        Some((32, 1))
    );
    assert_eq!(
        reclaim(&wal, 32, 150).err().map(|e| e.kind()),
        Some(ErrorKind::ResourceExhausted)
    );
    assert!(retained.exists());
}

#[test]
fn replaced_empty_segment_can_release_old_evidence() {
    let (_temporary, directory, original) = fixture("wal-storage-replace");
    let wal = directory.path(Area::Wal);
    assert!(preserve(&wal, 1, 32, 1).is_ok());
    fs::remove_file(wal.join(segment_name(1))).unwrap_or_else(|_| unreachable!());
    fs::write(
        wal.join(segment_name(1)),
        SegmentHeader::new(1, 0, 0).encode(),
    )
    .unwrap_or_else(|_| unreachable!());
    assert_eq!(
        measure(&wal).ok(),
        Some(u64::try_from(original.len() + 32).unwrap_or(u64::MAX))
    );
    assert!(frozen_boundary(&wal, 1).ok().flatten().is_none());
    assert_eq!(reclaim(&wal, 32, 100).ok(), Some(32));
    assert_eq!(
        fs::read_dir(wal.join(DAMAGED_DIRECTORY))
            .ok()
            .map(Iterator::count),
        Some(0)
    );
}

#[test]
fn archive_reclamation_is_oldest_first() {
    let (_temporary, directory, _) = fixture("wal-storage-order");
    let wal = directory.path(Area::Wal);
    assert!(preserve(&wal, 1, 32, 1).is_ok());
    fs::remove_file(wal.join(segment_name(1))).unwrap_or_else(|_| unreachable!());
    fs::write(
        wal.join(segment_name(2)),
        SegmentHeader::new(2, 0, 0).encode(),
    )
    .unwrap_or_else(|_| unreachable!());
    assert!(preserve(&wal, 2, 32, 2).is_ok());
    fs::remove_file(wal.join(segment_name(2))).unwrap_or_else(|_| unreachable!());
    assert_eq!(reclaim(&wal, 0, 32).ok(), Some(32));
    let retained = super::archives(&wal).unwrap_or_else(|_| unreachable!());
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].boundary.first_seq, 2);
}

#[test]
fn changing_cutoff_invalidates_recovery_evidence() {
    let (_temporary, directory, _) = fixture("wal-storage-descriptor");
    let wal = directory.path(Area::Wal);
    assert!(preserve(&wal, 1, 32, 1).is_ok());
    let mut retained = super::archives(&wal).unwrap_or_else(|_| unreachable!());
    let archived = retained.pop().unwrap_or_else(|| unreachable!());
    let mut altered = archived.boundary;
    altered.offset = 33;
    fs::rename(
        &archived.path,
        wal.join(DAMAGED_DIRECTORY).join(altered.name()),
    )
    .unwrap_or_else(|_| unreachable!());
    assert!(frozen_boundary(&wal, 1).ok().flatten().is_none());
}

#[test]
fn preservation_failures_keep_original_and_retry_is_durable() {
    use super::PreserveStep;
    for step in [
        PreserveStep::ParentSync,
        PreserveStep::OriginalSync,
        PreserveStep::Link,
        PreserveStep::ArchiveSync,
    ] {
        let (_temporary, directory, original) = fixture("wal-storage-fault");
        let wal = directory.path(Area::Wal);
        let result = super::preserve_with_hook(&wal, 1, 32, 1, |at| {
            if at == step {
                Err(std::io::Error::other("injected archive failure"))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err(), "{step:?}");
        if step == PreserveStep::ArchiveSync {
            assert!(frozen_boundary(&wal, 1).ok().flatten().is_some());
        }
        assert_eq!(fs::read(wal.join(segment_name(1))).ok(), Some(original));
        assert!(preserve(&wal, 1, 32, 1).is_ok());
        assert_eq!(preserve(&wal, 1, 32, 1).ok(), Some(false));
        assert!(frozen_boundary(&wal, 1).ok().flatten().is_some());
        assert_eq!(measure(&wal).ok(), Some(132));
    }
}
