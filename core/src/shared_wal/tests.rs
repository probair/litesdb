// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]
use super::*;
use crate::{
    CellValue, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, TableSpec, Validity,
    ValueType,
    fsutil::{Area, PublishStep, TestDir},
};
use std::{fs, io::Write};

fn id(value: u8) -> SharedDbId {
    SharedDbId::new([value; 16], [value.wrapping_add(1); 16])
}
fn spec() -> TableSpec {
    TableSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap()
}
fn observation(timestamp: i64) -> Observation {
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            CellValue::UInt(u64::try_from(timestamp).unwrap()),
        )],
    )
    .unwrap()
}
fn small() -> SharedWalOptions {
    SharedWalOptions {
        max_bytes: 32_768,
        segment_bytes: 512,
        buffer_bytes: 1024,
        ..SharedWalOptions::default()
    }
}

#[test]
fn interleaved_members_share_one_write_and_sync_with_local_sequences() {
    let root = TestDir::new("shared-wal");
    let owner = SharedWal::open(&root.path().join("owner"), SharedWalOptions::default()).unwrap();
    let a = owner
        .open_db(&root.path().join("a"), id(1), OpenOptions::default())
        .unwrap();
    let b = owner
        .open_db(&root.path().join("b"), id(2), OpenOptions::default())
        .unwrap();
    let ta = a.create_table(spec()).unwrap();
    let tb = b.create_table(spec()).unwrap();
    assert_eq!(ta, tb);
    assert_eq!(a.append(ta, &observation(1)).unwrap().get(), 2);
    assert_eq!(b.append(tb, &observation(10)).unwrap().get(), 2);
    assert_eq!(owner.maintenance_status().unwrap().write_calls(), 0);
    assert_eq!(owner.sync().unwrap().lsn(), 4);
    assert_eq!(a.sync().unwrap().seq(), 2);
    assert_eq!(b.sync().unwrap().seq(), 2);
    let status = owner.maintenance_status().unwrap();
    assert_eq!(status.write_calls(), 1);
    assert_eq!(status.sync_calls(), 1);
    b.append(tb, &observation(11)).unwrap();
    assert_eq!(b.maintenance_status().unwrap().durable_seq(), 2);
    assert_eq!(a.sync().unwrap().seq(), 2);
    assert_eq!(owner.maintenance_status().unwrap().sync_calls(), 1);
    b.sync().unwrap();
    assert_eq!(owner.maintenance_status().unwrap().sync_calls(), 2);
    assert_eq!(fs::read_dir(root.path().join("a/wal")).unwrap().count(), 0);
    drop(a);
    drop(b);
    drop(owner);
    let owner = SharedWal::open(&root.path().join("owner"), SharedWalOptions::default()).unwrap();
    let a = owner
        .open_db(&root.path().join("a"), id(1), OpenOptions::default())
        .unwrap();
    let b = owner
        .open_db(&root.path().join("b"), id(2), OpenOptions::default())
        .unwrap();
    assert_eq!(a.snapshot().table_last_timestamp(ta).unwrap(), Some(1));
    assert_eq!(b.snapshot().table_last_timestamp(tb).unwrap(), Some(11));
}
#[test]
fn closed_member_blocks_gc_until_its_independent_checkpoint() {
    let root = TestDir::new("shared-wal");
    let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
    let a = owner
        .open_db(&root.path().join("a"), id(1), OpenOptions::default())
        .unwrap();
    let b = owner
        .open_db(&root.path().join("b"), id(2), OpenOptions::default())
        .unwrap();
    let ta = a.create_table(spec()).unwrap();
    let tb = b.create_table(spec()).unwrap();
    b.append(tb, &observation(1)).unwrap();
    b.sync().unwrap();
    drop(b);
    for ts in 1..15 {
        a.append(ta, &observation(ts)).unwrap();
    }
    a.seal().unwrap();
    assert!(owner.maintenance_blockers(8).unwrap().contains(&id(2)));
    let count = fs::read_dir(root.path().join("owner/wal")).unwrap().count();
    assert!(count > 1);
    drop(a);
    drop(owner);
    let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
    let b = owner
        .open_db(&root.path().join("b"), id(2), OpenOptions::default())
        .unwrap();
    assert_eq!(b.snapshot().table_last_timestamp(tb).unwrap(), Some(1));
    b.seal().unwrap();
    assert!(fs::read_dir(root.path().join("owner/wal")).unwrap().count() < count);
}
#[test]
fn holding_one_database_engine_does_not_hold_shared_writer() {
    let root = TestDir::new("shared-wal");
    let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
    let a = owner
        .open_db(&root.path().join("a"), id(1), OpenOptions::default())
        .unwrap();
    let b = owner
        .open_db(&root.path().join("b"), id(2), OpenOptions::default())
        .unwrap();
    let guard = a.lock_engine().unwrap();
    let table = b.create_table(spec()).unwrap();
    b.append(table, &observation(1)).unwrap();
    b.sync().unwrap();
    drop(guard);
}
#[test]
fn ambiguous_physical_failure_poison_is_owner_wide() {
    for sync_failure in [false, true] {
        let root = TestDir::new("shared-wal");
        let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
        let a = owner
            .open_db(&root.path().join("a"), id(1), OpenOptions::default())
            .unwrap();
        let b = owner
            .open_db(&root.path().join("b"), id(2), OpenOptions::default())
            .unwrap();
        a.create_table(spec()).unwrap();
        {
            let mut state = owner.lock().unwrap();
            state.fail_sync = sync_failure;
            state.fail_write = !sync_failure;
        }
        assert_eq!(a.sync().unwrap_err().kind(), crate::ErrorKind::Poisoned);
        assert_eq!(
            b.create_table(spec()).unwrap_err().kind(),
            crate::ErrorKind::Poisoned
        );
    }
}
#[test]
fn failed_manifest_publication_keeps_records_for_two_reopens() {
    for step in [
        PublishStep::Write,
        PublishStep::FileSync,
        PublishStep::Rename,
        PublishStep::DirectorySync,
    ] {
        let root = TestDir::new("shared-wal");
        let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
        let a = owner
            .open_db(&root.path().join("a"), id(1), OpenOptions::default())
            .unwrap();
        let table = a.create_table(spec()).unwrap();
        a.append(table, &observation(7)).unwrap();
        a.sync().unwrap();
        a.directory.fail_publish(Area::Root, "MANIFEST", step);
        assert!(a.seal().is_err());
        drop(a);
        drop(owner);
        for _ in 0..2 {
            let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
            let a = owner
                .open_db(&root.path().join("a"), id(1), OpenOptions::default())
                .unwrap();
            assert_eq!(a.snapshot().table_last_timestamp(table).unwrap(), Some(7));
        }
    }
}
#[test]
fn torn_final_record_preserves_original_and_boundary_on_two_reopens() {
    let root = TestDir::new("shared-wal");
    let owner_path = root.path().join("owner");
    let owner = SharedWal::open(&owner_path, small()).unwrap();
    let a = owner
        .open_db(&root.path().join("a"), id(1), OpenOptions::default())
        .unwrap();
    let table = a.create_table(spec()).unwrap();
    a.append(table, &observation(5)).unwrap();
    a.sync().unwrap();
    drop(a);
    drop(owner);
    let segment = owner_path.join("wal").join(format::segment_name(1));
    fs::OpenOptions::new()
        .append(true)
        .open(&segment)
        .unwrap()
        .write_all(&[1, 2, 3])
        .unwrap();
    let original = fs::read(&segment).unwrap();
    for ts in 6..8 {
        let owner = SharedWal::open(&owner_path, small()).unwrap();
        let a = owner
            .open_db(&root.path().join("a"), id(1), OpenOptions::default())
            .unwrap();
        assert_eq!(
            a.snapshot().table_last_timestamp(table).unwrap(),
            Some(ts - 1)
        );
        a.append(table, &observation(ts)).unwrap();
        a.sync().unwrap();
        assert_eq!(fs::read(&segment).unwrap(), original);
    }
}
#[test]
fn old_unbound_manifest_is_rejected_without_registration() {
    let root = TestDir::new("shared-wal");
    let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
    let path = root.path().join("old");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("MANIFEST"), b"old bytes").unwrap();
    assert!(owner.open_db(&path, id(1), OpenOptions::default()).is_err());
    assert!(!path.join("SHARED").exists());
    assert_eq!(fs::read(path.join("MANIFEST")).unwrap(), b"old bytes");
    assert_eq!(owner.lock().unwrap().members.len(), 0);
}

#[test]
fn reopening_member_flushes_its_accepted_buffer_before_recovery() {
    let root = TestDir::new("buffered-reopen");
    let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
    let path = root.path().join("a");
    let a = owner.open_db(&path, id(1), OpenOptions::default()).unwrap();
    let table = a.create_table(spec()).unwrap();
    a.append(table, &observation(9)).unwrap();
    drop(a);
    assert_eq!(owner.maintenance_status().unwrap().write_calls(), 0);
    let a = owner.open_db(&path, id(1), OpenOptions::default()).unwrap();
    assert_eq!(a.snapshot().table_last_timestamp(table).unwrap(), Some(9));
    a.append(table, &observation(10)).unwrap();
    a.sync().unwrap();
}
#[test]
fn too_small_rotation_budget_is_rejected_before_creating_owner() {
    let root = TestDir::new("rotation-budget");
    let path = root.path().join("owner");
    let options = SharedWalOptions {
        max_bytes: 512,
        segment_bytes: 512,
        ..small()
    };
    assert!(SharedWal::open(&path, options).is_err());
    assert!(!path.exists());
    let owner = SharedWal::open(
        &path,
        SharedWalOptions {
            max_bytes: 1024,
            ..small()
        },
    )
    .unwrap();
    let a = owner
        .open_db(&root.path().join("a"), id(1), OpenOptions::default())
        .unwrap();
    let table = a.create_table(spec()).unwrap();
    for ts in 1..30 {
        a.append(table, &observation(ts)).unwrap();
        a.seal().unwrap();
    }
    assert!(owner.maintenance_status().unwrap().storage_bytes() <= 1024);
}
#[test]
fn try_batch_is_busy_without_partial_mutation() {
    let root = TestDir::new("try-batch");
    let db = Db::open(root.path(), OpenOptions::default()).unwrap();
    let table = db.create_table(spec()).unwrap();
    let guard = db.lock_engine().unwrap();
    assert!(
        db.try_append_batch(table, &[observation(1), observation(2)])
            .unwrap()
            .is_none()
    );
    drop(guard);
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), None);
    assert_eq!(
        db.try_append_batch(table, &[observation(1), observation(2)])
            .unwrap()
            .unwrap()
            .get(),
        3
    );
}
#[test]
fn owner_authority_allocation_is_bounded_before_reading() {
    let root = TestDir::new("owner-size");
    let path = root.path().join("owner");
    let owner = SharedWal::open(&path, small()).unwrap();
    drop(owner);
    fs::OpenOptions::new()
        .write(true)
        .open(path.join("OWNER"))
        .unwrap()
        .set_len(1_000_000)
        .unwrap();
    assert!(SharedWal::open(&path, small()).is_err());
    assert_eq!(fs::metadata(path.join("OWNER")).unwrap().len(), 1_000_000);
}
