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
    ArchiveOptions, CellValue, FieldId, FieldSchema, Observation, ObservationEntry, OpenOptions,
    RestoreBuilder, SeriesId, TableSpec, Validity, ValueType,
    fsutil::{PublishStep, TestDir},
};
use std::{fs, path::Path};
fn id() -> SharedDbId {
    SharedDbId::new([9; 16], [7; 16])
}
fn schema() -> TableSpec {
    TableSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap()
}
fn row(ts: i64) -> Observation {
    Observation::new(
        ts,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            CellValue::UInt(ts.cast_unsigned()),
        )],
    )
    .unwrap()
}
fn product(root: &Path) -> std::path::PathBuf {
    let owner = SharedWal::open(&root.join("source-owner"), SharedWalOptions::default()).unwrap();
    let db = owner
        .open_db_with_archive(
            &root.join("source-db"),
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    let table = db.create_table(schema()).unwrap();
    db.append(table, &row(1)).unwrap();
    db.sync().unwrap();
    let frozen = db
        .prepare_base_with_id(&root.join("base"), [3; 16])
        .unwrap();
    let base = db.describe_base(&frozen).unwrap();
    db.append(table, &row(2)).unwrap();
    db.sync().unwrap();
    let chunk = db.export_durable(base.cursor(), 4096).unwrap();
    let mut restore = RestoreBuilder::install(
        &base,
        &root.join("base"),
        &root.join("restore"),
        OpenOptions::default(),
    )
    .unwrap();
    restore.apply_archive(&chunk).unwrap();
    let result = restore.finish().unwrap();
    db.finish_base(frozen.id()).unwrap();
    drop(db);
    drop(owner);
    fs::remove_dir_all(root.join("source-owner")).unwrap();
    fs::remove_dir_all(root.join("source-db")).unwrap();
    result.directory().to_path_buf()
}
#[test]
fn moved_unbound_product_installs_under_new_shared_owner_and_archive_retry() {
    let root = TestDir::new("shared-install");
    let ready = product(root.path());
    assert!(ready.join("SEALED").exists());
    assert!(!ready.join("SHARED").exists());
    assert!(!ready.join("_shared").exists());
    let target = root.path().join("final-host");
    fs::rename(ready, &target).unwrap();
    let owner = SharedWal::open(
        &root.path().join("target-owner"),
        SharedWalOptions::default(),
    )
    .unwrap();
    assert!(
        owner
            .open_db(&target, id(), OpenOptions::default())
            .is_err()
    );
    let db = owner
        .install_restored_with_archive(
            &target,
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    let table = crate::TableId::new(1);
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), Some(2));
    let branch = db.archive_status().unwrap().durable_end();
    db.append(table, &row(3)).unwrap();
    db.sync().unwrap();
    let durable = db.archive_status().unwrap().durable_end();
    drop(db);
    let db = owner
        .install_restored_with_archive(
            &target,
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    assert_eq!(db.archive_status().unwrap().durable_end(), durable);
    assert!(durable.covers(branch).unwrap());
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), Some(3));
}
#[test]
fn imported_member_and_binding_faults_reopen_and_install_twice() {
    for shared in [false, true] {
        for step in [
            PublishStep::Write,
            PublishStep::FileSync,
            PublishStep::Rename,
            PublishStep::DirectorySync,
        ] {
            let root = TestDir::new("import-fault");
            let target = product(root.path());
            let owner_path = root.path().join("target-owner");
            let owner = SharedWal::open(&owner_path, SharedWalOptions::default()).unwrap();
            owner.lock().unwrap().registration_fault = Some((id(), shared, step));
            assert!(
                owner
                    .install_restored(&target, id(), OpenOptions::default())
                    .is_err()
            );
            drop(owner);
            for _ in 0..2 {
                let owner = SharedWal::open(&owner_path, SharedWalOptions::default()).unwrap();
                let db = owner
                    .install_restored(&target, id(), OpenOptions::default())
                    .unwrap();
                assert_eq!(
                    db.snapshot()
                        .table_last_timestamp(crate::TableId::new(1))
                        .unwrap(),
                    Some(2)
                );
            }
        }
    }
}
#[test]
fn explicit_discard_withdraws_unpublished_archive_but_respects_base_pins() {
    let root = TestDir::new("discard-archive");
    let owner_path = root.path().join("owner");
    let path = root.path().join("candidate");
    let owner = SharedWal::open(&owner_path, SharedWalOptions::default()).unwrap();
    let db = owner
        .open_db_with_archive(
            &path,
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    let table = db.create_table(schema()).unwrap();
    db.append(table, &row(1)).unwrap();
    db.sync().unwrap();
    let frozen = db
        .prepare_base_with_id(&root.path().join("pin"), [4; 16])
        .unwrap();
    drop(db);
    assert!(owner.discard_unpublished(&path, id()).is_err());
    let db = owner
        .open_db_with_archive(
            &path,
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    db.finish_base(frozen.id()).unwrap();
    drop(db);
    owner.discard_unpublished(&path, id()).unwrap();
    fs::remove_dir_all(&path).unwrap();
    drop(owner);
    for _ in 0..2 {
        let owner = SharedWal::open(&owner_path, SharedWalOptions::default()).unwrap();
        owner.discard_unpublished(&path, id()).unwrap();
    }
}
#[test]
fn normal_shared_open_cannot_open_bound_restore_work() {
    let root = TestDir::new("restore-isolation");
    let owner = SharedWal::open(&root.path().join("owner"), SharedWalOptions::default()).unwrap();
    let path = root.path().join("work");
    let db = owner.open_db(&path, id(), OpenOptions::default()).unwrap();
    drop(db);
    fs::write(path.join("RESTORE-WORK"), b"uncommitted").unwrap();
    assert!(owner.open_db(&path, id(), OpenOptions::default()).is_err());
}

#[test]
fn mixed_members_share_one_commit_and_new_member_open_does_not_flush_others() {
    let root = TestDir::new("mixed-group");
    let owner = SharedWal::open(&root.path().join("owner"), SharedWalOptions::default()).unwrap();
    let a = owner
        .open_db_with_archive(
            &root.path().join("a"),
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    let ta = a.create_table(schema()).unwrap();
    let b = owner
        .open_db(
            &root.path().join("b"),
            SharedDbId::new([1; 16], [2; 16]),
            OpenOptions::default(),
        )
        .unwrap();
    let tb = b.create_table(schema()).unwrap();
    a.append(ta, &row(1)).unwrap();
    b.append(tb, &row(8)).unwrap();
    assert_eq!(owner.maintenance_status().unwrap().write_calls(), 0);
    owner.sync().unwrap();
    a.sync().unwrap();
    b.sync().unwrap();
    let status = owner.maintenance_status().unwrap();
    assert_eq!(status.write_calls(), 1);
    assert_eq!(status.sync_calls(), 1);
    assert_eq!(a.archive_status().unwrap().durable_end().seq, 2);
    assert!(!root.path().join("a/archive").exists());
    assert!(!root.path().join("b/ARCHIVE").exists());
}
#[test]
fn export_catchup_releases_scratch_and_new_delta_does_not_reindex_old_history() {
    let root = TestDir::new("export-delta");
    let owner = SharedWal::open(&root.path().join("owner"), SharedWalOptions::default()).unwrap();
    let db = owner
        .open_db_with_archive(
            &root.path().join("db"),
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    let mut cursor = db.archive_status().unwrap().earliest();
    let table = db.create_table(schema()).unwrap();
    for ts in 1..=30 {
        db.append(table, &row(ts)).unwrap();
    }
    db.sync().unwrap();
    let end = db.archive_status().unwrap().durable_end();
    while cursor != end {
        cursor = db.export_durable(cursor, 400).unwrap().end();
    }
    assert_eq!(owner.lock().unwrap().scratch_bytes, 0);
    for ts in 31..=50 {
        db.append(table, &row(ts)).unwrap();
    }
    db.sync().unwrap();
    let chunk = db.export_durable(cursor, 400).unwrap();
    assert!(chunk.end().seq < 51);
    assert_eq!(owner.lock().unwrap().scratch_bytes, 20 * 16);
    drop(db);
    assert_eq!(owner.lock().unwrap().scratch_bytes, 0);
}
#[test]
fn cloned_sealed_products_start_distinct_archive_branches() {
    let root = TestDir::new("clone-branches");
    let ready = product(root.path());
    let marker = fs::read(ready.join("SEALED")).unwrap();
    let descriptor = crate::BaseDescriptor::from_bytes(&marker).unwrap();
    let copy = root.path().join("clone");
    fs::create_dir(&copy).unwrap();
    for file in descriptor.files() {
        let target = copy.join(file.relative_path());
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(ready.join(file.relative_path()), target).unwrap();
    }
    fs::write(copy.join("SEALED"), marker).unwrap();
    let a_owner =
        SharedWal::open(&root.path().join("a-owner"), SharedWalOptions::default()).unwrap();
    let b_owner =
        SharedWal::open(&root.path().join("b-owner"), SharedWalOptions::default()).unwrap();
    let a = a_owner
        .install_restored_with_archive(
            &ready,
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    let b = b_owner
        .install_restored_with_archive(
            &copy,
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    let ca = a.archive_status().unwrap().durable_end();
    let cb = b.archive_status().unwrap().durable_end();
    assert_ne!(ca.branch, cb.branch);
    assert_ne!(ca.branch, descriptor.cursor().branch);
    assert!(ca.covers(cb).is_err());
    let table = crate::TableId::new(1);
    a.append(table, &row(3)).unwrap();
    b.append(table, &row(4)).unwrap();
    a.sync().unwrap();
    b.sync().unwrap();
    assert!(
        a.archive_status()
            .unwrap()
            .durable_end()
            .covers(b.archive_status().unwrap().durable_end())
            .is_err()
    );
}
#[test]
fn base_cannot_be_created_in_any_managed_owner_subdirectory() {
    let root = TestDir::new("base-owner-path");
    let owner_path = root.path().join("owner");
    let owner = SharedWal::open(&owner_path, SharedWalOptions::default()).unwrap();
    let db = owner
        .open_db_with_archive(
            &root.path().join("db"),
            id(),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )
        .unwrap();
    db.create_table(schema()).unwrap();
    for area in ["tmp", "wal"] {
        let target = owner_path.join(area).join("base");
        assert!(db.prepare_base_with_id(&target, [4; 16]).is_err());
        assert!(!target.exists());
        assert!(db.pending_bases().unwrap().is_empty());
    }
    assert_eq!(owner.maintenance_status().unwrap().write_calls(), 0);
}
