// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    CellValue, Db, ErrorKind, FieldId, FieldSchema, Lookup, Observation, ObservationEntry,
    OpenOptions, SealPolicy, SeriesId, SharedDbId, SharedWal, SharedWalOptions, StreamKey,
    SyncPolicy, TableId, Validity, ValueType, VersionSpec,
    fsutil::{DbDir, TestDir},
    manifest,
};
use std::{
    fs,
    path::{Path, PathBuf},
};

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|_| unreachable!("valid schema"))
}
fn observation(timestamp: i64) -> Observation {
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            CellValue::UInt(u64::try_from(timestamp).unwrap_or_default()),
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid observation"))
}
fn catalog(root: &Path) -> manifest::Manifest {
    manifest::load(&DbDir::initialize(root).unwrap_or_else(|_| unreachable!("directory")))
        .unwrap_or_else(|_| unreachable!("catalog"))
}
fn segments(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut paths = fs::read_dir(root.join("_shared/wal"))
        .unwrap_or_else(|_| unreachable!("shared WAL"))
        .map(|entry| entry.unwrap_or_else(|_| unreachable!("entry")).path())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path).unwrap_or_else(|_| unreachable!("segment bytes"));
            (path, bytes)
        })
        .collect()
}
fn latest(database: &Db, table: TableId, timestamp: i64) {
    assert_eq!(
        database
            .snapshot()
            .latest(&[StreamKey::new(table, SeriesId::new(1), FieldId::new(1))])
            .ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(u64::try_from(timestamp).unwrap_or_default()),
            at_ts: timestamp
        }])
    );
}
fn populated(root: &Path) -> TableId {
    let database = Db::open(root, OpenOptions::default()).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("table"));
    for timestamp in [10, 20] {
        database
            .append(table, &observation(timestamp))
            .unwrap_or_else(|_| unreachable!("append"));
    }
    database.sync().unwrap_or_else(|_| unreachable!("sync"));
    table
}

#[test]
fn takeover_advances_identity_and_keeps_old_records_readable() {
    let root = TestDir::new("shared-takeover-advance");
    let table = populated(root.path());
    let before = catalog(root.path());
    let physical = segments(root.path());
    let database = Db::open(
        root.path(),
        OpenOptions {
            takeover: true,
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("takeover"));
    let after = catalog(root.path());
    assert_eq!(
        after.identity().generation(),
        before.identity().generation().saturating_add(1)
    );
    assert_eq!(
        after.identity().writer_epoch(),
        before.identity().writer_epoch().saturating_add(1)
    );
    assert_eq!(segments(root.path()), physical);
    latest(&database, table, 20);
    drop(database);
    for _ in 0..2 {
        let database = Db::open(root.path(), OpenOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen"));
        latest(&database, table, 20);
        assert_eq!(catalog(root.path()), after);
    }
}

#[test]
fn forged_shared_database_or_generation_is_corruption() {
    for identity_offset in [16_usize, 32] {
        let root = TestDir::new("shared-takeover-forged-id");
        populated(root.path());
        let (path, mut bytes) = segments(root.path()).remove(0);
        let start = 64_usize;
        let length = u32::from_le_bytes(
            bytes[start..start.saturating_add(4)]
                .try_into()
                .unwrap_or_else(|_| unreachable!("frame length")),
        ) as usize;
        let identity_byte = start.saturating_add(identity_offset).saturating_add(15);
        bytes[identity_byte] ^= 0x80;
        let end = start.saturating_add(length);
        let crc = crc32fast::hash(&bytes[start.saturating_add(8)..end]);
        bytes[start.saturating_add(4)..start.saturating_add(8)].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &bytes).unwrap_or_else(|_| unreachable!("forge identity"));
        for _ in 0..2 {
            assert_eq!(
                Db::open(root.path(), OpenOptions::default())
                    .err()
                    .map(|error| error.kind()),
                Some(ErrorKind::Corruption)
            );
            assert_eq!(fs::read(&path).ok(), Some(bytes.clone()));
        }
    }
}

#[test]
fn reopen_preserves_manifest_first_takeover_exactly_once() {
    let root = TestDir::new("shared-takeover-interrupted");
    let table = populated(root.path());
    let directory = DbDir::initialize(root.path()).unwrap_or_else(|_| unreachable!("directory"));
    let current = catalog(root.path());
    let next = current
        .successor_writer_epoch()
        .unwrap_or_else(|_| unreachable!("successor"));
    manifest::publish(&directory, Some(current.identity().generation()), &next)
        .unwrap_or_else(|_| unreachable!("publish epoch"));
    let physical = segments(root.path());
    for _ in 0..2 {
        let database = Db::open(root.path(), OpenOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen"));
        latest(&database, table, 20);
        assert_eq!(catalog(root.path()), next);
        assert_eq!(segments(root.path()), physical);
    }
}

#[test]
fn closed_old_generation_and_new_generation_never_share_facts_or_bindings() {
    let root = TestDir::new("shared-generation-isolation");
    let old_id = SharedDbId::new([1; 16], [2; 16]);
    let new_id = SharedDbId::new([1; 16], [3; 16]);
    let old_path = root.path().join("old");
    let new_path = root.path().join("new");
    let wal_path = root.path().join("owner");
    let owner = SharedWal::open(&wal_path, SharedWalOptions::default())
        .unwrap_or_else(|_| unreachable!("owner"));
    let old = owner
        .open_db(&old_path, old_id, OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("old"));
    let old_table = old
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("old table"));
    old.append(old_table, &observation(10))
        .unwrap_or_else(|_| unreachable!("old append"));
    old.sync().unwrap_or_else(|_| unreachable!("old sync"));
    drop(old);
    let binding = fs::read(old_path.join("SHARED")).unwrap_or_default();
    assert!(
        owner
            .open_db(&old_path, new_id, OpenOptions::default())
            .is_err()
    );
    assert_eq!(
        fs::read(old_path.join("SHARED")).unwrap_or_default(),
        binding
    );
    let new = owner
        .open_db(&new_path, new_id, OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("new"));
    let new_table = new
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("new table"));
    assert_eq!(new_table, old_table);
    new.append(new_table, &observation(100))
        .unwrap_or_else(|_| unreachable!("new append"));
    new.sync().unwrap_or_else(|_| unreachable!("new sync"));
    drop(new);
    drop(owner);
    for _ in 0..2 {
        let owner = SharedWal::open(&wal_path, SharedWalOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen owner"));
        let old = owner
            .open_db(&old_path, old_id, OpenOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen old"));
        let new = owner
            .open_db(&new_path, new_id, OpenOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen new"));
        latest(&old, old_table, 10);
        latest(&new, new_table, 100);
        assert_eq!(
            old.maintenance_status().unwrap_or_default().visible_seq(),
            new.maintenance_status().unwrap_or_default().visible_seq()
        );
    }
}

#[test]
fn reopened_low_watermark_seals_without_losing_recovered_records() {
    let root = TestDir::new("shared-reopen-maintenance");
    let table = populated(root.path());
    let before = catalog(root.path());
    let (database, report) = Db::open_with_report(
        root.path(),
        OpenOptions {
            sync_policy: SyncPolicy::Manual,
            seal_policy: SealPolicy {
                bytes: 90,
                ..SealPolicy::default()
            },
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("reopen"));
    assert_eq!(report.replayed_records(), 3);
    assert_eq!(report.recovery_checkpointed_records(), 0);
    assert_eq!(catalog(root.path()), before);
    let sealed = database
        .maintain()
        .unwrap_or_else(|_| unreachable!("maintain"))
        .sealed()
        .unwrap_or_else(|| unreachable!("scheduled seal"));
    assert_eq!(sealed.checkpointed_records(), 3);
    assert_eq!(sealed.unit_id(), Some(1));
    assert_eq!(
        database
            .maintenance_status()
            .unwrap_or_default()
            .wal_bytes(),
        0
    );
    latest(&database, table, 20);
    assert_eq!(
        catalog(root.path()).identity().writer_epoch(),
        before.identity().writer_epoch()
    );
}

#[test]
fn metadata_only_takeover_checkpoints_without_creating_a_unit() {
    let root = TestDir::new("shared-takeover-metadata");
    let database =
        Db::open(root.path(), OpenOptions::default()).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    database.sync().unwrap_or_else(|_| unreachable!("sync"));
    drop(database);
    let before = catalog(root.path());
    let database = Db::open(
        root.path(),
        OpenOptions {
            takeover: true,
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("takeover"));
    let sealed = database
        .seal()
        .unwrap_or_else(|_| unreachable!("seal metadata"));
    assert_eq!(sealed.checkpointed_records(), 1);
    assert_eq!(sealed.unit_id(), None);
    assert_eq!(
        catalog(root.path()).identity().writer_epoch(),
        before.identity().writer_epoch().saturating_add(1)
    );
    assert_eq!(catalog(root.path()).units().len(), 0);
    assert_eq!(
        database.snapshot().table_last_timestamp(table).ok(),
        Some(None)
    );
    assert_eq!(
        database
            .maintenance_status()
            .unwrap_or_default()
            .wal_bytes(),
        0
    );
}
