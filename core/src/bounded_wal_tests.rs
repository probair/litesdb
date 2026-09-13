// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, path::Path};

use super::{Db, OpenOptions, SealPolicy, SyncPolicy};
use crate::{
    CellValue, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId,
    SharedDbId, SharedWal, SharedWalOptions, Snapshot, StreamKey, TableId, Validity, ValueType,
    VersionSpec, fsutil::TestDir,
};

fn options() -> OpenOptions {
    OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 4096,
            ..SealPolicy::default()
        },
        ..OpenOptions::default()
    }
}

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|error| panic!("schema: {error:?}"))
}

fn observation(timestamp: i64) -> Observation {
    let value = if timestamp % 7 == 0 {
        CellValue::Null
    } else {
        CellValue::UInt(u64::try_from(timestamp).unwrap_or_default())
    };
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            value,
        )],
    )
    .unwrap_or_else(|error| panic!("observation: {error:?}"))
}

fn storage_bytes(root: &Path) -> u64 {
    fs::read_dir(root.join("_shared/wal"))
        .unwrap_or_else(|error| panic!("WAL directory: {error}"))
        .map(|entry| {
            entry
                .and_then(|entry| entry.metadata())
                .unwrap_or_else(|error| panic!("WAL metadata: {error}"))
                .len()
        })
        .sum()
}

fn facts(snapshot: &Snapshot, table: TableId) -> Vec<(i64, CellValue)> {
    let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
    let mut cursor = snapshot
        .scan(key, 0..1000)
        .unwrap_or_else(|error| panic!("scan: {error:?}"));
    let mut result = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|error| panic!("fact: {error:?}"))
    {
        result.push((fact.timestamp(), fact.value()));
    }
    result
}

fn open_shared(root: &Path) -> (SharedWal, Db) {
    let owner = SharedWal::open(
        &root.join("_shared"),
        SharedWalOptions {
            max_bytes: 4096,
            segment_bytes: 512,
            buffer_bytes: 512,
            ..SharedWalOptions::default()
        },
    )
    .unwrap_or_else(|error| panic!("shared owner: {error:?}"));
    let database = owner
        .open_db(root, SharedDbId::new([1; 16], [2; 16]), options())
        .unwrap_or_else(|error| panic!("shared database: {error:?}"));
    (owner, database)
}

#[test]
fn many_budget_cycles_preserve_nulls_snapshots_and_reopens() {
    let root = TestDir::new("bounded-wal-cycles");
    let (owner, database) = open_shared(root.path());
    let table = database
        .create_table(spec())
        .unwrap_or_else(|error| panic!("create: {error:?}"));
    let mut before = None;
    let mut checkpoints = 0_u32;
    for timestamp in 1..=500 {
        let status = database.maintenance_status().unwrap_or_default();
        if let Err(error) = database.append(table, &observation(timestamp)) {
            assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
            assert_eq!(
                database
                    .maintenance_status()
                    .unwrap_or_default()
                    .visible_seq(),
                status.visible_seq()
            );
            assert!(!owner.maintenance_blockers(1).unwrap_or_default().is_empty());
            database
                .seal()
                .unwrap_or_else(|error| panic!("capacity checkpoint: {error:?}"));
            checkpoints = checkpoints.saturating_add(1);
            database
                .append(table, &observation(timestamp))
                .unwrap_or_else(|error| panic!("retry {timestamp}: {error:?}"));
        }
        database
            .sync()
            .unwrap_or_else(|error| panic!("sync: {error:?}"));
        let actual = storage_bytes(root.path());
        assert!(actual <= 4096, "WAL occupies {actual} bytes");
        assert_eq!(
            owner
                .maintenance_status()
                .unwrap_or_default()
                .storage_bytes(),
            actual
        );
        if timestamp == 50 {
            before = Some(database.snapshot());
        }
    }
    assert!(checkpoints >= 4);
    let expected = facts(&database.snapshot(), table);
    assert_eq!(expected.len(), 500);
    let before = before.unwrap_or_else(|| unreachable!("snapshot captured"));
    assert_eq!(facts(&before, table), expected[..50]);
    drop(before);
    drop(database);
    drop(owner);
    for _ in 0..2 {
        let (owner, database) = open_shared(root.path());
        assert_eq!(facts(&database.snapshot(), table), expected);
        assert_eq!(
            owner
                .maintenance_status()
                .unwrap_or_default()
                .storage_bytes(),
            storage_bytes(root.path())
        );
        assert!(storage_bytes(root.path()) <= 4096);
        drop(database);
        drop(owner);
    }
}

#[test]
fn oversized_record_does_not_checkpoint_or_change_wal() {
    let root = TestDir::new("bounded-wal-large-record");
    let (owner, database) = open_shared(root.path());
    let table = database
        .create_table(spec())
        .unwrap_or_else(|error| panic!("create: {error:?}"));
    database
        .append(table, &observation(1))
        .unwrap_or_else(|error| panic!("append: {error:?}"));
    let before = database.maintenance_status().unwrap_or_default();
    let shared_before = owner.maintenance_status().unwrap_or_default();
    let manifest = fs::read(root.path().join("MANIFEST")).unwrap_or_default();
    let oversized = Observation::new(
        2,
        (1..=1024)
            .map(|series| {
                ObservationEntry::new(
                    SeriesId::new(series),
                    FieldId::new(1),
                    CellValue::UInt(u64::MAX),
                )
            })
            .collect(),
    )
    .unwrap_or_else(|error| panic!("large observation: {error:?}"));
    assert_eq!(
        database
            .append(table, &oversized)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    );
    let after = database.maintenance_status().unwrap_or_default();
    assert_eq!(
        owner.maintenance_status().unwrap_or_default(),
        shared_before
    );
    assert_eq!(after.visible_seq(), before.visible_seq());
    assert_eq!(after.pending_records(), before.pending_records());
    assert_eq!(after.wal_storage_bytes(), before.wal_storage_bytes());
    assert_eq!(
        fs::read(root.path().join("MANIFEST")).unwrap_or_default(),
        manifest
    );
    assert_eq!(
        database
            .append(table, &observation(2))
            .ok()
            .map(super::Seq::get),
        Some(before.visible_seq() + 1)
    );
}

#[test]
fn checkpoint_unit_failure_can_retry_and_reopen() {
    let root = TestDir::new("bounded-wal-seal-retry");
    let database =
        Db::open(root.path(), options()).unwrap_or_else(|error| panic!("open: {error:?}"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|error| panic!("create: {error:?}"));
    database
        .append(table, &observation(1))
        .unwrap_or_else(|error| panic!("append: {error:?}"));
    let obstacle = root.path().join("units/0000000000000001.lsu");
    fs::create_dir(&obstacle).unwrap_or_else(|error| panic!("obstacle: {error}"));
    assert_eq!(
        database.seal().err().map(|error| error.kind()),
        Some(ErrorKind::Io)
    );
    assert!(storage_bytes(root.path()) <= 4096);
    fs::remove_dir(obstacle).unwrap_or_else(|error| panic!("remove obstacle: {error}"));
    database
        .append(table, &observation(2))
        .unwrap_or_else(|error| panic!("retry append: {error:?}"));
    database
        .seal()
        .unwrap_or_else(|error| panic!("retry Seal: {error:?}"));
    let expected = facts(&database.snapshot(), table);
    drop(database);
    let reopened =
        Db::open(root.path(), options()).unwrap_or_else(|error| panic!("reopen: {error:?}"));
    assert_eq!(facts(&reopened.snapshot(), table), expected);
    assert_eq!(
        reopened
            .maintenance_status()
            .unwrap_or_default()
            .wal_bytes(),
        0
    );
    assert_eq!(
        reopened
            .maintenance_status()
            .unwrap_or_default()
            .pending_records(),
        0
    );
    assert!(
        storage_bytes(root.path()) > 64,
        "independent Seal must not force shared segment rotation"
    );
}

#[test]
fn manifest_failure_poison_blocks_writes_until_reopen() {
    let root = TestDir::new("bounded-wal-manifest-failure");
    let database =
        Db::open(root.path(), options()).unwrap_or_else(|error| panic!("open: {error:?}"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|error| panic!("create: {error:?}"));
    database
        .append(table, &observation(1))
        .unwrap_or_else(|error| panic!("append: {error:?}"));
    let path = root.path().join("MANIFEST");
    let backup = root.path().join("manifest.saved");
    fs::rename(&path, &backup).unwrap_or_else(|error| panic!("save manifest: {error}"));
    fs::create_dir(&path).unwrap_or_else(|error| panic!("obstacle: {error}"));
    assert!(database.seal().is_err());
    assert_eq!(
        database
            .append(table, &observation(2))
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Poisoned)
    );
    assert_eq!(
        database.seal().err().map(|error| error.kind()),
        Some(ErrorKind::Poisoned)
    );
    fs::remove_dir(&path).unwrap_or_else(|error| panic!("remove obstacle: {error}"));
    fs::rename(backup, path).unwrap_or_else(|error| panic!("restore manifest: {error}"));
    let expected = facts(&database.snapshot(), table);
    drop(database);
    let reopened =
        Db::open(root.path(), options()).unwrap_or_else(|error| panic!("reopen: {error:?}"));
    assert_eq!(facts(&reopened.snapshot(), table), expected);
}

#[test]
fn empty_seal_rejects_poisoned_writer() {
    let root = TestDir::new("bounded-wal-empty-poison");
    let database =
        Db::open(root.path(), options()).unwrap_or_else(|error| panic!("open: {error:?}"));
    database
        .lock_engine()
        .unwrap_or_else(|error| panic!("engine: {error:?}"))
        .writer
        .mark_poisoned();
    assert_eq!(
        database.seal().err().map(|error| error.kind()),
        Some(ErrorKind::Poisoned)
    );
}
