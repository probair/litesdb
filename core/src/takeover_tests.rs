// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    CellValue, Db, ErrorKind, FieldId, FieldSchema, Lookup, Observation, ObservationEntry,
    OpenOptions, SealPolicy, SeriesId, StreamKey, SyncPolicy, Validity, ValueType, VersionSpec,
    fsutil::{DbDir, TestDir},
    manifest,
    wal::SegmentHeader,
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
            CellValue::UInt(7),
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid observation"))
}

fn directory(root: &Path) -> DbDir {
    DbDir::initialize(root).unwrap_or_else(|_| unreachable!("database directory"))
}

fn segment_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths = fs::read_dir(root.join("wal"))
        .unwrap_or_else(|_| unreachable!("read WAL directory"))
        .map(|entry| entry.unwrap_or_else(|_| unreachable!("WAL entry")).path())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths
}

fn segment_epochs(root: &Path) -> Vec<u64> {
    let catalog = manifest::load(&directory(root)).unwrap_or_else(|_| unreachable!("manifest"));
    segment_paths(root)
        .iter()
        .map(|path| {
            let mut bytes = [0_u8; 32];
            fs::File::open(path)
                .and_then(|mut file| file.read_exact(&mut bytes))
                .unwrap_or_else(|_| unreachable!("segment header"));
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_else(|| unreachable!("segment name"));
            let first_seq = name
                .strip_suffix(".wal")
                .and_then(|digits| digits.parse::<u64>().ok())
                .unwrap_or_else(|| unreachable!("segment sequence"));
            SegmentHeader::decode(
                &bytes,
                first_seq,
                catalog.identity().shard_id(),
                catalog.identity().writer_epoch(),
            )
            .unwrap_or_else(|_| unreachable!("valid header"))
            .writer_epoch()
        })
        .collect()
}

fn rewrite_epoch(root: &Path, path: &Path, epoch: u64) {
    let catalog = manifest::load(&directory(root)).unwrap_or_else(|_| unreachable!("manifest"));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| unreachable!("segment name"));
    let first_seq = name
        .strip_suffix(".wal")
        .and_then(|digits| digits.parse::<u64>().ok())
        .unwrap_or_else(|| unreachable!("segment sequence"));
    let header = SegmentHeader::new(first_seq, catalog.identity().shard_id(), epoch).encode();
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap_or_else(|_| unreachable!("open segment"));
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.write_all(&header))
        .and_then(|()| file.sync_data())
        .unwrap_or_else(|_| unreachable!("rewrite epoch"));
}

#[test]
fn takeover_advances_identity_and_keeps_old_epoch_records_readable() {
    let root = TestDir::new("takeover-advance");
    let database =
        Db::open(root.path(), OpenOptions::default()).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    database
        .append(table, &observation(10))
        .unwrap_or_else(|_| unreachable!("append"));
    drop(database);
    let before = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());

    let database = Db::open(
        root.path(),
        OpenOptions {
            takeover: true,
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("takeover"));
    drop(database);
    let after = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());
    assert_eq!(
        after.identity().generation(),
        before.identity().generation() + 1
    );
    assert_eq!(
        after.identity().writer_epoch(),
        before.identity().writer_epoch() + 1
    );
    assert_eq!(segment_epochs(root.path()), vec![0, 1]);

    let database =
        Db::open(root.path(), OpenOptions::default()).unwrap_or_else(|_| unreachable!("reopen"));
    let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
    assert_eq!(
        database.snapshot().latest(&[key]).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(7),
            at_ts: 10,
        }])
    );
}

#[test]
fn future_and_decreasing_segment_epochs_are_corruption() {
    let future = TestDir::new("takeover-future");
    let database = Db::open(
        future.path(),
        OpenOptions {
            takeover: true,
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("open"));
    drop(database);
    let catalog = manifest::load(&directory(future.path())).unwrap_or_else(|_| unreachable!());
    let active = segment_paths(future.path())
        .pop()
        .unwrap_or_else(|| unreachable!("active segment"));
    rewrite_epoch(
        future.path(),
        &active,
        catalog.identity().writer_epoch().saturating_add(1),
    );
    assert_eq!(
        Db::open(future.path(), OpenOptions::default())
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );

    let decreasing = TestDir::new("takeover-decreasing");
    let database = Db::open(
        decreasing.path(),
        OpenOptions {
            takeover: true,
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("first takeover"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    database
        .append(table, &observation(10))
        .unwrap_or_else(|_| unreachable!("append"));
    drop(database);
    let database = Db::open(
        decreasing.path(),
        OpenOptions {
            takeover: true,
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("second takeover"));
    drop(database);
    let active = segment_paths(decreasing.path())
        .pop()
        .unwrap_or_else(|| unreachable!("active segment"));
    rewrite_epoch(decreasing.path(), &active, 0);
    assert_eq!(
        Db::open(decreasing.path(), OpenOptions::default())
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
}

#[test]
fn reopen_completes_manifest_first_takeover_exactly_once() {
    let root = TestDir::new("takeover-interrupted");
    let database =
        Db::open(root.path(), OpenOptions::default()).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    database
        .append(table, &observation(10))
        .unwrap_or_else(|_| unreachable!("append"));
    drop(database);

    let directory = directory(root.path());
    let current = manifest::load(&directory).unwrap_or_else(|_| unreachable!("manifest"));
    let next = current
        .successor_writer_epoch()
        .unwrap_or_else(|_| unreachable!("successor"));
    manifest::publish(&directory, Some(current.identity().generation()), &next)
        .unwrap_or_else(|_| unreachable!("publish identity"));
    assert_eq!(segment_epochs(root.path()), vec![0]);

    let database = Db::open(root.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("complete takeover"));
    drop(database);
    assert_eq!(segment_epochs(root.path()), vec![0, 1]);
    let count = segment_paths(root.path()).len();
    let database = Db::open(root.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("stable reopen"));
    drop(database);
    assert_eq!(segment_paths(root.path()).len(), count);
}

#[test]
fn takeover_capacity_is_recovered_before_identity_changes() {
    let root = TestDir::new("takeover-capacity");
    let options = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 185,
            ..SealPolicy::default()
        },
        wal_max_bytes: 185,
        ..OpenOptions::default()
    };
    let database = Db::open(root.path(), options).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    for timestamp in [10, 20] {
        database
            .append(table, &observation(timestamp))
            .unwrap_or_else(|_| unreachable!("append"));
    }
    let wal_bytes = database
        .maintenance_status()
        .unwrap_or_else(|_| unreachable!("status"))
        .wal_bytes();
    assert!(wal_bytes > 153);
    assert!(wal_bytes < 185);
    drop(database);
    let before = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());
    let (database, report) = Db::open_with_report(
        root.path(),
        OpenOptions {
            takeover: true,
            ..options
        },
    )
    .unwrap_or_else(|_| unreachable!("capacity-normalized takeover"));
    assert_eq!(report.recovery_checkpointed_records(), 3);
    assert_eq!(report.recovery_unit_id(), Some(1));
    assert_eq!(report.wal_bytes(), 64);
    let after = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());
    assert_eq!(
        after.identity().generation(),
        before.identity().generation() + 2
    );
    assert_eq!(
        after.identity().writer_epoch(),
        before.identity().writer_epoch() + 1
    );
    assert_eq!(after.units().len(), 1);
    let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
    assert_eq!(
        database.snapshot().latest(&[key]).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(7),
            at_ts: 20,
        }])
    );
    drop(database);
    assert_eq!(segment_epochs(root.path()), vec![0, 1]);
}

#[test]
fn open_seals_recovered_wal_that_exceeds_the_new_runtime_policy() {
    let root = TestDir::new("open-policy-shrink");
    let original = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 90,
            ..SealPolicy::default()
        },
        wal_max_bytes: 185,
        ..OpenOptions::default()
    };
    let database = Db::open(root.path(), original).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    for timestamp in [10, 20] {
        database
            .append(table, &observation(timestamp))
            .unwrap_or_else(|_| unreachable!("append"));
    }
    drop(database);
    let before = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());

    let (database, report) = Db::open_with_report(
        root.path(),
        OpenOptions {
            wal_max_bytes: 100,
            ..original
        },
    )
    .unwrap_or_else(|_| unreachable!("policy-normalized open"));
    assert_eq!(report.recovery_checkpointed_records(), 3);
    assert_eq!(report.recovery_unit_id(), Some(1));
    assert_eq!(report.wal_bytes(), 32);
    let after = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());
    assert_eq!(
        after.identity().generation(),
        before.identity().generation() + 1
    );
    assert_eq!(
        after.identity().writer_epoch(),
        before.identity().writer_epoch()
    );
    let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
    assert_eq!(
        database.snapshot().latest(&[key]).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(7),
            at_ts: 20,
        }])
    );
}

#[test]
fn metadata_only_takeover_checkpoints_without_creating_a_unit() {
    let root = TestDir::new("takeover-metadata-only");
    let options = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 1,
            ..SealPolicy::default()
        },
        wal_max_bytes: 185,
        ..OpenOptions::default()
    };
    let database = Db::open(root.path(), options).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    drop(database);
    let before = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());

    let (database, report) = Db::open_with_report(
        root.path(),
        OpenOptions {
            takeover: true,
            ..options
        },
    )
    .unwrap_or_else(|_| unreachable!("metadata takeover"));
    assert_eq!(report.recovery_checkpointed_records(), 1);
    assert_eq!(report.recovery_unit_id(), None);
    assert_eq!(report.wal_bytes(), 64);
    let after = manifest::load(&directory(root.path())).unwrap_or_else(|_| unreachable!());
    assert_eq!(
        after.identity().generation(),
        before.identity().generation() + 2
    );
    assert_eq!(after.units().len(), 0);
    let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
    assert_eq!(
        database.snapshot().latest(&[key]).ok(),
        Some(vec![Lookup::Missing])
    );
}

#[test]
fn full_wal_completes_an_already_published_epoch_after_recovery_seal() {
    let root = TestDir::new("takeover-interrupted-full");
    let options = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 90,
            ..SealPolicy::default()
        },
        wal_max_bytes: 185,
        ..OpenOptions::default()
    };
    let database = Db::open(root.path(), options).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    for timestamp in [10, 20] {
        database
            .append(table, &observation(timestamp))
            .unwrap_or_else(|_| unreachable!("append"));
    }
    drop(database);

    let directory = directory(root.path());
    let current = manifest::load(&directory).unwrap_or_else(|_| unreachable!("manifest"));
    let published = current
        .successor_writer_epoch()
        .unwrap_or_else(|_| unreachable!("successor"));
    manifest::publish(
        &directory,
        Some(current.identity().generation()),
        &published,
    )
    .unwrap_or_else(|_| unreachable!("publish identity"));

    let (database, report) = Db::open_with_report(root.path(), options)
        .unwrap_or_else(|_| unreachable!("complete interrupted takeover"));
    assert_eq!(report.recovery_checkpointed_records(), 3);
    assert_eq!(report.recovery_unit_id(), Some(1));
    assert_eq!(report.wal_bytes(), 64);
    let after = manifest::load(&directory).unwrap_or_else(|_| unreachable!("manifest"));
    assert_eq!(
        after.identity().generation(),
        published.identity().generation() + 1
    );
    assert_eq!(
        after.identity().writer_epoch(),
        published.identity().writer_epoch()
    );
    drop(database);
    assert_eq!(segment_epochs(root.path()), vec![0, 1]);
}
