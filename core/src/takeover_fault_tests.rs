// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::path::Path;

use crate::{
    CellValue, Db, FieldId, FieldSchema, Lookup, Observation, ObservationEntry, OpenOptions,
    SealPolicy, SeriesId, StreamKey, SyncPolicy, TableId, Validity, ValueType, VersionSpec,
    fsutil::{Area, DbDir, PublishStep, TestDir},
    manifest,
};

const STEPS: [PublishStep; 4] = [
    PublishStep::Write,
    PublishStep::FileSync,
    PublishStep::Rename,
    PublishStep::DirectorySync,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryTarget {
    Unit,
    Manifest,
}

fn options() -> OpenOptions {
    OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 90,
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

fn populated(name: &str) -> (TestDir, Db, TableId) {
    let root = TestDir::new(name);
    let database = Db::open(root.path(), options()).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    for timestamp in [10, 20] {
        database
            .append(table, &observation(timestamp))
            .unwrap_or_else(|_| unreachable!("append"));
    }
    (root, database, table)
}

fn catalog(root: &Path) -> manifest::Manifest {
    let directory = DbDir::initialize(root).unwrap_or_else(|_| unreachable!("directory"));
    manifest::load(&directory).unwrap_or_else(|_| unreachable!("manifest"))
}

fn assert_populated_reopens(
    root: &Path,
    table: TableId,
    generation: u64,
    epoch: u64,
    unit_count: usize,
) {
    for _ in 0..2 {
        let database = Db::open(root, options()).unwrap_or_else(|_| unreachable!("stable reopen"));
        let reopened = catalog(root);
        assert_eq!(reopened.identity().generation(), generation);
        assert_eq!(reopened.identity().writer_epoch(), epoch);
        assert_eq!(reopened.units().len(), unit_count);
        let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
        assert_eq!(
            database.snapshot().latest(&[key]).ok(),
            Some(vec![Lookup::Value {
                value: CellValue::UInt(7),
                at_ts: 20,
            }])
        );
        drop(database);
    }
}

fn assert_populated_takeover(
    root: &Path,
    table: TableId,
    generation: u64,
    epoch: u64,
    unit_count: usize,
) {
    let database = Db::open(
        root,
        OpenOptions {
            takeover: true,
            ..options()
        },
    )
    .unwrap_or_else(|_| unreachable!("takeover retry"));
    let taken_over = catalog(root);
    assert_eq!(taken_over.identity().generation(), generation);
    assert_eq!(taken_over.identity().writer_epoch(), epoch);
    assert_eq!(taken_over.units().len(), unit_count);
    drop(database);
    assert_populated_reopens(root, table, generation, epoch, unit_count);
}

#[test]
fn shared_seal_publication_matrix_is_reopen_stable() {
    for target in [RecoveryTarget::Unit, RecoveryTarget::Manifest] {
        for step in STEPS {
            let (root, database, table) = populated("takeover-fault-recovery");
            let before = catalog(root.path());
            let (area, name) = match target {
                RecoveryTarget::Unit => (Area::Units, "0000000000000001.lsu"),
                RecoveryTarget::Manifest => (Area::Root, "MANIFEST"),
            };
            database.directory.fail_publish(area, name, step);
            assert!(database.seal().is_err());
            drop(database);

            let checkpoint_committed =
                target == RecoveryTarget::Manifest && step == PublishStep::DirectorySync;
            assert_populated_reopens(
                root.path(),
                table,
                before.identity().generation() + u64::from(checkpoint_committed),
                before.identity().writer_epoch(),
                usize::from(checkpoint_committed),
            );
            assert_populated_takeover(
                root.path(),
                table,
                before.identity().generation() + u64::from(checkpoint_committed) + 1,
                before.identity().writer_epoch() + 1,
                usize::from(checkpoint_committed),
            );
        }
    }
}

#[test]
fn takeover_identity_publication_matrix_is_reopen_stable() {
    for step in STEPS {
        let (root, database, table) = populated("takeover-fault-identity");
        database
            .seal()
            .unwrap_or_else(|_| unreachable!("prepare seal"));
        let before = catalog(root.path());
        database
            .directory
            .fail_publish(Area::Root, "MANIFEST", step);
        let next = before
            .successor_writer_epoch()
            .unwrap_or_else(|_| unreachable!("epoch successor"));
        assert!(
            manifest::publish(
                &database.directory,
                Some(before.identity().generation()),
                &next
            )
            .is_err()
        );
        drop(database);

        let identity_committed = step == PublishStep::DirectorySync;
        assert_populated_reopens(
            root.path(),
            table,
            before.identity().generation() + u64::from(identity_committed),
            before.identity().writer_epoch() + u64::from(identity_committed),
            1,
        );
        assert_populated_takeover(
            root.path(),
            table,
            before.identity().generation() + 1 + u64::from(identity_committed),
            before.identity().writer_epoch() + 1 + u64::from(identity_committed),
            1,
        );
    }
}

#[test]
fn metadata_only_checkpoint_publication_matrix_is_reopen_stable() {
    for step in STEPS {
        let root = TestDir::new("shared-checkpoint-metadata-fault");
        let database = Db::open(root.path(), options()).unwrap_or_else(|_| unreachable!("open"));
        let table = database
            .create_table(spec())
            .unwrap_or_else(|_| unreachable!("create"));
        database
            .sync()
            .unwrap_or_else(|_| unreachable!("sync schema"));
        let before = catalog(root.path());
        database
            .directory
            .fail_publish(Area::Root, "MANIFEST", step);
        assert!(database.seal().is_err());
        drop(database);
        let committed = step == PublishStep::DirectorySync;
        for _ in 0..2 {
            let reopened =
                Db::open(root.path(), options()).unwrap_or_else(|_| unreachable!("reopen"));
            let after = catalog(root.path());
            assert_eq!(
                after.identity().generation(),
                before.identity().generation() + u64::from(committed)
            );
            assert_eq!(
                after.identity().writer_epoch(),
                before.identity().writer_epoch()
            );
            assert!(after.units().is_empty());
            assert_eq!(
                reopened.snapshot().table_last_timestamp(table).ok(),
                Some(None)
            );
            assert_eq!(
                reopened
                    .maintenance_status()
                    .unwrap_or_default()
                    .pending_records(),
                u64::from(!committed)
            );
        }
        let reopened = Db::open(root.path(), options()).unwrap_or_else(|_| unreachable!("retry"));
        reopened
            .seal()
            .unwrap_or_else(|_| unreachable!("retry seal"));
        reopened
            .append(table, &observation(30))
            .unwrap_or_else(|_| unreachable!("append after checkpoint"));
        reopened
            .sync()
            .unwrap_or_else(|_| unreachable!("sync continued history"));
        drop(reopened);
        for _ in 0..2 {
            let reopened = Db::open(root.path(), options())
                .unwrap_or_else(|_| unreachable!("continued reopen"));
            let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
            assert_eq!(
                reopened.snapshot().latest(&[key]).ok(),
                Some(vec![Lookup::Value {
                    value: CellValue::UInt(7),
                    at_ts: 30,
                }])
            );
        }
    }
}
