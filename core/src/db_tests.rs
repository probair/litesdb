// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Db, OpenOptions, SealPolicy, SyncPolicy};
use crate::{
    CellValue, CompactLevel, ErrorKind, FieldId, FieldSchema, Lookup, Observation,
    ObservationEntry, SeriesId, Slot, StreamKey, TableId, Validity, ValueType, VersionSpec,
    fsutil::TestDir,
};
use std::{fs, path::Path, time::Duration};

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|_| unreachable!("valid schema rejected"))
}

fn observation(timestamp: i64, value: CellValue) -> Observation {
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            value,
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid observation rejected"))
}

fn series_observation(timestamp: i64, series: u64, value: u64) -> Observation {
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(series),
            FieldId::new(1),
            CellValue::UInt(value),
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid sparse observation rejected"))
}

const fn key(table: TableId) -> StreamKey {
    StreamKey::new(table, SeriesId::new(1), FieldId::new(1))
}

fn scan(
    snapshot: &crate::Snapshot,
    stream: StreamKey,
    start: i64,
    end: i64,
) -> Vec<(i64, CellValue)> {
    let mut cursor = snapshot
        .scan(stream, start..end)
        .unwrap_or_else(|_| unreachable!("valid scan rejected"));
    let mut facts = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|_| unreachable!("valid cursor failed"))
    {
        facts.push((fact.timestamp(), fact.value()));
    }
    facts
}

fn assert_replaced_units_reaped(
    database: &Db,
    before: crate::Snapshot,
    after: crate::Snapshot,
    root: &Path,
) {
    for unit in [1_u64, 2] {
        assert!(root.join(format!("units/{unit:016x}.lsu")).exists());
    }
    drop(before);
    database
        .sync()
        .unwrap_or_else(|_| unreachable!("sync failed"));
    for unit in [1_u64, 2] {
        assert!(!root.join(format!("units/{unit:016x}.lsu")).exists());
    }
    drop(after);
}

#[test]
fn seal_compact_retain_and_two_reopens_preserve_visibility() {
    let temporary = TestDir::new("db-lifecycle");
    let database = Db::open(temporary.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("open failed"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    assert_eq!(table, TableId::new(1));
    database
        .append(table, &observation(10, CellValue::UInt(10)))
        .unwrap_or_else(|_| unreachable!("append failed"));
    database
        .append(table, &observation(20, CellValue::Null))
        .unwrap_or_else(|_| unreachable!("append failed"));
    assert_eq!(
        database.snapshot().latest(&[key(table)]).ok(),
        Some(vec![Lookup::Null { at_ts: 20 }])
    );
    let first_seal = database
        .seal()
        .unwrap_or_else(|_| unreachable!("Seal failed"));
    assert_eq!(first_seal.unit_id(), Some(1));

    database
        .append(table, &observation(30, CellValue::UInt(30)))
        .unwrap_or_else(|_| unreachable!("append failed"));
    database
        .append(table, &observation(40, CellValue::UInt(40)))
        .unwrap_or_else(|_| unreachable!("append failed"));
    let second_seal = database
        .seal()
        .unwrap_or_else(|_| unreachable!("Seal failed"));
    assert_eq!(second_seal.unit_id(), Some(2));
    let before = database.snapshot();
    let expected = [
        (10, CellValue::UInt(10)),
        (20, CellValue::Null),
        (30, CellValue::UInt(30)),
        (40, CellValue::UInt(40)),
    ];
    assert_eq!(scan(&before, key(table), 0, 50), expected);

    let compacted = database
        .compact(CompactLevel::Level0To1)
        .unwrap_or_else(|_| unreachable!("compaction failed"));
    assert_eq!(
        (compacted.input_units(), compacted.output_unit()),
        (2, Some(3))
    );
    assert_eq!(scan(&database.snapshot(), key(table), 0, 50), expected);
    assert_eq!(scan(&before, key(table), 0, 50), expected);

    let retained = database
        .retain(41)
        .unwrap_or_else(|_| unreachable!("retention failed"));
    assert_eq!((retained.removed_units(), retained.floor()), (1, Some(41)));
    let after = database.snapshot();
    assert_eq!(database.retention_floor(), Some(41));
    assert_eq!(
        after.value_at(&[key(table)], 41).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(40),
            at_ts: 40,
        }])
    );
    assert_eq!(
        after.sample(&[key(table)], 41..43, 1).ok(),
        Some(vec![vec![
            Slot::Value {
                value: CellValue::UInt(40),
                source_ts: 40,
                carried: true,
            },
            Slot::Value {
                value: CellValue::UInt(40),
                source_ts: 40,
                carried: true,
            },
        ]])
    );
    assert_eq!(
        after
            .scan(key(table), 40..42)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    assert_eq!(scan(&before, key(table), 0, 50), expected);
    assert_replaced_units_reaped(&database, before, after, temporary.path());
    drop(database);

    for _ in 0..2 {
        let reopened = Db::open(temporary.path(), OpenOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen failed"));
        assert_eq!(reopened.retention_floor(), Some(41));
        assert_eq!(
            reopened.snapshot().latest(&[key(table)]).ok(),
            Some(vec![Lookup::Value {
                value: CellValue::UInt(40),
                at_ts: 40,
            }])
        );
        drop(reopened);
    }
}

#[test]
fn rejected_mutation_has_no_side_effect() {
    let temporary = TestDir::new("db-invalid-mutation");
    let database = Db::open(temporary.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("open failed"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    let first = database
        .append(table, &observation(10, CellValue::UInt(1)))
        .unwrap_or_else(|_| unreachable!("append failed"));
    assert_eq!(first.get(), 2);
    assert_eq!(
        database
            .append(table, &observation(10, CellValue::UInt(2)))
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    let second = database
        .append(table, &observation(20, CellValue::UInt(3)))
        .unwrap_or_else(|_| unreachable!("append failed"));
    assert_eq!(second.get(), 3);
}

#[test]
fn maintenance_status_tracks_configured_policy_boundaries() {
    let invalid_root = TestDir::new("db-invalid-sync-policy");
    let invalid = OpenOptions {
        sync_policy: SyncPolicy::Interval {
            every: Duration::ZERO,
            bytes: 0,
        },
        ..OpenOptions::default()
    };
    assert_eq!(
        Db::open(invalid_root.path(), invalid)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );

    let root = TestDir::new("db-maintenance-status");
    let options = OpenOptions {
        sync_policy: SyncPolicy::Interval {
            every: Duration::from_secs(3_600),
            bytes: 1,
        },
        seal_policy: SealPolicy {
            bytes: 1,
            interval: Duration::from_secs(3_600),
            ..SealPolicy::default()
        },
        ..OpenOptions::default()
    };
    let database = Db::open(root.path(), options).unwrap_or_else(|_| unreachable!("open failed"));
    let initial = database
        .maintenance_status()
        .unwrap_or_else(|_| unreachable!("status failed"));
    assert!(!initial.sync_due());
    assert!(!initial.seal_due());
    assert_eq!((initial.visible_seq(), initial.durable_seq()), (0, 0));
    assert_eq!(initial.pending_records(), 0);
    assert_eq!(initial.level_units(), [0, 0, 0]);
    assert_eq!(initial.retention_floor(), None);
    let _ = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    let pending = database
        .maintenance_status()
        .unwrap_or_else(|_| unreachable!("status failed"));
    assert!(pending.sync_due());
    assert!(pending.seal_due());
    assert!(pending.unsynced_bytes() > 0);
    assert!(pending.wal_bytes() > 32);
    assert_eq!(pending.tail_bytes(), 0);
    assert_eq!((pending.visible_seq(), pending.durable_seq()), (1, 0));
    assert_eq!(pending.pending_records(), 1);
    assert_eq!(pending.sync_due_in(), Some(Duration::ZERO));
    assert_eq!(pending.seal_due_in(), Some(Duration::ZERO));
    database
        .sync()
        .unwrap_or_else(|_| unreachable!("sync failed"));
    assert!(
        !database
            .maintenance_status()
            .unwrap_or_else(|_| unreachable!("status failed"))
            .sync_due()
    );
    database
        .seal()
        .unwrap_or_else(|_| unreachable!("Seal failed"));
    let sealed = database
        .maintenance_status()
        .unwrap_or_else(|_| unreachable!("status failed"));
    assert!(!sealed.seal_due());
    assert_eq!((sealed.visible_seq(), sealed.durable_seq()), (1, 1));
    assert_eq!(sealed.pending_records(), 0);
}

#[test]
fn sparse_predecessor_crosses_multiple_omitted_sections() {
    let temporary = TestDir::new("db-sparse-predecessor");
    let database = Db::open(temporary.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("open failed"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    for (timestamp, series, value) in [(10, 1, 10), (20, 2, 20), (30, 2, 30)] {
        database
            .append(table, &series_observation(timestamp, series, value))
            .unwrap_or_else(|_| unreachable!("append failed"));
        database
            .seal()
            .unwrap_or_else(|_| unreachable!("Seal failed"));
    }

    let target = key(table);
    let expected = Lookup::Value {
        value: CellValue::UInt(10),
        at_ts: 10,
    };
    let snapshot = database.snapshot();
    assert_eq!(snapshot.value_at(&[target], 30).ok(), Some(vec![expected]));
    assert_eq!(snapshot.latest(&[target]).ok(), Some(vec![expected]));
    assert_eq!(
        snapshot.sample(&[target], 30..31, 1).ok(),
        Some(vec![vec![Slot::Value {
            value: CellValue::UInt(10),
            source_ts: 10,
            carried: true,
        }]])
    );
}

#[test]
fn locks_and_instances_are_isolated() {
    let first_root = TestDir::new("db-instance-one");
    let second_root = TestDir::new("db-instance-two");
    let first = Db::open(first_root.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("first open failed"));
    assert_eq!(
        Db::open(first_root.path(), OpenOptions::default())
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Io)
    );
    let second = Db::open(second_root.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("second open failed"));
    let first_table = first
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    let second_table = second
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    first
        .append(first_table, &observation(1, CellValue::UInt(11)))
        .unwrap_or_else(|_| unreachable!("append failed"));
    second
        .append(second_table, &observation(1, CellValue::UInt(22)))
        .unwrap_or_else(|_| unreachable!("append failed"));
    assert_eq!(
        first.snapshot().latest(&[key(first_table)]).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(11),
            at_ts: 1,
        }])
    );
    assert_eq!(
        second.snapshot().latest(&[key(second_table)]).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(22),
            at_ts: 1,
        }])
    );
}

#[test]
fn foreign_root_without_manifest_is_rejected_unchanged() {
    let temporary = TestDir::new("db-foreign-root");
    let marker = temporary.path().join("owner.data");
    fs::write(&marker, b"keep").unwrap_or_else(|_| unreachable!("fixture write failed"));
    assert_eq!(
        Db::open(temporary.path(), OpenOptions::default())
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
    assert_eq!(fs::read(marker).ok().as_deref(), Some(b"keep".as_slice()));
}
