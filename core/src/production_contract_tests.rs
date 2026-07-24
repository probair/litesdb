// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, time::Duration};

use crate::{
    CellValue, CompactLevel, CompactionPolicy, Db, ErrorKind, FieldId, FieldSchema, Observation,
    ObservationEntry, OpenOptions, SealPolicy, SeriesId, StreamKey, SyncPolicy, Validity,
    ValueType, VersionSpec, fsutil::TestDir,
};

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|_| unreachable!("valid schema"))
}

fn observation(timestamp: i64, value: u64) -> Observation {
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            CellValue::UInt(value),
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid observation"))
}

fn append_and_seal(database: &Db, table: crate::TableId, timestamp: i64) {
    let value = u64::try_from(timestamp).unwrap_or_default();
    database
        .append(table, &observation(timestamp, value))
        .unwrap_or_else(|_| unreachable!("append"));
    database.seal().unwrap_or_else(|_| unreachable!("seal"));
}

fn scan_values(database: &Db, key: StreamKey, end: i64) -> Vec<(i64, CellValue)> {
    let snapshot = database.snapshot();
    let mut cursor = snapshot
        .scan(key, i64::MIN..end)
        .unwrap_or_else(|_| unreachable!("scan"));
    let mut facts = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|_| unreachable!("cursor"))
    {
        facts.push((fact.timestamp(), fact.value()));
    }
    facts
}

#[test]
fn append_maintain_and_snapshot_reads_are_concurrently_consistent() {
    let root = TestDir::new("production-concurrency");
    let options = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 1,
            interval: Duration::from_secs(3_600),
            ..SealPolicy::default()
        },
        compaction: CompactionPolicy {
            l1_window: Duration::from_secs(100),
            l2_window: Duration::from_secs(200),
        },
        ..OpenOptions::default()
    };
    let database = Db::open(root.path(), options).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create table"));
    let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));

    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            for timestamp in 1_i64..=200 {
                let value = u64::try_from(timestamp).unwrap_or_default();
                database
                    .append(table, &observation(timestamp, value))
                    .unwrap_or_else(|_| unreachable!("append"));
            }
        });
        let maintainer = scope.spawn(|| {
            for _ in 0..200 {
                database
                    .maintain()
                    .unwrap_or_else(|_| unreachable!("maintain"));
            }
        });
        let readers = (0..3)
            .map(|_| {
                scope.spawn(|| {
                    for _ in 0..200 {
                        let snapshot = database.snapshot();
                        let mut cursor = snapshot
                            .scan(key, 0..201)
                            .unwrap_or_else(|_| unreachable!("scan"));
                        let mut previous = None;
                        while let Some(fact) = cursor
                            .next_fact()
                            .unwrap_or_else(|_| unreachable!("cursor"))
                        {
                            assert!(previous.is_none_or(|timestamp| timestamp < fact.timestamp()));
                            previous = Some(fact.timestamp());
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        assert!(writer.join().is_ok());
        assert!(maintainer.join().is_ok());
        for reader in readers {
            assert!(reader.join().is_ok());
        }
    });

    database.seal().unwrap_or_else(|_| unreachable!("seal"));
    let snapshot = database.snapshot();
    let mut cursor = snapshot
        .scan(key, 0..201)
        .unwrap_or_else(|_| unreachable!("final scan"));
    let mut actual = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|_| unreachable!("final cursor"))
    {
        actual.push((fact.timestamp(), fact.value()));
    }
    let expected = (1_i64..=200)
        .map(|timestamp| {
            (
                timestamp,
                CellValue::UInt(u64::try_from(timestamp).unwrap_or_default()),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn maintain_is_bounded_per_level_and_reports_remaining_windows() {
    let root = TestDir::new("maintain-bounded");
    let database = Db::open(
        root.path(),
        OpenOptions {
            sync_policy: SyncPolicy::Manual,
            compaction: CompactionPolicy {
                l1_window: Duration::from_secs(100),
                l2_window: Duration::from_secs(200),
            },
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    for timestamp in [10, 20, 110, 120, 210] {
        append_and_seal(&database, table, timestamp);
    }

    let first = database
        .maintain()
        .unwrap_or_else(|_| unreachable!("first maintain"));
    assert_eq!(
        first.compacted_l1().map(crate::CompactReport::input_units),
        Some(2)
    );
    assert_eq!(
        first.compacted_l2().map(crate::CompactReport::input_units),
        Some(1)
    );
    assert!(first.more_due());
    assert_eq!(
        database
            .maintenance_status()
            .ok()
            .map(crate::MaintenanceStatus::level_units),
        Some([3, 0, 1])
    );

    let second = database
        .maintain()
        .unwrap_or_else(|_| unreachable!("second maintain"));
    assert_eq!(
        second.compacted_l1().map(crate::CompactReport::input_units),
        Some(2)
    );
    assert_eq!(
        second.compacted_l2().map(crate::CompactReport::input_units),
        Some(1)
    );
    assert!(!second.more_due());
    assert_eq!(
        database
            .maintenance_status()
            .ok()
            .map(crate::MaintenanceStatus::level_units),
        Some([1, 0, 2])
    );
}

#[test]
fn late_l0_before_higher_levels_is_promoted_normally() {
    let root = TestDir::new("maintain-late-l0");
    let database = Db::open(
        root.path(),
        OpenOptions {
            sync_policy: SyncPolicy::Manual,
            compaction: CompactionPolicy {
                l1_window: Duration::from_secs(100),
                l2_window: Duration::from_secs(200),
            },
            ..OpenOptions::default()
        },
    )
    .unwrap_or_else(|_| unreachable!("open"));
    let advanced = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create advanced"));
    let delayed = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create delayed"));
    for timestamp in [10, 110, 210] {
        append_and_seal(&database, advanced, timestamp);
    }
    database
        .maintain()
        .unwrap_or_else(|_| unreachable!("initial promotion"));

    append_and_seal(&database, delayed, 5);
    let report = database
        .maintain()
        .unwrap_or_else(|_| unreachable!("late promotion"));
    assert_eq!(
        report.compacted_l1().map(crate::CompactReport::input_units),
        Some(1)
    );
    assert_eq!(
        report.compacted_l2().map(crate::CompactReport::input_units),
        Some(1)
    );
}

#[test]
fn maintain_and_explicit_maintenance_preserve_identical_facts() {
    let automatic_root = TestDir::new("maintain-automatic");
    let manual_root = TestDir::new("maintain-manual");
    let policy = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        seal_policy: SealPolicy {
            bytes: 1,
            interval: Duration::from_secs(3_600),
            ..SealPolicy::default()
        },
        compaction: CompactionPolicy {
            l1_window: Duration::from_secs(100),
            l2_window: Duration::from_secs(200),
        },
        ..OpenOptions::default()
    };
    let automatic =
        Db::open(automatic_root.path(), policy).unwrap_or_else(|_| unreachable!("automatic open"));
    let manual =
        Db::open(manual_root.path(), policy).unwrap_or_else(|_| unreachable!("manual open"));
    let automatic_table = automatic
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("automatic table"));
    let manual_table = manual
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("manual table"));
    for timestamp in [10, 20, 210] {
        automatic
            .append(
                automatic_table,
                &observation(timestamp, u64::try_from(timestamp).unwrap_or_default()),
            )
            .unwrap_or_else(|_| unreachable!("automatic append"));
        manual
            .append(
                manual_table,
                &observation(timestamp, u64::try_from(timestamp).unwrap_or_default()),
            )
            .unwrap_or_else(|_| unreachable!("manual append"));
    }
    automatic
        .maintain()
        .unwrap_or_else(|_| unreachable!("automatic maintain"));
    manual
        .seal()
        .unwrap_or_else(|_| unreachable!("manual seal"));
    manual
        .compact(CompactLevel::Level0To1)
        .unwrap_or_else(|_| unreachable!("manual L1"));
    manual
        .compact(CompactLevel::Level1To2)
        .unwrap_or_else(|_| unreachable!("manual L2"));
    let automatic_key = StreamKey::new(automatic_table, SeriesId::new(1), FieldId::new(1));
    let manual_key = StreamKey::new(manual_table, SeriesId::new(1), FieldId::new(1));
    assert_eq!(
        scan_values(&automatic, automatic_key, 300),
        scan_values(&manual, manual_key, 300)
    );
}

#[test]
fn compaction_policy_rejects_subsecond_or_reversed_windows() {
    for compaction in [
        CompactionPolicy {
            l1_window: Duration::from_millis(500),
            l2_window: Duration::from_secs(1),
        },
        CompactionPolicy {
            l1_window: Duration::from_secs(2),
            l2_window: Duration::from_secs(1),
        },
    ] {
        let root = TestDir::new("invalid-compaction-policy");
        assert_eq!(
            Db::open(
                root.path(),
                OpenOptions {
                    compaction,
                    ..OpenOptions::default()
                },
            )
            .err()
            .map(|error| error.kind()),
            Some(ErrorKind::InvalidArgument)
        );
    }
}

#[test]
fn maintain_reports_due_sync_and_seal_without_leaving_a_durability_gap() {
    let root = TestDir::new("maintain-due-sync-seal");
    let database = Db::open(
        root.path(),
        OpenOptions {
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
        },
    )
    .unwrap_or_else(|_| unreachable!("open"));
    database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create"));
    let report = database
        .maintain()
        .unwrap_or_else(|_| unreachable!("maintain"));
    assert_eq!(report.synced().map(crate::DurablePosition::seq), Some(1));
    assert!(report.sealed().is_some());
    let status = database
        .maintenance_status()
        .unwrap_or_else(|_| unreachable!("status"));
    assert_eq!(status.visible_seq(), status.durable_seq());
    assert_eq!(status.pending_records(), 0);
}

#[test]
fn open_report_counts_replay_and_physical_tail_repair() {
    let root = TestDir::new("open-report-repair");
    let database =
        Db::open(root.path(), OpenOptions::default()).unwrap_or_else(|_| unreachable!("open"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create table"));
    let repair_start = database
        .sync()
        .unwrap_or_else(|_| unreachable!("sync"))
        .offset();
    database
        .append(table, &observation(10, 10))
        .unwrap_or_else(|_| unreachable!("append"));
    drop(database);

    let mut segments = fs::read_dir(root.path().join("wal"))
        .unwrap_or_else(|_| unreachable!("read WAL"))
        .map(|entry| entry.unwrap_or_else(|_| unreachable!("WAL entry")).path())
        .collect::<Vec<_>>();
    segments.sort_unstable();
    let active = segments
        .last()
        .unwrap_or_else(|| unreachable!("active WAL"));
    let length = fs::metadata(active)
        .unwrap_or_else(|_| unreachable!("WAL metadata"))
        .len();
    fs::OpenOptions::new()
        .write(true)
        .open(active)
        .unwrap_or_else(|_| unreachable!("open WAL"))
        .set_len(length.saturating_sub(3))
        .unwrap_or_else(|_| unreachable!("truncate WAL"));

    let (database, report) = Db::open_with_report(root.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("reopen"));
    assert_eq!(report.replayed_records(), 1);
    assert_eq!(report.tail_repairs(), 1);
    assert_eq!(
        report.repaired_bytes(),
        length.saturating_sub(3).saturating_sub(repair_start)
    );
    assert_eq!(report.recovery_checkpointed_records(), 0);
    assert_eq!(report.recovery_unit_id(), None);
    assert!(report.wal_bytes() > 32);
    assert_eq!(
        database
            .maintenance_status()
            .ok()
            .map(crate::MaintenanceStatus::visible_seq),
        Some(1)
    );
}
