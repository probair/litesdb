// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    CellValue, CompactLevel, Db, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry,
    OpenOptions, Result, SeriesId, Snapshot, StreamKey, SyncPolicy, TableId, TableSpec, Validity,
    ValueType, fsutil::TestDir,
};

fn spec() -> Result<TableSpec> {
    TableSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
}

fn append(db: &Db, table: TableId, timestamp: i64) -> Result<()> {
    db.append(
        table,
        &Observation::new(
            timestamp,
            vec![ObservationEntry::new(
                SeriesId::new(1),
                FieldId::new(1),
                CellValue::UInt(7),
            )],
        )?,
    )?;
    Ok(())
}

fn timestamps(snapshot: &Snapshot, table: TableId, start: i64) -> Result<Vec<i64>> {
    let mut cursor = snapshot.scan(
        StreamKey::new(table, SeriesId::new(1), FieldId::new(1)),
        start..100,
    )?;
    let mut found = Vec::new();
    while let Some(fact) = cursor.next_fact()? {
        found.push(fact.timestamp());
    }
    Ok(found)
}

#[test]
fn table_timestamp_is_snapshot_visible_and_survives_wal_replay_and_drop() -> Result<()> {
    let root = TestDir::new("table-timestamp-visible");
    let options = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        ..OpenOptions::default()
    };
    let db = Db::open(root.path(), options)?;
    let before_create = db.snapshot();
    let table = db.create_table(spec()?)?;
    let empty = db.create_table(spec()?)?;
    let created = db.snapshot();
    assert_eq!(created.table_last_timestamp(table)?, None);
    assert_eq!(
        before_create
            .table_last_timestamp(table)
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    assert_eq!(
        created
            .table_last_timestamp(TableId::new(u32::MAX))
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    db.sync()?;
    append(&db, table, -10)?;
    let first = db.snapshot();
    assert_eq!(first.table_last_timestamp(table)?, Some(-10));
    assert!(db.maintenance_status()?.visible_seq() > db.maintenance_status()?.durable_seq());
    assert_eq!(created.table_last_timestamp(table)?, None);
    append(&db, table, 20)?;
    assert!(append(&db, table, 19).is_err());
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(20));
    assert_eq!(first.table_last_timestamp(table)?, Some(-10));
    db.sync()?;
    drop((before_create, created, first, db));

    let db = Db::open(root.path(), options)?;
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(20));
    assert_eq!(db.snapshot().table_last_timestamp(empty)?, None);
    let before_drop = db.snapshot();
    db.drop_table(table)?;
    assert_eq!(
        db.snapshot()
            .table_last_timestamp(table)
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    assert_eq!(before_drop.table_last_timestamp(table)?, Some(20));
    db.seal()?;
    drop((before_drop, db));
    let db = Db::open(root.path(), options)?;
    assert_eq!(
        db.snapshot()
            .table_last_timestamp(table)
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    assert_eq!(db.snapshot().table_last_timestamp(empty)?, None);
    Ok(())
}

#[test]
fn table_timestamp_survives_seal_compaction_and_complete_retention() -> Result<()> {
    let root = TestDir::new("table-timestamp-retained");
    let db = Db::open(root.path(), OpenOptions::default())?;
    let table = db.create_table(spec()?)?;
    for timestamp in [10, 20] {
        append(&db, table, timestamp)?;
        db.seal()?;
        assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(timestamp));
    }
    db.compact(CompactLevel::Level0To1)?;
    let old = db.snapshot();
    assert_eq!(old.table_last_timestamp(table)?, Some(20));
    assert_eq!(db.retain(100)?.removed_units(), 1);
    assert_eq!(db.snapshot().retention_floor(), Some(21));
    assert!(timestamps(&db.snapshot(), table, 21)?.is_empty());
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(20));
    assert_eq!(timestamps(&old, table, 0)?, vec![10, 20]);
    drop((old, db));
    let db = Db::open(root.path(), OpenOptions::default())?;
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(20));
    assert!(append(&db, table, 20).is_err());
    append(&db, table, 30)?;
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(30));
    Ok(())
}

#[test]
fn table_timestamp_never_reads_unit_files() -> Result<()> {
    let root = TestDir::new("table-timestamp-no-io");
    let db = Db::open(root.path(), OpenOptions::default())?;
    let table = db.create_table(spec()?)?;
    append(&db, table, 10)?;
    let unit = db
        .seal()?
        .unit_id()
        .ok_or_else(|| crate::Error::invalid("test", "missing unit"))?;
    let snapshot = db.snapshot();
    std::fs::remove_file(root.path().join(format!("units/{unit:016x}.lsu")))?;
    assert_eq!(snapshot.table_last_timestamp(table)?, Some(10));
    assert!(timestamps(&snapshot, table, 0).is_err());
    Ok(())
}

#[test]
fn directory_cache_budget_covers_maintenance_and_old_snapshots() -> Result<()> {
    for budget in [0, 1, 256] {
        let root = TestDir::new("directory-cache-lifecycle");
        let options = OpenOptions {
            directory_cache_bytes: budget,
            ..OpenOptions::default()
        };
        let db = Db::open(root.path(), options)?;
        assert_eq!(db.maintenance_status()?.directory_cache_bytes(), 0);
        let table = db.create_table(spec()?)?;
        append(&db, table, 10)?;
        db.seal()?;
        let first = db.snapshot();
        assert_eq!(timestamps(&first, table, 0)?, vec![10]);
        append(&db, table, 20)?;
        db.seal()?;
        let second = db.snapshot();
        assert_eq!(timestamps(&second, table, 0)?, vec![10, 20]);
        assert_eq!(db.compact(CompactLevel::Level0To1)?.input_units(), 2);
        append(&db, table, 30)?;
        db.seal()?;
        assert_eq!(db.retain(21)?.removed_units(), 1);
        for _ in 0..2 {
            assert_eq!(timestamps(&db.snapshot(), table, 21)?, vec![30]);
            assert_eq!(timestamps(&first, table, 0)?, vec![10]);
            assert_eq!(timestamps(&second, table, 0)?, vec![10, 20]);
            let charged = db.maintenance_status()?.directory_cache_bytes();
            assert!(charged <= u64::from(budget));
            if budget < 256 {
                assert_eq!(charged, 0);
            } else {
                assert!(charged > 0);
            }
        }
        drop((first, second, db));
        let db = Db::open(root.path(), options)?;
        assert_eq!(db.maintenance_status()?.directory_cache_bytes(), 0);
        assert_eq!(timestamps(&db.snapshot(), table, 21)?, vec![30]);
        assert!(db.maintenance_status()?.directory_cache_bytes() <= u64::from(budget));
    }
    Ok(())
}

#[test]
fn directory_cache_options_validate_before_creating_database() -> Result<()> {
    assert_eq!(OpenOptions::default().directory_cache_bytes, 1_048_576);
    let root = TestDir::new("directory-cache-option");
    let unused = root.path().join("database");
    for budget in [16_777_217, u32::MAX] {
        let error = Db::open(
            &unused,
            OpenOptions {
                directory_cache_bytes: budget,
                ..OpenOptions::default()
            },
        )
        .err();
        assert_eq!(error.map(|e| e.kind()), Some(ErrorKind::ResourceExhausted));
        assert!(!unused.exists());
    }
    let db = Db::open(
        &unused,
        OpenOptions {
            directory_cache_bytes: 16_777_216,
            ..OpenOptions::default()
        },
    )?;
    assert_eq!(db.maintenance_status()?.directory_cache_bytes(), 0);
    Ok(())
}

#[test]
fn scheduled_seal_uses_reopened_cache_budget_and_preserves_timestamp() -> Result<()> {
    for budget in [0, 256] {
        let root = TestDir::new("directory-cache-recovery-seal");
        let original = OpenOptions {
            sync_policy: SyncPolicy::Manual,
            seal_policy: crate::SealPolicy {
                bytes: 90,
                ..crate::SealPolicy::default()
            },
            ..OpenOptions::default()
        };
        let db = Db::open(root.path(), original)?;
        let table = db.create_table(spec()?)?;
        append(&db, table, 10)?;
        append(&db, table, 20)?;
        db.sync()?;
        drop(db);
        let (db, report) = Db::open_with_report(
            root.path(),
            OpenOptions {
                directory_cache_bytes: budget,
                ..original
            },
        )?;
        assert_eq!(report.replayed_records(), 3);
        assert_eq!(report.recovery_checkpointed_records(), 0);
        let sealed = db
            .maintain()?
            .sealed()
            .unwrap_or_else(|| unreachable!("scheduled seal"));
        assert_eq!(sealed.checkpointed_records(), 3);
        assert!(sealed.unit_id().is_some());
        assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(20));
        assert_eq!(timestamps(&db.snapshot(), table, 0)?, vec![10, 20]);
        let charged = db.maintenance_status()?.directory_cache_bytes();
        assert!(charged <= u64::from(budget));
        assert_eq!(charged == 0, budget == 0);
    }
    Ok(())
}
