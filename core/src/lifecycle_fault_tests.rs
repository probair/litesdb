// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::path::Path;

use super::{Db, OpenOptions};
use crate::{
    CellValue, CompactLevel, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry,
    SeriesId, StreamKey, TableId, Validity, ValueType, VersionSpec,
    fsutil::TestDir,
    retention::{fold, publish_heads},
    unit::{compact_units, publish_unit, seal_snapshot},
};

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|_| unreachable!("valid schema rejected"))
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
    .unwrap_or_else(|_| unreachable!("valid observation rejected"))
}

const fn key(table: TableId) -> StreamKey {
    StreamKey::new(table, SeriesId::new(1), FieldId::new(1))
}

fn two_units(label: &str) -> (TestDir, Db, TableId) {
    let temporary = TestDir::new(label);
    let database = Db::open(temporary.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("open failed"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    for timestamp in [10_i64, 20] {
        database
            .append(table, &observation(timestamp))
            .unwrap_or_else(|_| unreachable!("append failed"));
        database
            .seal()
            .unwrap_or_else(|_| unreachable!("Seal failed"));
    }
    (temporary, database, table)
}

fn state(database: &Db, table: TableId) -> (u64, Vec<(i64, CellValue)>) {
    let generation = database
        .lock_engine()
        .unwrap_or_else(|_| unreachable!("engine lock failed"))
        .manifest
        .identity()
        .generation();
    let snapshot = database.snapshot();
    let mut cursor = snapshot
        .scan(key(table), 0..100)
        .unwrap_or_else(|_| unreachable!("scan failed"));
    let mut facts = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|_| unreachable!("cursor failed"))
    {
        facts.push((fact.timestamp(), fact.value()));
    }
    (generation, facts)
}

fn assert_two_reopens(root: &Path, table: TableId, expected: &(u64, Vec<(i64, CellValue)>)) {
    for _ in 0..2 {
        let reopened = Db::open(root, OpenOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen failed"));
        assert_eq!(&state(&reopened, table), expected);
        drop(reopened);
    }
}

#[test]
fn seal_data_before_manifest_is_an_ignored_orphan() {
    let temporary = TestDir::new("fault-seal-before-manifest");
    let database = Db::open(temporary.path(), OpenOptions::default())
        .unwrap_or_else(|_| unreachable!("open failed"));
    let table = database
        .create_table(spec())
        .unwrap_or_else(|_| unreachable!("create failed"));
    database
        .append(table, &observation(10))
        .unwrap_or_else(|_| unreachable!("append failed"));
    database
        .sync()
        .unwrap_or_else(|_| unreachable!("sync failed"));
    {
        let engine = database
            .lock_engine()
            .unwrap_or_else(|_| unreachable!("engine lock failed"));
        let orphan =
            seal_snapshot(&engine.tail, 1).unwrap_or_else(|_| unreachable!("Seal assembly failed"));
        publish_unit(&database.directory, &orphan)
            .unwrap_or_else(|_| unreachable!("unit publication failed"));
    }
    let expected = state(&database, table);
    drop(database);
    assert_two_reopens(temporary.path(), table, &expected);
    assert!(!temporary.path().join("units/0000000000000001.lsu").exists());
}

#[test]
fn compact_data_before_manifest_preserves_source_units() {
    let (temporary, database, table) = two_units("fault-compact-before-manifest");
    {
        let engine = database
            .lock_engine()
            .unwrap_or_else(|_| unreachable!("engine lock failed"));
        let orphan = compact_units(
            engine.manifest.units(),
            engine.source.as_ref(),
            &engine.tail,
            3,
        )
        .unwrap_or_else(|_| unreachable!("compaction assembly failed"));
        publish_unit(&database.directory, &orphan)
            .unwrap_or_else(|_| unreachable!("unit publication failed"));
    }
    let expected = state(&database, table);
    drop(database);
    assert_two_reopens(temporary.path(), table, &expected);
    assert!(!temporary.path().join("units/0000000000000003.lsu").exists());
}

#[test]
fn retention_heads_before_manifest_do_not_advance_floor() {
    let (temporary, database, table) = two_units("fault-retain-before-manifest");
    database
        .compact(CompactLevel::Level0To1)
        .unwrap_or_else(|_| unreachable!("compaction failed"));
    {
        let engine = database
            .lock_engine()
            .unwrap_or_else(|_| unreachable!("engine lock failed"));
        let heads = fold(
            None,
            engine.manifest.units(),
            engine.source.as_ref(),
            &engine.tail,
            21,
        )
        .unwrap_or_else(|_| unreachable!("head fold failed"));
        publish_heads(&database.directory, 4, &heads)
            .unwrap_or_else(|_| unreachable!("head publication failed"));
    }
    let expected = state(&database, table);
    assert_eq!(database.retention_floor(), None);
    drop(database);
    assert_two_reopens(temporary.path(), table, &expected);
    assert!(!temporary.path().join("heads/0000000000000004.lsr").exists());
}

#[test]
fn manifest_before_delete_reopens_to_the_new_generation() {
    let (temporary, database, table) = two_units("fault-manifest-before-delete");
    let old = database.snapshot();
    database
        .compact(CompactLevel::Level0To1)
        .unwrap_or_else(|_| unreachable!("compaction failed"));
    let expected = state(&database, table);
    assert!(temporary.path().join("units/0000000000000001.lsu").exists());
    drop(database);
    assert_eq!(
        Db::open(temporary.path(), OpenOptions::default())
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Io)
    );
    assert_eq!(state_from_snapshot(&old, table), expected.1);
    drop(old);
    assert_two_reopens(temporary.path(), table, &expected);
    assert!(!temporary.path().join("units/0000000000000001.lsu").exists());
}

fn state_from_snapshot(snapshot: &crate::Snapshot, table: TableId) -> Vec<(i64, CellValue)> {
    let mut cursor = snapshot
        .scan(key(table), 0..100)
        .unwrap_or_else(|_| unreachable!("detached snapshot scan failed"));
    let mut facts = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|_| unreachable!("detached cursor failed"))
    {
        facts.push((fact.timestamp(), fact.value()));
    }
    facts
}
