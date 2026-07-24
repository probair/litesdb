// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Db, OpenOptions};
use crate::{
    CellValue, CompactLevel, FieldId, FieldSchema, Lookup, Observation, ObservationEntry, SeriesId,
    StreamKey, SumResult, TableId, Validity, ValueType, VersionSpec,
    fsutil::{Area, PublishStep, TestDir},
};

const STEPS: [PublishStep; 4] = [
    PublishStep::Write,
    PublishStep::FileSync,
    PublishStep::Rename,
    PublishStep::DirectorySync,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Data,
    Manifest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AggregateView {
    start: i64,
    end: i64,
    count: u64,
    nulls: u64,
    min: Option<CellValue>,
    max: Option<CellValue>,
    sum: SumResult,
    partial: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VisibleState {
    generation: u64,
    floor: Option<i64>,
    scan: Vec<(i64, CellValue)>,
    latest: Lookup,
    aggregate: Vec<AggregateView>,
}

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|_| unreachable!("valid schema rejected"))
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
    .unwrap_or_else(|_| unreachable!("valid observation rejected"))
}

const fn key(table: TableId) -> StreamKey {
    StreamKey::new(table, SeriesId::new(1), FieldId::new(1))
}

fn capture(database: &Db, table: TableId) -> VisibleState {
    let generation = database
        .lock_engine()
        .unwrap_or_else(|_| unreachable!("engine lock failed"))
        .manifest
        .identity()
        .generation();
    let floor = database.retention_floor();
    let start = floor.unwrap_or(0);
    let snapshot = database.snapshot();
    let mut cursor = snapshot
        .scan(key(table), start..100)
        .unwrap_or_else(|_| unreachable!("scan failed"));
    let mut scan = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|_| unreachable!("cursor failed"))
    {
        scan.push((fact.timestamp(), fact.value()));
    }
    let latest = snapshot
        .latest(&[key(table)])
        .unwrap_or_else(|_| unreachable!("latest failed"))[0];
    let aggregate = snapshot
        .aggregate(&[key(table)], start..100, 100)
        .unwrap_or_else(|_| unreachable!("aggregate failed"))
        .into_iter()
        .next()
        .unwrap_or_default()
        .into_iter()
        .map(|bucket| AggregateView {
            start: bucket.start_ts(),
            end: bucket.end_ts(),
            count: bucket.sample_count(),
            nulls: bucket.null_count(),
            min: bucket.min(),
            max: bucket.max(),
            sum: bucket.sum(),
            partial: bucket.is_partial(),
        })
        .collect();
    VisibleState {
        generation,
        floor,
        scan,
        latest,
        aggregate,
    }
}

fn oracle(generation: u64, floor: Option<i64>, facts: &[(i64, u64)]) -> VisibleState {
    let start = floor.unwrap_or(0);
    let scan = facts
        .iter()
        .filter(|(timestamp, _)| *timestamp >= start)
        .map(|(timestamp, value)| (*timestamp, CellValue::UInt(*value)))
        .collect::<Vec<_>>();
    let latest_fact = facts
        .last()
        .copied()
        .unwrap_or_else(|| unreachable!("oracle facts are empty"));
    let aggregate = if scan.is_empty() {
        Vec::new()
    } else {
        let values = scan
            .iter()
            .map(|(_, value)| match value {
                CellValue::UInt(value) => *value,
                _ => unreachable!("oracle value type changed"),
            })
            .collect::<Vec<_>>();
        vec![AggregateView {
            start,
            end: 100,
            count: u64::try_from(values.len()).unwrap_or(u64::MAX),
            nulls: 0,
            min: values.iter().min().copied().map(CellValue::UInt),
            max: values.iter().max().copied().map(CellValue::UInt),
            sum: SumResult::UInt(values.iter().map(|value| u128::from(*value)).sum()),
            partial: latest_fact.0 < 100,
        }]
    };
    VisibleState {
        generation,
        floor,
        scan,
        latest: Lookup::Value {
            value: CellValue::UInt(latest_fact.1),
            at_ts: latest_fact.0,
        },
        aggregate,
    }
}

fn assert_two_reopens(root: &std::path::Path, table: TableId, expected: &VisibleState) {
    for _ in 0..2 {
        let reopened = Db::open(root, OpenOptions::default())
            .unwrap_or_else(|_| unreachable!("reopen failed"));
        assert_eq!(&capture(&reopened, table), expected);
        drop(reopened);
    }
}

fn arm(database: &Db, target: Target, area: Area, name: &str, step: PublishStep) {
    let (area, name) = if target == Target::Manifest {
        (Area::Root, "MANIFEST")
    } else {
        (area, name)
    };
    database.directory.fail_publish(area, name, step);
}

#[test]
fn seal_four_step_matrix_reopens_to_one_oracle_state() {
    for target in [Target::Data, Target::Manifest] {
        for step in STEPS {
            let temporary = TestDir::new("matrix-seal");
            let database = Db::open(temporary.path(), OpenOptions::default())
                .unwrap_or_else(|_| unreachable!("open failed"));
            let table = database
                .create_table(spec())
                .unwrap_or_else(|_| unreachable!("create failed"));
            database
                .append(table, &observation(10, 10))
                .unwrap_or_else(|_| unreachable!("append failed"));
            database
                .sync()
                .unwrap_or_else(|_| unreachable!("sync failed"));
            let generation = capture(&database, table).generation;
            arm(&database, target, Area::Units, "0000000000000001.lsu", step);
            assert!(database.seal().is_err());
            drop(database);
            let advanced = target == Target::Manifest && step == PublishStep::DirectorySync;
            let expected = oracle(generation + u64::from(advanced), None, &[(10, 10)]);
            assert_two_reopens(temporary.path(), table, &expected);
        }
    }
}

#[test]
fn compact_four_step_matrix_reopens_to_one_oracle_state() {
    for target in [Target::Data, Target::Manifest] {
        for step in STEPS {
            let temporary = TestDir::new("matrix-compact");
            let database = Db::open(temporary.path(), OpenOptions::default())
                .unwrap_or_else(|_| unreachable!("open failed"));
            let table = database
                .create_table(spec())
                .unwrap_or_else(|_| unreachable!("create failed"));
            for (timestamp, value) in [(10, 10), (20, 20)] {
                database
                    .append(table, &observation(timestamp, value))
                    .unwrap_or_else(|_| unreachable!("append failed"));
                database
                    .seal()
                    .unwrap_or_else(|_| unreachable!("Seal failed"));
            }
            let generation = capture(&database, table).generation;
            arm(&database, target, Area::Units, "0000000000000003.lsu", step);
            assert!(database.compact(CompactLevel::Level0To1).is_err());
            drop(database);
            let advanced = target == Target::Manifest && step == PublishStep::DirectorySync;
            let expected = oracle(
                generation + u64::from(advanced),
                None,
                &[(10, 10), (20, 20)],
            );
            assert_two_reopens(temporary.path(), table, &expected);
        }
    }
}

#[test]
fn retention_four_step_matrix_reopens_to_one_oracle_state() {
    for target in [Target::Data, Target::Manifest] {
        for step in STEPS {
            let temporary = TestDir::new("matrix-retention");
            let database = Db::open(temporary.path(), OpenOptions::default())
                .unwrap_or_else(|_| unreachable!("open failed"));
            let table = database
                .create_table(spec())
                .unwrap_or_else(|_| unreachable!("create failed"));
            for (timestamp, value) in [(10, 10), (20, 20)] {
                database
                    .append(table, &observation(timestamp, value))
                    .unwrap_or_else(|_| unreachable!("append failed"));
                database
                    .seal()
                    .unwrap_or_else(|_| unreachable!("Seal failed"));
            }
            database
                .compact(CompactLevel::Level0To1)
                .unwrap_or_else(|_| unreachable!("compaction failed"));
            let generation = capture(&database, table).generation;
            let head_name = format!("{:016x}.lsr", generation.saturating_add(1));
            arm(&database, target, Area::Heads, &head_name, step);
            assert!(database.retain(21).is_err());
            drop(database);
            let advanced = target == Target::Manifest && step == PublishStep::DirectorySync;
            let expected = oracle(
                generation + u64::from(advanced),
                advanced.then_some(21),
                &[(10, 10), (20, 20)],
            );
            assert_two_reopens(temporary.path(), table, &expected);
        }
    }
}
