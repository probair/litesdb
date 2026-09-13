// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Counter, Metric, Ordering, Span, Stage, clock, snapshot};
use crate::{
    CellValue, Db, FieldId, FieldSchema, Observation, ObservationEntry, OpenOptions, Result,
    SeriesId, StreamKey, TableSpec, Validity, ValueType, fsutil::TestDir,
};

#[test]
fn local_metric_saturates_and_counts_only_valid_cpu_pairs() {
    let metric = Metric::new();
    metric.record(u64::MAX, Some(7));
    metric.record(2, None);
    assert_eq!(metric.calls.load(Ordering::Relaxed), 2);
    assert_eq!(metric.wall_ns.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(metric.max_wall_ns.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(metric.cpu_ns.load(Ordering::Relaxed), 7);
    assert_eq!(metric.cpu_samples.load(Ordering::Relaxed), 1);
}

#[test]
fn clock_and_timer_observe_completed_span() {
    let before = snapshot().stages[Stage::Append as usize];
    let cpu_start = clock::thread_cpu_ns();
    let timer = Span::new(Stage::Append);
    std::thread::sleep(std::time::Duration::from_millis(2));
    drop(timer);
    let cpu_end = clock::thread_cpu_ns();
    if clock::NAME != "unavailable" {
        assert!(cpu_start.is_some());
        assert!(cpu_end >= cpu_start);
    }
    let after = snapshot().stages[Stage::Append as usize];
    assert!(after.calls > before.calls);
    assert!(after.wall_ns > before.wall_ns);
}

#[test]
fn instrumented_write_preserves_snapshot_and_durable_reopen() -> Result<()> {
    let root = TestDir::new("bench-metrics-cow");
    let db = Db::open(root.path(), OpenOptions::default())?;
    let table = db.create_table(TableSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )?)?;
    let observation = |ts, value| {
        Observation::new(
            ts,
            vec![ObservationEntry::new(
                SeriesId::new(1),
                FieldId::new(1),
                CellValue::UInt(value),
            )],
        )
    };
    db.append(table, &observation(1, 11)?)?;
    let old = db.snapshot();
    let before = snapshot();
    let seq = db.append(table, &observation(2, 22)?)?;
    assert_eq!(db.sync()?.seq(), seq.get());
    let after = snapshot();
    assert!(
        after.counters[Counter::CowCopies as usize].value
            > before.counters[Counter::CowCopies as usize].value
    );
    assert!(
        after.counters[Counter::CowBytes as usize].value
            > before.counters[Counter::CowBytes as usize].value
    );
    assert!(
        after.stages[Stage::RecordWrite as usize].calls
            > before.stages[Stage::RecordWrite as usize].calls
    );
    assert_eq!(old.table_last_timestamp(table)?, Some(1));
    drop(old);
    db.seal()?;
    drop(db);
    let db = Db::open(root.path(), OpenOptions::default())?;
    let key = StreamKey::new(table, SeriesId::new(1), FieldId::new(1));
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(2));
    assert!(matches!(
        db.snapshot().latest(&[key])?.as_slice(),
        [crate::Lookup::Value {
            value: CellValue::UInt(22),
            at_ts: 2
        }]
    ));
    Ok(())
}
