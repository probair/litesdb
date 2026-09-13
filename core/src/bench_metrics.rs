// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{
    marker::PhantomData,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[path = "bench_metrics_clock.rs"]
mod clock;

pub const STAGE_COUNT: usize = 23;
pub const COUNTER_COUNT: usize = 11;

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Append,
    MutationLock,
    MutationGc,
    MutationValidate,
    WalCapacity,
    WalAppend,
    WalEncode,
    RecordWrite,
    TailApply,
    TailCow,
    DbSync,
    DataSync,
    Rotation,
    SegmentWrite,
    SegmentSync,
    WalDirectorySync,
    Seal,
    UnitSeal,
    ManifestPublish,
    SourceReopen,
    SnapshotBuild,
    GarbageCollect,
    WalReclaim,
}

const STAGE_NAMES: [&str; STAGE_COUNT] = [
    "append",
    "mutation_lock",
    "mutation_gc",
    "mutation_validate",
    "wal_capacity",
    "wal_append",
    "wal_encode",
    "record_write",
    "tail_apply",
    "tail_cow",
    "db_sync",
    "data_sync",
    "rotation",
    "segment_write",
    "segment_sync",
    "wal_directory_sync",
    "seal",
    "unit_seal",
    "manifest_publish",
    "source_reopen",
    "snapshot_build",
    "garbage_collect",
    "wal_reclaim",
];

#[derive(Clone, Copy)]
pub(crate) enum Counter {
    ObservationAttempts,
    ObservationEntries,
    AppliedRecords,
    RecordBytes,
    RecordWriteErrors,
    CowCopies,
    CowBytes,
    TailBytesBeforeApply,
    ReopenedUnits,
    SnapshotUnits,
    GarbageCandidates,
}

const COUNTER_NAMES: [&str; COUNTER_COUNT] = [
    "observation_attempts",
    "observation_entries",
    "applied_records",
    "record_bytes",
    "record_write_errors",
    "cow_copies",
    "cow_bytes",
    "tail_bytes_before_apply",
    "reopened_units",
    "snapshot_units",
    "garbage_candidates",
];

#[derive(Clone, Copy, Debug)]
pub struct StageSnapshot {
    pub name: &'static str,
    pub calls: u64,
    pub wall_ns: u64,
    pub cpu_ns: u64,
    pub cpu_samples: u64,
    pub max_wall_ns: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct CounterSnapshot {
    pub name: &'static str,
    pub value: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Snapshot {
    pub cpu_clock: &'static str,
    pub stages: [StageSnapshot; STAGE_COUNT],
    pub counters: [CounterSnapshot; COUNTER_COUNT],
}

#[repr(align(128))]
struct Metric {
    calls: AtomicU64,
    wall_ns: AtomicU64,
    cpu_ns: AtomicU64,
    cpu_samples: AtomicU64,
    max_wall_ns: AtomicU64,
}

impl Metric {
    const fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
            wall_ns: AtomicU64::new(0),
            cpu_ns: AtomicU64::new(0),
            cpu_samples: AtomicU64::new(0),
            max_wall_ns: AtomicU64::new(0),
        }
    }

    fn record(&self, wall: u64, cpu: Option<u64>) {
        add(&self.wall_ns, wall);
        self.max_wall_ns.fetch_max(wall, Ordering::Relaxed);
        if let Some(cpu) = cpu {
            add(&self.cpu_ns, cpu);
            add(&self.cpu_samples, 1);
        }
        add(&self.calls, 1);
    }
}

static METRICS: [Metric; STAGE_COUNT] = [const { Metric::new() }; STAGE_COUNT];
static COUNTERS: [AtomicU64; COUNTER_COUNT] = [const { AtomicU64::new(0) }; COUNTER_COUNT];

#[must_use]
pub fn snapshot() -> Snapshot {
    Snapshot {
        cpu_clock: clock::NAME,
        stages: std::array::from_fn(|index| {
            let metric = &METRICS[index];
            StageSnapshot {
                name: STAGE_NAMES[index],
                calls: metric.calls.load(Ordering::Relaxed),
                wall_ns: metric.wall_ns.load(Ordering::Relaxed),
                cpu_ns: metric.cpu_ns.load(Ordering::Relaxed),
                cpu_samples: metric.cpu_samples.load(Ordering::Relaxed),
                max_wall_ns: metric.max_wall_ns.load(Ordering::Relaxed),
            }
        }),
        counters: std::array::from_fn(|index| CounterSnapshot {
            name: COUNTER_NAMES[index],
            value: COUNTERS[index].load(Ordering::Relaxed),
        }),
    }
}

pub(crate) fn count(counter: Counter, value: u64) {
    add(&COUNTERS[counter as usize], value);
}

fn add(target: &AtomicU64, value: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

pub(crate) struct Span {
    stage: Stage,
    wall: Instant,
    cpu: Option<u64>,
    _same_thread: PhantomData<Rc<()>>,
}

impl Span {
    pub(crate) fn new(stage: Stage) -> Self {
        Self {
            stage,
            wall: Instant::now(),
            cpu: clock::thread_cpu_ns(),
            _same_thread: PhantomData,
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        let cpu = clock::thread_cpu_ns()
            .zip(self.cpu)
            .and_then(|(end, start)| end.checked_sub(start));
        let wall = u64::try_from(self.wall.elapsed().as_nanos()).unwrap_or(u64::MAX);
        METRICS[self.stage as usize].record(wall, cpu);
    }
}

#[cfg(test)]
#[path = "bench_metrics_tests.rs"]
mod tests;
