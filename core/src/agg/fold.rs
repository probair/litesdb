// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "exposed through Snapshot::aggregate later in M6")]

use std::mem::size_of;

use crate::{
    Error, Result, StreamKey, ValueType,
    agg::bucket::{Bucket, BucketBuilder},
    limits::{Limit, ensure_at_most},
    manifest::UnitMeta,
    query::{UnitSource, collect_output, group_keys, scatter, validate_key, visit_facts},
    wal::TailIndex,
};

struct AggregateState {
    value_type: ValueType,
    table_max: Option<i64>,
    current: Option<BucketBuilder>,
    buckets: Vec<Bucket>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "internal executor receives the captured snapshot context plus public request fields"
)]
pub(crate) fn aggregate(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    keys: &[StreamKey],
    start: i64,
    end: i64,
    bucket_width: u32,
    retention_floor: Option<i64>,
) -> Result<Vec<Vec<Bucket>>> {
    if start > end || bucket_width == 0 {
        return Err(Error::invalid(
            "aggregate",
            "range must be ordered and bucket width must be positive",
        ));
    }
    if retention_floor.is_some_and(|floor| start < floor) {
        return Err(Error::invalid(
            "range",
            "query starts below retention floor",
        ));
    }
    let bucket_count = measure_buckets(start, end, bucket_width)?;
    let total = bucket_count
        .checked_mul(keys.len())
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))?;
    ensure_at_most(Limit::QuerySlots, u64::try_from(total).unwrap_or(u64::MAX))?;
    ensure_bucket_memory(total)?;
    for key in keys {
        let _ = validate_key(schemas, *key)?;
    }
    let mut output = vec![None; keys.len()];
    for group in group_keys(keys) {
        let table = schemas
            .table(group.table())
            .ok_or_else(|| Error::invalid("key", "table is absent"))?;
        let mut states = group
            .keys()
            .iter()
            .map(|key| {
                Ok(AggregateState {
                    value_type: field_type(table, *key)?,
                    table_max: table.last_ts(),
                    current: None,
                    buckets: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        visit_facts(
            units,
            source,
            schemas,
            group.keys(),
            start,
            end,
            |index, fact| observe_fact(&mut states[index], start, end, bucket_width, fact),
        )?;
        let values = states
            .into_iter()
            .map(finish_state)
            .collect::<Result<Vec<_>>>()?;
        scatter(&mut output, group.destinations(), values)?;
    }
    collect_output(output)
}

fn field_type(table: &crate::wal::TailTable, key: StreamKey) -> Result<ValueType> {
    table
        .versions()
        .last()
        .and_then(|version| {
            version
                .fields()
                .binary_search_by_key(&key.field(), |field| field.field())
                .ok()
                .map(|index| version.fields()[index].value_type())
        })
        .ok_or_else(|| Error::corruption("aggregation", "field type disappeared"))
}

fn observe_fact(
    state: &mut AggregateState,
    start: i64,
    end: i64,
    width: u32,
    fact: crate::Fact,
) -> Result<()> {
    let geometry = geometry(start, end, width, fact.timestamp(), state.table_max)?;
    if state
        .current
        .as_ref()
        .is_some_and(|bucket| bucket.start_ts() != geometry.0)
    {
        let completed = state
            .current
            .take()
            .ok_or_else(|| Error::corruption("aggregation", "active bucket disappeared"))?;
        state.buckets.push(completed.finish()?);
    }
    let bucket = state.current.get_or_insert_with(|| {
        BucketBuilder::new(geometry.0, geometry.1, state.value_type, geometry.2)
    });
    bucket.observe(fact.value())
}

fn finish_state(mut state: AggregateState) -> Result<Vec<Bucket>> {
    if let Some(bucket) = state.current {
        state.buckets.push(bucket.finish()?);
    }
    Ok(state.buckets)
}

fn geometry(
    start: i64,
    end: i64,
    width: u32,
    fact_ts: i64,
    table_max: Option<i64>,
) -> Result<(i64, i64, bool)> {
    let relative = i128::from(fact_ts)
        .checked_sub(i128::from(start))
        .ok_or_else(|| Error::corruption("aggregation", "Fact precedes query range"))?;
    if relative < 0 || fact_ts >= end {
        return Err(Error::corruption(
            "aggregation",
            "cursor emitted a Fact outside query range",
        ));
    }
    let width = i128::from(width);
    let index = relative
        .checked_div(width)
        .ok_or_else(|| Error::corruption("aggregation", "bucket division failed"))?;
    let bucket_start = index
        .checked_mul(width)
        .and_then(|offset| i128::from(start).checked_add(offset))
        .ok_or_else(|| Error::limit("bucket_timestamp", u64::MAX, u64::MAX))?;
    let natural_end = bucket_start
        .checked_add(width)
        .ok_or_else(|| Error::limit("bucket_timestamp", u64::MAX, u64::MAX))?;
    let visible_end = natural_end.min(i128::from(end));
    let bucket_start = i64::try_from(bucket_start)
        .map_err(|_| Error::limit("bucket_timestamp", u64::MAX, u64::MAX))?;
    let visible_end = i64::try_from(visible_end)
        .map_err(|_| Error::limit("bucket_timestamp", u64::MAX, u64::MAX))?;
    let partial =
        natural_end > i128::from(end) || table_max.is_some_and(|maximum| maximum < visible_end);
    Ok((bucket_start, visible_end, partial))
}

fn measure_buckets(start: i64, end: i64, width: u32) -> Result<usize> {
    if start == end {
        return Ok(0);
    }
    let span = i128::from(end)
        .checked_sub(i128::from(start))
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))?;
    if span < 0 {
        return Err(Error::invalid("range", "range is reversed"));
    }
    let width = i128::from(width);
    span.checked_add(width.saturating_sub(1))
        .and_then(|value| value.checked_div(width))
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))
}

fn ensure_bucket_memory(count: usize) -> Result<()> {
    let bytes = count
        .checked_mul(size_of::<Bucket>())
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            Error::limit(
                "operation_memory_bytes",
                u64::MAX,
                Limit::OperationMemoryBytes.maximum(),
            )
        })?;
    ensure_at_most(Limit::OperationMemoryBytes, bytes)
}

#[cfg(test)]
#[path = "fold_tests.rs"]
mod tests;
