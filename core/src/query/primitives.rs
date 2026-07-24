// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::mem::size_of;

use crate::{
    CellValue, Error, Result, StreamKey,
    limits::{Limit, ensure_at_most},
    manifest::UnitMeta,
    query::{
        batch::{
            collect_output, group_keys, predecessors as batch_predecessors, scatter, visit_facts,
        },
        contract::VersionContract,
        cursor::{Fact, validate_key},
    },
    retention::RetentionHeads,
    unit::UnitSource,
    wal::{TailIndex, TailTable},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lookup {
    Value { value: CellValue, at_ts: i64 },
    Null { at_ts: i64 },
    Missing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Slot {
    Value {
        value: CellValue,
        source_ts: i64,
        carried: bool,
    },
    Null {
        source_ts: i64,
        carried: bool,
    },
    Gap,
}

struct SampleState {
    current: Option<Fact>,
    next_slot: usize,
    slots: Vec<Slot>,
}

#[cfg(test)]
pub(crate) fn value_at(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    keys: &[StreamKey],
    at: i64,
) -> Result<Vec<Lookup>> {
    value_at_with_heads(units, source, schemas, None, keys, at)
}

pub(crate) fn value_at_with_heads(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    heads: Option<&RetentionHeads>,
    keys: &[StreamKey],
    at: i64,
) -> Result<Vec<Lookup>> {
    validate_keys(schemas, keys)?;
    ensure_output_bytes::<Lookup>(keys.len())?;
    let mut output = vec![None; keys.len()];
    for group in group_keys(keys) {
        let facts = batch_predecessors(units, source, schemas, group.keys(), at)?;
        let values = group
            .keys()
            .iter()
            .zip(facts)
            .map(|(key, fact)| lookup(schemas, *key, with_head(fact, heads, *key, at), at))
            .collect::<Result<Vec<_>>>()?;
        scatter(&mut output, group.destinations(), values)?;
    }
    collect_output(output)
}

#[cfg(test)]
pub(crate) fn latest(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    keys: &[StreamKey],
) -> Result<Vec<Lookup>> {
    latest_with_heads(units, source, schemas, None, keys)
}

pub(crate) fn latest_with_heads(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    heads: Option<&RetentionHeads>,
    keys: &[StreamKey],
) -> Result<Vec<Lookup>> {
    validate_keys(schemas, keys)?;
    ensure_output_bytes::<Lookup>(keys.len())?;
    let mut output = vec![None; keys.len()];
    for group in group_keys(keys) {
        let table = schemas
            .table(group.table())
            .ok_or_else(|| Error::invalid("key", "table is absent"))?;
        let values = if let Some(at) = table.last_ts() {
            let facts = batch_predecessors(units, source, schemas, group.keys(), at)?;
            group
                .keys()
                .iter()
                .zip(facts)
                .map(|(key, fact)| lookup(schemas, *key, with_head(fact, heads, *key, at), at))
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![Lookup::Missing; group.keys().len()]
        };
        scatter(&mut output, group.destinations(), values)?;
    }
    collect_output(output)
}

#[cfg(test)]
pub(crate) fn sample(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    keys: &[StreamKey],
    start: i64,
    end: i64,
    step: u32,
) -> Result<Vec<Vec<Slot>>> {
    sample_with_heads(units, source, schemas, None, keys, start, end, step)
}

#[allow(
    clippy::too_many_arguments,
    reason = "captured snapshot context plus public grid arguments"
)]
pub(crate) fn sample_with_heads(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    heads: Option<&RetentionHeads>,
    keys: &[StreamKey],
    start: i64,
    end: i64,
    step: u32,
) -> Result<Vec<Vec<Slot>>> {
    validate_keys(schemas, keys)?;
    let slot_count = measure_grid(start, end, step)?;
    let total_slots = slot_count
        .checked_mul(keys.len())
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))?;
    ensure_at_most(
        Limit::QuerySlots,
        u64::try_from(total_slots).unwrap_or(u64::MAX),
    )?;
    ensure_output_bytes::<Slot>(total_slots)?;
    let mut output = vec![None; keys.len()];
    for group in group_keys(keys) {
        let predecessors = batch_predecessors(units, source, schemas, group.keys(), start)?;
        let mut states = group
            .keys()
            .iter()
            .zip(predecessors)
            .map(|(key, fact)| SampleState {
                current: with_head(fact, heads, *key, start),
                next_slot: 0,
                slots: Vec::with_capacity(slot_count),
            })
            .collect::<Vec<_>>();
        visit_facts(
            units,
            source,
            schemas,
            group.keys(),
            start,
            end,
            |index, fact| {
                fill_slots_before(
                    &mut states[index],
                    schemas,
                    group.keys()[index],
                    start,
                    step,
                    slot_count,
                    Some(fact.timestamp()),
                )?;
                states[index].current = Some(fact);
                Ok(())
            },
        )?;
        for (index, state) in states.iter_mut().enumerate() {
            fill_slots_before(
                state,
                schemas,
                group.keys()[index],
                start,
                step,
                slot_count,
                None,
            )?;
        }
        scatter(
            &mut output,
            group.destinations(),
            states.into_iter().map(|state| state.slots).collect(),
        )?;
    }
    collect_output(output)
}

fn with_head(
    fact: Option<Fact>,
    heads: Option<&RetentionHeads>,
    key: StreamKey,
    at: i64,
) -> Option<Fact> {
    fact.or_else(|| {
        heads
            .and_then(|entries| entries.find(key))
            .filter(|head| head.fact_ts() <= at)
            .map(|head| Fact::new(head.fact_ts(), head.value()))
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "one stream state plus captured grid and schema context"
)]
fn fill_slots_before(
    state: &mut SampleState,
    schemas: &TailIndex,
    key: StreamKey,
    start: i64,
    step: u32,
    slot_count: usize,
    next_fact_ts: Option<i64>,
) -> Result<()> {
    while state.next_slot < slot_count {
        let timestamp = grid_timestamp(start, step, state.next_slot)?;
        if next_fact_ts.is_some_and(|fact_ts| timestamp >= fact_ts) {
            break;
        }
        state
            .slots
            .push(slot(schemas, key, state.current, timestamp)?);
        state.next_slot = state
            .next_slot
            .checked_add(1)
            .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))?;
    }
    Ok(())
}

fn lookup(schemas: &TailIndex, key: StreamKey, fact: Option<Fact>, at: i64) -> Result<Lookup> {
    let Some(fact) = fact else {
        return Ok(Lookup::Missing);
    };
    let table = validate_key(schemas, key)?;
    if !fact_is_visible(table, key, fact, at)? {
        return Ok(Lookup::Missing);
    }
    match fact.value() {
        CellValue::Null => Ok(Lookup::Null {
            at_ts: fact.timestamp(),
        }),
        value => Ok(Lookup::Value {
            value,
            at_ts: fact.timestamp(),
        }),
    }
}

fn slot(schemas: &TailIndex, key: StreamKey, fact: Option<Fact>, at: i64) -> Result<Slot> {
    let Some(fact) = fact else {
        return Ok(Slot::Gap);
    };
    let table = validate_key(schemas, key)?;
    if !fact_is_visible(table, key, fact, at)? {
        return Ok(Slot::Gap);
    }
    let carried = fact.timestamp() < at;
    match fact.value() {
        CellValue::Null => Ok(Slot::Null {
            source_ts: fact.timestamp(),
            carried,
        }),
        value => Ok(Slot::Value {
            value,
            source_ts: fact.timestamp(),
            carried,
        }),
    }
}

fn fact_is_visible(table: &TailTable, key: StreamKey, fact: Fact, at: i64) -> Result<bool> {
    let contract = VersionContract::resolve(table.versions(), key.field(), fact.timestamp())?;
    let retired = table
        .retired_series_at(key.series())
        .into_iter()
        .chain(table.retired_field_at(key.field()))
        .max();
    if retired.is_some_and(|cutoff| at > cutoff && fact.timestamp() <= cutoff) {
        return Ok(false);
    }
    Ok(contract.is_live(fact.timestamp(), at))
}

fn validate_keys(schemas: &TailIndex, keys: &[StreamKey]) -> Result<()> {
    for key in keys {
        let _ = validate_key(schemas, *key)?;
    }
    Ok(())
}

fn measure_grid(start: i64, end: i64, step: u32) -> Result<usize> {
    if start > end || step == 0 {
        return Err(Error::invalid(
            "sample",
            "range must be ordered and step must be positive",
        ));
    }
    if start == end {
        return Ok(0);
    }
    let span = i128::from(end)
        .checked_sub(i128::from(start))
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))?;
    let step = i128::from(step);
    let count = span
        .checked_add(
            step.checked_sub(1).ok_or_else(|| {
                Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum())
            })?,
        )
        .and_then(|value| value.checked_div(step))
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))?;
    Ok(count)
}

fn grid_timestamp(start: i64, step: u32, index: usize) -> Result<i64> {
    let offset = i128::try_from(index)
        .ok()
        .and_then(|value| value.checked_mul(i128::from(step)))
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))?;
    i128::from(start)
        .checked_add(offset)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| Error::limit("query_slots", u64::MAX, Limit::QuerySlots.maximum()))
}

fn ensure_output_bytes<T>(count: usize) -> Result<()> {
    let bytes = count
        .checked_mul(size_of::<T>())
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
#[path = "primitives_tests.rs"]
mod tests;
