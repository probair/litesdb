// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::mem::size_of;

use crate::{
    Error, ObservationEntry, Result,
    limits::MAX_TAIL_INDEX_BYTES,
    wal::{RecordBody, TailIndex, TailRow},
};

pub(super) fn validate(tail: &TailIndex, seq: u64, body: &RecordBody) -> Result<()> {
    if seq != tail.next_seq() {
        return Err(Error::corruption("WAL replay", "target sequence mismatch"));
    }
    match body {
        RecordBody::AppendObservation { table, observation } => {
            let state = tail
                .table(*table)
                .ok_or_else(|| Error::corruption("WAL replay", "observation table is absent"))?;
            observation
                .validate_after(state.last_ts())
                .map_err(|_| Error::corruption("WAL replay", "table clock is not increasing"))?;
            let version = state
                .versions()
                .last()
                .ok_or_else(|| Error::corruption("WAL replay", "table has no version"))?;
            observation
                .validate_schema(version)
                .map_err(|_| Error::corruption("WAL replay", "observation violates schema"))?;
            let estimated = tail
                .estimated_bytes()
                .checked_add(estimate_row_bytes(observation.entries().len())?)
                .ok_or_else(tail_memory_overflow)?;
            if estimated > u64::from(MAX_TAIL_INDEX_BYTES) {
                return Err(Error::limit(
                    "tail_index_bytes",
                    estimated,
                    u64::from(MAX_TAIL_INDEX_BYTES),
                ));
            }
        }
        RecordBody::CreateTable { table, .. } => {
            let expected = tail
                .table_high_water()
                .checked_add(1)
                .ok_or_else(|| Error::corruption("WAL replay", "table id overflow"))?;
            if table.get() != expected || tail.table(*table).is_some() {
                return Err(Error::corruption(
                    "WAL replay",
                    "table id is reused or not continuous",
                ));
            }
        }
        RecordBody::NewTableVersion { table, spec } => {
            let state = tail
                .table(*table)
                .ok_or_else(|| Error::corruption("WAL replay", "version table is absent"))?;
            let previous = state
                .versions()
                .last()
                .ok_or_else(|| Error::corruption("WAL replay", "table has no version"))?;
            if previous.effective_from().is_none() || previous.successor(spec.clone()).is_err() {
                return Err(Error::corruption(
                    "WAL replay",
                    "version predecessor or successor is invalid",
                ));
            }
        }
        RecordBody::DropTable { table } | RecordBody::RetireSeries { table, .. } => {
            if tail.table(*table).is_none() {
                return Err(Error::corruption("WAL replay", "mutation table is absent"));
            }
        }
        RecordBody::RetireField { table, field, .. } => {
            let state = tail
                .table(*table)
                .ok_or_else(|| Error::corruption("WAL replay", "retired field table is absent"))?;
            let version = state
                .versions()
                .last()
                .ok_or_else(|| Error::corruption("WAL replay", "table has no version"))?;
            if version
                .fields()
                .binary_search_by_key(field, |item| item.field())
                .is_err()
            {
                return Err(Error::corruption("WAL replay", "retired field is absent"));
            }
        }
    }
    Ok(())
}

pub(super) fn estimate_row_bytes(entry_count: usize) -> Result<u64> {
    let row = u64::try_from(size_of::<TailRow>()).map_err(|_| tail_memory_overflow())?;
    let entry_width = size_of::<ObservationEntry>()
        .checked_add(size_of::<u32>())
        .ok_or_else(tail_memory_overflow)?;
    let entries = entry_count
        .checked_mul(entry_width)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(tail_memory_overflow)?;
    row.checked_add(entries).ok_or_else(tail_memory_overflow)
}

fn tail_memory_overflow() -> Error {
    Error::limit(
        "tail_index_bytes",
        u64::MAX,
        u64::from(MAX_TAIL_INDEX_BYTES),
    )
}
