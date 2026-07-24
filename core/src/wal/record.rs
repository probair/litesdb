// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by WAL writer and recovery later in M3")]

use crc32fast::Hasher;

use crate::{
    CellValue, FieldId, Observation, Result, SeriesId, TableId, ValueType, VersionSpec,
    limits::{Limit, ensure_at_most},
};

#[path = "record_decode.rs"]
mod decode_impl;

pub(crate) const RECORD_HEADER_BYTES: usize = 16;
const RECORD_TYPE_APPEND_OBSERVATION: u8 = 1;
const RECORD_TYPE_CREATE_TABLE: u8 = 2;
const RECORD_TYPE_NEW_TABLE_VERSION: u8 = 3;
const RECORD_TYPE_DROP_TABLE: u8 = 4;
const RECORD_TYPE_RETIRE_SERIES: u8 = 5;
const RECORD_TYPE_RETIRE_FIELD: u8 = 6;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecordBody {
    AppendObservation {
        table: TableId,
        observation: Observation,
    },
    CreateTable {
        table: TableId,
        spec: VersionSpec,
    },
    NewTableVersion {
        table: TableId,
        spec: VersionSpec,
    },
    DropTable {
        table: TableId,
    },
    RetireSeries {
        table: TableId,
        series: SeriesId,
        retire_ts: i64,
    },
    RetireField {
        table: TableId,
        field: FieldId,
        retire_ts: i64,
    },
}

impl RecordBody {
    const fn record_type(&self) -> u8 {
        match self {
            Self::AppendObservation { .. } => RECORD_TYPE_APPEND_OBSERVATION,
            Self::CreateTable { .. } => RECORD_TYPE_CREATE_TABLE,
            Self::NewTableVersion { .. } => RECORD_TYPE_NEW_TABLE_VERSION,
            Self::DropTable { .. } => RECORD_TYPE_DROP_TABLE,
            Self::RetireSeries { .. } => RECORD_TYPE_RETIRE_SERIES,
            Self::RetireField { .. } => RECORD_TYPE_RETIRE_FIELD,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedRecord {
    seq: u64,
    body: RecordBody,
}

impl DecodedRecord {
    pub(crate) const fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) const fn body(&self) -> &RecordBody {
        &self.body
    }

    pub(crate) fn into_body(self) -> RecordBody {
        self.body
    }
}

pub(crate) fn encode(seq: u64, body: &RecordBody) -> Result<Vec<u8>> {
    let payload_len = measure_payload(body)?;
    ensure_at_most(Limit::WalRecordPayload, u64::from(payload_len))?;
    let frame_len = frame_len_from_payload(payload_len)?;
    let capacity = usize::try_from(frame_len)
        .map_err(|_| crate::Error::limit("wal_record_bytes", u64::MAX, usize::MAX as u64))?;
    let mut payload =
        Vec::with_capacity(usize::try_from(payload_len).map_err(|_| {
            crate::Error::limit("wal_record_payload", u64::MAX, u64::from(u32::MAX))
        })?);
    encode_payload(body, &mut payload);
    if payload.len() != usize::try_from(payload_len).unwrap_or(usize::MAX) {
        return Err(crate::Error::corruption(
            "WAL record",
            "encoded payload differs from measurement",
        ));
    }

    let crc = record_crc(payload_len, seq, &payload);
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    if frame.len() != capacity {
        return Err(crate::Error::corruption(
            "WAL record",
            "encoded frame differs from measurement",
        ));
    }
    Ok(frame)
}

pub(crate) fn framed_len(prefix: &[u8]) -> Result<usize> {
    decode_impl::framed_len(prefix)
}

pub(crate) fn inspect(bytes: &[u8]) -> Result<u64> {
    decode_impl::inspect(bytes)
}

pub(crate) fn decode<F>(bytes: &[u8], expected_seq: u64, field_type: F) -> Result<DecodedRecord>
where
    F: FnMut(TableId, FieldId) -> Option<ValueType>,
{
    decode_impl::decode(bytes, expected_seq, field_type)
}

pub(crate) fn decode_inspected<F>(bytes: &[u8], seq: u64, field_type: F) -> Result<DecodedRecord>
where
    F: FnMut(TableId, FieldId) -> Option<ValueType>,
{
    decode_impl::decode_inspected(bytes, seq, field_type)
}

fn record_crc(payload_len: u32, seq: u64, payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(&seq.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

fn measure_payload(body: &RecordBody) -> Result<u32> {
    let length = match body {
        RecordBody::AppendObservation { observation, .. } => {
            let mut length = 17_u32;
            for entry in observation.entries() {
                length = checked_add(length, 11)?;
                length = checked_add(length, value_len(entry.value()))?;
            }
            length
        }
        RecordBody::CreateTable { spec, .. } | RecordBody::NewTableVersion { spec, .. } => {
            let fields = u32::try_from(spec.fields().len()).map_err(|_| {
                crate::Error::limit("logical_streams", u64::MAX, u64::from(u32::MAX))
            })?;
            let fields_len = fields.checked_mul(3).ok_or_else(|| {
                crate::Error::limit("wal_record_payload", u64::MAX, u64::from(u32::MAX))
            })?;
            checked_add(checked_add(10, validity_extra_len(spec))?, fields_len)?
        }
        RecordBody::DropTable { .. } => 5,
        RecordBody::RetireSeries { .. } => 21,
        RecordBody::RetireField { .. } => 15,
    };
    ensure_at_most(Limit::WalRecordPayload, u64::from(length))?;
    Ok(length)
}

const fn validity_extra_len(spec: &VersionSpec) -> u32 {
    if spec.validity().duration().is_some() {
        4
    } else {
        0
    }
}

const fn value_len(value: CellValue) -> u32 {
    match value {
        CellValue::Null => 0,
        CellValue::UInt(_) => 8,
        CellValue::Sq1(_) => 1,
        CellValue::F32Bits(_) => 4,
    }
}

fn checked_add(left: u32, right: u32) -> Result<u32> {
    left.checked_add(right)
        .ok_or_else(|| crate::Error::limit("wal_record_payload", u64::MAX, u64::from(u32::MAX)))
}

fn frame_len_from_payload(payload_len: u32) -> Result<u32> {
    let header = u32::try_from(RECORD_HEADER_BYTES)
        .map_err(|_| crate::Error::limit("wal_record_bytes", u64::MAX, u64::from(u32::MAX)))?;
    payload_len
        .checked_add(header)
        .ok_or_else(|| crate::Error::limit("wal_record_bytes", u64::MAX, u64::from(u32::MAX)))
}

fn encode_payload(body: &RecordBody, output: &mut Vec<u8>) {
    output.push(body.record_type());
    match body {
        RecordBody::AppendObservation { table, observation } => {
            put_u32(output, table.get());
            put_i64(output, observation.timestamp());
            put_u32(
                output,
                u32::try_from(observation.entries().len()).unwrap_or(u32::MAX),
            );
            for entry in observation.entries() {
                put_u64(output, entry.series().get());
                put_u16(output, entry.field().get());
                encode_cell(entry.value(), output);
            }
        }
        RecordBody::CreateTable { table, spec } | RecordBody::NewTableVersion { table, spec } => {
            put_u32(output, table.get());
            encode_spec(spec, output);
        }
        RecordBody::DropTable { table } => put_u32(output, table.get()),
        RecordBody::RetireSeries {
            table,
            series,
            retire_ts,
        } => {
            put_u32(output, table.get());
            put_u64(output, series.get());
            put_i64(output, *retire_ts);
        }
        RecordBody::RetireField {
            table,
            field,
            retire_ts,
        } => {
            put_u32(output, table.get());
            put_u16(output, field.get());
            put_i64(output, *retire_ts);
        }
    }
}

fn encode_spec(spec: &VersionSpec, output: &mut Vec<u8>) {
    match spec.validity().duration() {
        Some(seconds) => {
            output.push(0);
            put_u32(output, seconds.get());
        }
        None => output.push(1),
    }
    put_u32(
        output,
        u32::try_from(spec.fields().len()).unwrap_or(u32::MAX),
    );
    for field in spec.fields() {
        put_u16(output, field.field().get());
        output.push(field.value_type().tag());
    }
}

fn encode_cell(value: CellValue, output: &mut Vec<u8>) {
    match value {
        CellValue::Null => output.push(0),
        CellValue::UInt(value) => {
            output.push(1);
            put_u64(output, value);
        }
        CellValue::Sq1(value) => {
            output.push(1);
            output.push(value.code());
        }
        CellValue::F32Bits(value) => {
            output.push(1);
            put_u32(output, value.bits());
        }
    }
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
#[path = "record_tests.rs"]
mod tests;
