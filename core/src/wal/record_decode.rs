// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    CellValue, Error, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry, Result,
    SeriesId, Sq1, TableId, Validity, ValueType, VersionSpec,
    limits::{MAX_LOGICAL_STREAMS, MAX_WAL_RECORD_PAYLOAD},
};

use super::{
    DecodedRecord, RECORD_HEADER_BYTES, RECORD_TYPE_APPEND_OBSERVATION, RECORD_TYPE_CREATE_TABLE,
    RECORD_TYPE_DROP_TABLE, RECORD_TYPE_NEW_TABLE_VERSION, RECORD_TYPE_RETIRE_FIELD,
    RECORD_TYPE_RETIRE_SERIES, RecordBody, record_crc,
};

pub(super) fn framed_len(prefix: &[u8]) -> Result<usize> {
    let payload_len = read_prefix_u32(prefix)?;
    validate_payload_len(payload_len)?;
    usize::try_from(payload_len)
        .ok()
        .and_then(|length| length.checked_add(RECORD_HEADER_BYTES))
        .ok_or_else(|| Error::corruption("WAL record", "frame length overflow"))
}

pub(super) fn decode<F>(bytes: &[u8], expected_seq: u64, mut field_type: F) -> Result<DecodedRecord>
where
    F: FnMut(TableId, FieldId) -> Option<ValueType>,
{
    let seq = inspect(bytes)?;
    if seq != expected_seq {
        return Err(Error::corruption(
            "WAL record",
            "sequence is not continuous",
        ));
    }
    decode_inspected(bytes, seq, &mut field_type)
}

pub(super) fn decode_inspected<F>(
    bytes: &[u8],
    seq: u64,
    mut field_type: F,
) -> Result<DecodedRecord>
where
    F: FnMut(TableId, FieldId) -> Option<ValueType>,
{
    let payload = bytes
        .get(RECORD_HEADER_BYTES..)
        .ok_or_else(|| Error::corruption("WAL record", "payload is absent"))?;
    let body = decode_payload(payload, &mut field_type)?;
    Ok(DecodedRecord { seq, body })
}

pub(super) fn inspect(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < RECORD_HEADER_BYTES {
        return Err(Error::corruption("WAL record", "truncated frame header"));
    }
    let frame_len = framed_len(bytes)?;
    if bytes.len() != frame_len {
        return Err(Error::corruption("WAL record", "frame length mismatch"));
    }
    let payload_len = read_prefix_u32(bytes)?;
    let seq = read_at_u64(bytes, 4)?;
    let stored_crc = read_at_u32(bytes, 12)?;
    let payload = bytes
        .get(RECORD_HEADER_BYTES..)
        .ok_or_else(|| Error::corruption("WAL record", "payload is absent"))?;
    if record_crc(payload_len, seq, payload) != stored_crc {
        return Err(Error::corruption("WAL record", "CRC32 mismatch"));
    }
    Ok(seq)
}

fn decode_payload<F>(payload: &[u8], field_type: &mut F) -> Result<RecordBody>
where
    F: FnMut(TableId, FieldId) -> Option<ValueType>,
{
    let mut cursor = Cursor::new(payload);
    let record_type = cursor.read_u8()?;
    let table = TableId::new(cursor.read_u32()?);
    let body = match record_type {
        RECORD_TYPE_APPEND_OBSERVATION => decode_observation(table, &mut cursor, field_type)?,
        RECORD_TYPE_CREATE_TABLE => RecordBody::CreateTable {
            table,
            spec: decode_spec(&mut cursor)?,
        },
        RECORD_TYPE_NEW_TABLE_VERSION => RecordBody::NewTableVersion {
            table,
            spec: decode_spec(&mut cursor)?,
        },
        RECORD_TYPE_DROP_TABLE => RecordBody::DropTable { table },
        RECORD_TYPE_RETIRE_SERIES => RecordBody::RetireSeries {
            table,
            series: SeriesId::new(cursor.read_u64()?),
            retire_ts: cursor.read_i64()?,
        },
        RECORD_TYPE_RETIRE_FIELD => RecordBody::RetireField {
            table,
            field: FieldId::new(cursor.read_u16()?),
            retire_ts: cursor.read_i64()?,
        },
        _ => return Err(Error::corruption("WAL payload", "unknown record type")),
    };
    if !cursor.is_exhausted() {
        return Err(Error::corruption("WAL payload", "trailing bytes"));
    }
    Ok(body)
}

fn decode_observation<F>(
    table: TableId,
    cursor: &mut Cursor<'_>,
    field_type: &mut F,
) -> Result<RecordBody>
where
    F: FnMut(TableId, FieldId) -> Option<ValueType>,
{
    let timestamp = cursor.read_i64()?;
    let entry_count = cursor.read_u32()?;
    preflight_count(
        cursor.remaining(),
        entry_count,
        11,
        "entry count exceeds payload",
    )?;
    let mut previous = None;
    let observation = if entry_count == 1 {
        Observation::from_single(
            timestamp,
            decode_observation_entry(table, cursor, field_type, &mut previous)?,
        )
    } else {
        let capacity = usize::try_from(entry_count)
            .map_err(|_| Error::corruption("WAL payload", "entry count does not fit usize"))?;
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..entry_count {
            entries.push(decode_observation_entry(
                table,
                cursor,
                field_type,
                &mut previous,
            )?);
        }
        Observation::new(timestamp, entries)
            .map_err(|_| Error::corruption("WAL payload", "invalid observation"))?
    };
    Ok(RecordBody::AppendObservation { table, observation })
}

fn decode_observation_entry<F>(
    table: TableId,
    cursor: &mut Cursor<'_>,
    field_type: &mut F,
    previous: &mut Option<(SeriesId, FieldId)>,
) -> Result<ObservationEntry>
where
    F: FnMut(TableId, FieldId) -> Option<ValueType>,
{
    let series = SeriesId::new(cursor.read_u64()?);
    let field = FieldId::new(cursor.read_u16()?);
    let key = (series, field);
    if previous.is_some_and(|prior| prior >= key) {
        return Err(Error::corruption(
            "WAL payload",
            "entries are not strictly ordered",
        ));
    }
    let value_type = field_type(table, field)
        .ok_or_else(|| Error::corruption("WAL payload", "field is absent from schema"))?;
    let value = decode_cell(cursor, value_type)?;
    *previous = Some(key);
    Ok(ObservationEntry::new(series, field, value))
}

fn decode_spec(cursor: &mut Cursor<'_>) -> Result<VersionSpec> {
    let validity = match cursor.read_u8()? {
        0 => Validity::duration_seconds(cursor.read_u32()?)
            .map_err(|_| Error::corruption("WAL payload", "invalid finite validity"))?,
        1 => Validity::Forever,
        _ => return Err(Error::corruption("WAL payload", "unknown validity tag")),
    };
    let field_count = cursor.read_u32()?;
    preflight_count(
        cursor.remaining(),
        field_count,
        3,
        "field count exceeds payload",
    )?;
    let capacity = usize::try_from(field_count)
        .map_err(|_| Error::corruption("WAL payload", "field count does not fit usize"))?;
    let mut fields = Vec::with_capacity(capacity);
    for _ in 0..field_count {
        let field = FieldId::new(cursor.read_u16()?);
        let value_type = decode_value_type(cursor.read_u8()?)?;
        fields.push(FieldSchema::new(field, value_type));
    }
    VersionSpec::new(validity, fields)
        .map_err(|_| Error::corruption("WAL payload", "fields are not strictly ordered"))
}

fn decode_cell(cursor: &mut Cursor<'_>, value_type: ValueType) -> Result<CellValue> {
    match cursor.read_u8()? {
        0 => Ok(CellValue::Null),
        1 => match value_type {
            ValueType::UInt => Ok(CellValue::UInt(cursor.read_u64()?)),
            ValueType::Sq1 => Sq1::new(cursor.read_u8()?)
                .map(CellValue::Sq1)
                .ok_or_else(|| Error::corruption("WAL payload", "SQ1 value uses null code")),
            ValueType::F32Bits => Ok(CellValue::F32Bits(F32Bits::from_bits(cursor.read_u32()?))),
        },
        _ => Err(Error::corruption("WAL payload", "unknown cell tag")),
    }
}

fn decode_value_type(tag: u8) -> Result<ValueType> {
    match tag {
        0 => Ok(ValueType::UInt),
        1 => Ok(ValueType::Sq1),
        2 => Ok(ValueType::F32Bits),
        _ => Err(Error::corruption("WAL payload", "unknown value type tag")),
    }
}

fn preflight_count(remaining: usize, count: u32, width: usize, reason: &'static str) -> Result<()> {
    if count > MAX_LOGICAL_STREAMS {
        return Err(Error::corruption("WAL payload", reason));
    }
    let needed = usize::try_from(count)
        .ok()
        .and_then(|value| value.checked_mul(width))
        .ok_or_else(|| Error::corruption("WAL payload", "count byte size overflow"))?;
    if needed > remaining {
        return Err(Error::corruption("WAL payload", reason));
    }
    Ok(())
}

fn validate_payload_len(payload_len: u32) -> Result<()> {
    if payload_len == 0 || payload_len > MAX_WAL_RECORD_PAYLOAD {
        return Err(Error::corruption(
            "WAL record",
            "payload length is out of range",
        ));
    }
    Ok(())
}

fn read_prefix_u32(bytes: &[u8]) -> Result<u32> {
    let source = bytes
        .get(..4)
        .ok_or_else(|| Error::corruption("WAL record", "truncated payload length"))?;
    let array = <[u8; 4]>::try_from(source)
        .map_err(|_| Error::corruption("WAL record", "invalid payload length width"))?;
    Ok(u32::from_le_bytes(array))
}

fn read_at_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| Error::corruption("WAL record", "u32 offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption("WAL record", "truncated u32"))?;
    let array = <[u8; 4]>::try_from(source)
        .map_err(|_| Error::corruption("WAL record", "invalid u32 width"))?;
    Ok(u32::from_le_bytes(array))
}

fn read_at_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| Error::corruption("WAL record", "u64 offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption("WAL record", "truncated u64"))?;
    let array = <[u8; 8]>::try_from(source)
        .map_err(|_| Error::corruption("WAL record", "invalid u64 width"))?;
    Ok(u64::from_le_bytes(array))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn is_exhausted(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| Error::corruption("WAL payload", "truncated byte"))?;
        self.offset = self
            .offset
            .checked_add(1)
            .ok_or_else(|| Error::corruption("WAL payload", "cursor overflow"))?;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_array::<2>()?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_array::<8>()?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| Error::corruption("WAL payload", "cursor overflow"))?;
        let source = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::corruption("WAL payload", "truncated scalar"))?;
        let value = <[u8; N]>::try_from(source)
            .map_err(|_| Error::corruption("WAL payload", "invalid scalar width"))?;
        self.offset = end;
        Ok(value)
    }
}
