// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{FixedKind, bitmap_len, fixed_len, minimum_uint_width};
use crate::{
    CellValue, Error, F32Bits, Result, Sq1,
    limits::{Limit, ensure_at_most},
};

pub(super) fn decode(
    kind: FixedKind,
    bytes: &[u8],
    fact_count: u32,
    null_count: u32,
) -> Result<Vec<CellValue>> {
    if fact_count == 0 {
        return Err(Error::corruption("fixed", "fact count must be positive"));
    }
    ensure_at_most(Limit::SectionRows, u64::from(fact_count))?;
    if null_count > fact_count {
        return Err(Error::corruption("fixed", "null count exceeds fact count"));
    }
    let value_count = fact_count
        .checked_sub(null_count)
        .ok_or_else(|| Error::corruption("fixed", "value count underflow"))?;

    let (width, header_len) = match kind {
        FixedKind::UInt => {
            let Some(width) = bytes.first().copied() else {
                return Err(Error::corruption("fixed", "missing UInt width"));
            };
            (kind.width(Some(width))?, 1_u32)
        }
        FixedKind::Sq1 | FixedKind::F32Bits => (kind.width(None)?, 0),
    };
    let expected = fixed_len(kind, width, fact_count, null_count, value_count)?;
    let expected = usize::try_from(expected)
        .map_err(|_| Error::corruption("fixed", "byte length does not fit usize"))?;
    if bytes.len() != expected {
        return Err(Error::corruption("fixed", "value stream length mismatch"));
    }

    let bitmap_bytes = if null_count == 0 {
        0
    } else {
        bitmap_len(fact_count)?
    };
    let bitmap_start = usize::try_from(header_len)
        .map_err(|_| Error::corruption("fixed", "header length does not fit usize"))?;
    let bitmap_end = bitmap_start
        .checked_add(
            usize::try_from(bitmap_bytes)
                .map_err(|_| Error::corruption("fixed", "bitmap length does not fit usize"))?,
        )
        .ok_or_else(|| Error::corruption("fixed", "bitmap end overflow"))?;
    let Some(bitmap) = bytes.get(bitmap_start..bitmap_end) else {
        return Err(Error::corruption("fixed", "bitmap outside value stream"));
    };
    if null_count != 0 {
        validate_bitmap(bitmap, fact_count, null_count)?;
    }

    let capacity = usize::try_from(fact_count)
        .map_err(|_| Error::corruption("fixed", "fact count does not fit usize"))?;
    let mut facts = Vec::with_capacity(capacity);
    let mut value_offset = bitmap_end;
    let mut maximum = 0_u64;
    for index in 0..capacity {
        if null_count != 0 && bitmap_bit(bitmap, index)? {
            facts.push(CellValue::Null);
        } else {
            let value = read_value(kind, width, bytes, &mut value_offset)?;
            if let CellValue::UInt(value) = value {
                maximum = maximum.max(value);
            }
            facts.push(value);
        }
    }
    if value_offset != expected {
        return Err(Error::corruption("fixed", "unused value bytes"));
    }
    if kind == FixedKind::UInt && width != minimum_uint_width(maximum) {
        return Err(Error::corruption("fixed", "UInt width is not minimal"));
    }
    Ok(facts)
}

fn validate_bitmap(bitmap: &[u8], fact_count: u32, null_count: u32) -> Result<()> {
    let expected = usize::try_from(bitmap_len(fact_count)?)
        .map_err(|_| Error::corruption("fixed", "bitmap length does not fit usize"))?;
    if bitmap.len() != expected {
        return Err(Error::corruption("fixed", "null bitmap length mismatch"));
    }
    let tail = fact_count
        .checked_rem(8)
        .ok_or_else(|| Error::corruption("fixed", "tail-bit calculation failed"))?;
    if tail != 0 {
        let mask = 1_u8
            .checked_shl(tail)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| Error::corruption("fixed", "tail mask overflow"))?;
        let Some(last) = bitmap.last() else {
            return Err(Error::corruption("fixed", "null bitmap is empty"));
        };
        if last & !mask != 0 {
            return Err(Error::corruption("fixed", "null bitmap padding is nonzero"));
        }
    }
    let mut actual = 0_u32;
    for byte in bitmap {
        actual = actual
            .checked_add(byte.count_ones())
            .ok_or_else(|| Error::corruption("fixed", "null popcount overflow"))?;
    }
    if actual != null_count {
        return Err(Error::corruption("fixed", "null bitmap count mismatch"));
    }
    Ok(())
}

fn bitmap_bit(bitmap: &[u8], index: usize) -> Result<bool> {
    let byte = index
        .checked_div(8)
        .ok_or_else(|| Error::corruption("fixed", "bitmap division failed"))?;
    let shift = u32::try_from(
        index
            .checked_rem(8)
            .ok_or_else(|| Error::corruption("fixed", "bitmap remainder failed"))?,
    )
    .map_err(|_| Error::corruption("fixed", "bitmap shift does not fit u32"))?;
    let mask = 1_u8
        .checked_shl(shift)
        .ok_or_else(|| Error::corruption("fixed", "bitmap shift overflow"))?;
    bitmap
        .get(byte)
        .map(|value| value & mask != 0)
        .ok_or_else(|| Error::corruption("fixed", "bitmap lookup out of bounds"))
}

fn read_value(kind: FixedKind, width: u8, bytes: &[u8], offset: &mut usize) -> Result<CellValue> {
    let count = usize::from(width);
    let end = offset
        .checked_add(count)
        .ok_or_else(|| Error::corruption("fixed", "value offset overflow"))?;
    let Some(source) = bytes.get(*offset..end) else {
        return Err(Error::corruption("fixed", "truncated fixed value"));
    };
    *offset = end;
    match kind {
        FixedKind::UInt => {
            let mut raw = [0_u8; 8];
            let Some(target) = raw.get_mut(..count) else {
                return Err(Error::corruption("fixed", "UInt width exceeds eight"));
            };
            target.copy_from_slice(source);
            Ok(CellValue::UInt(u64::from_le_bytes(raw)))
        }
        FixedKind::Sq1 => {
            let Some(code) = source.first().copied() else {
                return Err(Error::corruption("fixed", "truncated SQ1 value"));
            };
            Sq1::new(code)
                .map(CellValue::Sq1)
                .ok_or_else(|| Error::corruption("fixed", "SQ1 value stream contains code 255"))
        }
        FixedKind::F32Bits => {
            let Ok(raw) = <[u8; 4]>::try_from(source) else {
                return Err(Error::corruption("fixed", "F32 value width is not four"));
            };
            Ok(CellValue::F32Bits(F32Bits::from_bits(u32::from_le_bytes(
                raw,
            ))))
        }
    }
}
