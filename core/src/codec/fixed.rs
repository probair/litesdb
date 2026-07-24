// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by selector and unit columns later in M2/M4"
)]

use crate::{
    CellValue, Error, Result, ValueType,
    limits::{Limit, MAX_SECTION_ROWS, ensure_at_most},
};

#[path = "fixed_decode.rs"]
mod decode_impl;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixedKind {
    UInt,
    Sq1,
    F32Bits,
}

impl FixedKind {
    pub(crate) const fn from_value_type(value_type: ValueType) -> Self {
        match value_type {
            ValueType::UInt => Self::UInt,
            ValueType::Sq1 => Self::Sq1,
            ValueType::F32Bits => Self::F32Bits,
        }
    }

    fn width(self, uint_width: Option<u8>) -> Result<u8> {
        match (self, uint_width) {
            (Self::UInt, Some(width @ (1 | 2 | 4 | 8))) => Ok(width),
            (Self::UInt, _) => Err(Error::corruption("fixed", "invalid UInt width")),
            (Self::Sq1, None) => Ok(1),
            (Self::F32Bits, None) => Ok(4),
            (Self::Sq1 | Self::F32Bits, Some(_)) => {
                Err(Error::corruption("fixed", "unexpected width header"))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FixedPlan {
    kind: FixedKind,
    width: u8,
    byte_len: u32,
    fact_count: u32,
    null_count: u32,
    value_count: u32,
}

impl FixedPlan {
    pub(crate) const fn kind(self) -> FixedKind {
        self.kind
    }

    pub(crate) const fn width(self) -> u8 {
        self.width
    }

    pub(crate) const fn byte_len(self) -> u32 {
        self.byte_len
    }

    pub(crate) const fn fact_count(self) -> u32 {
        self.fact_count
    }

    pub(crate) const fn null_count(self) -> u32 {
        self.null_count
    }

    pub(crate) const fn value_count(self) -> u32 {
        self.value_count
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FixedMeasurer {
    kind: FixedKind,
    fact_count: u32,
    null_count: u32,
    maximum: u64,
}

impl FixedMeasurer {
    pub(crate) const fn new(kind: FixedKind) -> Self {
        Self {
            kind,
            fact_count: 0,
            null_count: 0,
            maximum: 0,
        }
    }

    pub(crate) fn observe(&mut self, fact: CellValue) -> Result<()> {
        let fact_count = self
            .fact_count
            .checked_add(1)
            .ok_or_else(|| Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS)))?;
        ensure_at_most(Limit::SectionRows, u64::from(fact_count))?;
        match (self.kind, fact) {
            (_, CellValue::Null) => {
                self.null_count = self.null_count.checked_add(1).ok_or_else(|| {
                    Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS))
                })?;
            }
            (FixedKind::UInt, CellValue::UInt(value)) => {
                self.maximum = self.maximum.max(value);
            }
            (FixedKind::Sq1, CellValue::Sq1(_)) | (FixedKind::F32Bits, CellValue::F32Bits(_)) => {}
            _ => return Err(Error::invalid("facts", "value does not match fixed kind")),
        }
        self.fact_count = fact_count;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<FixedPlan> {
        if self.fact_count == 0 {
            return Err(Error::invalid("facts", "fixed stream must not be empty"));
        }
        let value_count = self
            .fact_count
            .checked_sub(self.null_count)
            .ok_or_else(|| Error::corruption("fixed", "null count exceeds fact count"))?;
        let width = match self.kind {
            FixedKind::UInt => minimum_uint_width(self.maximum),
            FixedKind::Sq1 => 1,
            FixedKind::F32Bits => 4,
        };
        let byte_len = fixed_len(
            self.kind,
            width,
            self.fact_count,
            self.null_count,
            value_count,
        )?;
        Ok(FixedPlan {
            kind: self.kind,
            width,
            byte_len,
            fact_count: self.fact_count,
            null_count: self.null_count,
            value_count,
        })
    }
}

pub(crate) fn measure(facts: &[CellValue], kind: FixedKind) -> Result<FixedPlan> {
    let mut measurer = FixedMeasurer::new(kind);
    for fact in facts {
        measurer.observe(*fact)?;
    }
    measurer.finish()
}

pub(crate) fn encode(facts: &[CellValue], plan: FixedPlan, output: &mut [u8]) -> Result<usize> {
    if measure(facts, plan.kind)? != plan {
        return Err(Error::corruption("fixed", "encoding plan mismatch"));
    }
    let expected = usize::try_from(plan.byte_len)
        .map_err(|_| Error::corruption("fixed", "byte length does not fit usize"))?;
    if output.len() != expected {
        return Err(Error::invalid("output", "length must equal measured size"));
    }
    output.fill(0);

    let mut value_offset = 0_usize;
    if plan.kind == FixedKind::UInt {
        write_byte(output, &mut value_offset, plan.width)?;
    }
    let bitmap_start = value_offset;
    if plan.null_count != 0 {
        let bitmap_bytes = usize::try_from(bitmap_len(plan.fact_count)?)
            .map_err(|_| Error::corruption("fixed", "bitmap length does not fit usize"))?;
        value_offset = value_offset
            .checked_add(bitmap_bytes)
            .ok_or_else(|| Error::corruption("fixed", "value offset overflow"))?;
    }

    for (index, fact) in facts.iter().enumerate() {
        if *fact == CellValue::Null {
            set_bitmap_bit(output, bitmap_start, index)?;
        } else {
            write_value(*fact, plan.kind, plan.width, output, &mut value_offset)?;
        }
    }
    if value_offset != expected {
        return Err(Error::corruption(
            "fixed",
            "encoded byte count differs from plan",
        ));
    }
    Ok(value_offset)
}

pub(crate) fn decode(
    kind: FixedKind,
    bytes: &[u8],
    fact_count: u32,
    null_count: u32,
) -> Result<Vec<CellValue>> {
    decode_impl::decode(kind, bytes, fact_count, null_count)
}

const fn minimum_uint_width(maximum: u64) -> u8 {
    if maximum <= u8::MAX as u64 {
        1
    } else if maximum <= u16::MAX as u64 {
        2
    } else if maximum <= u32::MAX as u64 {
        4
    } else {
        8
    }
}

fn fixed_len(
    kind: FixedKind,
    width: u8,
    fact_count: u32,
    null_count: u32,
    value_count: u32,
) -> Result<u32> {
    let header = u32::from(kind == FixedKind::UInt);
    let bitmap = if null_count == 0 {
        0
    } else {
        bitmap_len(fact_count)?
    };
    value_count
        .checked_mul(u32::from(width))
        .and_then(|values| values.checked_add(header))
        .and_then(|length| length.checked_add(bitmap))
        .ok_or_else(|| Error::corruption("fixed", "stream length overflow"))
}

fn bitmap_len(bit_count: u32) -> Result<u32> {
    bit_count
        .checked_add(7)
        .and_then(|value| value.checked_div(8))
        .ok_or_else(|| Error::corruption("fixed", "bitmap length overflow"))
}

fn set_bitmap_bit(output: &mut [u8], start: usize, index: usize) -> Result<()> {
    let byte = start
        .checked_add(
            index
                .checked_div(8)
                .ok_or_else(|| Error::corruption("fixed", "bitmap division failed"))?,
        )
        .ok_or_else(|| Error::corruption("fixed", "bitmap offset overflow"))?;
    let shift = u32::try_from(
        index
            .checked_rem(8)
            .ok_or_else(|| Error::corruption("fixed", "bitmap remainder failed"))?,
    )
    .map_err(|_| Error::corruption("fixed", "bitmap shift does not fit u32"))?;
    let mask = 1_u8
        .checked_shl(shift)
        .ok_or_else(|| Error::corruption("fixed", "bitmap shift overflow"))?;
    let Some(target) = output.get_mut(byte) else {
        return Err(Error::corruption("fixed", "bitmap offset outside output"));
    };
    *target |= mask;
    Ok(())
}

fn write_value(
    fact: CellValue,
    kind: FixedKind,
    width: u8,
    output: &mut [u8],
    offset: &mut usize,
) -> Result<()> {
    let (source, count) = match (kind, fact) {
        (FixedKind::UInt, CellValue::UInt(value)) => (value.to_le_bytes(), usize::from(width)),
        (FixedKind::Sq1, CellValue::Sq1(value)) => {
            let mut bytes = [0_u8; 8];
            bytes[0] = value.code();
            (bytes, 1)
        }
        (FixedKind::F32Bits, CellValue::F32Bits(value)) => {
            let mut bytes = [0_u8; 8];
            bytes[..4].copy_from_slice(&value.bits().to_le_bytes());
            (bytes, 4)
        }
        _ => {
            return Err(Error::corruption(
                "fixed",
                "value kind changed after measure",
            ));
        }
    };
    let end = offset
        .checked_add(count)
        .ok_or_else(|| Error::corruption("fixed", "value offset overflow"))?;
    let Some(target) = output.get_mut(*offset..end) else {
        return Err(Error::corruption("fixed", "value exceeds measured output"));
    };
    let Some(source) = source.get(..count) else {
        return Err(Error::corruption("fixed", "value width exceeds source"));
    };
    target.copy_from_slice(source);
    *offset = end;
    Ok(())
}

fn write_byte(output: &mut [u8], offset: &mut usize, byte: u8) -> Result<()> {
    let Some(target) = output.get_mut(*offset) else {
        return Err(Error::corruption("fixed", "header exceeds measured output"));
    };
    *target = byte;
    *offset = offset
        .checked_add(1)
        .ok_or_else(|| Error::corruption("fixed", "header offset overflow"))?;
    Ok(())
}

#[cfg(test)]
#[path = "fixed_tests.rs"]
mod tests;
