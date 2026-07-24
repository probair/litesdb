// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by unit sealing and decoding in M4")]

use super::FactShape;
use crate::{
    Error, Result,
    limits::{Limit, MAX_SECTION_ROWS, ensure_at_most},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PresenceEncoding {
    AllFact,
    Bitmap,
}

impl PresenceEncoding {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::AllFact => 0,
            Self::Bitmap => 1,
        }
    }

    pub(crate) const fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::AllFact),
            1 => Ok(Self::Bitmap),
            _ => Err(Error::corruption("presence", "unknown encoding tag")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PresencePlan {
    encoding: PresenceEncoding,
    byte_len: u32,
    fact_count: u32,
}

impl PresencePlan {
    pub(crate) const fn encoding(self) -> PresenceEncoding {
        self.encoding
    }

    pub(crate) const fn byte_len(self) -> u32 {
        self.byte_len
    }

    pub(crate) const fn fact_count(self) -> u32 {
        self.fact_count
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DecodedPresence<'a> {
    row_count: u32,
    encoding: PresenceEncoding,
    bytes: &'a [u8],
}

impl DecodedPresence<'_> {
    pub(crate) const fn row_count(self) -> u32 {
        self.row_count
    }

    pub(crate) fn is_fact(self, row: u32) -> Option<bool> {
        if row >= self.row_count {
            return None;
        }
        match self.encoding {
            PresenceEncoding::AllFact => Some(true),
            PresenceEncoding::Bitmap => {
                let byte = usize::try_from(row.checked_div(8)?).ok()?;
                let shift = row.checked_rem(8)?;
                let mask = 1_u8.checked_shl(shift)?;
                self.bytes.get(byte).map(|value| value & mask != 0)
            }
        }
    }
}

pub(crate) fn measure(shapes: &[FactShape]) -> Result<PresencePlan> {
    if shapes.is_empty() {
        return Err(Error::invalid("rows", "section must not be empty"));
    }
    let row_count = u32::try_from(shapes.len())
        .map_err(|_| Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS)))?;
    ensure_at_most(Limit::SectionRows, u64::from(row_count))?;

    let mut fact_count = 0_u32;
    for shape in shapes {
        if shape.is_fact() {
            fact_count = fact_count.checked_add(1).ok_or_else(|| {
                Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS))
            })?;
        }
    }
    let encoding = if fact_count == row_count {
        PresenceEncoding::AllFact
    } else {
        PresenceEncoding::Bitmap
    };
    let byte_len = match encoding {
        PresenceEncoding::AllFact => 0,
        PresenceEncoding::Bitmap => bitmap_len(row_count)?,
    };
    Ok(PresencePlan {
        encoding,
        byte_len,
        fact_count,
    })
}

pub(crate) fn encode(shapes: &[FactShape], plan: PresencePlan, output: &mut [u8]) -> Result<usize> {
    if measure(shapes)? != plan {
        return Err(Error::corruption("presence", "encoding plan mismatch"));
    }
    let expected = usize::try_from(plan.byte_len)
        .map_err(|_| Error::corruption("presence", "byte length does not fit usize"))?;
    if output.len() != expected {
        return Err(Error::invalid("output", "length must equal measured size"));
    }
    output.fill(0);
    if plan.encoding == PresenceEncoding::Bitmap {
        for (row, shape) in shapes.iter().enumerate() {
            if shape.is_fact() {
                let byte = row
                    .checked_div(8)
                    .ok_or_else(|| Error::corruption("presence", "bitmap division failed"))?;
                let shift = u32::try_from(
                    row.checked_rem(8)
                        .ok_or_else(|| Error::corruption("presence", "bitmap remainder failed"))?,
                )
                .map_err(|_| Error::corruption("presence", "bit index does not fit u32"))?;
                let mask = 1_u8
                    .checked_shl(shift)
                    .ok_or_else(|| Error::corruption("presence", "invalid bit index"))?;
                let Some(target) = output.get_mut(byte) else {
                    return Err(Error::corruption("presence", "bitmap offset out of bounds"));
                };
                *target |= mask;
            }
        }
    }
    Ok(expected)
}

pub(crate) fn decode(
    encoding_tag: u8,
    row_count: u32,
    expected_fact_count: u32,
    bytes: &[u8],
) -> Result<DecodedPresence<'_>> {
    if row_count == 0 {
        return Err(Error::corruption("presence", "row count must be positive"));
    }
    ensure_at_most(Limit::SectionRows, u64::from(row_count))?;
    if expected_fact_count > row_count {
        return Err(Error::corruption(
            "presence",
            "fact count exceeds row count",
        ));
    }

    let encoding = PresenceEncoding::from_tag(encoding_tag)?;
    match encoding {
        PresenceEncoding::AllFact => {
            if !bytes.is_empty() {
                return Err(Error::corruption(
                    "presence",
                    "AllFact payload must be empty",
                ));
            }
            if expected_fact_count != row_count {
                return Err(Error::corruption(
                    "presence",
                    "AllFact count must equal row count",
                ));
            }
        }
        PresenceEncoding::Bitmap => validate_bitmap(row_count, expected_fact_count, bytes)?,
    }
    Ok(DecodedPresence {
        row_count,
        encoding,
        bytes,
    })
}

fn bitmap_len(row_count: u32) -> Result<u32> {
    row_count
        .checked_add(7)
        .and_then(|value| value.checked_div(8))
        .ok_or_else(|| Error::corruption("presence", "bitmap length overflow"))
}

fn validate_bitmap(row_count: u32, expected_fact_count: u32, bytes: &[u8]) -> Result<()> {
    let expected_len = usize::try_from(bitmap_len(row_count)?)
        .map_err(|_| Error::corruption("presence", "bitmap length does not fit usize"))?;
    if bytes.len() != expected_len {
        return Err(Error::corruption("presence", "bitmap length mismatch"));
    }

    let used_tail_bits = row_count
        .checked_rem(8)
        .ok_or_else(|| Error::corruption("presence", "tail-bit calculation failed"))?;
    if used_tail_bits != 0 {
        let valid_mask = 1_u8
            .checked_shl(used_tail_bits)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| Error::corruption("presence", "tail mask overflow"))?;
        let Some(last) = bytes.last() else {
            return Err(Error::corruption("presence", "bitmap is empty"));
        };
        if last & !valid_mask != 0 {
            return Err(Error::corruption(
                "presence",
                "tail padding bits must be zero",
            ));
        }
    }

    let mut actual_fact_count = 0_u32;
    for byte in bytes {
        actual_fact_count = actual_fact_count
            .checked_add(byte.count_ones())
            .ok_or_else(|| Error::corruption("presence", "bitmap popcount overflow"))?;
    }
    if actual_fact_count != expected_fact_count {
        return Err(Error::corruption("presence", "bitmap fact count mismatch"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "presence_tests.rs"]
mod tests;
