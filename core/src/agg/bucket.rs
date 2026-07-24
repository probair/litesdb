// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "returned by Snapshot::aggregate later in M6")]

use crate::{CellValue, Error, F32Bits, Result, ValueType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SumResult {
    UInt(u128),
    Sq1Fp4(u128),
    NotProvided,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bucket {
    start_ts: i64,
    end_ts: i64,
    sample_count: u64,
    null_count: u64,
    min: Option<CellValue>,
    max: Option<CellValue>,
    sum: SumResult,
    is_partial: bool,
}

impl Bucket {
    #[must_use]
    pub const fn start_ts(self) -> i64 {
        self.start_ts
    }

    #[must_use]
    pub const fn end_ts(self) -> i64 {
        self.end_ts
    }

    #[must_use]
    pub const fn sample_count(self) -> u64 {
        self.sample_count
    }

    #[must_use]
    pub const fn null_count(self) -> u64 {
        self.null_count
    }

    #[must_use]
    pub const fn min(self) -> Option<CellValue> {
        self.min
    }

    #[must_use]
    pub const fn max(self) -> Option<CellValue> {
        self.max
    }

    #[must_use]
    pub const fn sum(self) -> SumResult {
        self.sum
    }

    #[must_use]
    pub const fn is_partial(self) -> bool {
        self.is_partial
    }
}

pub(super) struct BucketBuilder {
    start_ts: i64,
    end_ts: i64,
    value_type: ValueType,
    sample_count: u64,
    null_count: u64,
    min: Option<CellValue>,
    max: Option<CellValue>,
    sum: u128,
    is_partial: bool,
}

impl BucketBuilder {
    pub(super) const fn new(
        start_ts: i64,
        end_ts: i64,
        value_type: ValueType,
        is_partial: bool,
    ) -> Self {
        Self {
            start_ts,
            end_ts,
            value_type,
            sample_count: 0,
            null_count: 0,
            min: None,
            max: None,
            sum: 0,
            is_partial,
        }
    }

    pub(super) const fn start_ts(&self) -> i64 {
        self.start_ts
    }

    pub(super) fn observe(&mut self, value: CellValue) -> Result<()> {
        self.sample_count = self
            .sample_count
            .checked_add(1)
            .ok_or_else(|| Error::limit("bucket_samples", u64::MAX, u64::MAX))?;
        if value == CellValue::Null {
            self.null_count = self
                .null_count
                .checked_add(1)
                .ok_or_else(|| Error::limit("bucket_nulls", u64::MAX, u64::MAX))?;
            return Ok(());
        }
        if !value.matches(self.value_type) {
            return Err(Error::corruption(
                "aggregation",
                "Fact type disagrees with field type",
            ));
        }
        self.min = Some(match self.min {
            Some(current) if compare(self.value_type, current, value)? <= 0 => current,
            Some(_) | None => value,
        });
        self.max = Some(match self.max {
            Some(current) if compare(self.value_type, current, value)? >= 0 => current,
            Some(_) | None => value,
        });
        let increment = match value {
            CellValue::UInt(value) => u128::from(value),
            CellValue::Sq1(value) => u128::from(value.fp4()),
            CellValue::F32Bits(_) => 0,
            CellValue::Null => {
                return Err(Error::corruption(
                    "aggregation",
                    "Null reached value accumulator",
                ));
            }
        };
        self.sum = self
            .sum
            .checked_add(increment)
            .ok_or_else(|| Error::limit("bucket_sum", u64::MAX, u64::MAX))?;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Bucket> {
        if self.sample_count == 0 {
            return Err(Error::corruption(
                "aggregation",
                "empty bucket must not be materialized",
            ));
        }
        let sum = match self.value_type {
            ValueType::UInt => SumResult::UInt(self.sum),
            ValueType::Sq1 => SumResult::Sq1Fp4(self.sum),
            ValueType::F32Bits => SumResult::NotProvided,
        };
        Ok(Bucket {
            start_ts: self.start_ts,
            end_ts: self.end_ts,
            sample_count: self.sample_count,
            null_count: self.null_count,
            min: self.min,
            max: self.max,
            sum,
            is_partial: self.is_partial,
        })
    }
}

fn compare(value_type: ValueType, left: CellValue, right: CellValue) -> Result<i8> {
    let ordering = match (value_type, left, right) {
        (ValueType::UInt, CellValue::UInt(left), CellValue::UInt(right)) => left.cmp(&right),
        (ValueType::Sq1, CellValue::Sq1(left), CellValue::Sq1(right)) => left.cmp(&right),
        (ValueType::F32Bits, CellValue::F32Bits(left), CellValue::F32Bits(right)) => {
            f32_order_key(left).cmp(&f32_order_key(right))
        }
        _ => {
            return Err(Error::corruption(
                "aggregation",
                "extrema contain inconsistent types",
            ));
        }
    };
    Ok(match ordering {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    })
}

const fn f32_order_key(value: F32Bits) -> u32 {
    let bits = value.bits();
    if bits & 0x8000_0000 == 0 {
        bits ^ 0x8000_0000
    } else {
        !bits
    }
}

#[cfg(test)]
#[path = "bucket_tests.rs"]
mod tests;
