// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{BucketBuilder, SumResult};
use crate::{CellValue, ErrorKind, F32Bits, Sq1, ValueType};

#[test]
fn uint_bucket_is_exact() {
    let mut builder = BucketBuilder::new(0, 10, ValueType::UInt, false);
    for value in [CellValue::UInt(9), CellValue::Null, CellValue::UInt(4)] {
        builder
            .observe(value)
            .unwrap_or_else(|_| unreachable!("valid UInt rejected"));
    }
    let bucket = builder.finish().unwrap_or_else(|_| unreachable!());
    assert_eq!((bucket.start_ts(), bucket.end_ts()), (0, 10));
    assert_eq!((bucket.sample_count(), bucket.null_count()), (3, 1));
    assert_eq!(bucket.min(), Some(CellValue::UInt(4)));
    assert_eq!(bucket.max(), Some(CellValue::UInt(9)));
    assert_eq!(bucket.sum(), SumResult::UInt(13));
    assert!(!bucket.is_partial());
}

#[test]
fn sq1_bucket_sums_exact_fp4_values() {
    let low = Sq1::new(50).unwrap_or_else(|| unreachable!());
    let high = Sq1::new(78).unwrap_or_else(|| unreachable!());
    let mut builder = BucketBuilder::new(0, 10, ValueType::Sq1, false);
    builder
        .observe(CellValue::Sq1(high))
        .unwrap_or_else(|_| unreachable!());
    builder
        .observe(CellValue::Sq1(low))
        .unwrap_or_else(|_| unreachable!());
    let bucket = builder.finish().unwrap_or_else(|_| unreachable!());
    assert_eq!(bucket.min(), Some(CellValue::Sq1(low)));
    assert_eq!(bucket.max(), Some(CellValue::Sq1(high)));
    assert_eq!(bucket.sum(), SumResult::Sq1Fp4(170_000));
}

#[test]
fn f32_bucket_preserves_special_bits_in_total_order() {
    let negative_zero = F32Bits::from_bits(0x8000_0000);
    let positive_zero = F32Bits::from_bits(0);
    let infinity = F32Bits::from_bits(0x7f80_0000);
    let nan = F32Bits::from_bits(0x7fc0_1234);
    let mut builder = BucketBuilder::new(0, 10, ValueType::F32Bits, true);
    for value in [nan, positive_zero, negative_zero, infinity] {
        builder
            .observe(CellValue::F32Bits(value))
            .unwrap_or_else(|_| unreachable!("valid F32 bits rejected"));
    }
    let bucket = builder.finish().unwrap_or_else(|_| unreachable!());
    assert_eq!(bucket.min(), Some(CellValue::F32Bits(negative_zero)));
    assert_eq!(bucket.max(), Some(CellValue::F32Bits(nan)));
    assert_eq!(bucket.sum(), SumResult::NotProvided);
    assert!(bucket.is_partial());
}

#[test]
fn all_null_and_wrong_type_paths_are_explicit() {
    let mut all_null = BucketBuilder::new(0, 1, ValueType::UInt, false);
    all_null
        .observe(CellValue::Null)
        .unwrap_or_else(|_| unreachable!());
    let bucket = all_null.finish().unwrap_or_else(|_| unreachable!());
    assert_eq!((bucket.min(), bucket.max()), (None, None));
    assert_eq!(bucket.sum(), SumResult::UInt(0));
    assert_eq!(bucket.null_count(), 1);

    let mut wrong = BucketBuilder::new(0, 1, ValueType::UInt, false);
    assert_eq!(
        wrong
            .observe(CellValue::F32Bits(F32Bits::from_bits(1)))
            .map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}
