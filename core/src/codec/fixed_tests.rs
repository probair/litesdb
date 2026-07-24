// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{FixedKind, FixedPlan, decode, encode, measure};
use crate::{CellValue, ErrorKind, F32Bits, Sq1, limits::MAX_SECTION_ROWS};

fn sq1(code: u8) -> CellValue {
    let Some(value) = Sq1::new(code) else {
        unreachable!("test requested SQ1 null code as value");
    };
    CellValue::Sq1(value)
}

fn encoded(facts: &[CellValue], kind: FixedKind) -> (FixedPlan, Vec<u8>) {
    let Ok(plan) = measure(facts, kind) else {
        unreachable!("valid fixed fixture rejected");
    };
    let Ok(length) = usize::try_from(plan.byte_len()) else {
        unreachable!("bounded fixed length does not fit usize");
    };
    let mut bytes = vec![0_u8; length];
    let Ok(written) = encode(facts, plan, &mut bytes) else {
        unreachable!("measured fixed fixture failed to encode");
    };
    assert_eq!(written, bytes.len());
    (plan, bytes)
}

#[test]
fn uint_width_boundaries_are_minimal() {
    for (value, expected) in [
        (0, 1),
        (u64::from(u8::MAX), 1),
        (u64::from(u8::MAX) + 1, 2),
        (u64::from(u16::MAX), 2),
        (u64::from(u16::MAX) + 1, 4),
        (u64::from(u32::MAX), 4),
        (u64::from(u32::MAX) + 1, 8),
        (u64::MAX, 8),
    ] {
        let Ok(plan) = measure(&[CellValue::UInt(value)], FixedKind::UInt) else {
            unreachable!();
        };
        assert_eq!(plan.width(), expected, "value {value}");
    }
}

#[test]
fn uint_wire_bytes_and_all_null_width_are_exact() {
    let facts = [CellValue::UInt(1), CellValue::Null, CellValue::UInt(0x0203)];
    let (plan, bytes) = encoded(&facts, FixedKind::UInt);
    assert_eq!(plan.kind(), FixedKind::UInt);
    assert_eq!(
        (plan.fact_count(), plan.null_count(), plan.value_count()),
        (3, 1, 2)
    );
    assert_eq!(bytes, [2, 0b0000_0010, 1, 0, 3, 2]);
    assert_eq!(
        decode(FixedKind::UInt, &bytes, 3, 1).ok().as_deref(),
        Some(facts.as_slice())
    );

    let nulls = [CellValue::Null; 3];
    let (plan, bytes) = encoded(&nulls, FixedKind::UInt);
    assert_eq!(plan.width(), 1);
    assert_eq!(bytes, [1, 0b0000_0111]);
    assert_eq!(
        decode(FixedKind::UInt, &bytes, 3, 3).ok().as_deref(),
        Some(nulls.as_slice())
    );
}

#[test]
fn sq1_and_f32_round_trip_exactly() {
    let sq1_facts = [sq1(1), CellValue::Null, sq1(254)];
    let (_, bytes) = encoded(&sq1_facts, FixedKind::Sq1);
    assert_eq!(bytes, [0b0000_0010, 1, 254]);
    assert_eq!(
        decode(FixedKind::Sq1, &bytes, 3, 1).ok().as_deref(),
        Some(sq1_facts.as_slice())
    );

    let f32_facts = [
        CellValue::F32Bits(F32Bits::from_bits(0x8000_0000)),
        CellValue::Null,
        CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234)),
    ];
    let (_, bytes) = encoded(&f32_facts, FixedKind::F32Bits);
    assert_eq!(
        bytes,
        [0b0000_0010, 0x00, 0x00, 0x00, 0x80, 0x34, 0x12, 0xc0, 0x7f,]
    );
    assert_eq!(
        decode(FixedKind::F32Bits, &bytes, 3, 1).ok().as_deref(),
        Some(f32_facts.as_slice())
    );
}

#[test]
fn malformed_streams_are_rejected() {
    let cases = [
        decode(FixedKind::UInt, &[], 1, 0),
        decode(FixedKind::UInt, &[3, 1, 0, 0], 1, 0),
        decode(FixedKind::UInt, &[2, 1, 0], 1, 0),
        decode(FixedKind::UInt, &[1, 1], 2, 0),
        decode(FixedKind::UInt, &[1, 0b1000_0001], 1, 1),
        decode(FixedKind::UInt, &[1, 0], 1, 1),
        decode(FixedKind::Sq1, &[255], 1, 0),
        decode(FixedKind::Sq1, &[], 1, 0),
        decode(FixedKind::F32Bits, &[0; 3], 1, 0),
        decode(FixedKind::UInt, &[1, 1], 0, 0),
        decode(FixedKind::UInt, &[1, 1], 1, 2),
    ];
    for result in cases {
        let Err(error) = result else {
            unreachable!("malformed fixed stream accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }

    let Err(error) = decode(FixedKind::UInt, &[1], MAX_SECTION_ROWS + 1, 0) else {
        unreachable!("over-limit fixed count accepted");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
}

#[test]
fn input_type_and_count_are_checked() {
    let Err(error) = measure(&[sq1(1)], FixedKind::UInt) else {
        unreachable!("mismatched fixed value accepted");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let max = vec![CellValue::Null; usize::try_from(MAX_SECTION_ROWS).unwrap_or(0)];
    assert!(measure(&max, FixedKind::F32Bits).is_ok());
    let mut over = max;
    over.push(CellValue::Null);
    let Err(error) = measure(&over, FixedKind::F32Bits) else {
        unreachable!("over-limit fixed input accepted");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
    let Err(error) = measure(&[], FixedKind::UInt) else {
        unreachable!("empty fixed input accepted");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn encoder_requires_exact_plan_and_output() {
    let facts = [CellValue::UInt(1)];
    let Ok(plan) = measure(&facts, FixedKind::UInt) else {
        unreachable!();
    };
    assert_eq!(
        encode(&facts, plan, &mut []).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
    assert_eq!(
        encode(&[CellValue::UInt(256)], plan, &mut [0; 2]).map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}
