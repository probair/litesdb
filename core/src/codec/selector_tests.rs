// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ValueEncoding, decode, encode, select};
use crate::{CellValue, ErrorKind, F32Bits, Sq1, ValueType};

fn sq1(code: u8) -> CellValue {
    let Some(value) = Sq1::new(code) else {
        unreachable!("test requested SQ1 null code as value");
    };
    CellValue::Sq1(value)
}

fn encoded(facts: &[CellValue], value_type: ValueType) -> (ValueEncoding, Vec<u8>) {
    let Ok(selection) = select(facts, value_type) else {
        unreachable!("valid selector fixture rejected");
    };
    let Ok(length) = usize::try_from(selection.byte_len()) else {
        unreachable!("bounded selection length does not fit usize");
    };
    let mut bytes = vec![0_u8; length];
    let Ok(written) = encode(facts, value_type, selection, &mut bytes) else {
        unreachable!("measured selector fixture failed to encode");
    };
    assert_eq!(written, bytes.len());
    (selection.encoding(), bytes)
}

#[test]
fn strict_smaller_and_tie_rules_are_deterministic() {
    let Ok(smaller) = select(&[CellValue::UInt(1)], ValueType::UInt) else {
        unreachable!();
    };
    assert_eq!(smaller.encoding(), ValueEncoding::Nvr);
    assert_eq!(smaller.byte_len(), 1);

    let Ok(tie) = select(&[CellValue::UInt(64)], ValueType::UInt) else {
        unreachable!();
    };
    assert_eq!(tie.encoding(), ValueEncoding::Fixed);
    assert_eq!(tie.byte_len(), 2);
}

#[test]
fn extreme_diff2_domain_falls_back_to_fixed() {
    let facts = [CellValue::UInt(0), CellValue::UInt(u64::MAX)];
    let Ok(selection) = select(&facts, ValueType::UInt) else {
        unreachable!("extreme diff2 sequence should retain fixed candidate");
    };
    assert_eq!(selection.encoding(), ValueEncoding::Fixed);
    let (encoding, bytes) = encoded(&facts, ValueType::UInt);
    assert_eq!(encoding, ValueEncoding::Fixed);
    assert_eq!(
        decode(ValueType::UInt, encoding.tag(), &bytes, 2, 0)
            .ok()
            .as_deref(),
        Some(facts.as_slice())
    );
}

#[test]
fn uint_nvr_bytes_and_round_trip_are_exact() {
    let facts = [CellValue::UInt(1), CellValue::UInt(2), CellValue::UInt(3)];
    let (encoding, bytes) = encoded(&facts, ValueType::UInt);
    assert_eq!(encoding, ValueEncoding::Nvr);
    assert_eq!(bytes, [0x01, 0x02, 0x81]);
    assert_eq!(
        decode(ValueType::UInt, encoding.tag(), &bytes, 3, 0)
            .ok()
            .as_deref(),
        Some(facts.as_slice())
    );
}

#[test]
fn sq1_and_f32_nvr_round_trip_exactly() {
    let sq1_facts = [sq1(1), sq1(2), sq1(3), sq1(4)];
    let (encoding, bytes) = encoded(&sq1_facts, ValueType::Sq1);
    assert_eq!(encoding, ValueEncoding::Nvr);
    assert_eq!(bytes, [0x01, 0x02, 0x82]);
    assert_eq!(
        decode(ValueType::Sq1, encoding.tag(), &bytes, 4, 0)
            .ok()
            .as_deref(),
        Some(sq1_facts.as_slice())
    );

    let f32_facts = [
        CellValue::F32Bits(F32Bits::from_bits(0)),
        CellValue::F32Bits(F32Bits::from_bits(0)),
        CellValue::Null,
    ];
    let (encoding, bytes) = encoded(&f32_facts, ValueType::F32Bits);
    assert_eq!(encoding, ValueEncoding::Nvr);
    assert_eq!(bytes, [0x82, 0xc1]);
    assert_eq!(
        decode(ValueType::F32Bits, encoding.tag(), &bytes, 3, 1)
            .ok()
            .as_deref(),
        Some(f32_facts.as_slice())
    );
}

#[test]
fn all_null_stream_is_counted_and_round_trips() {
    let facts = [CellValue::Null; 8];
    let Ok(selection) = select(&facts, ValueType::UInt) else {
        unreachable!();
    };
    assert_eq!(selection.encoding(), ValueEncoding::Nvr);
    assert_eq!((selection.fact_count(), selection.null_count()), (8, 8));
    let (_, bytes) = encoded(&facts, ValueType::UInt);
    assert_eq!(bytes, [0xc8]);
    assert_eq!(
        decode(ValueType::UInt, 1, &bytes, 8, 8).ok().as_deref(),
        Some(facts.as_slice())
    );
}

#[test]
fn write_side_contracts_are_strict() {
    let Err(error) = select(&[sq1(1)], ValueType::UInt) else {
        unreachable!("mismatched selector type accepted");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let Err(error) = select(&[], ValueType::UInt) else {
        unreachable!("empty selector input accepted");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let first = [CellValue::UInt(1)];
    let Ok(selection) = select(&first, ValueType::UInt) else {
        unreachable!();
    };
    assert_eq!(
        encode(&first, ValueType::UInt, selection, &mut []).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
    assert_eq!(
        encode(&[CellValue::UInt(64)], ValueType::UInt, selection, &mut [0])
            .map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}

#[test]
fn malformed_read_side_inputs_are_rejected() {
    let cases = [
        decode(ValueType::UInt, 2, &[1], 1, 0),
        decode(ValueType::UInt, 1, &[1], 2, 0),
        decode(ValueType::Sq1, 1, &[0x7f, 0x03], 1, 0),
    ];
    for result in cases {
        let Err(error) = result else {
            unreachable!("malformed selector stream accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}
