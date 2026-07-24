// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{CellValue, F32Bits, ValueType};

#[test]
fn float_bits_round_trip_without_numeric_interpretation() {
    let patterns = [
        0x0000_0000,
        0x8000_0000,
        0x0000_0001,
        0x007f_ffff,
        0x7f80_0000,
        0xff80_0000,
        0x7fc0_0001,
        0xffa1_2345,
    ];
    for bits in patterns {
        assert_eq!(F32Bits::from_bits(bits).bits(), bits);
    }
}

#[test]
fn sq1_null_code_enters_the_common_null_path() {
    assert_eq!(CellValue::sq1(255), CellValue::Null);
    let CellValue::Sq1(value) = CellValue::sq1(254) else {
        unreachable!();
    };
    assert_eq!(value.code(), 254);
}

#[test]
fn explicit_null_matches_every_field_but_values_match_one_type() {
    for expected in [ValueType::UInt, ValueType::Sq1, ValueType::F32Bits] {
        assert!(CellValue::Null.matches(expected));
    }

    let values = [
        (CellValue::UInt(7), ValueType::UInt),
        (CellValue::sq1(1), ValueType::Sq1),
        (
            CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234)),
            ValueType::F32Bits,
        ),
    ];
    for (value, expected) in values {
        assert_eq!(value.value_type(), Some(expected));
        assert!(value.matches(expected));
        for other in [ValueType::UInt, ValueType::Sq1, ValueType::F32Bits] {
            assert_eq!(value.matches(other), expected == other);
        }
    }
}

#[test]
fn value_type_tags_are_format_stable() {
    assert_eq!(ValueType::UInt.tag(), 0);
    assert_eq!(ValueType::Sq1.tag(), 1);
    assert_eq!(ValueType::F32Bits.tag(), 2);
}
