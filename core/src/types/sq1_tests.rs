// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SQ1_FP4, Sq1};

#[test]
fn table_has_exact_endpoints_and_segment_steps() {
    assert_eq!(SQ1_FP4.len(), 255);
    assert_eq!((SQ1_FP4[0], SQ1_FP4[254]), (0, 1_000_000));

    for (code, pair) in SQ1_FP4.windows(2).enumerate() {
        let step = pair[1] - pair[0];
        let expected = match code {
            0..=49 => 1_000,
            50..=77 => 2_500,
            78..=253 => 5_000,
            _ => unreachable!(),
        };
        assert_eq!(step, expected, "step after code {code}");
    }
}

#[test]
fn code_255_is_reserved_for_null() {
    assert_eq!(Sq1::new(u8::MAX), None);
    for code in 0..u8::MAX {
        let Some(value) = Sq1::new(code) else {
            unreachable!("non-null code rejected");
        };
        assert_eq!(value.code(), code);
        assert_eq!(value.fp4(), SQ1_FP4[usize::from(code)]);
    }
}
