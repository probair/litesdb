// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

const VALUE_COUNT: usize = 255;

pub(crate) const SQ1_FP4: [u32; VALUE_COUNT] = build_fp4();

#[allow(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    reason = "the compile-time loop is bounded to SQ1 code points 0..=254"
)]
const fn build_fp4() -> [u32; VALUE_COUNT] {
    let mut values = [0; VALUE_COUNT];
    let mut index = 0;
    while index < VALUE_COUNT {
        let code = index as u32;
        values[index] = if code <= 50 {
            code * 1_000
        } else if code <= 78 {
            50_000 + (code - 50) * 2_500
        } else {
            120_000 + (code - 78) * 5_000
        };
        index += 1;
    }
    values
}

const _: () = {
    assert!(SQ1_FP4[0] == 0);
    assert!(SQ1_FP4[50] == 50_000);
    assert!(SQ1_FP4[78] == 120_000);
    assert!(SQ1_FP4[254] == 1_000_000);
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Sq1(u8);

impl Sq1 {
    #[must_use]
    pub const fn new(code: u8) -> Option<Self> {
        if code == u8::MAX {
            None
        } else {
            Some(Self(code))
        }
    }

    #[must_use]
    pub const fn code(self) -> u8 {
        self.0
    }

    #[must_use]
    pub const fn fp4(self) -> u32 {
        SQ1_FP4[self.0 as usize]
    }
}

#[cfg(test)]
#[path = "sq1_tests.rs"]
mod tests;
