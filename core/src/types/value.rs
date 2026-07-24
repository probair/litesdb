// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::Sq1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ValueType {
    UInt,
    Sq1,
    F32Bits,
}

impl ValueType {
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::UInt => 0,
            Self::Sq1 => 1,
            Self::F32Bits => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct F32Bits(u32);

impl F32Bits {
    #[must_use]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CellValue {
    Null,
    UInt(u64),
    Sq1(Sq1),
    F32Bits(F32Bits),
}

impl CellValue {
    #[must_use]
    pub const fn sq1(code: u8) -> Self {
        match Sq1::new(code) {
            Some(value) => Self::Sq1(value),
            None => Self::Null,
        }
    }

    #[must_use]
    pub const fn value_type(self) -> Option<ValueType> {
        match self {
            Self::Null => None,
            Self::UInt(_) => Some(ValueType::UInt),
            Self::Sq1(_) => Some(ValueType::Sq1),
            Self::F32Bits(_) => Some(ValueType::F32Bits),
        }
    }

    #[must_use]
    pub const fn matches(self, expected: ValueType) -> bool {
        match self.value_type() {
            None => true,
            Some(actual) => actual.tag() == expected.tag(),
        }
    }
}

#[cfg(test)]
#[path = "value_tests.rs"]
mod tests;
