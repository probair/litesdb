// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by selector and typed column codecs later in M2"
)]

use crate::{Error, F32Bits, Result, codec::nvr::NvrSymbol};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Diff2Value {
    Null,
    Unsigned(u64),
    Timestamp(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecodedDiff2 {
    Null,
    Unsigned(u64),
    Timestamp(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Preprocessed {
    Symbol(NvrSymbol),
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Diff2Kind {
    Unsigned { maximum: u64 },
    Timestamp,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Diff2Encoder {
    kind: Diff2Kind,
    previous_value: Option<i128>,
    previous_delta: Option<i128>,
    available: bool,
}

impl Diff2Encoder {
    pub(crate) const fn unsigned(maximum: u64) -> Self {
        Self::new(Diff2Kind::Unsigned { maximum })
    }

    pub(crate) const fn timestamps() -> Self {
        Self::new(Diff2Kind::Timestamp)
    }

    const fn new(kind: Diff2Kind) -> Self {
        Self {
            kind,
            previous_value: None,
            previous_delta: None,
            available: true,
        }
    }

    pub(crate) fn observe(&mut self, value: Diff2Value) -> Result<Preprocessed> {
        if !self.available {
            return Ok(Preprocessed::Unavailable);
        }
        let Some(value) = self.input_value(value)? else {
            return Ok(Preprocessed::Symbol(NvrSymbol::Null));
        };

        let encoded = match self.previous_value {
            None => self.encode_first(value),
            Some(previous) => self.encode_next(value, previous),
        };
        let Some(encoded) = encoded else {
            self.available = false;
            return Ok(Preprocessed::Unavailable);
        };
        Ok(Preprocessed::Symbol(NvrSymbol::Value(encoded)))
    }

    fn input_value(self, value: Diff2Value) -> Result<Option<i128>> {
        match (self.kind, value) {
            (Diff2Kind::Unsigned { .. }, Diff2Value::Null) => Ok(None),
            (Diff2Kind::Timestamp, Diff2Value::Null) => Err(Error::invalid(
                "timestamp",
                "timestamp stream cannot contain null",
            )),
            (Diff2Kind::Unsigned { maximum }, Diff2Value::Unsigned(value)) if value <= maximum => {
                Ok(Some(i128::from(value)))
            }
            (Diff2Kind::Unsigned { .. }, Diff2Value::Unsigned(_)) => Err(Error::invalid(
                "value",
                "unsigned value exceeds logical domain",
            )),
            (Diff2Kind::Timestamp, Diff2Value::Timestamp(value)) => Ok(Some(i128::from(value))),
            (Diff2Kind::Unsigned { .. }, Diff2Value::Timestamp(_))
            | (Diff2Kind::Timestamp, Diff2Value::Unsigned(_)) => Err(Error::invalid(
                "value",
                "value kind does not match predictor",
            )),
        }
    }

    fn encode_first(&mut self, value: i128) -> Option<u64> {
        let encoded = match self.kind {
            Diff2Kind::Unsigned { .. } => u64::try_from(value).ok(),
            Diff2Kind::Timestamp => zigzag(value),
        }?;
        self.previous_value = Some(value);
        Some(encoded)
    }

    fn encode_next(&mut self, value: i128, previous: i128) -> Option<u64> {
        let delta = value.checked_sub(previous)?;
        let predicted = match self.previous_delta {
            Some(previous_delta) => delta.checked_sub(previous_delta)?,
            None => delta,
        };
        let encoded = zigzag(predicted)?;
        self.previous_value = Some(value);
        self.previous_delta = Some(delta);
        Some(encoded)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Diff2Decoder {
    kind: Diff2Kind,
    previous_value: Option<i128>,
    previous_delta: Option<i128>,
}

impl Diff2Decoder {
    pub(crate) const fn unsigned(maximum: u64) -> Self {
        Self::new(Diff2Kind::Unsigned { maximum })
    }

    pub(crate) const fn timestamps() -> Self {
        Self::new(Diff2Kind::Timestamp)
    }

    const fn new(kind: Diff2Kind) -> Self {
        Self {
            kind,
            previous_value: None,
            previous_delta: None,
        }
    }

    pub(crate) fn decode(&mut self, symbol: NvrSymbol) -> Result<DecodedDiff2> {
        if symbol == NvrSymbol::Null {
            return if matches!(self.kind, Diff2Kind::Unsigned { .. }) {
                Ok(DecodedDiff2::Null)
            } else {
                Err(Error::corruption("diff2", "timestamp stream contains null"))
            };
        }
        let NvrSymbol::Value(encoded) = symbol else {
            return Err(Error::corruption("diff2", "invalid NVR symbol"));
        };

        let value = match self.previous_value {
            None => self.decode_first(encoded)?,
            Some(previous) => self.decode_next(encoded, previous)?,
        };
        self.output_value(value)
    }

    fn decode_first(&mut self, encoded: u64) -> Result<i128> {
        let value = match self.kind {
            Diff2Kind::Unsigned { .. } => i128::from(encoded),
            Diff2Kind::Timestamp => unzigzag(encoded)?,
        };
        self.validate_domain(value)?;
        self.previous_value = Some(value);
        Ok(value)
    }

    fn decode_next(&mut self, encoded: u64, previous: i128) -> Result<i128> {
        let predicted = unzigzag(encoded)?;
        let delta = match self.previous_delta {
            Some(previous_delta) => previous_delta
                .checked_add(predicted)
                .ok_or_else(|| Error::corruption("diff2", "delta overflow"))?,
            None => predicted,
        };
        let value = previous
            .checked_add(delta)
            .ok_or_else(|| Error::corruption("diff2", "value overflow"))?;
        self.validate_domain(value)?;
        self.previous_value = Some(value);
        self.previous_delta = Some(delta);
        Ok(value)
    }

    fn validate_domain(self, value: i128) -> Result<()> {
        match self.kind {
            Diff2Kind::Unsigned { maximum } if value >= 0 && value <= i128::from(maximum) => Ok(()),
            Diff2Kind::Timestamp
                if value >= i128::from(i64::MIN) && value <= i128::from(i64::MAX) =>
            {
                Ok(())
            }
            Diff2Kind::Unsigned { .. } => {
                Err(Error::corruption("diff2", "unsigned value outside domain"))
            }
            Diff2Kind::Timestamp => Err(Error::corruption("diff2", "timestamp outside i64 domain")),
        }
    }

    fn output_value(self, value: i128) -> Result<DecodedDiff2> {
        match self.kind {
            Diff2Kind::Unsigned { .. } => u64::try_from(value)
                .map(DecodedDiff2::Unsigned)
                .map_err(|_| Error::corruption("diff2", "unsigned conversion failed")),
            Diff2Kind::Timestamp => i64::try_from(value)
                .map(DecodedDiff2::Timestamp)
                .map_err(|_| Error::corruption("diff2", "timestamp conversion failed")),
        }
    }
}

fn zigzag(value: i128) -> Option<u64> {
    let encoded = if value >= 0 {
        value.checked_mul(2)?
    } else {
        value.checked_neg()?.checked_mul(2)?.checked_sub(1)?
    };
    u64::try_from(encoded).ok()
}

fn unzigzag(encoded: u64) -> Result<i128> {
    let magnitude = i128::from(
        encoded
            .checked_shr(1)
            .ok_or_else(|| Error::corruption("diff2", "zigzag shift failed"))?,
    );
    if encoded & 1 == 0 {
        Ok(magnitude)
    } else {
        magnitude
            .checked_add(1)
            .and_then(i128::checked_neg)
            .ok_or_else(|| Error::corruption("diff2", "zigzag inverse overflow"))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct XorEncoder {
    previous: Option<u32>,
}

impl XorEncoder {
    pub(crate) fn observe(&mut self, value: Option<F32Bits>) -> NvrSymbol {
        let Some(value) = value else {
            return NvrSymbol::Null;
        };
        let bits = value.bits();
        let encoded = self.previous.map_or(bits, |previous| previous ^ bits);
        self.previous = Some(bits);
        NvrSymbol::Value(u64::from(encoded))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct XorDecoder {
    previous: Option<u32>,
}

impl XorDecoder {
    pub(crate) fn decode(&mut self, symbol: NvrSymbol) -> Result<Option<F32Bits>> {
        let NvrSymbol::Value(encoded) = symbol else {
            return Ok(None);
        };
        let encoded = u32::try_from(encoded)
            .map_err(|_| Error::corruption("F32 XOR", "encoded value exceeds u32"))?;
        let bits = self.previous.map_or(encoded, |previous| previous ^ encoded);
        self.previous = Some(bits);
        Ok(Some(F32Bits::from_bits(bits)))
    }
}

#[cfg(test)]
#[path = "diff2_tests.rs"]
mod tests;
