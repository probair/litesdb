// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by typed preprocessors later in M2")]

use crate::{
    Error, Result,
    limits::{Limit, MAX_SECTION_ROWS, ensure_at_most},
};

#[path = "nvr_decode.rs"]
mod decode_impl;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NvrSymbol {
    Null,
    Value(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NvrMode {
    Values,
    Timestamps,
}

impl NvrMode {
    const fn allows_null(self) -> bool {
        matches!(self, Self::Values)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NvrPlan {
    byte_len: u32,
    fact_count: u32,
    null_count: u32,
}

impl NvrPlan {
    pub(crate) const fn byte_len(self) -> u32 {
        self.byte_len
    }

    pub(crate) const fn fact_count(self) -> u32 {
        self.fact_count
    }

    pub(crate) const fn null_count(self) -> u32 {
        self.null_count
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    Lit(u64),
    ZeroRun(u64),
    NullRun(u64),
}

impl Token {
    const fn first_payload_bits(self) -> u32 {
        match self {
            Self::Lit(_) => 6,
            Self::ZeroRun(_) => 5,
            Self::NullRun(_) => 4,
        }
    }

    const fn payload(self) -> u64 {
        match self {
            Self::Lit(value) | Self::ZeroRun(value) | Self::NullRun(value) => value,
        }
    }

    const fn first_byte_parts(self) -> (u8, u8, u8) {
        match self {
            Self::Lit(_) => (0x00, 0x40, 0x3f),
            Self::ZeroRun(_) => (0x80, 0x20, 0x1f),
            Self::NullRun(_) => (0xc0, 0x10, 0x0f),
        }
    }

    const fn repeats_run(self, previous: Option<Self>) -> bool {
        matches!(
            (previous, self),
            (Some(Self::ZeroRun(_)), Self::ZeroRun(_)) | (Some(Self::NullRun(_)), Self::NullRun(_))
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct NvrState {
    mode: NvrMode,
    pending: Option<Token>,
    fact_count: u32,
    null_count: u32,
}

impl NvrState {
    const fn new(mode: NvrMode) -> Self {
        Self {
            mode,
            pending: None,
            fact_count: 0,
            null_count: 0,
        }
    }

    fn observe(&mut self, symbol: NvrSymbol) -> Result<Option<Token>> {
        let fact_count = self
            .fact_count
            .checked_add(1)
            .ok_or_else(|| Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS)))?;
        ensure_at_most(Limit::SectionRows, u64::from(fact_count))?;
        let next = match symbol {
            NvrSymbol::Value(0) => Token::ZeroRun(1),
            NvrSymbol::Value(value) => Token::Lit(value),
            NvrSymbol::Null if self.mode.allows_null() => Token::NullRun(1),
            NvrSymbol::Null => {
                return Err(Error::invalid(
                    "symbols",
                    "timestamp NVR stream cannot contain null",
                ));
            }
        };
        let completed = match (self.pending, next) {
            (Some(Token::ZeroRun(count)), Token::ZeroRun(_)) => {
                self.pending = Some(Token::ZeroRun(count.checked_add(1).ok_or_else(|| {
                    Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS))
                })?));
                None
            }
            (Some(Token::NullRun(count)), Token::NullRun(_)) => {
                self.pending = Some(Token::NullRun(count.checked_add(1).ok_or_else(|| {
                    Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS))
                })?));
                None
            }
            (Some(previous), next) => {
                self.pending = Some(next);
                Some(previous)
            }
            (None, next) => {
                self.pending = Some(next);
                None
            }
        };
        self.fact_count = fact_count;
        if symbol == NvrSymbol::Null {
            self.null_count = self.null_count.checked_add(1).ok_or_else(|| {
                Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS))
            })?;
        }
        Ok(completed)
    }

    fn finish(self) -> Result<(Token, u32, u32)> {
        let Some(token) = self.pending else {
            return Err(Error::invalid("symbols", "NVR stream must not be empty"));
        };
        Ok((token, self.fact_count, self.null_count))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NvrMeasurer {
    state: NvrState,
    byte_len: u32,
}

impl NvrMeasurer {
    pub(crate) const fn new(mode: NvrMode) -> Self {
        Self {
            state: NvrState::new(mode),
            byte_len: 0,
        }
    }

    pub(crate) fn observe(&mut self, symbol: NvrSymbol) -> Result<()> {
        if let Some(token) = self.state.observe(symbol)? {
            self.add_token(token)?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<NvrPlan> {
        let (token, fact_count, null_count) = self.state.finish()?;
        self.add_token(token)?;
        Ok(NvrPlan {
            byte_len: self.byte_len,
            fact_count,
            null_count,
        })
    }

    fn add_token(&mut self, token: Token) -> Result<()> {
        self.byte_len = self
            .byte_len
            .checked_add(token_len(token)?)
            .ok_or_else(|| Error::limit("nvr_bytes", u64::MAX, u64::from(u32::MAX)))?;
        Ok(())
    }
}

pub(crate) struct NvrEncoder<'a> {
    state: NvrState,
    output: &'a mut [u8],
    offset: usize,
}

impl<'a> NvrEncoder<'a> {
    pub(crate) fn new(mode: NvrMode, output: &'a mut [u8]) -> Self {
        Self {
            state: NvrState::new(mode),
            output,
            offset: 0,
        }
    }

    pub(crate) fn observe(&mut self, symbol: NvrSymbol) -> Result<()> {
        if let Some(token) = self.state.observe(symbol)? {
            write_token(token, self.output, &mut self.offset)?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(NvrPlan, usize)> {
        let (token, fact_count, null_count) = self.state.finish()?;
        write_token(token, self.output, &mut self.offset)?;
        let byte_len = u32::try_from(self.offset)
            .map_err(|_| Error::corruption("NVR", "written length does not fit u32"))?;
        Ok((
            NvrPlan {
                byte_len,
                fact_count,
                null_count,
            },
            self.offset,
        ))
    }
}

pub(crate) fn measure(symbols: &[NvrSymbol], mode: NvrMode) -> Result<NvrPlan> {
    let mut measurer = NvrMeasurer::new(mode);
    for symbol in symbols {
        measurer.observe(*symbol)?;
    }
    measurer.finish()
}

pub(crate) fn encode(
    symbols: &[NvrSymbol],
    mode: NvrMode,
    plan: NvrPlan,
    output: &mut [u8],
) -> Result<usize> {
    if measure(symbols, mode)? != plan {
        return Err(Error::corruption("NVR", "encoding plan mismatch"));
    }
    let expected = usize::try_from(plan.byte_len)
        .map_err(|_| Error::corruption("NVR", "byte length does not fit usize"))?;
    if output.len() != expected {
        return Err(Error::invalid("output", "length must equal measured size"));
    }

    let mut encoder = NvrEncoder::new(mode, output);
    for symbol in symbols {
        encoder.observe(*symbol)?;
    }
    let (actual, written) = encoder.finish()?;
    if actual != plan || written != expected {
        return Err(Error::corruption("NVR", "encoded result differs from plan"));
    }
    Ok(written)
}

pub(crate) fn decode(
    bytes: &[u8],
    mode: NvrMode,
    expected_fact_count: u32,
    expected_null_count: u32,
) -> Result<Vec<NvrSymbol>> {
    decode_impl::decode(bytes, mode, expected_fact_count, expected_null_count)
}

fn token_len(token: Token) -> Result<u32> {
    let payload = token.payload();
    if payload == 0 {
        return Err(Error::corruption("NVR", "token payload must be nonzero"));
    }
    let mut remaining = payload
        .checked_shr(token.first_payload_bits())
        .ok_or_else(|| Error::corruption("NVR", "invalid first payload width"))?;
    let mut length = 1_u32;
    while remaining != 0 {
        length = length
            .checked_add(1)
            .ok_or_else(|| Error::corruption("NVR", "token length overflow"))?;
        remaining = remaining
            .checked_shr(7)
            .ok_or_else(|| Error::corruption("NVR", "invalid continuation width"))?;
    }
    Ok(length)
}

fn write_token(token: Token, output: &mut [u8], offset: &mut usize) -> Result<()> {
    let payload = token.payload();
    if payload == 0 {
        return Err(Error::corruption("NVR", "token payload must be nonzero"));
    }
    let (tag, continuation, mask) = token.first_byte_parts();
    let first_payload = u8::try_from(payload & u64::from(mask))
        .map_err(|_| Error::corruption("NVR", "first payload does not fit byte"))?;
    let mut remaining = payload
        .checked_shr(token.first_payload_bits())
        .ok_or_else(|| Error::corruption("NVR", "invalid first payload width"))?;
    let first = tag | first_payload | if remaining == 0 { 0 } else { continuation };
    write_byte(output, offset, first)?;

    while remaining != 0 {
        let payload = u8::try_from(remaining & 0x7f)
            .map_err(|_| Error::corruption("NVR", "continuation payload does not fit byte"))?;
        remaining = remaining
            .checked_shr(7)
            .ok_or_else(|| Error::corruption("NVR", "invalid continuation width"))?;
        write_byte(
            output,
            offset,
            payload | if remaining == 0 { 0 } else { 0x80 },
        )?;
    }
    Ok(())
}

fn write_byte(output: &mut [u8], offset: &mut usize, byte: u8) -> Result<()> {
    let Some(target) = output.get_mut(*offset) else {
        return Err(Error::corruption("NVR", "encoder exceeded measured output"));
    };
    *target = byte;
    *offset = offset
        .checked_add(1)
        .ok_or_else(|| Error::corruption("NVR", "output offset overflow"))?;
    Ok(())
}

#[cfg(test)]
#[path = "nvr_tests.rs"]
mod tests;
