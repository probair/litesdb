// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{NvrMode, NvrSymbol, Token};
use crate::{
    Error, Result,
    limits::{Limit, ensure_at_most},
};

pub(crate) fn decode(
    bytes: &[u8],
    mode: NvrMode,
    expected_fact_count: u32,
    expected_null_count: u32,
) -> Result<Vec<NvrSymbol>> {
    if expected_fact_count == 0 {
        return Err(Error::corruption("NVR", "fact count must be positive"));
    }
    ensure_at_most(Limit::SectionRows, u64::from(expected_fact_count))?;
    if expected_null_count > expected_fact_count {
        return Err(Error::corruption("NVR", "null count exceeds fact count"));
    }

    let counts = scan_tokens(bytes, mode, expected_fact_count, |_| Ok(()))?;
    if counts.0 != expected_fact_count || counts.1 != expected_null_count {
        return Err(Error::corruption(
            "NVR",
            "token counts disagree with directory",
        ));
    }

    let capacity = usize::try_from(expected_fact_count)
        .map_err(|_| Error::corruption("NVR", "fact count does not fit usize"))?;
    let mut symbols = Vec::with_capacity(capacity);
    scan_tokens(bytes, mode, expected_fact_count, |token| {
        append_token(token, &mut symbols)
    })?;
    if symbols.len() != capacity {
        return Err(Error::corruption("NVR", "decoded symbol count changed"));
    }
    Ok(symbols)
}

fn scan_tokens<F>(
    bytes: &[u8],
    mode: NvrMode,
    maximum_facts: u32,
    mut emit: F,
) -> Result<(u32, u32)>
where
    F: FnMut(Token) -> Result<()>,
{
    if bytes.is_empty() {
        return Err(Error::corruption("NVR", "value stream is empty"));
    }
    let mut offset = 0_usize;
    let mut facts = 0_u32;
    let mut nulls = 0_u32;
    let mut previous = None;
    while offset < bytes.len() {
        let (token, next) = parse_token(bytes, offset)?;
        if token.repeats_run(previous) {
            return Err(Error::corruption(
                "NVR",
                "adjacent equal runs must be merged",
            ));
        }
        if matches!(token, Token::NullRun(_)) && !mode.allows_null() {
            return Err(Error::corruption("NVR", "timestamp stream contains NRUN"));
        }
        let represented = match token {
            Token::Lit(_) => 1,
            Token::ZeroRun(count) | Token::NullRun(count) => u32::try_from(count)
                .map_err(|_| Error::corruption("NVR", "run length exceeds row domain"))?,
        };
        facts = facts
            .checked_add(represented)
            .ok_or_else(|| Error::corruption("NVR", "fact count overflow"))?;
        if facts > maximum_facts {
            return Err(Error::corruption(
                "NVR",
                "tokens exceed expected fact count",
            ));
        }
        if matches!(token, Token::NullRun(_)) {
            nulls = nulls
                .checked_add(represented)
                .ok_or_else(|| Error::corruption("NVR", "null count overflow"))?;
        }
        emit(token)?;
        previous = Some(token);
        offset = next;
    }
    Ok((facts, nulls))
}

fn parse_token(bytes: &[u8], offset: usize) -> Result<(Token, usize)> {
    let Some(first) = bytes.get(offset).copied() else {
        return Err(Error::corruption("NVR", "token starts beyond input"));
    };
    let (kind, continuation, first_mask, first_bits) = if first & 0x80 == 0 {
        (0_u8, first & 0x40 != 0, 0x3f_u8, 6_u32)
    } else if first & 0xc0 == 0x80 {
        (1, first & 0x20 != 0, 0x1f, 5)
    } else if first & 0xe0 == 0xc0 {
        (2, first & 0x10 != 0, 0x0f, 4)
    } else {
        return Err(Error::corruption("NVR", "reserved token tag"));
    };

    let mut payload = u64::from(first & first_mask);
    let mut next = offset
        .checked_add(1)
        .ok_or_else(|| Error::corruption("NVR", "input offset overflow"))?;
    let mut shift = first_bits;
    let mut length = 1_u8;
    let mut more = continuation;
    while more {
        if length == 10 {
            return Err(Error::corruption("NVR", "token exceeds ten bytes"));
        }
        let Some(byte) = bytes.get(next).copied() else {
            return Err(Error::corruption("NVR", "truncated continuation"));
        };
        let chunk = u64::from(byte & 0x7f);
        let maximum_chunk = u64::MAX
            .checked_shr(shift)
            .ok_or_else(|| Error::corruption("NVR", "continuation shift overflow"))?;
        if chunk > maximum_chunk {
            return Err(Error::corruption("NVR", "token payload exceeds u64"));
        }
        let shifted = chunk
            .checked_shl(shift)
            .ok_or_else(|| Error::corruption("NVR", "continuation shift overflow"))?;
        payload |= shifted;
        next = next
            .checked_add(1)
            .ok_or_else(|| Error::corruption("NVR", "input offset overflow"))?;
        length = length
            .checked_add(1)
            .ok_or_else(|| Error::corruption("NVR", "token length overflow"))?;
        more = byte & 0x80 != 0;
        if more {
            shift = shift
                .checked_add(7)
                .ok_or_else(|| Error::corruption("NVR", "continuation shift overflow"))?;
        } else if chunk == 0 {
            return Err(Error::corruption("NVR", "varint is not shortest"));
        }
    }

    if payload == 0 {
        return Err(Error::corruption("NVR", "token payload must be nonzero"));
    }
    let token = match kind {
        0 => Token::Lit(payload),
        1 => Token::ZeroRun(payload),
        2 => Token::NullRun(payload),
        _ => return Err(Error::corruption("NVR", "unknown parsed token kind")),
    };
    Ok((token, next))
}

fn append_token(token: Token, output: &mut Vec<NvrSymbol>) -> Result<()> {
    match token {
        Token::Lit(value) => output.push(NvrSymbol::Value(value)),
        Token::ZeroRun(count) | Token::NullRun(count) => {
            let count = usize::try_from(count)
                .map_err(|_| Error::corruption("NVR", "run length does not fit usize"))?;
            let new_len = output
                .len()
                .checked_add(count)
                .ok_or_else(|| Error::corruption("NVR", "decoded length overflow"))?;
            let symbol = if matches!(token, Token::ZeroRun(_)) {
                NvrSymbol::Value(0)
            } else {
                NvrSymbol::Null
            };
            output.resize(new_len, symbol);
        }
    }
    Ok(())
}
