// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by Seal section assembly and decode in M4"
)]

use crate::{
    Error, Result,
    codec::{
        diff2::{DecodedDiff2, Diff2Decoder, Diff2Encoder, Diff2Value, Preprocessed},
        nvr::{self, NvrEncoder, NvrMeasurer, NvrMode, NvrPlan},
    },
    limits::{Limit, MAX_SECTION_ROWS, ensure_at_most},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimestampEncoding {
    Fixed,
    Nvr,
}

impl TimestampEncoding {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Fixed => 0,
            Self::Nvr => 1,
        }
    }

    const fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::Fixed),
            1 => Ok(Self::Nvr),
            _ => Err(Error::corruption(
                "timestamp encoding",
                "unknown encoding tag",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimestampPlan {
    encoding: TimestampEncoding,
    fixed_len: u32,
    nvr: Option<NvrPlan>,
    row_count: u32,
}

impl TimestampPlan {
    pub(crate) const fn encoding(self) -> TimestampEncoding {
        self.encoding
    }

    pub(crate) const fn byte_len(self) -> u32 {
        match (self.encoding, self.nvr) {
            (TimestampEncoding::Nvr, Some(plan)) => plan.byte_len(),
            (TimestampEncoding::Fixed | TimestampEncoding::Nvr, None)
            | (TimestampEncoding::Fixed, Some(_)) => self.fixed_len,
        }
    }

    pub(crate) const fn row_count(self) -> u32 {
        self.row_count
    }
}

pub(crate) fn select(timestamps: &[i64]) -> Result<TimestampPlan> {
    let row_count = validate_source(timestamps)?;
    let fixed_len = row_count
        .checked_mul(8)
        .ok_or_else(|| Error::limit("timestamp_bytes", u64::MAX, u64::from(u32::MAX)))?;
    let mut predictor = Diff2Encoder::timestamps();
    let mut measurer = Some(NvrMeasurer::new(NvrMode::Timestamps));
    for timestamp in timestamps {
        if let Some(active) = measurer.as_mut() {
            match predictor.observe(Diff2Value::Timestamp(*timestamp))? {
                Preprocessed::Symbol(symbol) => active.observe(symbol)?,
                Preprocessed::Unavailable => measurer = None,
            }
        }
    }
    let nvr = measurer.map(NvrMeasurer::finish).transpose()?;
    if let Some(plan) = nvr
        && (plan.fact_count() != row_count || plan.null_count() != 0)
    {
        return Err(Error::corruption(
            "timestamp selector",
            "candidate counts disagree",
        ));
    }
    let encoding = match nvr {
        Some(plan) if plan.byte_len() < fixed_len => TimestampEncoding::Nvr,
        Some(_) | None => TimestampEncoding::Fixed,
    };
    Ok(TimestampPlan {
        encoding,
        fixed_len,
        nvr,
        row_count,
    })
}

pub(crate) fn encode(timestamps: &[i64], plan: TimestampPlan, output: &mut [u8]) -> Result<usize> {
    if select(timestamps)? != plan {
        return Err(Error::corruption(
            "timestamp selector",
            "encoding plan mismatch",
        ));
    }
    let expected = usize::try_from(plan.byte_len())
        .map_err(|_| Error::corruption("timestamp selector", "length does not fit usize"))?;
    if output.len() != expected {
        return Err(Error::invalid("output", "length must equal measured size"));
    }
    match plan.encoding {
        TimestampEncoding::Fixed => encode_fixed(timestamps, output)?,
        TimestampEncoding::Nvr => encode_nvr(timestamps, plan, output)?,
    }
    Ok(expected)
}

fn encode_fixed(timestamps: &[i64], output: &mut [u8]) -> Result<()> {
    for (timestamp, chunk) in timestamps.iter().zip(output.chunks_exact_mut(8)) {
        chunk.copy_from_slice(&timestamp.to_le_bytes());
    }
    if !output.chunks_exact(8).remainder().is_empty() {
        return Err(Error::corruption(
            "timestamp selector",
            "fixed output is not i64 aligned",
        ));
    }
    Ok(())
}

fn encode_nvr(timestamps: &[i64], plan: TimestampPlan, output: &mut [u8]) -> Result<()> {
    let Some(expected_plan) = plan.nvr else {
        return Err(Error::corruption(
            "timestamp selector",
            "NVR plan is absent",
        ));
    };
    let mut predictor = Diff2Encoder::timestamps();
    let mut encoder = NvrEncoder::new(NvrMode::Timestamps, output);
    for timestamp in timestamps {
        let Preprocessed::Symbol(symbol) = predictor.observe(Diff2Value::Timestamp(*timestamp))?
        else {
            return Err(Error::corruption(
                "timestamp selector",
                "selected NVR became unavailable",
            ));
        };
        encoder.observe(symbol)?;
    }
    let (actual, written) = encoder.finish()?;
    if actual != expected_plan || written != output.len() {
        return Err(Error::corruption(
            "timestamp selector",
            "encoded result differs from plan",
        ));
    }
    Ok(())
}

pub(crate) fn decode(
    encoding_tag: u8,
    bytes: &[u8],
    row_count: u32,
    expected_min: i64,
    expected_max: i64,
) -> Result<Vec<i64>> {
    validate_row_count(row_count)?;
    let timestamps = match TimestampEncoding::from_tag(encoding_tag)? {
        TimestampEncoding::Fixed => decode_fixed(bytes, row_count)?,
        TimestampEncoding::Nvr => decode_nvr(bytes, row_count)?,
    };
    validate_decoded(&timestamps, expected_min, expected_max)?;
    Ok(timestamps)
}

fn decode_fixed(bytes: &[u8], row_count: u32) -> Result<Vec<i64>> {
    let expected = row_count
        .checked_mul(8)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| Error::corruption("timestamps", "fixed length overflow"))?;
    if bytes.len() != expected {
        return Err(Error::corruption(
            "timestamps",
            "fixed stream length mismatch",
        ));
    }
    bytes
        .chunks_exact(8)
        .map(|chunk| {
            <[u8; 8]>::try_from(chunk)
                .map(i64::from_le_bytes)
                .map_err(|_| Error::corruption("timestamps", "invalid fixed scalar width"))
        })
        .collect()
}

fn decode_nvr(bytes: &[u8], row_count: u32) -> Result<Vec<i64>> {
    let symbols = nvr::decode(bytes, NvrMode::Timestamps, row_count, 0)?;
    let mut predictor = Diff2Decoder::timestamps();
    symbols
        .into_iter()
        .map(|symbol| match predictor.decode(symbol)? {
            DecodedDiff2::Timestamp(timestamp) => Ok(timestamp),
            DecodedDiff2::Null | DecodedDiff2::Unsigned(_) => Err(Error::corruption(
                "timestamps",
                "inverse predictor returned a non-timestamp",
            )),
        })
        .collect()
}

fn validate_source(timestamps: &[i64]) -> Result<u32> {
    let row_count = u32::try_from(timestamps.len())
        .map_err(|_| Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS)))?;
    validate_row_count(row_count)?;
    if timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(Error::invalid(
            "timestamps",
            "section timestamps must be strictly increasing",
        ));
    }
    Ok(row_count)
}

fn validate_row_count(row_count: u32) -> Result<()> {
    if row_count == 0 {
        return Err(Error::corruption(
            "timestamps",
            "row count must be positive",
        ));
    }
    ensure_at_most(Limit::SectionRows, u64::from(row_count))
}

fn validate_decoded(timestamps: &[i64], expected_min: i64, expected_max: i64) -> Result<()> {
    if timestamps.first() != Some(&expected_min)
        || timestamps.last() != Some(&expected_max)
        || timestamps.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(Error::corruption(
            "timestamps",
            "decoded order or range disagrees with directory",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "timestamp_tests.rs"]
mod tests;
