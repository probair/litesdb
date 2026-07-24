// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by unit sealing and reads in M4/M5")]

use crate::{
    CellValue, Error, Result, Sq1, ValueType,
    codec::{
        diff2::{
            DecodedDiff2, Diff2Decoder, Diff2Encoder, Diff2Value, Preprocessed, XorDecoder,
            XorEncoder,
        },
        fixed::{self, FixedKind, FixedMeasurer, FixedPlan},
        nvr::{self, NvrEncoder, NvrMeasurer, NvrMode, NvrPlan, NvrSymbol},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueEncoding {
    Fixed,
    Nvr,
}

impl ValueEncoding {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Fixed => 0,
            Self::Nvr => 1,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::Fixed),
            1 => Ok(Self::Nvr),
            _ => Err(Error::corruption("value encoding", "unknown encoding tag")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Selection {
    encoding: ValueEncoding,
    fixed: FixedPlan,
    nvr: Option<NvrPlan>,
}

impl Selection {
    pub(crate) const fn encoding(self) -> ValueEncoding {
        self.encoding
    }

    pub(crate) const fn byte_len(self) -> u32 {
        match (self.encoding, self.nvr) {
            (ValueEncoding::Nvr, Some(plan)) => plan.byte_len(),
            (ValueEncoding::Fixed | ValueEncoding::Nvr, None) | (ValueEncoding::Fixed, Some(_)) => {
                self.fixed.byte_len()
            }
        }
    }

    pub(crate) const fn fact_count(self) -> u32 {
        self.fixed.fact_count()
    }

    pub(crate) const fn null_count(self) -> u32 {
        self.fixed.null_count()
    }
}

#[derive(Clone, Copy, Debug)]
enum Predictor {
    UInt(Diff2Encoder),
    Sq1(Diff2Encoder),
    F32(XorEncoder),
}

impl Predictor {
    fn new(value_type: ValueType) -> Self {
        match value_type {
            ValueType::UInt => Self::UInt(Diff2Encoder::unsigned(u64::MAX)),
            ValueType::Sq1 => Self::Sq1(Diff2Encoder::unsigned(254)),
            ValueType::F32Bits => Self::F32(XorEncoder::default()),
        }
    }

    fn observe(&mut self, fact: CellValue) -> Result<Preprocessed> {
        match (self, fact) {
            (Self::UInt(predictor) | Self::Sq1(predictor), CellValue::Null) => {
                predictor.observe(Diff2Value::Null)
            }
            (Self::UInt(predictor), CellValue::UInt(value)) => {
                predictor.observe(Diff2Value::Unsigned(value))
            }
            (Self::Sq1(predictor), CellValue::Sq1(value)) => {
                predictor.observe(Diff2Value::Unsigned(u64::from(value.code())))
            }
            (Self::F32(predictor), CellValue::Null) => {
                Ok(Preprocessed::Symbol(predictor.observe(None)))
            }
            (Self::F32(predictor), CellValue::F32Bits(value)) => {
                Ok(Preprocessed::Symbol(predictor.observe(Some(value))))
            }
            _ => Err(Error::invalid(
                "facts",
                "value does not match selector type",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Reconstructor {
    UInt(Diff2Decoder),
    Sq1(Diff2Decoder),
    F32(XorDecoder),
}

impl Reconstructor {
    fn new(value_type: ValueType) -> Self {
        match value_type {
            ValueType::UInt => Self::UInt(Diff2Decoder::unsigned(u64::MAX)),
            ValueType::Sq1 => Self::Sq1(Diff2Decoder::unsigned(254)),
            ValueType::F32Bits => Self::F32(XorDecoder::default()),
        }
    }

    fn decode(&mut self, symbol: NvrSymbol) -> Result<CellValue> {
        match self {
            Self::UInt(decoder) => match decoder.decode(symbol)? {
                DecodedDiff2::Null => Ok(CellValue::Null),
                DecodedDiff2::Unsigned(value) => Ok(CellValue::UInt(value)),
                DecodedDiff2::Timestamp(_) => {
                    Err(Error::corruption("selector", "unexpected timestamp"))
                }
            },
            Self::Sq1(decoder) => match decoder.decode(symbol)? {
                DecodedDiff2::Null => Ok(CellValue::Null),
                DecodedDiff2::Unsigned(value) => decode_sq1(value),
                DecodedDiff2::Timestamp(_) => {
                    Err(Error::corruption("selector", "unexpected timestamp"))
                }
            },
            Self::F32(decoder) => Ok(decoder
                .decode(symbol)?
                .map_or(CellValue::Null, CellValue::F32Bits)),
        }
    }
}

fn decode_sq1(value: u64) -> Result<CellValue> {
    let code = u8::try_from(value)
        .map_err(|_| Error::corruption("selector", "SQ1 code does not fit byte"))?;
    Sq1::new(code)
        .map(CellValue::Sq1)
        .ok_or_else(|| Error::corruption("selector", "SQ1 null code appeared as a non-null value"))
}

pub(crate) fn select(facts: &[CellValue], value_type: ValueType) -> Result<Selection> {
    let kind = FixedKind::from_value_type(value_type);
    let mut fixed_measurer = FixedMeasurer::new(kind);
    let mut predictor = Predictor::new(value_type);
    let mut nvr_measurer = Some(NvrMeasurer::new(NvrMode::Values));

    for fact in facts {
        fixed_measurer.observe(*fact)?;
        if nvr_measurer.is_some() {
            match predictor.observe(*fact)? {
                Preprocessed::Symbol(symbol) => {
                    let Some(measurer) = nvr_measurer.as_mut() else {
                        return Err(Error::corruption("selector", "NVR measurer disappeared"));
                    };
                    measurer.observe(symbol)?;
                }
                Preprocessed::Unavailable => nvr_measurer = None,
            }
        }
    }

    let fixed = fixed_measurer.finish()?;
    let nvr = nvr_measurer.map(NvrMeasurer::finish).transpose()?;
    if let Some(plan) = nvr
        && (plan.fact_count() != fixed.fact_count() || plan.null_count() != fixed.null_count())
    {
        return Err(Error::corruption("selector", "candidate counts disagree"));
    }
    let encoding = match nvr {
        Some(plan) if plan.byte_len() < fixed.byte_len() => ValueEncoding::Nvr,
        Some(_) | None => ValueEncoding::Fixed,
    };
    Ok(Selection {
        encoding,
        fixed,
        nvr,
    })
}

pub(crate) fn encode(
    facts: &[CellValue],
    value_type: ValueType,
    selection: Selection,
    output: &mut [u8],
) -> Result<usize> {
    if select(facts, value_type)? != selection {
        return Err(Error::corruption("selector", "encoding plan mismatch"));
    }
    match selection.encoding {
        ValueEncoding::Fixed => fixed::encode(facts, selection.fixed, output),
        ValueEncoding::Nvr => encode_nvr(facts, value_type, selection, output),
    }
}

fn encode_nvr(
    facts: &[CellValue],
    value_type: ValueType,
    selection: Selection,
    output: &mut [u8],
) -> Result<usize> {
    let Some(plan) = selection.nvr else {
        return Err(Error::corruption("selector", "NVR plan is absent"));
    };
    let expected = usize::try_from(plan.byte_len())
        .map_err(|_| Error::corruption("selector", "byte length does not fit usize"))?;
    if output.len() != expected {
        return Err(Error::invalid("output", "length must equal measured size"));
    }
    let mut predictor = Predictor::new(value_type);
    let mut encoder = NvrEncoder::new(NvrMode::Values, output);
    for fact in facts {
        let Preprocessed::Symbol(symbol) = predictor.observe(*fact)? else {
            return Err(Error::corruption(
                "selector",
                "selected NVR became unavailable",
            ));
        };
        encoder.observe(symbol)?;
    }
    let (actual, written) = encoder.finish()?;
    if actual != plan || written != expected {
        return Err(Error::corruption(
            "selector",
            "encoded result differs from plan",
        ));
    }
    Ok(written)
}

pub(crate) fn decode(
    value_type: ValueType,
    encoding_tag: u8,
    bytes: &[u8],
    fact_count: u32,
    null_count: u32,
) -> Result<Vec<CellValue>> {
    match ValueEncoding::from_tag(encoding_tag)? {
        ValueEncoding::Fixed => fixed::decode(
            FixedKind::from_value_type(value_type),
            bytes,
            fact_count,
            null_count,
        ),
        ValueEncoding::Nvr => {
            let symbols = nvr::decode(bytes, NvrMode::Values, fact_count, null_count)?;
            let mut decoder = Reconstructor::new(value_type);
            symbols
                .into_iter()
                .map(|symbol| decoder.decode(symbol))
                .collect()
        }
    }
}

#[cfg(test)]
#[path = "selector_tests.rs"]
mod tests;
