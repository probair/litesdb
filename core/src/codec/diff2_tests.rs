// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    DecodedDiff2, Diff2Decoder, Diff2Encoder, Diff2Value, Preprocessed, XorDecoder, XorEncoder,
};
use crate::{ErrorKind, F32Bits, codec::nvr::NvrSymbol};

fn symbol(value: Preprocessed) -> NvrSymbol {
    let Preprocessed::Symbol(symbol) = value else {
        unreachable!("test sequence unexpectedly exceeded the NVR domain");
    };
    symbol
}

#[test]
fn unsigned_diff2_round_trips_and_null_does_not_advance() {
    let input = [
        Diff2Value::Unsigned(10),
        Diff2Value::Null,
        Diff2Value::Unsigned(13),
        Diff2Value::Unsigned(16),
    ];
    let mut writer = Diff2Encoder::unsigned(u64::MAX);
    let mut symbols = Vec::new();
    for value in input {
        let Ok(result) = writer.observe(value) else {
            unreachable!("valid UInt rejected");
        };
        symbols.push(symbol(result));
    }
    assert_eq!(
        symbols,
        [
            NvrSymbol::Value(10),
            NvrSymbol::Null,
            NvrSymbol::Value(6),
            NvrSymbol::Value(0),
        ]
    );

    let mut reader = Diff2Decoder::unsigned(u64::MAX);
    let mut values = Vec::new();
    for item in symbols {
        let Ok(value) = reader.decode(item) else {
            unreachable!("encoder output rejected");
        };
        values.push(value);
    }
    assert_eq!(
        values,
        [
            DecodedDiff2::Unsigned(10),
            DecodedDiff2::Null,
            DecodedDiff2::Unsigned(13),
            DecodedDiff2::Unsigned(16),
        ]
    );
}

#[test]
fn timestamp_extremes_use_full_zigzag_domain_then_fallback() {
    let mut low = Diff2Encoder::timestamps();
    let Ok(first) = low.observe(Diff2Value::Timestamp(i64::MIN)) else {
        unreachable!();
    };
    assert_eq!(first, Preprocessed::Symbol(NvrSymbol::Value(u64::MAX)));

    let mut decoder = Diff2Decoder::timestamps();
    assert_eq!(
        decoder
            .decode(NvrSymbol::Value(u64::MAX))
            .map_err(|error| error.kind()),
        Ok(DecodedDiff2::Timestamp(i64::MIN))
    );

    assert_eq!(
        low.observe(Diff2Value::Timestamp(i64::MAX))
            .map_err(|error| error.kind()),
        Ok(Preprocessed::Unavailable)
    );
    assert_eq!(
        low.observe(Diff2Value::Timestamp(0))
            .map_err(|error| error.kind()),
        Ok(Preprocessed::Unavailable)
    );
}

#[test]
fn unsigned_zigzag_overflow_is_candidate_unavailable() {
    let mut encoder = Diff2Encoder::unsigned(u64::MAX);
    assert_eq!(
        encoder
            .observe(Diff2Value::Unsigned(0))
            .map_err(|error| error.kind()),
        Ok(Preprocessed::Symbol(NvrSymbol::Value(0)))
    );
    assert_eq!(
        encoder
            .observe(Diff2Value::Unsigned(u64::MAX))
            .map_err(|error| error.kind()),
        Ok(Preprocessed::Unavailable)
    );
}

#[test]
fn sq1_domain_rejects_code_255() {
    let mut encoder = Diff2Encoder::unsigned(254);
    let Err(error) = encoder.observe(Diff2Value::Unsigned(255)) else {
        unreachable!("SQ1 code 255 accepted as a value");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let mut decoder = Diff2Decoder::unsigned(254);
    let Err(error) = decoder.decode(NvrSymbol::Value(255)) else {
        unreachable!("persisted SQ1 code 255 accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn decoder_rejects_reconstructed_domain_overflow() {
    let mut unsigned = Diff2Decoder::unsigned(u64::MAX);
    assert!(unsigned.decode(NvrSymbol::Value(0)).is_ok());
    let Err(error) = unsigned.decode(NvrSymbol::Value(1)) else {
        unreachable!("negative UInt reconstruction accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);

    let mut timestamps = Diff2Decoder::timestamps();
    assert!(timestamps.decode(NvrSymbol::Value(u64::MAX - 1)).is_ok());
    let Err(error) = timestamps.decode(NvrSymbol::Value(2)) else {
        unreachable!("timestamp overflow accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn f32_xor_is_bit_exact_and_null_stable() {
    let input = [
        Some(F32Bits::from_bits(0x8000_0000)),
        None,
        Some(F32Bits::from_bits(0x7fc0_1234)),
        Some(F32Bits::from_bits(0x7fc0_1234)),
        Some(F32Bits::from_bits(0x0000_0001)),
    ];
    let mut writer = XorEncoder::default();
    let stream = input.map(|value| writer.observe(value));
    assert_eq!(stream[0], NvrSymbol::Value(0x8000_0000));
    assert_eq!(stream[1], NvrSymbol::Null);
    assert_eq!(stream[2], NvrSymbol::Value(0xffc0_1234));
    assert_eq!(stream[3], NvrSymbol::Value(0));

    let mut reader = XorDecoder::default();
    let mut values = Vec::new();
    for item in stream {
        let Ok(value) = reader.decode(item) else {
            unreachable!("XOR encoder output rejected");
        };
        values.push(value);
    }
    assert_eq!(values, input);
}

#[test]
fn f32_xor_rejects_values_wider_than_u32() {
    let mut decoder = XorDecoder::default();
    let Err(error) = decoder.decode(NvrSymbol::Value(u64::from(u32::MAX) + 1)) else {
        unreachable!("wide F32 XOR value accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn predictor_kind_mismatch_is_invalid_argument() {
    let mut unsigned = Diff2Encoder::unsigned(u64::MAX);
    let mut timestamps = Diff2Encoder::timestamps();
    for result in [
        unsigned.observe(Diff2Value::Timestamp(0)),
        timestamps.observe(Diff2Value::Unsigned(0)),
        timestamps.observe(Diff2Value::Null),
    ] {
        let Err(error) = result else {
            unreachable!("mismatched predictor input accepted");
        };
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}
