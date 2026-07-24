// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{NvrMode, NvrPlan, NvrSymbol, decode, encode, measure};
use crate::{ErrorKind, limits::MAX_SECTION_ROWS};

fn encoded(symbols: &[NvrSymbol], mode: NvrMode) -> (NvrPlan, Vec<u8>) {
    let Ok(plan) = measure(symbols, mode) else {
        unreachable!("valid NVR fixture rejected");
    };
    let Ok(length) = usize::try_from(plan.byte_len()) else {
        unreachable!("bounded NVR length does not fit usize");
    };
    let mut bytes = vec![0_u8; length];
    let Ok(written) = encode(symbols, mode, plan, &mut bytes) else {
        unreachable!("measured NVR fixture failed to encode");
    };
    assert_eq!(written, bytes.len());
    (plan, bytes)
}

#[test]
fn wire_prefixes_and_run_merging_are_exact() {
    let symbols = [
        NvrSymbol::Value(1),
        NvrSymbol::Value(0),
        NvrSymbol::Value(0),
        NvrSymbol::Null,
        NvrSymbol::Null,
        NvrSymbol::Value(64),
    ];
    let (plan, bytes) = encoded(&symbols, NvrMode::Values);
    assert_eq!(plan.fact_count(), 6);
    assert_eq!(plan.null_count(), 2);
    assert_eq!(bytes, [0x01, 0x82, 0xc2, 0x40, 0x01]);

    let (_, lit63) = encoded(&[NvrSymbol::Value(63)], NvrMode::Values);
    let (_, zero31) = encoded(&vec![NvrSymbol::Value(0); 31], NvrMode::Values);
    let (_, zero32) = encoded(&vec![NvrSymbol::Value(0); 32], NvrMode::Values);
    let (_, null15) = encoded(&vec![NvrSymbol::Null; 15], NvrMode::Values);
    let (_, null16) = encoded(&vec![NvrSymbol::Null; 16], NvrMode::Values);
    assert_eq!(lit63, [0x3f]);
    assert_eq!(zero31, [0x9f]);
    assert_eq!(zero32, [0xa0, 0x01]);
    assert_eq!(null15, [0xcf]);
    assert_eq!(null16, [0xd0, 0x01]);
}

#[test]
fn measure_encode_decode_are_identical() {
    let symbols = [
        NvrSymbol::Value(0),
        NvrSymbol::Value(1),
        NvrSymbol::Value(63),
        NvrSymbol::Value(64),
        NvrSymbol::Value(u32::MAX.into()),
        NvrSymbol::Value(u64::MAX),
        NvrSymbol::Null,
        NvrSymbol::Value(0),
    ];
    let (plan, bytes) = encoded(&symbols, NvrMode::Values);
    assert_eq!(usize::try_from(plan.byte_len()).ok(), Some(bytes.len()));
    let Ok(decoded) = decode(
        &bytes,
        NvrMode::Values,
        plan.fact_count(),
        plan.null_count(),
    ) else {
        unreachable!("canonical NVR stream rejected");
    };
    assert_eq!(decoded, symbols);
}

#[test]
fn timestamp_mode_forbids_null_runs() {
    let Err(error) = measure(&[NvrSymbol::Null], NvrMode::Timestamps) else {
        unreachable!("timestamp null accepted by measure");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let Err(error) = decode(&[0xc1], NvrMode::Timestamps, 1, 1) else {
        unreachable!("timestamp NRUN accepted by decoder");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn malformed_and_noncanonical_tokens_are_rejected() {
    let cases: &[(&[u8], u32, u32)] = &[
        (&[0xe0], 1, 0),
        (&[0x00], 1, 0),
        (&[0x80], 1, 0),
        (&[0xc0], 1, 1),
        (&[0x40], 1, 0),
        (&[0x40, 0x00], 1, 0),
        (&[0x81, 0x81], 2, 0),
        (&[0xc1, 0xc1], 2, 2),
        (&[0x82], 1, 0),
        (&[0xc2], 2, 1),
        (&[], 1, 0),
    ];
    for (bytes, facts, nulls) in cases {
        let Err(error) = decode(bytes, NvrMode::Values, *facts, *nulls) else {
            unreachable!("malformed stream accepted: {bytes:?}");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption, "{bytes:?}");
    }
}

#[test]
fn oversized_varints_are_rejected() {
    let too_long = [
        0x40, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
    ];
    let overflow = [0x40, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x04];
    for bytes in [&too_long[..], &overflow[..]] {
        let Err(error) = decode(bytes, NvrMode::Values, 1, 0) else {
            unreachable!("oversized varint accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn persisted_counts_are_bounded_and_cross_checked() {
    let Err(error) = decode(&[0x01], NvrMode::Values, MAX_SECTION_ROWS + 1, 0) else {
        unreachable!("over-limit fact count accepted");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);

    for (facts, nulls) in [(0, 0), (1, 2), (2, 0), (1, 1)] {
        let Err(error) = decode(&[0x01], NvrMode::Values, facts, nulls) else {
            unreachable!("inconsistent directory counts accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn encoder_requires_exact_plan_and_output() {
    let symbols = [NvrSymbol::Value(1)];
    let Ok(plan) = measure(&symbols, NvrMode::Values) else {
        unreachable!();
    };
    assert_eq!(
        encode(&symbols, NvrMode::Values, plan, &mut []).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );

    let changed = [NvrSymbol::Value(64)];
    assert_eq!(
        encode(&changed, NvrMode::Values, plan, &mut [0]).map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}

#[test]
fn input_count_limit_is_exact() {
    let max = vec![NvrSymbol::Value(0); usize::try_from(MAX_SECTION_ROWS).unwrap_or(0)];
    assert!(measure(&max, NvrMode::Values).is_ok());

    let mut over = max;
    over.push(NvrSymbol::Value(0));
    let Err(error) = measure(&over, NvrMode::Values) else {
        unreachable!("over-limit NVR input accepted");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);

    let Err(error) = measure(&[], NvrMode::Values) else {
        unreachable!("empty NVR input accepted");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}
