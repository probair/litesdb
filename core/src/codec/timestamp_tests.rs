// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{TimestampEncoding, decode, encode, select};
use crate::ErrorKind;

fn encoded(source: &[i64]) -> (u8, Vec<u8>) {
    let plan = select(source).unwrap_or_else(|_| unreachable!("valid timestamps rejected"));
    let mut bytes = vec![0; usize::try_from(plan.byte_len()).unwrap_or(0)];
    let written =
        encode(source, plan, &mut bytes).unwrap_or_else(|_| unreachable!("encoding failed"));
    assert_eq!(written, bytes.len());
    (plan.encoding().tag(), bytes)
}

#[test]
fn regular_clock_selects_nvr_and_round_trips() {
    let source = [-10, 0, 10, 20, 30, 40];
    let plan = select(&source).unwrap_or_else(|_| unreachable!());
    assert_eq!(plan.encoding(), TimestampEncoding::Nvr);
    assert_eq!(plan.row_count(), 6);
    let (tag, bytes) = encoded(&source);
    assert_eq!(decode(tag, &bytes, 6, -10, 40).ok(), Some(source.to_vec()));
}

#[test]
fn ties_and_extreme_deltas_choose_fixed() {
    let tie = [1_i64 << 49];
    let tie_plan = select(&tie).unwrap_or_else(|_| unreachable!());
    assert_eq!(tie_plan.byte_len(), 8);
    assert_eq!(tie_plan.encoding(), TimestampEncoding::Fixed);

    let extreme = [i64::MIN, i64::MAX];
    let plan = select(&extreme).unwrap_or_else(|_| unreachable!());
    assert_eq!(plan.encoding(), TimestampEncoding::Fixed);
    let (tag, bytes) = encoded(&extreme);
    assert_eq!(
        decode(tag, &bytes, 2, i64::MIN, i64::MAX).ok(),
        Some(extreme.to_vec())
    );
}

#[test]
fn encode_rejects_invalid_source_or_output() {
    for source in [&[][..], &[2, 1], &[1, 1]] {
        assert!(select(source).is_err());
    }
    let source = [1, 2, 3];
    let plan = select(&source).unwrap_or_else(|_| unreachable!());
    let mut wrong = vec![0; usize::try_from(plan.byte_len()).unwrap_or(0) + 1];
    assert_eq!(
        encode(&source, plan, &mut wrong).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
}

#[test]
fn decode_rejects_malformed_streams_and_metadata() {
    let source = [100, 110, 120, 130];
    let (tag, bytes) = encoded(&source);
    for result in [
        decode(2, &bytes, 4, 100, 130),
        decode(tag, &bytes, 0, 100, 130),
        decode(tag, &bytes, 4, 99, 130),
        decode(tag, &bytes, 4, 100, 131),
        decode(tag, &bytes[..bytes.len() - 1], 4, 100, 130),
    ] {
        assert_eq!(
            result.map_err(|error| error.kind()),
            Err(ErrorKind::Corruption)
        );
    }

    let mut fixed = Vec::new();
    for timestamp in [1_i64, 3, 2] {
        fixed.extend_from_slice(&timestamp.to_le_bytes());
    }
    assert_eq!(
        decode(0, &fixed, 3, 1, 2).map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}
