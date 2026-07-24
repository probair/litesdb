// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{PresenceEncoding, decode, encode, measure};
use crate::{ErrorKind, codec::FactShape, limits::MAX_SECTION_ROWS};

fn encoded(shapes: &[FactShape]) -> (PresenceEncoding, u32, Vec<u8>) {
    let Ok(plan) = measure(shapes) else {
        unreachable!("valid presence fixture rejected");
    };
    let Ok(length) = usize::try_from(plan.byte_len()) else {
        unreachable!("bounded length does not fit usize");
    };
    let mut bytes = vec![0_u8; length];
    let Ok(written) = encode(shapes, plan, &mut bytes) else {
        unreachable!("measured presence fixture failed to encode");
    };
    assert_eq!(written, bytes.len());
    (plan.encoding(), plan.fact_count(), bytes)
}

#[test]
fn all_fact_is_zero_bytes() {
    let shapes = [FactShape::Value, FactShape::Null, FactShape::Value];
    let (encoding, fact_count, bytes) = encoded(&shapes);
    assert_eq!(encoding, PresenceEncoding::AllFact);
    assert_eq!(encoding.tag(), 0);
    assert_eq!(fact_count, 3);
    assert!(bytes.is_empty());

    let Ok(decoded) = decode(encoding.tag(), 3, fact_count, &bytes) else {
        unreachable!("canonical AllFact stream rejected");
    };
    assert_eq!(decoded.row_count(), 3);
    for row in 0..3 {
        assert_eq!(decoded.is_fact(row), Some(true));
    }
    assert_eq!(decoded.is_fact(3), None);
}

#[test]
fn bitmap_is_low_bit_first_and_preserves_null_as_fact() {
    let shapes = [
        FactShape::Value,
        FactShape::Absent,
        FactShape::Null,
        FactShape::Absent,
        FactShape::Absent,
        FactShape::Value,
        FactShape::Absent,
        FactShape::Null,
        FactShape::Value,
    ];
    let (encoding, fact_count, bytes) = encoded(&shapes);
    assert_eq!(encoding, PresenceEncoding::Bitmap);
    assert_eq!(encoding.tag(), 1);
    assert_eq!(fact_count, 5);
    assert_eq!(bytes, [0b1010_0101, 0b0000_0001]);

    let Ok(decoded) = decode(encoding.tag(), 9, fact_count, &bytes) else {
        unreachable!("canonical bitmap rejected");
    };
    for (row, shape) in shapes.into_iter().enumerate() {
        let Ok(row) = u32::try_from(row) else {
            unreachable!();
        };
        assert_eq!(decoded.is_fact(row), Some(shape != FactShape::Absent));
    }
}

#[test]
fn row_count_boundaries_are_checked() {
    let max = vec![FactShape::Absent; usize::try_from(MAX_SECTION_ROWS).unwrap_or(0)];
    let Ok(plan) = measure(&max) else {
        unreachable!("exact section row limit rejected");
    };
    assert_eq!(plan.byte_len(), 8_192);

    let mut over = max;
    over.push(FactShape::Absent);
    let Err(error) = measure(&over) else {
        unreachable!("over-limit section accepted");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);

    let Err(error) = measure(&[]) else {
        unreachable!("empty section accepted");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn malformed_streams_are_rejected() {
    let cases = [
        decode(2, 1, 1, &[]),
        decode(0, 1, 1, &[0]),
        decode(0, 1, 0, &[]),
        decode(1, 9, 1, &[1]),
        decode(1, 9, 1, &[1, 0b1000_0000]),
        decode(1, 9, 2, &[1, 0]),
        decode(1, 1, 2, &[1]),
        decode(1, 0, 0, &[]),
    ];
    for result in cases {
        let Err(error) = result else {
            unreachable!("malformed presence stream accepted");
        };
        assert!(matches!(error.kind(), ErrorKind::Corruption));
    }

    let Err(error) = decode(1, MAX_SECTION_ROWS + 1, 0, &[]) else {
        unreachable!("over-limit persisted row count accepted");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
}

#[test]
fn encoder_requires_the_exact_plan_and_output_size() {
    let shapes = [FactShape::Value, FactShape::Absent];
    let Ok(plan) = measure(&shapes) else {
        unreachable!();
    };
    assert_eq!(
        encode(&shapes, plan, &mut []).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );

    let other = [FactShape::Value, FactShape::Value];
    assert_eq!(
        encode(&other, plan, &mut [0]).map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}
