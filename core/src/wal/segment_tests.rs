// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SEGMENT_HEADER_BYTES, SegmentHeader, parse_segment_name, segment_name};
use crate::ErrorKind;

#[test]
fn segment_names_are_exact_and_round_trip() {
    for sequence in [0, 1, 42, u64::MAX] {
        let name = segment_name(sequence);
        assert_eq!(name.len(), 24);
        assert_eq!(parse_segment_name(&name).ok(), Some(sequence));
    }
}

#[test]
fn malformed_segment_names_are_rejected() {
    for name in [
        "00000000000000000001.log",
        "1.wal",
        "0000000000000000000x.wal",
        "18446744073709551616.wal",
        "000000000000000000001.wal",
    ] {
        let Err(error) = parse_segment_name(name) else {
            unreachable!("malformed WAL segment name accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn segment_header_bytes_are_exact() {
    let header = SegmentHeader::new(
        0x0102_0304_0506_0708,
        0x1112_1314_1516_1718,
        0x2122_2324_2526_2728,
    );
    let bytes = header.encode();
    assert_eq!(bytes.len(), SEGMENT_HEADER_BYTES);
    assert_eq!(&bytes[0..8], b"LSW1\x01\x00\x00\x00");
    assert_eq!(&bytes[8..16], &[8, 7, 6, 5, 4, 3, 2, 1]);
    assert_eq!(&bytes[16..24], &[24, 23, 22, 21, 20, 19, 18, 17]);
    assert_eq!(&bytes[24..32], &[40, 39, 38, 37, 36, 35, 34, 33]);
    assert_eq!(
        SegmentHeader::decode(
            &bytes,
            header.first_seq(),
            header.shard_id(),
            header.writer_epoch(),
        )
        .ok(),
        Some(header)
    );
}

#[test]
fn malformed_segment_headers_are_rejected() {
    let header = SegmentHeader::new(7, 11, 13);
    let bytes = header.encode();
    let mut cases = Vec::new();
    cases.push(bytes[..31].to_vec());
    cases.push([bytes.as_slice(), &[0]].concat());
    for index in [0, 4, 6, 8, 16] {
        let mut changed = bytes;
        changed[index] ^= 1;
        cases.push(changed.to_vec());
    }
    for case in cases {
        let Err(error) = SegmentHeader::decode(&case, 7, 11, 13) else {
            unreachable!("malformed WAL segment header accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }

    assert!(SegmentHeader::decode(&bytes, 8, 11, 13).is_err());
    assert!(SegmentHeader::decode(&bytes, 7, 12, 13).is_err());
    assert_eq!(
        SegmentHeader::decode(&bytes, 7, 11, 14)
            .ok()
            .map(SegmentHeader::writer_epoch),
        Some(13)
    );
    assert!(SegmentHeader::decode(&bytes, 7, 11, 12).is_err());
}
