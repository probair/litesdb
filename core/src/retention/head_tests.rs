// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::fs;

use super::{RetentionHead, RetentionHeads, decode, encode, head_name, publish};
use crate::{
    CellValue, ErrorKind, F32Bits, FieldId, SeriesId, Sq1, TableId,
    fsutil::{Area, DbDir, TestDir},
};

fn fixture() -> RetentionHeads {
    RetentionHeads::new(
        100,
        vec![
            RetentionHead::new(
                TableId::new(1),
                SeriesId::new(1),
                FieldId::new(1),
                90,
                CellValue::Null,
            ),
            RetentionHead::new(
                TableId::new(1),
                SeriesId::new(1),
                FieldId::new(2),
                91,
                CellValue::UInt(u64::MAX),
            ),
            RetentionHead::new(
                TableId::new(1),
                SeriesId::new(2),
                FieldId::new(1),
                92,
                CellValue::Sq1(Sq1::new(254).unwrap_or_else(|| unreachable!())),
            ),
            RetentionHead::new(
                TableId::new(2),
                SeriesId::new(1),
                FieldId::new(1),
                93,
                CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234)),
            ),
        ],
    )
    .unwrap_or_else(|_| unreachable!("valid heads rejected"))
}

fn refresh_body_crc(bytes: &mut [u8]) {
    let footer = bytes.len().saturating_sub(4);
    let checksum = crc32fast::hash(&bytes[24..footer]);
    bytes[footer..].copy_from_slice(&checksum.to_le_bytes());
}

fn refresh_header_crc(bytes: &mut [u8]) {
    let checksum = crc32fast::hash(&bytes[..20]);
    bytes[20..24].copy_from_slice(&checksum.to_le_bytes());
}

#[test]
fn all_types_round_trip_bit_exactly() {
    let heads = fixture();
    let bytes = encode(&heads).unwrap_or_else(|_| unreachable!("encode failed"));
    assert_eq!(&bytes[..4], b"LSR1");
    assert_eq!(
        u32::from_le_bytes(bytes[16..20].try_into().unwrap_or([0; 4])),
        4
    );
    assert_eq!(decode(&bytes, 100).ok(), Some(heads));
}

#[test]
fn logical_order_and_floor_are_strict() {
    let head = RetentionHead::new(
        TableId::new(1),
        SeriesId::new(1),
        FieldId::new(1),
        100,
        CellValue::Null,
    );
    assert_eq!(
        RetentionHeads::new(100, vec![head])
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    let earlier = RetentionHead::new(
        TableId::new(1),
        SeriesId::new(1),
        FieldId::new(1),
        99,
        CellValue::Null,
    );
    assert_eq!(
        RetentionHeads::new(100, vec![earlier, earlier])
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
}

#[test]
fn malformed_file_matrix_is_rejected() {
    let bytes = encode(&fixture()).unwrap_or_else(|_| unreachable!("encode failed"));
    assert_eq!(
        decode(&bytes, 101).err().map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );

    let mut bad_header = bytes.clone();
    bad_header[6] = 1;
    assert_eq!(
        decode(&bad_header, 100).err().map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );

    let mut bad_count = bytes.clone();
    bad_count[16..20].copy_from_slice(&5_u32.to_le_bytes());
    refresh_header_crc(&mut bad_count);
    assert_eq!(
        decode(&bad_count, 100).err().map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );

    let mut bad_padding = bytes.clone();
    let first_payload = 24 + 8 + 19;
    bad_padding[first_payload + 1] = 1;
    refresh_body_crc(&mut bad_padding);
    assert_eq!(
        decode(&bad_padding, 100).err().map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );

    assert_eq!(
        decode(&bytes[..bytes.len().saturating_sub(1)], 100)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
}

#[test]
fn publication_never_overwrites_a_generation() {
    let temporary = TestDir::new("retention-head-publish");
    let directory = DbDir::initialize(temporary.path()).unwrap_or_else(|_| unreachable!());
    let heads = fixture();
    assert_eq!(head_name(7), "0000000000000007.lsr");
    let name = publish(&directory, 7, &heads).unwrap_or_else(|_| unreachable!("publish failed"));
    let bytes = fs::read(directory.file(Area::Heads, &name))
        .unwrap_or_else(|_| unreachable!("published file absent"));
    assert_eq!(decode(&bytes, 100).ok(), Some(heads.clone()));
    assert_eq!(
        publish(&directory, 7, &heads)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
}
