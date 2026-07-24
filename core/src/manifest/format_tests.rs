// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{MANIFEST_HEADER_BYTES, decode, encode};
use crate::{
    ErrorKind, FieldId, FieldSchema, SeriesId, TableId, TableVersion, Validity, ValueType,
    manifest::catalog::{
        FieldRetirement, Manifest, ManifestIdentity, RetentionState, SeriesRetirement,
        TableCatalog, UnitMeta,
    },
    wal::Checkpoint,
};

fn manifest() -> Manifest {
    let version = TableVersion::restore(
        1,
        Validity::duration_seconds(9).unwrap_or(Validity::Forever),
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::F32Bits),
        ],
        Some(-10),
    )
    .unwrap_or_else(|_| unreachable!());
    let table = TableCatalog::restore(
        TableId::new(3),
        Some(20),
        vec![version],
        vec![SeriesRetirement::new(SeriesId::new(4), 5)],
        vec![FieldRetirement::new(FieldId::new(2), 6)],
    )
    .unwrap_or_else(|_| unreachable!());
    let unit =
        UnitMeta::new(7, 1, -10, 20, 1, 2, 100, 0x1122_3344).unwrap_or_else(|_| unreachable!());
    Manifest::restore(
        ManifestIdentity::new(8, 7, 3, 0, 0),
        Checkpoint::new(1, 32, 9).unwrap_or_else(|_| unreachable!()),
        RetentionState::new(Some(-20), Some(2)),
        vec![table],
        vec![unit],
    )
    .unwrap_or_else(|_| unreachable!())
}

fn refresh_crc(bytes: &mut [u8]) {
    let crc = crc32fast::hash(&bytes[MANIFEST_HEADER_BYTES..]);
    bytes[20..24].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn complete_manifest_round_trip_and_header_are_exact() {
    let manifest = manifest();
    let bytes = encode(&manifest).unwrap_or_else(|_| unreachable!());
    assert_eq!(&bytes[..8], b"LSM1\x01\x00\x00\x00");
    assert_eq!(&bytes[8..16], &8_u64.to_le_bytes());
    assert_eq!(
        usize::try_from(u32::from_le_bytes(
            bytes[16..20].try_into().unwrap_or([0; 4])
        ))
        .ok()
        .and_then(|length| length.checked_add(MANIFEST_HEADER_BYTES)),
        Some(bytes.len())
    );
    assert_eq!(
        u32::from_le_bytes(bytes[20..24].try_into().unwrap_or([0; 4])),
        crc32fast::hash(&bytes[MANIFEST_HEADER_BYTES..])
    );
    assert_eq!(decode(&bytes).ok(), Some(manifest));
}

#[test]
fn malformed_header_and_crc_are_rejected() {
    let bytes = encode(&manifest()).unwrap_or_else(|_| unreachable!());
    let mut cases = vec![bytes[..23].to_vec(), [bytes.as_slice(), &[0]].concat()];
    for index in [0, 4, 6, 16, 20] {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        cases.push(changed);
    }
    let mut over_limit = bytes;
    over_limit[16..20].copy_from_slice(&67_108_865_u32.to_le_bytes());
    cases.push(over_limit);
    for candidate in cases {
        let Err(error) = decode(&candidate) else {
            unreachable!("malformed MANIFEST frame accepted");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Corruption | ErrorKind::ResourceExhausted
        ));
    }
}

#[test]
fn duplicate_generation_is_cross_checked() {
    let mut bytes = encode(&manifest()).unwrap_or_else(|_| unreachable!());
    let body_generation = MANIFEST_HEADER_BYTES + 24 + 8 + 4;
    bytes[body_generation..body_generation + 8].copy_from_slice(&9_u64.to_le_bytes());
    refresh_crc(&mut bytes);
    let Err(error) = decode(&bytes) else {
        unreachable!("body generation replay accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn malformed_body_tags_and_counts_are_rejected() {
    let mut option = encode(&manifest()).unwrap_or_else(|_| unreachable!());
    let retention_floor_tag = MANIFEST_HEADER_BYTES + 24 + 36;
    option[retention_floor_tag] = 2;
    refresh_crc(&mut option);

    let mut count = encode(&manifest()).unwrap_or_else(|_| unreachable!());
    let table_count = retention_floor_tag + 1 + 8 + 1 + 8;
    count[table_count..table_count + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    refresh_crc(&mut count);
    for candidate in [option, count] {
        let Err(error) = decode(&candidate) else {
            unreachable!("malformed MANIFEST body accepted");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Corruption | ErrorKind::ResourceExhausted
        ));
    }
}
