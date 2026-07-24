// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    TABLE_DIRECTORY_ENTRY_BYTES, TableDirectoryEntry, UNIT_FOOTER_BYTES, UNIT_HEADER_BYTES,
    UnitHeader, decode, encode, parse_unit_name, unit_name,
};
use crate::{ErrorKind, TableId};

fn fixture() -> Vec<u8> {
    let directory_end =
        u64::try_from(UNIT_HEADER_BYTES + 2 * TABLE_DIRECTORY_ENTRY_BYTES).unwrap_or(u64::MAX);
    let first = TableDirectoryEntry::new(TableId::new(1), 1, 10, 11, 2, directory_end, 3)
        .unwrap_or_else(|_| unreachable!());
    let second = TableDirectoryEntry::new(TableId::new(2), 3, 20, 20, 1, directory_end + 3, 2)
        .unwrap_or_else(|_| unreachable!());
    let header = UnitHeader::new(0, 7, 10, 20, 2, 3).unwrap_or_else(|_| unreachable!());
    encode(header, &[first, second], &[b"abc", b"de"]).unwrap_or_else(|_| unreachable!())
}

fn refresh_crcs(bytes: &mut [u8]) {
    let header_crc = crc32fast::hash(&bytes[..44]);
    bytes[44..48].copy_from_slice(&header_crc.to_le_bytes());
    let footer = bytes.len().saturating_sub(UNIT_FOOTER_BYTES);
    let body_crc = crc32fast::hash(&bytes[UNIT_HEADER_BYTES..footer]);
    bytes[footer..footer + 4].copy_from_slice(&body_crc.to_le_bytes());
}

#[test]
fn unit_names_are_canonical() {
    for unit_id in [0, 1, 0xfeed_beef, u64::MAX] {
        let name = unit_name(unit_id);
        assert_eq!(name.len(), 20);
        assert_eq!(parse_unit_name(&name).ok(), Some(unit_id));
    }
    for name in [
        "1.lsu",
        "000000000000000G.lsu",
        "FFFFFFFFFFFFFFFF.lsu",
        "0000000000000001.wal",
    ] {
        assert_eq!(
            parse_unit_name(name).map_err(|error| error.kind()),
            Err(ErrorKind::Corruption)
        );
    }
}

#[test]
fn unit_envelope_round_trip_is_exact() {
    let bytes = fixture();
    assert_eq!(&bytes[..8], b"LSU1\x01\x00\x00\x00");
    assert_eq!(
        bytes.len(),
        UNIT_HEADER_BYTES + 2 * TABLE_DIRECTORY_ENTRY_BYTES + 5 + UNIT_FOOTER_BYTES
    );
    let layout = decode(&bytes, 7).unwrap_or_else(|_| unreachable!("valid unit rejected"));
    assert_eq!(layout.header().unit_id(), 7);
    assert_eq!(layout.header().level(), 0);
    assert_eq!(layout.header().section_count(), 2);
    assert_eq!(layout.header().total_rows(), 3);
    assert_eq!(layout.sections()[0].table(), TableId::new(1));
    assert_eq!(layout.sections()[1].version_no(), 3);
    assert_eq!(layout.sections()[1].row_count(), 1);
    assert_eq!(
        layout.file_len(),
        u64::try_from(bytes.len()).unwrap_or(u64::MAX)
    );
    assert_eq!(
        layout.body_crc32(),
        crc32fast::hash(&bytes[UNIT_HEADER_BYTES..bytes.len() - UNIT_FOOTER_BYTES])
    );
}

#[test]
fn malformed_header_and_footer_are_rejected() {
    let bytes = fixture();
    let mut cases = vec![
        bytes[..bytes.len() - 1].to_vec(),
        [bytes.as_slice(), &[0]].concat(),
    ];
    for index in [
        0,
        4,
        6,
        7,
        44,
        bytes.len() - 16,
        bytes.len() - 12,
        bytes.len() - 1,
    ] {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        cases.push(changed);
    }
    for candidate in cases {
        let Err(error) = decode(&candidate, 7) else {
            unreachable!("malformed unit envelope accepted");
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Corruption | ErrorKind::ResourceExhausted
        ));
    }
    assert_eq!(
        decode(&bytes, 8).map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}

#[test]
fn malformed_directory_semantics_are_rejected() {
    let bytes = fixture();
    let mutations = [
        (UNIT_HEADER_BYTES, 3_u32.to_le_bytes().to_vec()),
        (UNIT_HEADER_BYTES + 16, 9_i64.to_le_bytes().to_vec()),
        (UNIT_HEADER_BYTES + 24, 0_u32.to_le_bytes().to_vec()),
        (UNIT_HEADER_BYTES + 28, 999_u64.to_le_bytes().to_vec()),
        (UNIT_HEADER_BYTES + 40, 1_u32.to_le_bytes().to_vec()),
        (36, 4_u64.to_le_bytes().to_vec()),
    ];
    for (offset, value) in mutations {
        let mut changed = bytes.clone();
        changed[offset..offset + value.len()].copy_from_slice(&value);
        refresh_crcs(&mut changed);
        let Err(error) = decode(&changed, 7) else {
            unreachable!("invalid table directory accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn encode_requires_exact_section_plan() {
    let header = UnitHeader::new(0, 1, 1, 1, 1, 1).unwrap_or_else(|_| unreachable!());
    let offset = u64::try_from(UNIT_HEADER_BYTES + TABLE_DIRECTORY_ENTRY_BYTES).unwrap_or(u64::MAX);
    let entry = TableDirectoryEntry::new(TableId::new(1), 1, 1, 1, 1, offset, 1)
        .unwrap_or_else(|_| unreachable!());
    assert_eq!(
        encode(header, &[entry], &[]).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
    assert_eq!(
        encode(header, &[entry], &[b"xx"]).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
}
