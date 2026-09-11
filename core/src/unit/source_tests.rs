// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
};

use super::{FileUnitSource, UnitSource};
use crate::{
    ErrorKind, TableId,
    fsutil::TestDir,
    manifest::UnitMeta,
    unit::format::{self, TableDirectoryEntry, UnitHeader},
};

fn artifact(unit_id: u64, timestamp: i64) -> (Vec<u8>, UnitMeta, TableDirectoryEntry) {
    let payload = vec![u8::try_from(unit_id).unwrap_or_default(); 8];
    let offset = format::UNIT_HEADER_BYTES
        .checked_add(format::TABLE_DIRECTORY_ENTRY_BYTES)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    let entry = TableDirectoryEntry::new(
        TableId::new(1),
        1,
        timestamp,
        timestamp,
        1,
        offset,
        u32::try_from(payload.len()).unwrap_or_default(),
    )
    .unwrap_or_else(|_| unreachable!("valid directory entry rejected"));
    let header = UnitHeader::new(0, unit_id, timestamp, timestamp, 1, 1)
        .unwrap_or_else(|_| unreachable!("valid unit header rejected"));
    let bytes = format::encode(header, &[entry], &[payload.as_slice()])
        .unwrap_or_else(|_| unreachable!("valid unit encoding failed"));
    let layout = format::decode(&bytes, unit_id)
        .unwrap_or_else(|_| unreachable!("valid unit decoding failed"));
    let meta = UnitMeta::new(
        unit_id,
        0,
        timestamp,
        timestamp,
        1,
        1,
        layout.file_len(),
        layout.body_crc32(),
    )
    .unwrap_or_else(|_| unreachable!("valid unit metadata rejected"));
    (bytes, meta, entry)
}

fn write_artifact(root: &std::path::Path, bytes: &[u8], meta: UnitMeta) {
    fs::write(root.join(format::unit_name(meta.unit_id())), bytes)
        .unwrap_or_else(|_| unreachable!("fixture unit write failed"));
}

#[test]
fn source_uses_the_bounded_directory_lru() {
    let temporary = TestDir::new("source-directory-lru");
    let root = temporary.path().join("units");
    fs::create_dir_all(&root).unwrap_or_else(|_| unreachable!("fixture directory failed"));
    let first = artifact(1, 10);
    let second = artifact(2, 20);
    write_artifact(&root, &first.0, first.1);
    write_artifact(&root, &second.0, second.1);

    let source = FileUnitSource::open(&root, &[first.1, second.1], 256)
        .unwrap_or_else(|_| unreachable!("source open failed"));
    assert_eq!(source.cache_usage().ok(), Some((0, 0)));
    assert_eq!(
        source.table_sections(first.1, TableId::new(1)).ok(),
        Some(vec![first.2])
    );
    assert_eq!(
        source.table_sections(second.1, TableId::new(1)).ok(),
        Some(vec![second.2])
    );
    let (pages, used) = source
        .cache_usage()
        .unwrap_or_else(|_| unreachable!("cache usage failed"));
    assert_eq!(pages, 1);
    assert!(used <= 256);
}

#[test]
fn body_corruption_is_rejected_on_first_section_read() {
    let temporary = TestDir::new("source-lazy-integrity");
    let root = temporary.path().join("units");
    fs::create_dir_all(&root).unwrap_or_else(|_| unreachable!("fixture directory failed"));
    let (bytes, meta, entry) = artifact(1, 10);
    write_artifact(&root, &bytes, meta);
    let source = FileUnitSource::open(&root, &[meta], crate::limits::DEFAULT_DIRECTORY_CACHE_BYTES)
        .unwrap_or_else(|_| unreachable!("clean source open failed"));

    let mut file = OpenOptions::new()
        .write(true)
        .open(root.join(format::unit_name(meta.unit_id())))
        .unwrap_or_else(|_| unreachable!("fixture reopen failed"));
    file.seek(SeekFrom::Start(entry.section_offset()))
        .unwrap_or_else(|_| unreachable!("fixture seek failed"));
    file.write_all(&[0xff])
        .unwrap_or_else(|_| unreachable!("fixture mutation failed"));
    file.sync_all()
        .unwrap_or_else(|_| unreachable!("fixture sync failed"));

    assert_eq!(
        source.table_sections(meta, TableId::new(1)).ok(),
        Some(vec![entry])
    );
    assert_eq!(
        source
            .section_bytes(meta, entry)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
}
