// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, os::unix::fs::MetadataExt, path::PathBuf};

use super::{Checkpoint, RecoveryOutcome, recover};
use crate::{
    CellValue, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, TableId,
    Validity, ValueType, VersionSpec,
    fsutil::{Area, DbDir, TestDir, sync_directory},
    wal::{
        RecordBody, ReplayTarget, TailIndex, WalWriter, WriterConfig, record,
        segment::{SegmentHeader, segment_name},
        storage,
    },
};

fn create_table() -> RecordBody {
    let spec = VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|error| panic!("valid schema: {error:?}"));
    RecordBody::CreateTable {
        table: TableId::new(1),
        spec,
    }
}

fn observation(timestamp: i64) -> RecordBody {
    RecordBody::AppendObservation {
        table: TableId::new(1),
        observation: Observation::new(
            timestamp,
            vec![ObservationEntry::new(
                SeriesId::new(1),
                FieldId::new(1),
                CellValue::UInt(7),
            )],
        )
        .unwrap_or_else(|error| panic!("valid observation: {error:?}")),
    }
}

fn write_segment(directory: &DbDir, first_seq: u64, bytes: &[u8]) {
    let path = directory.file(Area::Wal, &segment_name(first_seq));
    fs::write(&path, bytes).unwrap_or_else(|error| panic!("fixture write: {error}"));
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .unwrap_or_else(|error| panic!("fixture file sync: {error}"));
    sync_directory(&directory.path(Area::Wal))
        .unwrap_or_else(|error| panic!("fixture directory sync: {error:?}"));
}

fn fixture(label: &str) -> (TestDir, DbDir, Vec<u8>, TailIndex) {
    let temporary = TestDir::new(label);
    let directory = DbDir::initialize(&temporary.path().join("db"))
        .unwrap_or_else(|error| panic!("fixture directory: {error:?}"));
    let mut bytes = SegmentHeader::new(1, 0, 0).encode().to_vec();
    let mut expected = TailIndex::new(0, 1);
    for (seq, body) in [(1, create_table()), (2, observation(10))] {
        bytes.extend_from_slice(
            &record::encode(seq, &body).unwrap_or_else(|error| panic!("fixture record: {error:?}")),
        );
        expected
            .apply(seq, body)
            .unwrap_or_else(|error| panic!("fixture replay: {error:?}"));
    }
    bytes.extend_from_slice(&[0xab; 3]);
    write_segment(&directory, 1, &bytes);
    (temporary, directory, bytes, expected)
}

fn replay(directory: &DbDir) -> (TailIndex, RecoveryOutcome) {
    let mut tail = TailIndex::new(0, 1);
    let checkpoint =
        Checkpoint::new(1, 32, 1).unwrap_or_else(|error| panic!("initial checkpoint: {error:?}"));
    let outcome = recover(directory, checkpoint, 0, 0, &mut tail)
        .unwrap_or_else(|error| panic!("boundary recovery: {error:?}"));
    (tail, outcome)
}

fn retained_path(directory: &DbDir) -> PathBuf {
    let retained: Vec<_> = fs::read_dir(directory.path(Area::Wal).join("damaged"))
        .unwrap_or_else(|error| panic!("evidence directory: {error}"))
        .map(|entry| {
            entry
                .unwrap_or_else(|error| panic!("evidence entry: {error}"))
                .path()
        })
        .collect();
    assert_eq!(retained.len(), 1);
    retained[0].clone()
}

#[test]
fn preserved_boundary_replays_twice_before_successor_creation() {
    let (_temporary, directory, original, expected) = fixture("wal-boundary-before-roll");
    for pass in 0..2 {
        let (tail, outcome) = replay(&directory);
        assert_eq!(tail, expected);
        assert_eq!(outcome.next_seq(), 3);
        assert_eq!(outcome.tail_repairs(), u64::from(pass == 0));
        assert!(outcome.needs_rotation());
        assert_eq!(outcome.active_offset(), original.len() as u64 - 3);
        assert_eq!(
            fs::read(directory.file(Area::Wal, &segment_name(1))).ok(),
            Some(original.clone())
        );
        assert_eq!(
            fs::read(retained_path(&directory)).ok(),
            Some(original.clone())
        );
        assert_eq!(outcome.storage_bytes(), original.len() as u64);
    }
}

#[test]
fn frozen_predecessor_replays_and_cannot_be_evicted_before_checkpoint() {
    let (_temporary, directory, original, mut expected) = fixture("wal-boundary-after-roll");
    let (_, first) = replay(&directory);
    let body = observation(20);
    let mut successor = SegmentHeader::new(first.next_seq(), 0, 0).encode().to_vec();
    successor.extend_from_slice(
        &record::encode(first.next_seq(), &body)
            .unwrap_or_else(|error| panic!("successor record: {error:?}")),
    );
    write_segment(&directory, first.next_seq(), &successor);
    expected
        .apply(first.next_seq(), body)
        .unwrap_or_else(|error| panic!("expected successor: {error:?}"));
    let wal = directory.path(Area::Wal);
    let expected_bytes = (original.len() + successor.len()) as u64;
    for _ in 0..2 {
        let (tail, outcome) = replay(&directory);
        assert_eq!(tail, expected);
        assert_eq!((outcome.next_seq(), outcome.active_segment()), (4, 3));
        assert_eq!(outcome.tail_repairs(), 0);
        assert!(!outcome.needs_rotation());
        assert_eq!(outcome.storage_bytes(), expected_bytes);
        assert_eq!(
            storage::reclaim(&wal, 1, expected_bytes)
                .err()
                .map(|error| error.kind()),
            Some(ErrorKind::ResourceExhausted)
        );
        assert_eq!(
            fs::read(retained_path(&directory)).ok(),
            Some(original.clone())
        );
        assert_eq!(
            fs::read(wal.join(segment_name(1))).ok(),
            Some(original.clone())
        );
        assert_eq!(
            fs::read(wal.join(segment_name(3))).ok(),
            Some(successor.clone())
        );
    }
}

#[test]
fn frozen_header_prefix_rebuilds_same_name_without_changing_original() {
    let temporary = TestDir::new("wal-boundary-empty-rebuild");
    let directory = DbDir::initialize(&temporary.path().join("db"))
        .unwrap_or_else(|error| panic!("fixture directory: {error:?}"));
    let mut original = SegmentHeader::new(1, 0, 0).encode().to_vec();
    original.extend_from_slice(&[0xab; 3]);
    write_segment(&directory, 1, &original);
    let (_, outcome) = replay(&directory);
    assert_eq!((outcome.active_offset(), outcome.next_seq()), (32, 1));
    let retained = retained_path(&directory);
    let old_inode = fs::metadata(&retained)
        .unwrap_or_else(|error| panic!("original inode: {error}"))
        .ino();
    let config = WriterConfig::new(65_584, 4096, 4096)
        .unwrap_or_else(|error| panic!("writer policy: {error:?}"));
    let mut writer = WalWriter::resume(&directory, config, 0, 0, outcome)
        .unwrap_or_else(|error| panic!("writer resume: {error:?}"));
    let checkpoint = writer
        .prepare_checkpoint(&directory)
        .unwrap_or_else(|error| panic!("empty replacement: {error:?}"));
    assert_eq!((checkpoint.segment(), checkpoint.offset()), (1, 32));
    assert_ne!(
        fs::metadata(directory.file(Area::Wal, &segment_name(1)))
            .unwrap_or_else(|error| panic!("new inode: {error}"))
            .ino(),
        old_inode
    );
    assert_eq!(writer.storage_bytes(), original.len() as u64 + 32);
    let body = create_table();
    assert_eq!(
        writer
            .append(&body)
            .ok()
            .map(crate::wal::writer::AppendOutcome::seq),
        Some(1)
    );
    assert!(writer.sync().is_ok());
    drop(writer);
    let mut expected = TailIndex::new(0, 1);
    expected
        .apply(1, body)
        .unwrap_or_else(|error| panic!("expected schema: {error:?}"));
    for _ in 0..2 {
        let (tail, outcome) = replay(&directory);
        assert_eq!(tail, expected);
        assert_eq!(outcome.next_seq(), 2);
        assert_eq!(outcome.tail_repairs(), 0);
        assert!(!outcome.needs_rotation());
        assert_eq!(fs::read(&retained).ok(), Some(original.clone()));
    }
}
