// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, io::Write};

use super::{Checkpoint, recover};
use crate::{
    CellValue, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, TableId,
    Validity, ValueType, VersionSpec,
    fsutil::{Area, DbDir, TestDir},
    wal::{
        record::{self, RecordBody},
        segment::{SegmentHeader, segment_name},
        tail::TailIndex,
        writer::{DurablePosition, WalWriter, WriterConfig},
    },
};

fn database(label: &str) -> (TestDir, DbDir) {
    let temporary = TestDir::new(label);
    let directory = DbDir::initialize(&temporary.path().join("db"))
        .unwrap_or_else(|_| unreachable!("test database initialization failed"));
    (temporary, directory)
}

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::duration_seconds(9).unwrap_or(Validity::Forever),
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|_| unreachable!())
}

fn create_table(table: u32) -> RecordBody {
    RecordBody::CreateTable {
        table: TableId::new(table),
        spec: spec(),
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
        .unwrap_or_else(|_| unreachable!()),
    }
}

fn segment(first_seq: u64, records: &[(u64, RecordBody)]) -> Vec<u8> {
    let mut bytes = SegmentHeader::new(first_seq, 0, 0).encode().to_vec();
    for (seq, body) in records {
        bytes.extend_from_slice(&record::encode(*seq, body).unwrap_or_else(|_| unreachable!()));
    }
    bytes
}

fn write_segment(directory: &DbDir, first_seq: u64, bytes: &[u8]) {
    fs::write(directory.file(Area::Wal, &segment_name(first_seq)), bytes)
        .unwrap_or_else(|_| unreachable!("test segment write failed"));
}

fn initial_checkpoint() -> Checkpoint {
    Checkpoint::new(1, 32, 1).unwrap_or_else(|_| unreachable!())
}

fn assert_repaired_state_is_stable(
    directory: &DbDir,
    expected: &TailIndex,
    next_seq: u64,
    active_offset: u64,
) {
    let mut reopened = TailIndex::new(0, 1);
    let outcome = recover(directory, initial_checkpoint(), 0, 0, &mut reopened)
        .unwrap_or_else(|_| unreachable!("second recovery failed"));
    assert_eq!(&reopened, expected);
    assert_eq!(
        (outcome.next_seq(), outcome.active_offset()),
        (next_seq, active_offset)
    );
    assert!(!outcome.repaired_tail());
}

#[test]
fn repeated_reopen_is_deterministic() {
    let (_temporary, directory) = database("wal-recover-repeat");
    write_segment(
        &directory,
        1,
        &segment(1, &[(1, create_table(1)), (2, observation(10))]),
    );
    let mut first = TailIndex::new(0, 1);
    let outcome = recover(&directory, initial_checkpoint(), 0, 0, &mut first)
        .unwrap_or_else(|_| unreachable!("first recovery failed"));
    assert_eq!((outcome.next_seq(), outcome.active_segment()), (3, 1));
    assert!(!outcome.repaired_tail());
    assert_eq!(
        outcome.active_offset(),
        fs::metadata(directory.file(Area::Wal, &segment_name(1)))
            .unwrap_or_else(|_| unreachable!())
            .len()
    );
    assert_eq!(outcome.wal_bytes(), outcome.active_offset());

    let mut second = TailIndex::new(0, 1);
    let second_outcome = recover(&directory, initial_checkpoint(), 0, 0, &mut second)
        .unwrap_or_else(|_| unreachable!("second recovery failed"));
    assert_eq!(second, first);
    assert_eq!(second_outcome, outcome);
}

#[test]
fn every_final_record_tear_preserves_the_original() {
    let first = record::encode(1, &create_table(1)).unwrap_or_else(|_| unreachable!());
    let second = record::encode(2, &observation(10)).unwrap_or_else(|_| unreachable!());
    let header = SegmentHeader::new(1, 0, 0).encode();
    let complete_prefix = header.len().saturating_add(first.len());
    for cut in 1..second.len() {
        let (_temporary, directory) = database("wal-recover-tear");
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(&first);
        bytes.extend_from_slice(&second[..cut]);
        write_segment(&directory, 1, &bytes);
        let mut target = TailIndex::new(0, 1);
        let outcome = recover(&directory, initial_checkpoint(), 0, 0, &mut target)
            .unwrap_or_else(|_| unreachable!("tail tear was not recoverable"));
        assert_eq!(outcome.next_seq(), 2, "cut {cut}");
        assert!(outcome.repaired_tail(), "cut {cut}");
        assert_eq!(
            fs::metadata(directory.file(Area::Wal, &segment_name(1)))
                .unwrap_or_else(|_| unreachable!())
                .len(),
            u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        );
        assert_eq!(
            fs::read(directory.file(Area::Wal, &segment_name(1))).ok(),
            Some(bytes)
        );
        assert_repaired_state_is_stable(
            &directory,
            &target,
            2,
            u64::try_from(complete_prefix).unwrap_or(u64::MAX),
        );
    }
}

#[test]
fn crc_failure_is_repairable_only_at_the_physical_tail() {
    let (_temporary, directory) = database("wal-recover-crc-tail");
    let first = record::encode(1, &create_table(1)).unwrap_or_else(|_| unreachable!());
    let mut second = record::encode(2, &observation(10)).unwrap_or_else(|_| unreachable!());
    second[12] ^= 1;
    let mut bytes = SegmentHeader::new(1, 0, 0).encode().to_vec();
    bytes.extend_from_slice(&first);
    let repair_offset = bytes.len();
    bytes.extend_from_slice(&second);
    write_segment(&directory, 1, &bytes);
    let mut target = TailIndex::new(0, 1);
    let outcome = recover(&directory, initial_checkpoint(), 0, 0, &mut target)
        .unwrap_or_else(|_| unreachable!("final CRC failure was not repaired"));
    assert!(outcome.repaired_tail());
    assert_eq!(
        outcome.active_offset(),
        u64::try_from(repair_offset).unwrap_or(u64::MAX)
    );
    assert_repaired_state_is_stable(
        &directory,
        &target,
        2,
        u64::try_from(repair_offset).unwrap_or(u64::MAX),
    );

    let (_temporary, directory) = database("wal-recover-crc-middle");
    let third = record::encode(
        3,
        &RecordBody::DropTable {
            table: TableId::new(1),
        },
    )
    .unwrap_or_else(|_| unreachable!());
    bytes.extend_from_slice(&third);
    write_segment(&directory, 1, &bytes);
    let mut target = TailIndex::new(0, 1);
    let Err(error) = recover(&directory, initial_checkpoint(), 0, 0, &mut target) else {
        unreachable!("non-tail CRC failure was repaired");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn complete_semantic_errors_and_gaps_are_corruption() {
    for records in [vec![(1, create_table(2))], vec![(2, create_table(1))]] {
        let (_temporary, directory) = database("wal-recover-semantic");
        write_segment(&directory, 1, &segment(1, &records));
        let original_len = fs::metadata(directory.file(Area::Wal, &segment_name(1)))
            .unwrap_or_else(|_| unreachable!())
            .len();
        let mut target = TailIndex::new(0, 1);
        let Err(error) = recover(&directory, initial_checkpoint(), 0, 0, &mut target) else {
            unreachable!("complete invalid record accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert_eq!(
            fs::metadata(directory.file(Area::Wal, &segment_name(1)))
                .unwrap_or_else(|_| unreachable!())
                .len(),
            original_len
        );
    }
}

#[test]
fn segment_identity_and_cross_segment_replay_are_strict() {
    let (_temporary, directory) = database("wal-recover-segments");
    write_segment(&directory, 1, &segment(1, &[(1, create_table(1))]));
    write_segment(
        &directory,
        2,
        &segment(
            2,
            &[(
                2,
                RecordBody::DropTable {
                    table: TableId::new(1),
                },
            )],
        ),
    );
    let mut target = TailIndex::new(0, 1);
    let outcome = recover(&directory, initial_checkpoint(), 0, 0, &mut target)
        .unwrap_or_else(|_| unreachable!("cross-segment recovery failed"));
    assert_eq!((outcome.next_seq(), outcome.active_segment()), (3, 2));
    assert!(target.table(TableId::new(1)).is_none());

    let (_temporary, directory) = database("wal-recover-owner");
    let mut file = fs::File::create(directory.file(Area::Wal, &segment_name(1)))
        .unwrap_or_else(|_| unreachable!());
    file.write_all(&SegmentHeader::new(1, 9, 0).encode())
        .unwrap_or_else(|_| unreachable!());
    let mut target = TailIndex::new(0, 1);
    let Err(error) = recover(&directory, initial_checkpoint(), 0, 0, &mut target) else {
        unreachable!("wrong segment owner accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn recovered_writer_continues_without_gap_or_overwrite() {
    let (_temporary, directory) = database("wal-recover-resume");
    write_segment(
        &directory,
        1,
        &segment(1, &[(1, create_table(1)), (2, observation(10))]),
    );
    let mut target = TailIndex::new(0, 1);
    let recovery = recover(&directory, initial_checkpoint(), 0, 0, &mut target)
        .unwrap_or_else(|_| unreachable!());
    let config = WriterConfig::new(65_584, 1_000_000, 500_000).unwrap_or_else(|_| unreachable!());
    let mut writer = WalWriter::resume(&directory, config, 0, 0, recovery)
        .unwrap_or_else(|_| unreachable!("recovered writer did not resume"));
    let outcome = writer
        .append(&RecordBody::DropTable {
            table: TableId::new(1),
        })
        .unwrap_or_else(|_| unreachable!());
    assert_eq!(outcome.seq(), 3);
    assert_eq!(writer.sync().ok().map(DurablePosition::seq), Some(3));
    drop(writer);

    let mut reopened = TailIndex::new(0, 1);
    let outcome = recover(&directory, initial_checkpoint(), 0, 0, &mut reopened)
        .unwrap_or_else(|_| unreachable!("resumed WAL did not reopen"));
    assert_eq!(outcome.next_seq(), 4);
    assert!(reopened.table(TableId::new(1)).is_none());
}
