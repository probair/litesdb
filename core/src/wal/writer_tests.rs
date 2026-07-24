// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs::File, io, io::Write, path::Path};

use super::{IoStep, WalIo, WalWriter, WriterConfig};
use crate::{
    ErrorKind, FieldId, FieldSchema, TableId, Validity, ValueType, VersionSpec,
    fsutil::{Area, DbDir, TestDir},
    wal::{
        Checkpoint,
        record::{self, RecordBody},
        recover,
        segment::SEGMENT_HEADER_BYTES,
        tail::TailIndex,
    },
};

fn database(label: &str, wal_max_bytes: u32, seal_bytes: u32) -> (TestDir, DbDir, WriterConfig) {
    let temporary = TestDir::new(label);
    let directory = DbDir::initialize(&temporary.path().join("db"))
        .unwrap_or_else(|_| unreachable!("test database initialization failed"));
    let segment_bytes = 65_584;
    let config = WriterConfig::new(segment_bytes, wal_max_bytes, seal_bytes)
        .unwrap_or_else(|_| unreachable!("test writer configuration rejected"));
    (temporary, directory, config)
}

fn drop_record(table: u32) -> RecordBody {
    RecordBody::DropTable {
        table: TableId::new(table),
    }
}

fn maximum_record() -> RecordBody {
    let fields = (0..21_842)
        .map(|field| FieldSchema::new(FieldId::new(field), ValueType::UInt))
        .collect();
    RecordBody::CreateTable {
        table: TableId::new(1),
        spec: VersionSpec::new(Validity::Forever, fields).unwrap_or_else(|_| unreachable!()),
    }
}

#[test]
fn append_and_sync_positions_are_exact() {
    let (_temporary, directory, config) = database("wal-writer-position", 1_000, 100);
    let mut writer = WalWriter::create(&directory, config, 0, 0, 1)
        .unwrap_or_else(|_| unreachable!("writer creation failed"));
    let outcome = writer
        .append(&drop_record(7))
        .unwrap_or_else(|_| unreachable!("append failed"));
    assert_eq!(outcome.seq(), 1);
    assert!(!outcome.seal_recommended());
    let position = writer
        .sync()
        .unwrap_or_else(|_| unreachable!("sync failed"));
    assert_eq!(
        (position.seq(), position.segment(), position.offset()),
        (1, 1, 53)
    );

    let path = directory.file(Area::Wal, "00000000000000000001.wal");
    let bytes = std::fs::read(path).unwrap_or_else(|_| unreachable!());
    assert_eq!(bytes.len(), 53);
    assert!(record::decode(&bytes[SEGMENT_HEADER_BYTES..], 1, |_, _| None).is_ok());
}

#[test]
fn soft_and_hard_capacity_watermarks_are_exact() {
    let (_temporary, directory, config) = database("wal-writer-capacity", 100, 60);
    let mut writer =
        WalWriter::create(&directory, config, 0, 0, 1).unwrap_or_else(|_| unreachable!());
    assert!(
        !writer
            .append(&drop_record(1))
            .unwrap_or_else(|_| unreachable!())
            .seal_recommended()
    );
    assert!(
        writer
            .append(&drop_record(2))
            .unwrap_or_else(|_| unreachable!())
            .seal_recommended()
    );
    assert!(
        writer
            .append(&drop_record(3))
            .unwrap_or_else(|_| unreachable!())
            .seal_recommended()
    );
    let Err(error) = writer.append(&drop_record(4)) else {
        unreachable!("hard WAL capacity accepted another record");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
    assert!(writer.sync().is_ok(), "capacity rejection must not poison");
}

#[test]
fn minimum_wal_budget_includes_takeover_headroom() {
    let segment_bytes = 65_584;
    assert_eq!(
        WriterConfig::new(segment_bytes, 84, 84)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    assert!(WriterConfig::new(segment_bytes, 85, 85).is_ok());
}

#[test]
fn maximum_record_rolls_at_the_exact_segment_boundary() {
    let (_temporary, directory, config) = database("wal-writer-roll", 200_000, 150_000);
    let mut writer =
        WalWriter::create(&directory, config, 0, 0, 1).unwrap_or_else(|_| unreachable!());
    assert_eq!(
        writer
            .append(&maximum_record())
            .ok()
            .map(super::AppendOutcome::seq),
        Some(1)
    );
    assert_eq!(
        writer
            .append(&drop_record(2))
            .ok()
            .map(super::AppendOutcome::seq),
        Some(2)
    );
    let first = std::fs::metadata(directory.file(Area::Wal, "00000000000000000001.wal"))
        .unwrap_or_else(|_| unreachable!());
    let second = std::fs::metadata(directory.file(Area::Wal, "00000000000000000002.wal"))
        .unwrap_or_else(|_| unreachable!());
    assert_eq!(first.len(), 65_584);
    assert_eq!(second.len(), 53);
    assert_eq!(
        writer.sync().ok().map(super::DurablePosition::segment),
        Some(2)
    );
}

#[derive(Debug, Default)]
struct FaultIo {
    fail: Option<IoStep>,
    partial_record_bytes: usize,
    calls: usize,
}

impl WalIo for FaultIo {
    fn create_segment(&mut self, path: &Path) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    }

    fn write_all(&mut self, step: IoStep, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        self.calls = self.calls.saturating_add(1);
        if self.fail == Some(step) {
            if step == IoStep::RecordWrite {
                let count = self.partial_record_bytes.min(bytes.len());
                file.write_all(&bytes[..count])?;
            }
            return Err(io::Error::other("injected WAL write failure"));
        }
        file.write_all(bytes)
    }

    fn sync_data(&mut self, step: IoStep, file: &File) -> io::Result<()> {
        self.calls = self.calls.saturating_add(1);
        if self.fail == Some(step) {
            Err(io::Error::other("injected WAL sync failure"))
        } else {
            file.sync_data()
        }
    }

    fn sync_directory(&mut self, step: IoStep, path: &Path) -> io::Result<()> {
        self.calls = self.calls.saturating_add(1);
        if self.fail == Some(step) {
            Err(io::Error::other("injected WAL directory sync failure"))
        } else {
            File::open(path)?.sync_all()
        }
    }
}

fn fault_writer(label: &str) -> (TestDir, DbDir, WalWriter<FaultIo>) {
    let (temporary, directory, config) = database(label, 200_000, 100_000);
    let writer = WalWriter::create_with_io(&directory, config, 0, 0, 1, FaultIo::default())
        .unwrap_or_else(|_| unreachable!());
    (temporary, directory, writer)
}

#[test]
fn partial_record_write_is_irreversibly_poisoned() {
    let (_temporary, directory, mut writer) = fault_writer("wal-writer-partial");
    writer.io.fail = Some(IoStep::RecordWrite);
    writer.io.partial_record_bytes = 5;
    let Err(error) = writer.append(&drop_record(1)) else {
        unreachable!("partial record write was acknowledged");
    };
    assert_eq!(error.kind(), ErrorKind::Poisoned);
    let calls = writer.io.calls;
    assert_eq!(
        writer.append(&drop_record(2)).map_err(|error| error.kind()),
        Err(ErrorKind::Poisoned)
    );
    assert_eq!(
        writer.sync().map_err(|error| error.kind()),
        Err(ErrorKind::Poisoned)
    );
    assert_eq!(writer.io.calls, calls, "poisoned calls must not touch I/O");
    drop(writer);

    let checkpoint = Checkpoint::new(1, 32, 1).unwrap_or_else(|_| unreachable!());
    for _ in 0..2 {
        let mut tail = TailIndex::new(0, 1);
        let outcome = recover(&directory, checkpoint, 0, 0, &mut tail)
            .unwrap_or_else(|_| unreachable!("poison artifact did not recover"));
        assert_eq!((outcome.next_seq(), tail.next_seq()), (1, 1));
    }
}

#[test]
fn sync_failure_is_irreversibly_poisoned() {
    let (_temporary, _directory, mut writer) = fault_writer("wal-writer-sync");
    assert!(writer.append(&drop_record(1)).is_ok());
    writer.io.fail = Some(IoStep::ExplicitDataSync);
    assert_eq!(
        writer.sync().map_err(|error| error.kind()),
        Err(ErrorKind::Poisoned)
    );
    assert_eq!(
        writer.append(&drop_record(2)).map_err(|error| error.kind()),
        Err(ErrorKind::Poisoned)
    );
}

#[test]
fn segment_roll_failures_are_poisoned() {
    for step in [
        IoStep::RollOldDataSync,
        IoStep::SegmentHeaderWrite,
        IoStep::SegmentDataSync,
        IoStep::WalDirectorySync,
    ] {
        let (_temporary, _directory, mut writer) = fault_writer("wal-writer-roll-failure");
        assert!(writer.append(&maximum_record()).is_ok());
        writer.io.fail = Some(step);
        let Err(error) = writer.append(&drop_record(2)) else {
            unreachable!("failed segment roll was acknowledged");
        };
        assert_eq!(error.kind(), ErrorKind::Poisoned, "step {step:?}");
        assert_eq!(
            writer.sync().map_err(|error| error.kind()),
            Err(ErrorKind::Poisoned)
        );
    }
}
