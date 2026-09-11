// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ArchiveOptions, ExportChunk};
use crate::{
    CellValue, Db, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, OpenOptions,
    Result, SealPolicy, SeriesId, SyncPolicy, TableId, TableSpec, Validity, ValueType,
    fsutil::{Area, PublishStep, TestDir},
};
use std::{fs, time::Duration};

fn table(db: &Db) -> Result<TableId> {
    db.create_table(TableSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )?)
}
fn append(db: &Db, table: TableId, ts: i64) -> Result<()> {
    db.append(
        table,
        &Observation::new(
            ts,
            vec![ObservationEntry::new(
                SeriesId::new(1),
                FieldId::new(1),
                CellValue::UInt(u64::try_from(ts).unwrap_or_default()),
            )],
        )?,
    )?;
    Ok(())
}
fn options() -> OpenOptions {
    OpenOptions {
        sync_policy: SyncPolicy::Manual,
        wal_max_bytes: 512,
        seal_policy: SealPolicy {
            bytes: 200,
            interval: Duration::from_secs(3600),
            ..SealPolicy::default()
        },
        ..OpenOptions::default()
    }
}

#[test]
fn durable_export_survives_automatic_seals_and_reopen() -> Result<()> {
    let root = TestDir::new("archive-cycles");
    let db = Db::open_with_archive(root.path(), options(), ArchiveOptions::default())?;
    let beginning = db.archive_status()?.durable_end();
    let table = table(&db)?;
    assert!(db.export_durable(beginning, 4096)?.is_empty());
    for ts in 1..=100 {
        append(&db, table, ts)?;
    }
    db.sync()?;
    let end = db.archive_status()?.durable_end();
    assert_eq!(end.seq, 101);
    let mut cursor = beginning;
    let mut records = 0;
    while cursor != end {
        let chunk = db.export_durable(cursor, 400)?;
        assert!(!chunk.is_empty());
        assert!(chunk.to_bytes().len() <= 400);
        assert_eq!(ExportChunk::from_bytes(&chunk.to_bytes())?, chunk);
        records += chunk.end().seq - chunk.start().seq;
        cursor = chunk.end();
    }
    assert_eq!(records, 101);
    assert!(db.export_durable(beginning, 216).is_err());
    drop(db);
    assert!(Db::open(root.path(), options()).is_err());
    let db = Db::open_with_archive(root.path(), options(), ArchiveOptions::default())?;
    assert_eq!(db.archive_status()?.durable_end(), end);
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(100));
    assert_eq!(db.export_durable(beginning, 400)?.start(), beginning);
    Ok(())
}

#[test]
fn unsynced_primary_suffix_is_reconciled_before_recovery_seal() -> Result<()> {
    let root = TestDir::new("archive-catchup");
    let opts = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        ..OpenOptions::default()
    };
    let db = Db::open_with_archive(root.path(), opts, ArchiveOptions::default())?;
    let start = db.archive_status()?.durable_end();
    let table = table(&db)?;
    append(&db, table, 10)?;
    assert_eq!(db.archive_status()?.durable_end(), start);
    drop(db);
    let db = Db::open_with_archive(root.path(), options(), ArchiveOptions::default())?;
    assert_eq!(db.archive_status()?.durable_end().seq, 2);
    assert_eq!(db.export_durable(start, 4096)?.end().seq, 2);
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(10));
    Ok(())
}

#[test]
fn torn_unsynced_primary_record_is_not_exported() -> Result<()> {
    let root = TestDir::new("archive-torn-primary");
    let opts = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        ..OpenOptions::default()
    };
    let db = Db::open_with_archive(root.path(), opts, ArchiveOptions::default())?;
    let table = table(&db)?;
    db.sync()?;
    let end = db.archive_status()?.durable_end();
    append(&db, table, 10)?;
    drop(db);
    let wal = root.path().join("wal/00000000000000000001.wal");
    let file = fs::OpenOptions::new().write(true).open(&wal)?;
    file.set_len(file.metadata()?.len() - 3)?;
    drop(file);
    let db = Db::open_with_archive(root.path(), opts, ArchiveOptions::default())?;
    assert_eq!(db.archive_status()?.durable_end(), end);
    assert_eq!(db.snapshot().table_last_timestamp(table)?, None);
    assert!(db.export_durable(end, 4096)?.is_empty());
    Ok(())
}

#[test]
fn archive_publish_failure_preserves_primary_and_reopens_twice() -> Result<()> {
    for step in [
        PublishStep::Write,
        PublishStep::FileSync,
        PublishStep::Rename,
        PublishStep::DirectorySync,
    ] {
        let root = TestDir::new("archive-publish-fault");
        let db = Db::open_with_archive(root.path(), options(), ArchiveOptions::default())?;
        let start = db.archive_status()?.durable_end();
        let table = table(&db)?;
        append(&db, table, 10)?;
        db.directory.fail_publish(Area::Root, "ARCHIVE", step);
        assert_eq!(db.seal().err().map(|e| e.kind()), Some(ErrorKind::Poisoned));
        assert!(!db.archive_status()?.healthy());
        drop(db);
        for _ in 0..2 {
            let db = Db::open_with_archive(root.path(), options(), ArchiveOptions::default())?;
            let chunk = db.export_durable(start, 4096)?;
            assert_eq!(chunk.end().seq, 2);
            assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(10));
        }
    }
    Ok(())
}

#[test]
fn quota_rejection_has_no_primary_effect_and_release_frees_space() -> Result<()> {
    let root = TestDir::new("archive-quota");
    let archive = ArchiveOptions { max_bytes: 131_072 };
    let db = Db::open_with_archive(root.path(), options(), archive)?;
    let start = db.archive_status()?.earliest();
    let table = table(&db)?;
    let mut ts = 1;
    loop {
        match append(&db, table, ts) {
            Ok(()) => ts += 1,
            Err(error) => {
                assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
                break;
            }
        }
        assert!(ts < 5000);
    }
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(ts - 1));
    db.sync()?;
    let end = db.archive_status()?.durable_end();
    db.release_archive(end)?;
    assert_eq!(db.archive_status()?.bytes(), 8192);
    assert!(db.export_durable(start, 4096).is_err());
    append(&db, table, ts)?;
    db.sync()?;
    assert_eq!(db.export_durable(end, 4096)?.end().seq, end.seq + 1);
    Ok(())
}

#[test]
fn opaque_boundaries_and_export_integrity_are_strict() -> Result<()> {
    let root = TestDir::new("archive-integrity");
    let db = Db::open_with_archive(root.path(), options(), ArchiveOptions::default())?;
    let start = db.archive_status()?.earliest();
    let table = table(&db)?;
    append(&db, table, 10)?;
    db.sync()?;
    let chunk = db.export_durable(start, 4096)?;
    let bytes = chunk.to_bytes();
    for end in 0..bytes.len() {
        assert!(ExportChunk::from_bytes(&bytes[..end]).is_err());
    }
    let mut bad = start.to_bytes();
    bad[8] ^= 1;
    let bad = super::ArchiveCursor::from_bytes(&bad)?;
    assert!(db.export_durable(bad, 4096).is_err());
    let mut bad = bytes;
    let last = bad.len() - 1;
    bad[last] ^= 1;
    assert!(ExportChunk::from_bytes(&bad).is_err());
    Ok(())
}
