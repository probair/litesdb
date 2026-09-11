// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::*;
use crate::archive::{ArchiveOptions, BaseDescriptor};
use crate::{
    CellValue, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, StreamKey,
    SyncPolicy, TableId, TableSpec, Validity, ValueType,
    fsutil::{PublishStep, TestDir},
};

fn make_table(db: &Db) -> Result<TableId> {
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
fn timestamps(db: &Db, table: TableId) -> Result<Vec<i64>> {
    let snapshot = db.snapshot();
    let mut cursor = snapshot.scan(
        StreamKey::new(table, SeriesId::new(1), FieldId::new(1)),
        0..100_000,
    )?;
    let mut found = Vec::new();
    while let Some(fact) = cursor.next_fact()? {
        found.push(fact.timestamp());
    }
    Ok(found)
}

#[test]
fn frozen_base_survives_source_maintenance_and_pin_reopen() -> Result<()> {
    let root = TestDir::new("archive-base-pin");
    let source = root.path().join("source");
    let db = Db::open_with_archive(&source, OpenOptions::default(), ArchiveOptions::default())?;
    let table = make_table(&db)?;
    append(&db, table, 10)?;
    let frozen = db.prepare_base(&root.path().join("base"))?;
    let descriptor = db.describe_base(&frozen)?;
    assert_eq!(
        BaseDescriptor::from_bytes(&descriptor.to_bytes())?,
        descriptor
    );
    append(&db, table, 20)?;
    db.seal()?;
    db.compact(crate::CompactLevel::Level0To1)?;
    db.retain(100)?;
    let end = db.archive_status()?.durable_end();
    assert!(db.release_archive(end).is_err());
    let chunk = db.export_durable(frozen.cursor(), 4096)?;
    assert_eq!(db.describe_base(&frozen)?, descriptor);
    drop(db);
    let db = Db::open_with_archive(&source, OpenOptions::default(), ArchiveOptions::default())?;
    assert!(db.release_archive(end).is_err());
    let mut restore = RestoreBuilder::install(
        &descriptor,
        frozen.directory(),
        &root.path().join("restore"),
        OpenOptions::default(),
    )?;
    assert_eq!(restore.apply_archive(&chunk)?, end);
    assert_eq!(restore.apply_archive(&chunk)?, end);
    let result = restore.finish()?;
    let restored = Db::open(result.directory(), OpenOptions::default())?;
    assert_eq!(timestamps(&restored, table)?, vec![10, 20]);
    assert_eq!(restored.snapshot().table_last_timestamp(table)?, Some(20));
    db.finish_base(frozen.id())?;
    db.release_archive(end)?;
    drop(restored);
    let promoted = Db::open_with_archive(
        result.directory(),
        OpenOptions::default(),
        ArchiveOptions::default(),
    )?;
    let branch = promoted.archive_status()?.durable_end();
    assert_eq!(branch.database, end.database);
    assert_ne!(branch.branch, end.branch);
    assert!(branch.covers(end).is_err());
    Ok(())
}

#[test]
fn restore_current_publication_is_atomic_with_source_receipt() -> Result<()> {
    for step in [
        PublishStep::Write,
        PublishStep::FileSync,
        PublishStep::Rename,
        PublishStep::DirectorySync,
    ] {
        let root = TestDir::new("restore-current-fault");
        let db = Db::open_with_archive(
            &root.path().join("source"),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )?;
        let table = make_table(&db)?;
        append(&db, table, 10)?;
        let frozen = db.prepare_base(&root.path().join("base"))?;
        let descriptor = db.describe_base(&frozen)?;
        append(&db, table, 20)?;
        db.sync()?;
        let chunk = db.export_durable(frozen.cursor(), 4096)?;
        let target = root.path().join("restore");
        let mut builder = RestoreBuilder::install(
            &descriptor,
            frozen.directory(),
            &target,
            OpenOptions::default(),
        )?;
        builder.directory.fail_publish(Area::Root, "CURRENT", step);
        assert!(builder.apply_archive(&chunk).is_err());
        assert_eq!(
            builder.apply_archive(&chunk).err().map(|e| e.kind()),
            Some(crate::ErrorKind::Poisoned)
        );
        drop(builder);
        let expected = if step == PublishStep::DirectorySync {
            chunk.end()
        } else {
            chunk.start()
        };
        for _ in 0..2 {
            let builder = RestoreBuilder::open(&target, OpenOptions::default())?;
            assert_eq!(builder.applied_cursor(), expected);
            let path = restore_io::generation_path(&target, builder.state.generation);
            assert!(Db::open(&path, OpenOptions::default()).is_err());
        }
        let mut builder = RestoreBuilder::open(&target, OpenOptions::default())?;
        assert_eq!(builder.apply_archive(&chunk)?, chunk.end());
        let result = builder.finish()?;
        let restored = Db::open(result.directory(), OpenOptions::default())?;
        assert_eq!(timestamps(&restored, table)?, vec![10, 20]);
    }
    Ok(())
}

#[test]
fn finish_publication_is_retryable_after_every_fault() -> Result<()> {
    for step in [
        PublishStep::Write,
        PublishStep::FileSync,
        PublishStep::Rename,
        PublishStep::DirectorySync,
    ] {
        let root = TestDir::new("restore-finish-fault");
        let db = Db::open_with_archive(
            &root.path().join("source"),
            OpenOptions::default(),
            ArchiveOptions::default(),
        )?;
        let table = make_table(&db)?;
        append(&db, table, 10)?;
        let frozen = db.prepare_base(&root.path().join("base"))?;
        let descriptor = db.describe_base(&frozen)?;
        let target = root.path().join("restore");
        let builder = RestoreBuilder::install(
            &descriptor,
            frozen.directory(),
            &target,
            OpenOptions::default(),
        )?;
        builder.directory.fail_publish(Area::Root, "FINISHED", step);
        assert!(builder.finish().is_err());
        assert!(Db::open(&target.join("ready"), OpenOptions::default()).is_err());
        let builder = RestoreBuilder::open(&target, OpenOptions::default())?;
        let result = builder.finish()?;
        let restored = Db::open(result.directory(), OpenOptions::default())?;
        assert_eq!(timestamps(&restored, table)?, vec![10]);
        drop(restored);
        let again = RestoreBuilder::open(&target, OpenOptions::default())?.finish()?;
        assert_eq!(again.source_cursor(), result.source_cursor());
    }
    Ok(())
}

#[test]
fn restore_rejects_wrong_base_and_discontinuous_chunks() -> Result<()> {
    let root = TestDir::new("restore-untrusted");
    let db = Db::open_with_archive(
        &root.path().join("source"),
        OpenOptions::default(),
        ArchiveOptions::default(),
    )?;
    let table = make_table(&db)?;
    let frozen = db.prepare_base(&root.path().join("base"))?;
    let descriptor = db.describe_base(&frozen)?;
    let mut bad = descriptor.clone();
    bad.files[0].sha256[0] ^= 1;
    assert!(
        RestoreBuilder::install(
            &bad,
            frozen.directory(),
            &root.path().join("bad"),
            OpenOptions::default()
        )
        .is_err()
    );
    bad.files[0].path = "../MANIFEST".to_owned();
    assert!(BaseDescriptor::from_bytes(&bad.to_bytes()).is_err());
    append(&db, table, 10)?;
    db.sync()?;
    let first = db.export_durable(frozen.cursor(), 4096)?;
    append(&db, table, 20)?;
    db.sync()?;
    let second = db.export_durable(first.end(), 4096)?;
    let mut builder = RestoreBuilder::install(
        &descriptor,
        frozen.directory(),
        &root.path().join("restore"),
        OpenOptions::default(),
    )?;
    assert!(builder.apply_archive(&second).is_err());
    assert_eq!(builder.applied_cursor(), frozen.cursor());
    assert_eq!(builder.apply_archive(&first)?, first.end());
    let mut wrong = second.clone();
    wrong.start.branch[0] ^= 1;
    assert!(builder.apply_archive(&wrong).is_err());
    assert_eq!(builder.apply_archive(&second)?, second.end());
    Ok(())
}

#[test]
fn incremental_restore_exceeds_twenty_five_mib_without_unbounded_wal() -> Result<()> {
    let root = TestDir::new("restore-large-archive");
    let options = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        directory_cache_bytes: 0,
        ..OpenOptions::default()
    };
    let db = Db::open_with_archive(
        &root.path().join("source"),
        options,
        ArchiveOptions::default(),
    )?;
    let table = make_table(&db)?;
    let frozen = db.prepare_base(&root.path().join("base"))?;
    let descriptor = db.describe_base(&frozen)?;
    for ts in 1..=6000 {
        let entries = (1..=250)
            .map(|series| {
                ObservationEntry::new(
                    SeriesId::new(series),
                    FieldId::new(1),
                    CellValue::UInt((u64::try_from(ts).unwrap_or_default()) * 256 + series),
                )
            })
            .collect();
        db.append(table, &Observation::new(ts, entries)?)?;
    }
    db.sync()?;
    let end = db.archive_status()?.durable_end();
    let target = root.path().join("restore");
    let mut builder = RestoreBuilder::install(&descriptor, frozen.directory(), &target, options)?;
    let mut cursor = frozen.cursor();
    let mut raw_bytes = 0usize;
    let mut chunks = 0;
    while cursor != end {
        let chunk = db.export_durable(cursor, 524_288)?;
        let mut records = Reader::new(&chunk.records);
        while !records.remaining().is_empty() {
            raw_bytes += read_record(&mut records)?.3.len();
        }
        cursor = builder.apply_archive(&chunk)?;
        assert_eq!(cursor, chunk.end());
        chunks += 1;
        if chunks % 7 == 0 {
            drop(builder);
            builder = RestoreBuilder::open(&target, options)?;
            assert_eq!(builder.applied_cursor(), cursor);
        }
        let work = Db::open_for_restore(
            &restore_io::generation_path(&target, builder.state.generation),
            options,
        )?;
        assert!(work.maintenance_status()?.wal_storage_bytes() <= u64::from(options.wal_max_bytes));
    }
    println!(
        "raw_wal_bytes={raw_bytes} chunks={chunks} source_archive_charged_bytes={}",
        db.archive_status()?.bytes()
    );
    assert!(raw_bytes > 26_214_400);
    assert!(chunks > 50);
    let result = builder.finish()?;
    let restored = Db::open(result.directory(), options)?;
    assert_eq!(restored.snapshot().table_last_timestamp(table)?, Some(6000));
    assert_eq!(
        timestamps(&restored, table)?,
        (1..=6000).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn archive_replay_preserves_all_mutation_families_and_takeover() -> Result<()> {
    let root = TestDir::new("restore-mutations");
    let source = root.path().join("source");
    let options = OpenOptions::default();
    let db = Db::open_with_archive(&source, options, ArchiveOptions::default())?;
    let fields = vec![
        FieldSchema::new(FieldId::new(1), ValueType::UInt),
        FieldSchema::new(FieldId::new(2), ValueType::Sq1),
        FieldSchema::new(FieldId::new(3), ValueType::F32Bits),
    ];
    let table = db.create_table(TableSpec::new(
        Validity::duration_seconds(9)?,
        fields.clone(),
    )?)?;
    let dropped = make_table(&db)?;
    let frozen = db.prepare_base(&root.path().join("base"))?;
    let descriptor = db.describe_base(&frozen)?;
    db.append(
        table,
        &Observation::new(
            10,
            vec![
                ObservationEntry::new(SeriesId::new(1), FieldId::new(1), CellValue::UInt(7)),
                ObservationEntry::new(SeriesId::new(1), FieldId::new(2), CellValue::Null),
                ObservationEntry::new(
                    SeriesId::new(1),
                    FieldId::new(3),
                    CellValue::F32Bits(crate::F32Bits::from_bits(0x7fc0_0123)),
                ),
            ],
        )?,
    )?;
    let mut next = fields;
    next.push(FieldSchema::new(FieldId::new(4), ValueType::UInt));
    db.new_table_version(table, TableSpec::new(Validity::Forever, next)?)?;
    db.append(
        table,
        &Observation::new(
            20,
            vec![
                ObservationEntry::new(SeriesId::new(1), FieldId::new(1), CellValue::Null),
                ObservationEntry::new(SeriesId::new(1), FieldId::new(2), CellValue::sq1(176)),
                ObservationEntry::new(
                    SeriesId::new(1),
                    FieldId::new(3),
                    CellValue::F32Bits(crate::F32Bits::from_bits(0x8000_0000)),
                ),
                ObservationEntry::new(SeriesId::new(1), FieldId::new(4), CellValue::UInt(55)),
            ],
        )?,
    )?;
    db.retire_series(table, SeriesId::new(1), 21)?;
    db.retire_field(table, FieldId::new(4), 22)?;
    db.drop_table(dropped)?;
    db.sync()?;
    drop(db);
    let db = Db::open_with_archive(
        &source,
        OpenOptions {
            takeover: true,
            ..options
        },
        ArchiveOptions::default(),
    )?;
    append(&db, table, 30)?;
    db.sync()?;
    let chunk = db.export_durable(frozen.cursor(), 4096)?;
    let mut builder = RestoreBuilder::install(
        &descriptor,
        frozen.directory(),
        &root.path().join("restore"),
        options,
    )?;
    builder.apply_archive(&chunk)?;
    let result = builder.finish()?;
    let restored = Db::open(result.directory(), options)?;
    let keys: Vec<_> = (1..=4)
        .map(|field| StreamKey::new(table, SeriesId::new(1), FieldId::new(field)))
        .collect();
    for ts in [10, 18, 19, 20, 21, 22, 29, 30] {
        assert_eq!(
            restored.snapshot().value_at(&keys, ts)?,
            db.snapshot().value_at(&keys, ts)?
        );
    }
    assert_eq!(
        restored.snapshot().table_versions(table)?,
        db.snapshot().table_versions(table)?
    );
    assert!(restored.snapshot().table_last_timestamp(dropped).is_err());
    assert!(chunk.end().covers(frozen.cursor())?);
    Ok(())
}

#[test]
fn damaged_capture_pin_cannot_relax_retention() -> Result<()> {
    let root = TestDir::new("base-pin-checksum");
    let source = root.path().join("source");
    let db = Db::open_with_archive(&source, OpenOptions::default(), ArchiveOptions::default())?;
    let table = make_table(&db)?;
    let frozen = db.prepare_base(&root.path().join("base"))?;
    assert_eq!(db.pending_bases()?, vec![(frozen.id(), frozen.cursor())]);
    append(&db, table, 10)?;
    db.sync()?;
    let end = db.archive_status()?.durable_end();
    let before = fs::read(source.join("ARCHIVE"))?;
    let pin = source.join(crate::archive::base::pin_name(frozen.id()));
    let mut bytes = fs::read(&pin)?;
    bytes[88] ^= 128;
    fs::write(pin, bytes)?;
    assert_eq!(
        db.release_archive(end).err().map(|e| e.kind()),
        Some(crate::ErrorKind::Corruption)
    );
    assert_eq!(fs::read(source.join("ARCHIVE"))?, before);
    db.finish_base(frozen.id())?;
    db.finish_base(frozen.id())?;
    db.release_archive(end)?;
    Ok(())
}

#[test]
fn frozen_and_installed_bases_validate_existing_unit_integrity() -> Result<()> {
    let root = TestDir::new("base-body-crc");
    let db = Db::open_with_archive(
        &root.path().join("source"),
        OpenOptions::default(),
        ArchiveOptions::default(),
    )?;
    let table = make_table(&db)?;
    append(&db, table, 10)?;
    let frozen = db.prepare_base(&root.path().join("base"))?;
    let mut descriptor = db.describe_base(&frozen)?;
    let unit = descriptor
        .files
        .iter_mut()
        .find(|file| file.path.starts_with("units/"))
        .ok_or_else(|| crate::archive::invalid("test unit absent"))?;
    let path = frozen.directory().join(&unit.path);
    let mut bytes = fs::read(&path)?;
    let index = bytes.len() - 20;
    bytes[index] ^= 1;
    fs::write(&path, &bytes)?;
    unit.sha256 = Sha256::digest(&bytes).into();
    assert_eq!(
        db.describe_base(&frozen).err().map(|e| e.kind()),
        Some(crate::ErrorKind::Corruption)
    );
    let target = root.path().join("restore");
    assert_eq!(
        RestoreBuilder::install(
            &descriptor,
            frozen.directory(),
            &target,
            OpenOptions::default()
        )
        .err()
        .map(|e| e.kind()),
        Some(crate::ErrorKind::Corruption)
    );
    assert!(!target.join("CURRENT").exists());
    Ok(())
}
