// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ArchiveCursor, ArchiveOptions, ExportChunk, RestoreBuilder};
use crate::{
    CellValue, Db, FieldId, FieldSchema, Observation, ObservationEntry, OpenOptions, Result,
    SeriesId, TableId, TableSpec, Validity, ValueType, fsutil::TestDir,
};

#[test]
fn caller_known_base_id_preserves_other_owners_on_failed_prepare() -> Result<()> {
    use crate::fsutil::{Area, PublishStep};
    for step in [
        PublishStep::Write,
        PublishStep::FileSync,
        PublishStep::Rename,
        PublishStep::DirectorySync,
    ] {
        let root = TestDir::new("archive-owned-base-pin");
        let source = root.path().join("source");
        let db = Db::open_with_archive(&source, OpenOptions::default(), ArchiveOptions::default())?;
        let retained = db.prepare_base(&root.path().join("other-owner"))?;
        let id = [19; 16];
        db.directory
            .fail_publish(Area::Root, &super::base::pin_name(id), step);
        assert!(
            db.prepare_base_with_id(&root.path().join("ours"), id)
                .is_err()
        );
        drop(db);
        let db = Db::open_with_archive(&source, OpenOptions::default(), ArchiveOptions::default())?;
        db.finish_base(id)?;
        db.finish_base(id)?;
        assert_eq!(
            db.pending_bases()?,
            vec![(retained.id(), retained.cursor())]
        );
        let frozen = db.prepare_base_with_id(&root.path().join("retry"), [20; 16])?;
        assert_eq!(frozen.id(), [20; 16]);
        assert!(
            db.prepare_base_with_id(&root.path().join("collision"), [20; 16])
                .is_err()
        );
        assert!(
            db.prepare_base_with_id(&root.path().join("zero"), [0; 16])
                .is_err()
        );
    }
    Ok(())
}

fn append(db: &Db, table: TableId, timestamp: i64) -> Result<ArchiveCursor> {
    db.append(
        table,
        &Observation::new(
            timestamp,
            vec![ObservationEntry::new(
                SeriesId::new(1),
                FieldId::new(1),
                CellValue::UInt(timestamp.unsigned_abs()),
            )],
        )?,
    )?;
    db.sync()?;
    Ok(db.archive_status()?.durable_end())
}

#[test]
fn suffix_requires_an_exact_authenticated_record_boundary() -> Result<()> {
    let root = TestDir::new("archive-suffix-boundaries");
    let db = Db::open_with_archive(
        &root.path().join("source"),
        OpenOptions::default(),
        ArchiveOptions::default(),
    )?;
    let earlier = db.archive_status()?.durable_end();
    let table = db.create_table(TableSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )?)?;
    let old_base = db.prepare_base(&root.path().join("base-old"))?;
    append(&db, table, 10)?;
    let newer_base = db.prepare_base(&root.path().join("base-new"))?;
    let through = append(&db, table, 20)?;
    let whole = db.export_durable(old_base.cursor(), 4096)?;
    assert_eq!(whole.suffix_after(whole.start())?, Some(whole.clone()));
    assert_eq!(whole.suffix_after(whole.end())?, None);
    assert!(whole.suffix_after(earlier).is_err());
    let suffix = whole
        .suffix_after(newer_base.cursor())?
        .ok_or_else(|| super::invalid("missing interior suffix"))?;
    assert_eq!(suffix.start(), newer_base.cursor());
    assert_eq!(suffix.end(), through);
    assert!(suffix.to_bytes().len() < whole.to_bytes().len());
    assert_eq!(ExportChunk::from_bytes(&suffix.to_bytes())?, suffix);
    for offset in [8, 24, 56, 72] {
        let mut forged = newer_base.cursor().to_bytes();
        forged[offset] ^= 1;
        assert!(
            whole
                .suffix_after(ArchiveCursor::from_bytes(&forged)?)
                .is_err()
        );
    }
    let after = append(&db, table, 30)?;
    assert!(whole.suffix_after(after).is_err());
    let empty = db.export_durable(after, 4096)?;
    assert!(empty.is_empty());
    assert_eq!(empty.suffix_after(after)?, None);
    let descriptor = db.describe_base(&newer_base)?;
    let path = root.path().join("restore");
    let mut restore = RestoreBuilder::install(
        &descriptor,
        newer_base.directory(),
        &path,
        OpenOptions::default(),
    )?;
    assert_eq!(restore.apply_archive(&suffix)?, through);
    drop(restore);
    let mut restore = RestoreBuilder::open(&path, OpenOptions::default())?;
    assert_eq!(restore.applied_cursor(), through);
    assert_eq!(
        restore.apply_archive(&db.export_durable(through, 4096)?)?,
        after
    );
    let restored = restore.finish()?;
    let db = Db::open(restored.directory(), OpenOptions::default())?;
    assert_eq!(db.snapshot().table_last_timestamp(table)?, Some(30));
    Ok(())
}
