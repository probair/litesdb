// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]
use super::*;
use crate::{
    CellValue, Db, FieldId, FieldSchema, Observation, ObservationEntry, OpenOptions, SeriesId,
    TableSpec, Validity, ValueType, fsutil::TestDir,
};
use std::fs;
fn id(n: u8) -> SharedDbId {
    SharedDbId::new([n; 16], [n; 16])
}
fn schema() -> TableSpec {
    TableSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap()
}
fn row(ts: i64) -> Observation {
    Observation::new(
        ts,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            CellValue::UInt(ts.cast_unsigned()),
        )],
    )
    .unwrap()
}
fn small() -> SharedWalOptions {
    SharedWalOptions {
        max_bytes: 1024,
        segment_bytes: 512,
        buffer_bytes: 512,
        ..SharedWalOptions::default()
    }
}
#[test]
fn batch_capacity_rejection_accepts_no_prefix_and_member_remains_maintainable() {
    let root = TestDir::new("batch-capacity");
    let owner = SharedWal::open(&root.path().join("owner"), small()).unwrap();
    let db = owner
        .open_db(&root.path().join("db"), id(1), OpenOptions::default())
        .unwrap();
    let table = db.create_table(schema()).unwrap();
    db.append(table, &row(1)).unwrap();
    db.append(table, &row(2)).unwrap();
    db.append(table, &row(3)).unwrap();
    db.sync().unwrap();
    let before = db.maintenance_status().unwrap().visible_seq();
    let batch = vec![row(4), row(5), row(6), row(7)];
    assert_eq!(
        db.try_append_batch(table, &batch).unwrap_err().kind(),
        crate::ErrorKind::ResourceExhausted
    );
    assert_eq!(db.maintenance_status().unwrap().visible_seq(), before);
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), Some(3));
    db.seal().unwrap();
    assert!(db.try_append_batch(table, &batch).unwrap().is_some());
    db.sync().unwrap();
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), Some(7));
}
#[test]
fn invalid_batch_order_is_rejected_before_owner_acceptance() {
    let root = TestDir::new("batch-order");
    let db = Db::open(root.path(), OpenOptions::default()).unwrap();
    let table = db.create_table(schema()).unwrap();
    assert!(db.try_append_batch(table, &[row(2), row(1)]).is_err());
    assert_eq!(db.maintenance_status().unwrap().visible_seq(), 1);
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), None);
}
#[test]
fn sealed_export_moves_and_reopens_without_source_owner() {
    let root = TestDir::new("portable-sealed");
    let source = root.path().join("source");
    let export = root.path().join("export");
    let moved = root.path().join("moved");
    let db = Db::open(&source, OpenOptions::default()).unwrap();
    let table = db.create_table(schema()).unwrap();
    db.append(table, &row(3)).unwrap();
    db.export_sealed(&export).unwrap();
    drop(db);
    fs::remove_dir_all(&source).unwrap();
    fs::rename(&export, &moved).unwrap();
    let db = Db::open(&moved, OpenOptions::default()).unwrap();
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), Some(3));
    db.append(table, &row(4)).unwrap();
    db.sync().unwrap();
    drop(db);
    let db = Db::open(&moved, OpenOptions::default()).unwrap();
    assert_eq!(db.snapshot().table_last_timestamp(table).unwrap(), Some(4));
}
#[test]
fn retired_identity_survives_directory_deletion_and_cannot_be_reused() {
    let root = TestDir::new("retired-member");
    let owner_path = root.path().join("owner");
    let path = root.path().join("candidate");
    let owner = SharedWal::open(&owner_path, small()).unwrap();
    owner.discard_unpublished(&path, id(1)).unwrap();
    assert!(!path.exists());
    let db = owner.open_db(&path, id(1), OpenOptions::default()).unwrap();
    let table = db.create_table(schema()).unwrap();
    db.append(table, &row(1)).unwrap();
    db.sync().unwrap();
    assert!(owner.discard_unpublished(&path, id(1)).is_err());
    let snapshot = db.snapshot();
    drop(db);
    assert!(owner.discard_unpublished(&path, id(1)).is_err());
    drop(snapshot);
    owner.discard_unpublished(&path, id(1)).unwrap();
    fs::remove_dir_all(&path).unwrap();
    owner.discard_unpublished(&path, id(1)).unwrap();
    drop(owner);
    for _ in 0..2 {
        let owner = SharedWal::open(&owner_path, small()).unwrap();
        owner.discard_unpublished(&path, id(1)).unwrap();
        assert!(owner.open_db(&path, id(1), OpenOptions::default()).is_err());
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
    }
}

#[test]
fn valid_frame_after_permanent_retirement_is_corruption() {
    use std::io::Write as _;
    let root = TestDir::new("retired-later-frame");
    let owner_path = root.path().join("owner");
    let path = root.path().join("db");
    let owner = SharedWal::open(&owner_path, SharedWalOptions::default()).unwrap();
    let db = owner.open_db(&path, id(1), OpenOptions::default()).unwrap();
    let table = db.create_table(schema()).unwrap();
    db.sync().unwrap();
    drop(db);
    owner.discard_unpublished(&path, id(1)).unwrap();
    let previous = owner.lock().unwrap().members.get(&id(1)).unwrap().latest;
    drop(owner);
    fs::remove_dir_all(&path).unwrap();
    let raw = crate::wal::record::encode(
        2,
        &crate::wal::RecordBody::AppendObservation {
            table,
            observation: row(1),
        },
    )
    .unwrap();
    let frame = format::encode(id(1), 2, 2, previous, &raw).unwrap();
    let segment = owner_path.join("wal").join(format::segment_name(1));
    fs::OpenOptions::new()
        .append(true)
        .open(segment)
        .unwrap()
        .write_all(&frame)
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            SharedWal::open(&owner_path, SharedWalOptions::default())
                .err()
                .unwrap()
                .kind(),
            crate::ErrorKind::Corruption
        );
    }
}
