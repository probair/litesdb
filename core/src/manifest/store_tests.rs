// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::fs;

use super::{load, publish};
use crate::{
    CellValue, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, TableId,
    TableVersion, Validity, ValueType,
    fsutil::{Area, DbDir, TestDir},
    manifest::catalog::{Manifest, ManifestIdentity, RetentionState, TableCatalog},
    wal::{Checkpoint, RecordBody, SegmentHeader, encode_record, recover, segment_name},
};

fn database(label: &str) -> (TestDir, DbDir) {
    let temporary = TestDir::new(label);
    let directory = DbDir::initialize(&temporary.path().join("db"))
        .unwrap_or_else(|_| unreachable!("test database initialization failed"));
    (temporary, directory)
}

fn empty_manifest(generation: u64) -> Manifest {
    Manifest::restore(
        ManifestIdentity::new(generation, 0, 0, 0, 0),
        Checkpoint::new(1, 32, 1).unwrap_or_else(|_| unreachable!()),
        RetentionState::default(),
        vec![],
        vec![],
    )
    .unwrap_or_else(|_| unreachable!())
}

#[test]
fn publication_switches_only_to_the_next_generation() {
    let (_temporary, directory) = database("manifest-store-generation");
    let zero = empty_manifest(0);
    publish(&directory, None, &zero).unwrap_or_else(|_| unreachable!());
    assert_eq!(load(&directory).ok(), Some(zero));

    let one = empty_manifest(1);
    publish(&directory, Some(0), &one).unwrap_or_else(|_| unreachable!());
    assert_eq!(load(&directory).ok(), Some(one.clone()));
    let Err(error) = publish(&directory, Some(1), &one) else {
        unreachable!("replayed MANIFEST generation accepted");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(load(&directory).ok(), Some(one));
}

#[test]
fn corrupt_visible_manifest_is_rejected() {
    let (_temporary, directory) = database("manifest-store-corrupt");
    publish(&directory, None, &empty_manifest(0)).unwrap_or_else(|_| unreachable!());
    let path = directory.file(Area::Root, "MANIFEST");
    let mut bytes = fs::read(&path).unwrap_or_else(|_| unreachable!());
    let last = bytes.len().saturating_sub(1);
    bytes[last] ^= 1;
    fs::write(path, bytes).unwrap_or_else(|_| unreachable!());
    let Err(error) = load(&directory) else {
        unreachable!("corrupt MANIFEST accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

fn active_table() -> TableCatalog {
    let version = TableVersion::restore(
        1,
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
        Some(10),
    )
    .unwrap_or_else(|_| unreachable!());
    TableCatalog::restore(TableId::new(1), Some(10), vec![version], vec![], vec![])
        .unwrap_or_else(|_| unreachable!())
}

#[test]
fn loaded_manifest_drives_schema_aware_wal_recovery() {
    let (_temporary, directory) = database("manifest-store-recovery");
    let old = encode_record(
        1,
        &RecordBody::CreateTable {
            table: TableId::new(1),
            spec: crate::VersionSpec::new(
                Validity::Forever,
                vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
            )
            .unwrap_or_else(|_| unreachable!()),
        },
    )
    .unwrap_or_else(|_| unreachable!());
    let observation = Observation::new(
        11,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            CellValue::UInt(9),
        )],
    )
    .unwrap_or_else(|_| unreachable!());
    let suffix = encode_record(
        2,
        &RecordBody::AppendObservation {
            table: TableId::new(1),
            observation,
        },
    )
    .unwrap_or_else(|_| unreachable!());
    let mut wal = SegmentHeader::new(1, 0, 0).encode().to_vec();
    wal.extend_from_slice(&old);
    let offset = u64::try_from(wal.len()).unwrap_or(u64::MAX);
    wal.extend_from_slice(&suffix);
    fs::write(directory.file(Area::Wal, &segment_name(1)), wal).unwrap_or_else(|_| unreachable!());

    let manifest = Manifest::restore(
        ManifestIdentity::new(0, 0, 1, 0, 0),
        Checkpoint::new(1, offset, 2).unwrap_or_else(|_| unreachable!()),
        RetentionState::default(),
        vec![active_table()],
        vec![],
    )
    .unwrap_or_else(|_| unreachable!());
    publish(&directory, None, &manifest).unwrap_or_else(|_| unreachable!());

    let loaded = load(&directory).unwrap_or_else(|_| unreachable!("MANIFEST load failed"));
    let mut target = loaded
        .replay_target()
        .unwrap_or_else(|_| unreachable!("replay target restore failed"));
    let outcome = recover(
        &directory,
        loaded.checkpoint(),
        loaded.identity().shard_id(),
        loaded.identity().writer_epoch(),
        &mut target,
    )
    .unwrap_or_else(|_| unreachable!("schema-aware WAL recovery failed"));
    assert_eq!(outcome.next_seq(), 3);
    let table = target
        .table(TableId::new(1))
        .unwrap_or_else(|| unreachable!());
    assert_eq!(table.last_ts(), Some(11));
    assert_eq!(table.rows().len(), 1);
}
