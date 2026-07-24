// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{collections::BTreeMap, fs};

use super::{assemble, assemble_and_publish, publish};
use crate::{
    CellValue, ErrorKind, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId,
    TableId, Validity, ValueType, VersionSpec,
    fsutil::{Area, DbDir, TestDir},
    limits::MAX_OPERATION_RSS_BYTES,
    unit::{decode, format},
    wal::{RecordBody, ReplayTarget, TailIndex},
};

type FactKey = (TableId, u32, i64, SeriesId, FieldId);

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .unwrap_or_else(|_| unreachable!("fixture scalar is truncated")),
    )
}

fn refresh_body_crc(bytes: &mut [u8]) {
    let footer = bytes.len().saturating_sub(format::UNIT_FOOTER_BYTES);
    let body_crc = crc32fast::hash(&bytes[format::UNIT_HEADER_BYTES..footer]);
    bytes[footer..footer + 4].copy_from_slice(&body_crc.to_le_bytes());
}

fn expected_facts(snapshot: &TailIndex) -> BTreeMap<FactKey, CellValue> {
    let mut facts = BTreeMap::new();
    for (table, state) in snapshot.tables() {
        for row in state.rows() {
            for entry in state
                .row_entries(row)
                .unwrap_or_else(|| unreachable!("tail entries invalid"))
            {
                facts.insert(
                    (
                        table,
                        row.version_no(),
                        row.timestamp(),
                        entry.series(),
                        entry.field(),
                    ),
                    entry.value(),
                );
            }
        }
    }
    facts
}

fn spec(fields: &[(u16, ValueType)]) -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        fields
            .iter()
            .map(|(field, kind)| FieldSchema::new(FieldId::new(*field), *kind))
            .collect(),
    )
    .unwrap_or_else(|_| unreachable!("valid schema rejected"))
}

fn observation(timestamp: i64, values: &[(u64, u16, CellValue)]) -> Observation {
    Observation::new(
        timestamp,
        values
            .iter()
            .map(|(series, field, value)| {
                ObservationEntry::new(SeriesId::new(*series), FieldId::new(*field), *value)
            })
            .collect(),
    )
    .unwrap_or_else(|_| unreachable!("valid observation rejected"))
}

fn apply(snapshot: &mut TailIndex, seq: u64, body: RecordBody) {
    snapshot
        .apply(seq, body)
        .unwrap_or_else(|_| unreachable!("valid replay mutation rejected"));
}

fn snapshot() -> TailIndex {
    let mut tail = TailIndex::new(0, 1);
    apply(
        &mut tail,
        1,
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(&[(1, ValueType::UInt), (2, ValueType::Sq1)]),
        },
    );
    apply(
        &mut tail,
        2,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(10, &[(1, 1, CellValue::UInt(7)), (2, 2, CellValue::Null)]),
        },
    );
    apply(
        &mut tail,
        3,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(20, &[(1, 1, CellValue::UInt(9))]),
        },
    );
    apply(
        &mut tail,
        4,
        RecordBody::NewTableVersion {
            table: TableId::new(1),
            spec: spec(&[
                (1, ValueType::UInt),
                (2, ValueType::Sq1),
                (3, ValueType::F32Bits),
            ]),
        },
    );
    apply(
        &mut tail,
        5,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(
                30,
                &[
                    (1, 1, CellValue::UInt(11)),
                    (1, 3, CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234))),
                ],
            ),
        },
    );
    apply(
        &mut tail,
        6,
        RecordBody::CreateTable {
            table: TableId::new(2),
            spec: spec(&[(1, ValueType::UInt)]),
        },
    );
    apply(
        &mut tail,
        7,
        RecordBody::AppendObservation {
            table: TableId::new(2),
            observation: observation(15, &[(9, 1, CellValue::UInt(1))]),
        },
    );
    tail
}

fn measurement_snapshot() -> TailIndex {
    let mut tail = TailIndex::new(0, 1);
    let mut seq = 1_u64;
    for table in 1..=8 {
        apply(
            &mut tail,
            seq,
            RecordBody::CreateTable {
                table: TableId::new(table),
                spec: spec(&[(1, ValueType::UInt)]),
            },
        );
        seq = seq.saturating_add(1);
    }
    for row in 0_u64..2_048 {
        for table in 1..=8 {
            let timestamp = i64::try_from(row.saturating_mul(3)).unwrap_or(i64::MAX);
            let base = row.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(table);
            apply(
                &mut tail,
                seq,
                RecordBody::AppendObservation {
                    table: TableId::new(table),
                    observation: observation(
                        timestamp,
                        &[
                            (1, 1, CellValue::UInt(base ^ 0xa076_1d64_78bd_642f)),
                            (2, 1, CellValue::UInt(base ^ 0xe703_7ed1_a0b4_28db)),
                            (3, 1, CellValue::UInt(base ^ 0x8ebc_6af0_9c88_c6e3)),
                            (4, 1, CellValue::UInt(base ^ 0x5899_65cc_7537_4cc3)),
                        ],
                    ),
                },
            );
            seq = seq.saturating_add(1);
        }
    }
    tail
}

fn sparse_split_snapshot() -> TailIndex {
    let mut tail = TailIndex::new(0, 1);
    apply(
        &mut tail,
        1,
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(&[(1, ValueType::UInt)]),
        },
    );
    for index in 0..1_500_u64 {
        apply(
            &mut tail,
            index.saturating_add(2),
            RecordBody::AppendObservation {
                table: TableId::new(1),
                observation: observation(
                    i64::try_from(index.saturating_add(1)).unwrap_or(i64::MAX),
                    &[(index.saturating_add(1), 1, CellValue::UInt(index))],
                ),
            },
        );
    }
    tail
}

fn vm_hwm_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    line.split_ascii_whitespace().nth(1)?.parse().ok()
}

#[test]
fn snapshot_assembly_produces_exact_l0_envelope() {
    let sealed = assemble(&snapshot(), 42).unwrap_or_else(|_| unreachable!("Seal failed"));
    assert_eq!(sealed.name(), "000000000000002a.lsu");
    let layout = format::decode(sealed.bytes(), 42).unwrap_or_else(|_| unreachable!());
    assert_eq!(layout.header().level(), 0);
    assert_eq!(layout.header().section_count(), 3);
    assert_eq!(layout.header().total_rows(), 4);
    assert_eq!(
        layout
            .sections()
            .iter()
            .map(|entry| (entry.table(), entry.version_no(), entry.row_count()))
            .collect::<Vec<_>>(),
        [
            (TableId::new(1), 1, 2),
            (TableId::new(1), 2, 1),
            (TableId::new(2), 1, 1),
        ]
    );
    assert_eq!(sealed.meta().unit_id(), 42);
    assert_eq!(sealed.meta().file_len(), layout.file_len());
    assert_eq!(sealed.meta().body_crc32(), layout.body_crc32());
}

#[test]
fn first_section_wire_bytes_are_canonical() {
    let sealed = assemble(&snapshot(), 42).unwrap_or_else(|_| unreachable!());
    let layout = format::decode(sealed.bytes(), 42).unwrap_or_else(|_| unreachable!());
    let first = layout.sections()[0];
    let start = usize::try_from(first.section_offset()).unwrap_or(usize::MAX);
    let end = start + usize::try_from(first.section_len()).unwrap_or(0);
    let section = &sealed.bytes()[start..end];

    assert_eq!(&section[..7], &[1, 2, 0, 0, 0, 0x14, 0x14]);
    assert_eq!(read_u32(section, 7), 2);
    let first_column = 11;
    let second_column = first_column + 32;
    assert_eq!(
        &section[first_column..first_column + 16],
        &[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0]
    );
    assert_eq!(read_u32(section, first_column + 16), 0);
    assert_eq!(read_u32(section, first_column + 20), 2);
    assert_eq!(read_u32(section, first_column + 24), 2);
    assert_eq!(read_u32(section, first_column + 28), 0);
    assert_eq!(
        &section[second_column..second_column + 16],
        &[2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 0, 0, 0]
    );
    assert_eq!(read_u32(section, second_column + 16), 1);
    assert_eq!(read_u32(section, second_column + 20), 1);
    assert_eq!(read_u32(section, second_column + 24), 0);
    assert_eq!(read_u32(section, second_column + 28), 1);
    assert_eq!(&section[75..], &[0x07, 0x04, 0x01, 0x01]);
}

#[test]
fn decoded_facts_equal_the_tail_snapshot_bitwise() {
    let snapshot = snapshot();
    let sealed = assemble(&snapshot, 42).unwrap_or_else(|_| unreachable!());
    let mut actual = BTreeMap::new();
    decode::scan(sealed.bytes(), 42, &snapshot, |entry, section| {
        for (row, timestamp) in section.timestamps().iter().enumerate() {
            for column in section.columns() {
                let _ = column.value_type();
                if let Some(value) = column.cells().get(row).copied().flatten() {
                    actual.insert(
                        (
                            entry.table(),
                            entry.version_no(),
                            *timestamp,
                            column.series(),
                            column.field(),
                        ),
                        value,
                    );
                }
            }
        }
        Ok(())
    })
    .unwrap_or_else(|_| unreachable!("valid unit scan failed"));
    assert_eq!(actual, expected_facts(&snapshot));
    assert_eq!(
        actual.get(&(TableId::new(1), 1, 10, SeriesId::new(2), FieldId::new(2),)),
        Some(&CellValue::Null)
    );
    assert!(!actual.contains_key(&(TableId::new(1), 1, 20, SeriesId::new(2), FieldId::new(2),)));
}

#[test]
fn malformed_section_matrix_is_rejected() {
    let snapshot = snapshot();
    let sealed = assemble(&snapshot, 42).unwrap_or_else(|_| unreachable!());
    let layout = format::decode(sealed.bytes(), 42).unwrap_or_else(|_| unreachable!());
    let start = usize::try_from(layout.sections()[0].section_offset()).unwrap_or(usize::MAX);
    let directory = start + 11;
    let second = directory + 32;
    let payload = directory + 64;
    let duplicate_key = [
        1_u64.to_le_bytes().as_slice(),
        1_u16.to_le_bytes().as_slice(),
    ]
    .concat();
    let mutations = [
        (start, vec![2]),
        (start + 1, u32::MAX.to_le_bytes().to_vec()),
        (start + 7, 0_u32.to_le_bytes().to_vec()),
        (directory + 10, vec![2]),
        (directory + 11, vec![2]),
        (directory + 13, vec![1]),
        (directory + 20, u32::MAX.to_le_bytes().to_vec()),
        (directory + 24, 3_u32.to_le_bytes().to_vec()),
        (second, duplicate_key),
        (payload, vec![0]),
    ];
    for (offset, value) in mutations {
        let mut changed = sealed.bytes().to_vec();
        changed[offset..offset + value.len()].copy_from_slice(&value);
        refresh_body_crc(&mut changed);
        let result = decode::scan(&changed, 42, &snapshot, |_, _| Ok(()));
        assert_eq!(
            result.map_err(|error| error.kind()),
            Err(ErrorKind::Corruption)
        );
    }
}

#[test]
fn publication_is_durable_and_no_overwrite() {
    assert_eq!(
        assemble(&TailIndex::new(0, 1), 1).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
    let temporary = TestDir::new("seal-publish");
    let directory = DbDir::initialize(temporary.path()).unwrap_or_else(|_| unreachable!());
    let sealed = assemble(&snapshot(), 1).unwrap_or_else(|_| unreachable!());
    publish(&directory, &sealed).unwrap_or_else(|_| unreachable!("publish failed"));
    assert_eq!(
        fs::read(directory.file(Area::Units, sealed.name())).ok(),
        Some(sealed.bytes().to_vec())
    );
    assert_eq!(
        publish(&directory, &sealed).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
}

#[test]
fn streaming_publication_matches_canonical_bytes() {
    let temporary = TestDir::new("unit-streaming-publication");
    let directory = DbDir::initialize(temporary.path())
        .unwrap_or_else(|_| unreachable!("database directory failed"));
    let tail = snapshot();
    let expected = assemble(&tail, 7).unwrap_or_else(|_| unreachable!("assembly failed"));
    let meta = assemble_and_publish(&directory, &tail, 7)
        .unwrap_or_else(|_| unreachable!("streaming publication failed"));
    let bytes = fs::read(directory.file(Area::Units, expected.name()))
        .unwrap_or_else(|_| unreachable!("published unit read failed"));
    assert_eq!(bytes, expected.bytes());
    assert_eq!(meta, expected.meta());
}

#[test]
fn sparse_section_working_set_is_split_before_encoding() {
    let sealed = assemble(&sparse_split_snapshot(), 1)
        .unwrap_or_else(|_| unreachable!("bounded sparse assembly failed"));
    assert!(sealed.meta().section_count() > 1);
    assert_eq!(sealed.meta().total_rows(), 1_500);
}

#[test]
fn seal_peak_rss_probe() {
    if std::env::var_os("LITESDB_RSS_PROBE").is_none() {
        return;
    }
    let snapshot = measurement_snapshot();
    let tail_hwm = vm_hwm_kib().unwrap_or(u64::MAX);
    let estimated_tail = snapshot.estimated_bytes();
    let temporary = TestDir::new("seal-rss-streaming");
    let directory = DbDir::initialize(temporary.path()).unwrap_or_else(|_| unreachable!());
    let meta = assemble_and_publish(&directory, &snapshot, 1)
        .unwrap_or_else(|_| unreachable!("streaming Seal failed"));
    let seal_hwm = vm_hwm_kib().unwrap_or(u64::MAX);
    eprintln!(
        "seal_rss_probe tail_estimated_bytes={estimated_tail} tail_vm_hwm_kib={tail_hwm} seal_vm_hwm_kib={seal_hwm} unit_bytes={}",
        meta.file_len()
    );
    let bytes = fs::read(directory.file(Area::Units, "0000000000000001.lsu"))
        .unwrap_or_else(|_| unreachable!("published unit read failed"));
    let layout = format::decode(&bytes, 1).unwrap_or_else(|_| unreachable!("unit decode failed"));
    assert_eq!(layout.header().total_rows(), meta.total_rows());
    assert!(
        seal_hwm.saturating_mul(1_024) <= u64::from(MAX_OPERATION_RSS_BYTES),
        "Seal VmHWM exceeded the operation hard limit"
    );
}
