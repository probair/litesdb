// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{RECORD_HEADER_BYTES, RecordBody, decode, encode, framed_len, record_crc};
use crate::{
    CellValue, ErrorKind, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId,
    Sq1, TableId, Validity, ValueType, VersionSpec,
};

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::duration_seconds(9).unwrap_or(Validity::Forever),
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::F32Bits),
        ],
    )
    .unwrap_or_else(|_| unreachable!("valid schema fixture rejected"))
}

fn field_type(_table: TableId, field: FieldId) -> Option<ValueType> {
    match field.get() {
        1 => Some(ValueType::UInt),
        2 => Some(ValueType::F32Bits),
        3 => Some(ValueType::Sq1),
        _ => None,
    }
}

fn refresh_crc(bytes: &mut [u8], seq: u64) {
    let payload_len =
        u32::try_from(bytes.len().saturating_sub(RECORD_HEADER_BYTES)).unwrap_or(u32::MAX);
    let crc = record_crc(payload_len, seq, &bytes[RECORD_HEADER_BYTES..]);
    bytes[12..16].copy_from_slice(&crc.to_le_bytes());
}

fn round_trip(body: &RecordBody, seq: u64) {
    let Ok(bytes) = encode(seq, body) else {
        unreachable!("valid record fixture rejected");
    };
    assert_eq!(framed_len(&bytes).ok(), Some(bytes.len()));
    let Ok(decoded) = decode(&bytes, seq, field_type) else {
        unreachable!("encoded record failed to decode");
    };
    assert_eq!(decoded.seq(), seq);
    assert_eq!(decoded.body(), body);
    assert_eq!(decoded.into_body(), *body);
}

#[test]
fn observation_frame_is_exact_and_round_trips() {
    let sq1 = Sq1::new(254).unwrap_or_else(|| unreachable!());
    let observation = Observation::new(
        -2,
        vec![
            ObservationEntry::new(SeriesId::new(4), FieldId::new(1), CellValue::UInt(5)),
            ObservationEntry::new(SeriesId::new(4), FieldId::new(2), CellValue::Null),
            ObservationEntry::new(
                SeriesId::new(5),
                FieldId::new(2),
                CellValue::F32Bits(F32Bits::from_bits(0x8000_0000)),
            ),
            ObservationEntry::new(SeriesId::new(6), FieldId::new(3), CellValue::Sq1(sq1)),
        ],
    )
    .unwrap_or_else(|_| unreachable!());
    let body = RecordBody::AppendObservation {
        table: TableId::new(3),
        observation,
    };
    let bytes = encode(7, &body).unwrap_or_else(|_| unreachable!());
    let payload_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0; 4]));
    assert_eq!(payload_len, 74);
    assert_eq!(&bytes[4..12], &7_u64.to_le_bytes());
    assert_eq!(bytes[RECORD_HEADER_BYTES], 1);
    assert_eq!(
        u32::from_le_bytes(bytes[12..16].try_into().unwrap_or([0; 4])),
        record_crc(payload_len, 7, &bytes[RECORD_HEADER_BYTES..])
    );
    round_trip(&body, 7);
}

#[test]
fn metadata_variants_round_trip() {
    let bodies = [
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(),
        },
        RecordBody::NewTableVersion {
            table: TableId::new(1),
            spec: VersionSpec::new(Validity::Forever, vec![]).unwrap_or_else(|_| unreachable!()),
        },
        RecordBody::DropTable {
            table: TableId::new(2),
        },
        RecordBody::RetireSeries {
            table: TableId::new(3),
            series: SeriesId::new(4),
            retire_ts: -5,
        },
        RecordBody::RetireField {
            table: TableId::new(6),
            field: FieldId::new(7),
            retire_ts: 8,
        },
    ];
    for (index, body) in bodies.iter().enumerate() {
        round_trip(body, u64::try_from(index).unwrap_or(u64::MAX));
    }
}

#[test]
fn malformed_frames_are_rejected() {
    let body = RecordBody::DropTable {
        table: TableId::new(9),
    };
    let bytes = encode(10, &body).unwrap_or_else(|_| unreachable!());
    let mut zero_len = bytes.clone();
    zero_len[0..4].copy_from_slice(&0_u32.to_le_bytes());
    let mut over_len = bytes.clone();
    over_len[0..4].copy_from_slice(&65_537_u32.to_le_bytes());
    let mut bad_crc = bytes.clone();
    bad_crc[12] ^= 1;
    let mut bad_tag = bytes.clone();
    bad_tag[RECORD_HEADER_BYTES] = 7;
    let cases = [
        decode(&bytes[..15], 10, field_type),
        decode(&bytes[..bytes.len().saturating_sub(1)], 10, field_type),
        decode(&[bytes.as_slice(), &[0]].concat(), 10, field_type),
        decode(&zero_len, 10, field_type),
        decode(&over_len, 10, field_type),
        decode(&bytes, 11, field_type),
        decode(&bad_crc, 10, field_type),
        decode(&bad_tag, 10, field_type),
    ];
    for result in cases {
        let Err(error) = result else {
            unreachable!("malformed WAL frame accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn malformed_observation_payloads_are_rejected() {
    let observation = Observation::new(
        1,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(3),
            CellValue::Sq1(Sq1::new(1).unwrap_or_else(|| unreachable!())),
        )],
    )
    .unwrap_or_else(|_| unreachable!());
    let body = RecordBody::AppendObservation {
        table: TableId::new(1),
        observation,
    };
    let bytes = encode(1, &body).unwrap_or_else(|_| unreachable!());

    let mut unknown_field = bytes.clone();
    unknown_field[RECORD_HEADER_BYTES + 25] = 9;
    refresh_crc(&mut unknown_field, 1);
    let mut null_code = bytes;
    let last = null_code.len().saturating_sub(1);
    null_code[last] = 255;
    refresh_crc(&mut null_code, 1);
    for candidate in [unknown_field, null_code] {
        let Err(error) = decode(&candidate, 1, field_type) else {
            unreachable!("invalid complete observation accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn payload_limit_is_checked_before_frame_allocation() {
    let mut entries = Vec::new();
    entries.push(ObservationEntry::new(
        SeriesId::new(0),
        FieldId::new(1),
        CellValue::UInt(0),
    ));
    entries.push(ObservationEntry::new(
        SeriesId::new(0),
        FieldId::new(2),
        CellValue::F32Bits(F32Bits::from_bits(0)),
    ));
    for field in [3, 4] {
        entries.push(ObservationEntry::new(
            SeriesId::new(0),
            FieldId::new(field),
            CellValue::Sq1(Sq1::new(0).unwrap_or_else(|| unreachable!())),
        ));
    }
    for series in 1..=5_951 {
        entries.push(ObservationEntry::new(
            SeriesId::new(series),
            FieldId::new(1),
            CellValue::Null,
        ));
    }
    let observation = Observation::new(1, entries).unwrap_or_else(|_| unreachable!());
    let body = RecordBody::AppendObservation {
        table: TableId::new(1),
        observation,
    };
    let bytes = encode(1, &body).unwrap_or_else(|_| unreachable!());
    assert_eq!(bytes.len(), RECORD_HEADER_BYTES + 65_536);

    let RecordBody::AppendObservation { table, observation } = body else {
        unreachable!();
    };
    let mut over = observation.entries().to_vec();
    over.push(ObservationEntry::new(
        SeriesId::new(5_952),
        FieldId::new(1),
        CellValue::Null,
    ));
    let over = RecordBody::AppendObservation {
        table,
        observation: Observation::new(1, over).unwrap_or_else(|_| unreachable!()),
    };
    let Err(error) = encode(2, &over) else {
        unreachable!("over-limit WAL payload accepted");
    };
    assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
}
