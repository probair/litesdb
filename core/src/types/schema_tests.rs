// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{FieldSchema, TableVersion, Validity, VersionSpec};
use crate::{ErrorKind, FieldId, ValueType};

fn field(id: u16, value_type: ValueType) -> FieldSchema {
    FieldSchema::new(FieldId::new(id), value_type)
}

#[test]
fn finite_validity_rejects_zero_and_forever_has_no_duration() {
    let Err(error) = Validity::duration_seconds(0) else {
        unreachable!();
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let Ok(finite) = Validity::duration_seconds(9) else {
        unreachable!("valid validity rejected");
    };
    assert_eq!(finite.duration().map(std::num::NonZero::get), Some(9));
    assert_eq!(Validity::Forever.duration(), None);
}

#[test]
fn field_identifiers_must_be_strictly_increasing() {
    let validity = Validity::Forever;
    for fields in [
        vec![field(2, ValueType::UInt), field(1, ValueType::Sq1)],
        vec![field(1, ValueType::UInt), field(1, ValueType::UInt)],
    ] {
        let Err(error) = VersionSpec::new(validity, fields) else {
            unreachable!();
        };
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    assert!(
        VersionSpec::new(
            validity,
            vec![field(1, ValueType::UInt), field(2, ValueType::Sq1)]
        )
        .is_ok()
    );
}

#[test]
fn successor_preserves_existing_fields_and_activates_once() {
    let Ok(initial_spec) = VersionSpec::new(
        Validity::Forever,
        vec![field(1, ValueType::UInt), field(3, ValueType::F32Bits)],
    ) else {
        unreachable!("valid spec rejected");
    };
    let initial = TableVersion::initial(initial_spec);
    assert_eq!(initial.version_no(), 1);
    assert_eq!(initial.effective_from(), None);

    let Ok(changed_type) = VersionSpec::new(
        Validity::Forever,
        vec![field(1, ValueType::Sq1), field(3, ValueType::F32Bits)],
    ) else {
        unreachable!("ordered spec rejected");
    };
    assert_eq!(
        changed_type
            .validate_successor(&initial)
            .map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );

    let Ok(removed) = VersionSpec::new(Validity::Forever, vec![field(3, ValueType::F32Bits)])
    else {
        unreachable!("ordered spec rejected");
    };
    assert_eq!(
        removed
            .validate_successor(&initial)
            .map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );

    let Ok(next_spec) = VersionSpec::new(
        Validity::duration_seconds(30).unwrap_or(Validity::Forever),
        vec![
            field(1, ValueType::UInt),
            field(2, ValueType::Sq1),
            field(3, ValueType::F32Bits),
        ],
    ) else {
        unreachable!("valid successor rejected");
    };
    let Ok(next) = initial.successor(next_spec) else {
        unreachable!("valid successor rejected");
    };
    assert_eq!(next.version_no(), 2);
    assert_eq!(next.fields().len(), 3);
    let Ok(active) = next.activate(1_000) else {
        unreachable!("inactive version rejected");
    };
    assert_eq!(active.effective_from(), Some(1_000));
    assert_eq!(
        active.activate(1_001).map_err(|error| error.kind()),
        Err(ErrorKind::InvalidArgument)
    );
}
