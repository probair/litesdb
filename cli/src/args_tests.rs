// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Command, parse};
use litesdb_core::{CellValue, CompactLevel, TableId};

fn arguments(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

#[test]
fn write_commands_parse_typed_values() {
    let create = parse(arguments(&[
        "create-table",
        "/tmp/db",
        "forever",
        "1:uint",
        "2:f32",
    ]));
    assert!(matches!(create, Ok(Command::CreateTable { .. })));

    let append = parse(arguments(&[
        "append",
        "/tmp/db",
        "7",
        "10",
        "1:1:u:42",
        "1:2:f:7fc01234",
    ]));
    let Ok(Command::Append {
        table, observation, ..
    }) = append
    else {
        unreachable!("valid append did not parse")
    };
    assert_eq!(table, TableId::new(7));
    assert_eq!(observation.entries()[0].value(), CellValue::UInt(42));
    assert_eq!(
        observation.entries()[1].value(),
        CellValue::F32Bits(litesdb_core::F32Bits::from_bits(0x7fc0_1234))
    );
}

#[test]
fn query_and_admin_commands_parse() {
    let latest = parse(arguments(&["latest", "/tmp/db", "1:2:3", "4:5:6"]));
    let Ok(Command::Latest { keys, .. }) = latest else {
        unreachable!("valid latest did not parse")
    };
    assert_eq!((keys[0].table().get(), keys[1].table().get()), (1, 4));

    let compact = parse(arguments(&["compact", "/tmp/db", "l1"]));
    assert!(matches!(
        compact,
        Ok(Command::Compact {
            level: CompactLevel::Level1To2,
            ..
        })
    ));
    assert!(matches!(
        parse(arguments(&["maintain", "/tmp/db"])),
        Ok(Command::Maintain { .. })
    ));
}

#[test]
fn malformed_inputs_are_rejected() {
    for values in [
        vec!["seal"],
        vec!["retain", "/tmp/db", "x"],
        vec!["scan", "/tmp/db", "1:2", "0", "1"],
        vec!["scan", "/tmp/db", "1:2:3", "2", "1"],
        vec!["append", "/tmp/db", "1", "1", "1:1:f:xyz"],
        vec!["compact", "/tmp/db", "l2"],
    ] {
        assert!(parse(arguments(&values)).is_err());
    }
}
