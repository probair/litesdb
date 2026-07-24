// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use litesdb_core::{Bucket, CellValue, Lookup, Slot, SumResult};

pub(crate) fn cell(value: CellValue) -> String {
    match value {
        CellValue::Null => "null".to_owned(),
        CellValue::UInt(value) => format!("u:{value}"),
        CellValue::Sq1(value) => format!("q:{}", value.code()),
        CellValue::F32Bits(value) => format!("f:{:08x}", value.bits()),
        _ => "unsupported".to_owned(),
    }
}

pub(crate) fn lookup(value: Lookup) -> String {
    match value {
        Lookup::Value { value, at_ts } => format!("value\t{at_ts}\t{}", cell(value)),
        Lookup::Null { at_ts } => format!("null\t{at_ts}"),
        Lookup::Missing => "missing".to_owned(),
    }
}

pub(crate) fn slot(value: Slot) -> String {
    match value {
        Slot::Value {
            value,
            source_ts,
            carried,
        } => format!("value\t{source_ts}\t{carried}\t{}", cell(value)),
        Slot::Null { source_ts, carried } => format!("null\t{source_ts}\t{carried}"),
        Slot::Gap => "gap".to_owned(),
    }
}

pub(crate) fn bucket(value: Bucket) -> String {
    let minimum = value.min().map_or_else(|| "null".to_owned(), cell);
    let maximum = value.max().map_or_else(|| "null".to_owned(), cell);
    let sum = match value.sum() {
        SumResult::UInt(sum) => format!("u:{sum}"),
        SumResult::Sq1Fp4(sum) => format!("qfp4:{sum}"),
        SumResult::NotProvided => "na".to_owned(),
    };
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        value.start_ts(),
        value.end_ts(),
        value.sample_count(),
        value.null_count(),
        minimum,
        maximum,
        sum,
        value.is_partial()
    )
}
