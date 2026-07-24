// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{FieldId, SeriesId, StreamKey, TableId};

#[test]
fn identifiers_preserve_the_full_format_width() {
    assert_eq!(TableId::new(u32::MAX).get(), u32::MAX);
    assert_eq!(SeriesId::new(u64::MAX).get(), u64::MAX);
    assert_eq!(FieldId::new(u16::MAX).get(), u16::MAX);
}

#[test]
fn stream_keys_order_lexicographically_by_table_series_field() {
    let keys = [
        StreamKey::new(TableId::new(1), SeriesId::new(2), FieldId::new(0)),
        StreamKey::new(TableId::new(1), SeriesId::new(1), FieldId::new(1)),
        StreamKey::new(TableId::new(2), SeriesId::new(0), FieldId::new(0)),
        StreamKey::new(TableId::new(1), SeriesId::new(1), FieldId::new(0)),
    ];
    let mut sorted = keys;
    sorted.sort_unstable();

    assert_eq!(sorted, [keys[3], keys[1], keys[0], keys[2]]);
    assert_eq!(sorted[0].table(), TableId::new(1));
    assert_eq!(sorted[0].series(), SeriesId::new(1));
    assert_eq!(sorted[0].field(), FieldId::new(0));
}
