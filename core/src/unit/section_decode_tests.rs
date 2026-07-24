// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::preflight_decode_memory;
use crate::ErrorKind;

#[test]
fn sparse_row_alignment_is_preflighted_before_column_allocation() {
    assert!(preflight_decode_memory(1, 65_536, 4, 3, 65_536).is_ok());
    assert_eq!(
        preflight_decode_memory(1, 65_536, 4, 4, 65_536)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    );
}
