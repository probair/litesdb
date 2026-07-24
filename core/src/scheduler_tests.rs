// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::time::Duration;

use super::select_closed_window;
use crate::manifest::UnitMeta;

fn unit(id: u64, level: u8, min_ts: i64, max_ts: i64) -> UnitMeta {
    UnitMeta::new(
        id,
        level,
        min_ts,
        max_ts,
        1,
        1,
        100,
        u32::try_from(id).unwrap_or_default(),
    )
    .unwrap_or_else(|_| unreachable!("valid test unit"))
}

#[test]
fn exact_window_end_closes_but_preceding_tick_does_not() {
    let units = [unit(1, 0, 10, 20)];
    assert_eq!(
        select_closed_window(&units, 0, Duration::from_secs(100), Some(99))
            .unwrap_or_else(|_| unreachable!()),
        None
    );
    assert_eq!(
        select_closed_window(&units, 0, Duration::from_secs(100), Some(100))
            .unwrap_or_else(|_| unreachable!()),
        Some((0, 1))
    );
}

#[test]
fn closed_singleton_is_eligible_for_promotion() {
    let units = [unit(1, 0, 0, 5), unit(2, 1, 200, 220)];
    assert_eq!(
        select_closed_window(&units, 0, Duration::from_secs(100), Some(220))
            .unwrap_or_else(|_| unreachable!()),
        Some((0, 1))
    );
}

#[test]
fn late_l0_after_higher_level_catalog_entries_remains_selectable() {
    let units = [unit(1, 2, 0, 10), unit(2, 1, 50, 60), unit(3, 0, 70, 80)];
    assert_eq!(
        select_closed_window(&units, 0, Duration::from_secs(50), Some(150))
            .unwrap_or_else(|_| unreachable!()),
        Some((2, 3))
    );
}

#[test]
fn selector_absorbs_only_adjacent_units_inside_first_window() {
    let units = [unit(1, 0, -90, -80), unit(2, 0, -10, -1), unit(3, 0, 0, 1)];
    assert_eq!(
        select_closed_window(&units, 0, Duration::from_secs(100), Some(100))
            .unwrap_or_else(|_| unreachable!()),
        Some((0, 2))
    );
}
