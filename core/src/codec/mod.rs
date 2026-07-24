// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

mod diff2;
mod fixed;
mod nvr;
pub(crate) mod presence;
pub(crate) mod selector;
pub(crate) mod timestamp;

#[allow(dead_code, reason = "constructed by unit sealing in M4")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FactShape {
    Absent,
    Null,
    Value,
}

impl FactShape {
    const fn is_fact(self) -> bool {
        !matches!(self, Self::Absent)
    }
}
