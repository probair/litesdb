// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurablePosition {
    pub(crate) seq: u64,
    pub(crate) segment: u64,
    pub(crate) offset: u64,
}

impl DurablePosition {
    pub(crate) const fn new(seq: u64, segment: u64, offset: u64) -> Self {
        Self {
            seq,
            segment,
            offset,
        }
    }
    #[must_use]
    pub const fn seq(self) -> u64 {
        self.seq
    }

    #[must_use]
    pub const fn segment(self) -> u64 {
        self.segment
    }

    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }
}
