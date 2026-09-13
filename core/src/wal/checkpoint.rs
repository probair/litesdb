// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{Error, Result};
const SEGMENT_HEADER_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    segment_first_seq: u64,
    offset: u64,
    next_seq: u64,
}

impl Checkpoint {
    pub(crate) fn new(segment_first_seq: u64, offset: u64, next_seq: u64) -> Result<Self> {
        let header = u64::try_from(SEGMENT_HEADER_BYTES)
            .map_err(|_| Error::corruption("WAL checkpoint", "header length does not fit u64"))?;
        if next_seq == 0 || segment_first_seq > next_seq || offset < header {
            return Err(Error::corruption(
                "WAL checkpoint",
                "sequence or offset relationship is invalid",
            ));
        }
        Ok(Self {
            segment_first_seq,
            offset,
            next_seq,
        })
    }

    pub(crate) const fn segment_first_seq(self) -> u64 {
        self.segment_first_seq
    }

    pub(crate) const fn offset(self) -> u64 {
        self.offset
    }

    pub(crate) const fn next_seq(self) -> u64 {
        self.next_seq
    }
}
