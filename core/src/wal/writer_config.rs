// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    Error, Result,
    limits::{MAX_WAL_BYTES, MAX_WAL_RECORD_PAYLOAD, MAX_WAL_SEGMENT_BYTES},
    wal::{record, segment::SEGMENT_HEADER_BYTES},
};

const MIN_RECORD_FRAME_BYTES: u32 = 21;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriterConfig {
    pub(super) segment_limit: u32,
    pub(super) wal_limit: u32,
    pub(super) seal_threshold: u32,
}

impl WriterConfig {
    pub(crate) fn new(segment_bytes: u32, wal_max_bytes: u32, seal_bytes: u32) -> Result<Self> {
        let maximum_frame =
            MAX_WAL_RECORD_PAYLOAD
                .checked_add(u32::try_from(record::RECORD_HEADER_BYTES).map_err(|_| {
                    Error::invalid("segment_bytes", "record header does not fit u32")
                })?)
                .ok_or_else(|| Error::invalid("segment_bytes", "record frame size overflow"))?;
        let minimum_segment =
            maximum_frame
                .checked_add(u32::try_from(SEGMENT_HEADER_BYTES).map_err(|_| {
                    Error::invalid("segment_bytes", "segment header does not fit u32")
                })?)
                .ok_or_else(|| Error::invalid("segment_bytes", "minimum segment size overflow"))?;
        if segment_bytes < minimum_segment {
            return Err(Error::invalid(
                "segment_bytes",
                "segment must contain a maximum-size record",
            ));
        }
        if segment_bytes > MAX_WAL_SEGMENT_BYTES {
            return Err(Error::limit(
                "wal_segment_bytes",
                u64::from(segment_bytes),
                u64::from(MAX_WAL_SEGMENT_BYTES),
            ));
        }
        let minimum_wal = u32::try_from(SEGMENT_HEADER_BYTES)
            .ok()
            .and_then(|header| header.checked_mul(2))
            .and_then(|headers| headers.checked_add(MIN_RECORD_FRAME_BYTES))
            .ok_or_else(|| Error::invalid("wal_max_bytes", "minimum WAL size overflow"))?;
        if wal_max_bytes < minimum_wal || seal_bytes == 0 || seal_bytes > wal_max_bytes {
            return Err(Error::invalid(
                "wal policy",
                "watermarks are empty, inverted, or cannot hold takeover headers and one record",
            ));
        }
        if wal_max_bytes > MAX_WAL_BYTES {
            return Err(Error::limit(
                "wal_bytes",
                u64::from(wal_max_bytes),
                u64::from(MAX_WAL_BYTES),
            ));
        }
        Ok(Self {
            segment_limit: segment_bytes,
            wal_limit: wal_max_bytes,
            seal_threshold: seal_bytes,
        })
    }
}
