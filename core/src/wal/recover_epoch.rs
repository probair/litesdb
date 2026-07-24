// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SegmentFile, open_segment};
use crate::{Error, Result, fsutil::DbDir};

pub(super) fn validate_prefix(
    directory: &DbDir,
    segments: &[SegmentFile],
    expected_shard_id: u64,
    maximum_writer_epoch: u64,
) -> Result<Option<u64>> {
    let mut previous = None;
    for segment in segments {
        let Some((_, _, _, header)) = open_segment(
            directory,
            segment,
            true,
            false,
            expected_shard_id,
            maximum_writer_epoch,
        )?
        else {
            return Err(Error::corruption(
                "WAL recovery",
                "historical segment header is absent",
            ));
        };
        ensure_epoch_order(previous, header.writer_epoch())?;
        previous = Some(header.writer_epoch());
    }
    Ok(previous)
}

pub(super) fn ensure_epoch_order(previous: Option<u64>, current: u64) -> Result<()> {
    if previous.is_some_and(|epoch| current < epoch) {
        Err(Error::corruption(
            "WAL recovery",
            "writer epoch decreases across segment order",
        ))
    } else {
        Ok(())
    }
}
