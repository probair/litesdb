// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::fs::OpenOptions;

use super::{SEGMENT_HEADER_BYTES, SegmentHeader, SystemWalIo, WalWriter, segment_name};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir, publish_atomically},
};

impl WalWriter<SystemWalIo> {
    pub(crate) fn recovery_seal_required(&self, epoch: u64, tail_nonempty: bool) -> Result<bool> {
        self.validate_epoch(epoch)?;
        if self.wal_bytes > u64::from(self.config.wal_limit) {
            if !tail_nonempty {
                return Err(Error::corruption(
                    "WAL takeover",
                    "empty recovery tail exceeds the runtime WAL policy",
                ));
            }
            return Ok(true);
        }
        if epoch == self.segment_epoch || !tail_nonempty {
            return Ok(false);
        }
        Ok(
            self.wal_bytes >= u64::from(self.config.seal_threshold)
                || !self.has_epoch_headroom()?,
        )
    }

    pub(crate) fn ensure_epoch_adoptable(&self, epoch: u64) -> Result<()> {
        self.ensure_healthy()?;
        self.validate_epoch(epoch)?;
        if epoch != self.segment_epoch && !self.has_epoch_headroom()? {
            let header = header_len()?;
            let required = self.wal_bytes.checked_add(header).ok_or_else(|| {
                Error::limit("wal_bytes", u64::MAX, u64::from(self.config.wal_limit))
            })?;
            return Err(Error::limit(
                "wal_bytes",
                required,
                u64::from(self.config.wal_limit),
            ));
        }
        Ok(())
    }

    fn has_epoch_headroom(&self) -> Result<bool> {
        let header = header_len()?;
        if self.offset == header {
            return Ok(true);
        }
        Ok(self
            .wal_bytes
            .checked_add(header)
            .is_some_and(|required| required <= u64::from(self.config.wal_limit)))
    }

    fn validate_epoch(&self, epoch: u64) -> Result<()> {
        if epoch < self.segment_epoch || epoch < self.writer_epoch {
            return Err(Error::corruption("WAL takeover", "writer epoch regressed"));
        }
        Ok(())
    }

    pub(crate) fn adopt_epoch(&mut self, directory: &DbDir, epoch: u64) -> Result<()> {
        self.ensure_epoch_adoptable(epoch)?;
        self.writer_epoch = epoch;
        if self.segment_epoch == epoch {
            return Ok(());
        }
        if self.offset == header_len()? {
            self.rebuild_empty_segment(directory, epoch)
        } else {
            self.roll_segment(self.next_seq)
        }
    }

    fn rebuild_empty_segment(&mut self, directory: &DbDir, epoch: u64) -> Result<()> {
        let name = segment_name(self.segment_first_seq);
        let header = SegmentHeader::new(self.segment_first_seq, self.shard_id, epoch).encode();
        publish_atomically(directory, Area::Wal, &name, &header)?;
        let canonical = self.wal_directory.join(name);
        let replacement = OpenOptions::new().read(true).append(true).open(canonical)?;
        self.file = replacement;
        self.segment_epoch = epoch;
        self.unsynced_bytes = 0;
        self.durable = super::DurablePosition {
            seq: self.last_seq,
            segment: self.segment_first_seq,
            offset: self.offset,
        };
        Ok(())
    }
}

fn header_len() -> Result<u64> {
    u64::try_from(SEGMENT_HEADER_BYTES)
        .map_err(|_| Error::corruption("WAL takeover", "header length does not fit u64"))
}
