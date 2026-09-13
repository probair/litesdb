// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SharedWal, State, format};
use crate::{Result, fsutil::Area};
use std::fs;
impl SharedWal {
    pub(super) fn gc_locked(&self, state: &mut State) -> Result<()> {
        #[cfg(feature = "bench-metrics")]
        let _profile = crate::bench_metrics::Span::new(crate::bench_metrics::Stage::WalReclaim);
        loop {
            let Some((&number, segment)) = state.segments.first_key_value() else {
                return Ok(());
            };
            if number == state.segment
                || segment.members.iter().any(|(id, seq)| {
                    state.members.get(id).is_none_or(|member| {
                        !member.retired
                            && (member.checkpoint < *seq || member.archive_limit() < *seq)
                    })
                })
            {
                return Ok(());
            }
            let length = segment.length;
            let index = 128_u64.saturating_add((segment.members.len() as u64).saturating_mul(96));
            let path = self
                .inner
                .directory
                .file(Area::Wal, &format::segment_name(number));
            if let Err(error) = fs::remove_file(&path)
                .map_err(crate::Error::from)
                .and_then(|()| self.inner.directory.sync(Area::Wal))
            {
                state.poison = true;
                return Err(error);
            }
            state.segments.remove(&number);
            state.storage = state.storage.saturating_sub(length);
            state.charged = state.charged.saturating_sub(index);
            let evidence = self
                .inner
                .directory
                .file(Area::Root, &super::recover::boundary_name(number));
            if evidence.try_exists()? {
                fs::remove_file(evidence)?;
                self.inner.directory.sync(Area::Root)?;
            }
        }
    }
}
