// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SharedDbId, SharedWal, State, format};
use crate::{
    Error, Observation, Result, TableId,
    wal::{RecordBody, record},
};
impl SharedWal {
    pub(super) fn append_batch_raw(
        &self,
        id: SharedDbId,
        first: u64,
        raws: &[Vec<u8>],
    ) -> Result<()> {
        #[cfg(feature = "bench-metrics")]
        let _profile = crate::bench_metrics::Span::new(crate::bench_metrics::Stage::WalAppend);
        let mut state = self.lock()?;
        self.preflight_batch(&mut state, id, first, raws)?;
        let mut seq = first;
        for raw in raws {
            if self.append_locked(&mut state, id, seq, raw).is_err() {
                state.poison = true;
                return Err(Error::Poisoned);
            }
            seq = format::add(seq, 1)?;
        }
        Ok(())
    }
    fn preflight_batch(
        &self,
        state: &mut State,
        id: SharedDbId,
        first: u64,
        raws: &[Vec<u8>],
    ) -> Result<()> {
        let member = state
            .members
            .get(&id)
            .ok_or_else(|| format::invalid("batch member absent"))?;
        if first != format::add(member.seq, 1)? {
            return Err(format::invalid("batch local sequence"));
        }
        format::add(first, raws.len() as u64)?;
        format::add(state.lsn, raws.len() as u64)?;
        let mut offset = state.offset;
        let mut growth = 0_u64;
        let mut index_growth = 0_u64;
        let mut represented = state
            .segments
            .get(&state.segment)
            .is_some_and(|segment| segment.members.contains_key(&id));
        let mut raw_growth = 0_u64;
        for raw in raws {
            let length = format::add(raw.len() as u64, format::FRAME_HEADER as u64)?;
            if length > u64::from(self.inner.options.buffer_bytes) {
                return Err(Error::limit(
                    "shared_wal_buffer_bytes",
                    length,
                    self.inner.options.buffer_bytes.into(),
                ));
            }
            if format::add(length, format::SEGMENT_HEADER as u64)?
                > u64::from(self.inner.options.segment_bytes)
            {
                return Err(Error::limit(
                    "shared_wal_record_bytes",
                    length,
                    self.inner.options.segment_bytes.into(),
                ));
            }
            if format::add(offset, length)? > u64::from(self.inner.options.segment_bytes) {
                growth = format::add(growth, format::SEGMENT_HEADER as u64)?;
                index_growth = format::add(index_growth, 128)?;
                offset = format::SEGMENT_HEADER as u64;
                represented = false;
            }
            if !represented {
                index_growth = format::add(index_growth, 96)?;
                represented = true;
            }
            offset = format::add(offset, length)?;
            growth = format::add(growth, length)?;
            raw_growth = format::add(raw_growth, length)?;
        }
        #[cfg(feature = "archive")]
        if let Some(protection) = &member.archive {
            let total = format::add(protection.bytes(), raw_growth)?;
            if total > protection.options.max_bytes {
                return Err(Error::limit(
                    "archive_bytes",
                    total,
                    protection.options.max_bytes,
                ));
            }
        }
        #[cfg(not(feature = "archive"))]
        let _ = raw_growth;
        if format::add(state.storage, growth)? > self.inner.options.max_bytes {
            self.gc_locked(state)?;
        }
        let total = format::add(state.storage, growth)?;
        if total > self.inner.options.max_bytes {
            return Err(Error::limit(
                "shared_wal_storage_bytes",
                total,
                self.inner.options.max_bytes,
            ));
        }
        let total = format::add(state.charged, index_growth)?;
        if total > self.inner.options.index_bytes {
            return Err(Error::limit(
                "shared_wal_index_bytes",
                total,
                self.inner.options.index_bytes,
            ));
        }
        Ok(())
    }
}
impl super::SharedMember {
    pub(crate) fn append_batch(
        &mut self,
        table: TableId,
        observations: &[Observation],
    ) -> Result<u64> {
        let first = format::add(self.seq, 1)?;
        let mut seq = first;
        let mut bytes = 0_u64;
        let mut raws = Vec::new();
        for observation in observations {
            let body = RecordBody::AppendObservation {
                table,
                observation: observation.clone(),
            };
            let length = u64::from(record::encoded_len(&body)?);
            bytes = format::add(
                bytes,
                format::add(length, std::mem::size_of::<Vec<u8>>() as u64)?,
            )?;
            if bytes > u64::from(crate::limits::MAX_OPERATION_MEMORY_BYTES) {
                return Err(Error::limit(
                    "operation_memory_bytes",
                    bytes,
                    crate::limits::MAX_OPERATION_MEMORY_BYTES.into(),
                ));
            }
            raws.push(record::encode(seq, &body)?);
            seq = format::add(seq, 1)?;
        }
        self.owner.append_batch_raw(self.id, first, &raws)?;
        self.seq = seq.saturating_sub(1);
        Ok(self.seq)
    }
}
