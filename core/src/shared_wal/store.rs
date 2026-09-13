// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    Inner, Segment, SharedDbId, SharedWal, SharedWalOptions, State,
    format::{self, Pointer, SEGMENT_HEADER},
};
#[cfg(feature = "bench-metrics")]
use crate::bench_metrics::{self, Counter, Span, Stage};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir, DbLock, publish_atomically},
};

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex},
};

pub(super) fn open(root: &Path, options: SharedWalOptions) -> Result<SharedWal> {
    fs::create_dir_all(root)?;
    let root = root.canonicalize()?;
    let lock = DbLock::acquire(&root)?;
    let directory = DbDir::initialize(&root)?;
    directory.clear_temporary()?;
    let path = directory.file(Area::Root, "OWNER");
    let identity = if path.try_exists()? {
        if fs::metadata(&path)?.len() != 28 {
            return Err(format::invalid("owner identity size"));
        }
        let bytes = fs::read(&path)?;
        let body = format::checked(&bytes, b"LSSO\x02\0\0\0")?;
        if body.len() != 24 {
            return Err(format::invalid("owner identity size"));
        }
        format::array(body, 8)?
    } else {
        if fs::read_dir(directory.path(Area::Wal))?.next().is_some() {
            return Err(format::invalid("WAL without owner identity"));
        }
        let mut identity = [0; 16];
        File::open("/dev/urandom")?.read_exact(&mut identity)?;
        let mut bytes = b"LSSO\x02\0\0\0".to_vec();
        bytes.extend_from_slice(&identity);
        publish_atomically(&directory, Area::Root, "OWNER", &format::checksum(bytes))?;
        identity
    };
    let (members, charged) = super::registry::load(&directory, identity, options)?;
    let state = super::recover::scan(&directory, identity, options, members, charged)?;
    Ok(SharedWal {
        inner: Arc::new(Inner {
            directory,
            identity,
            options,
            _lock: lock,
            state: Mutex::new(state),
        }),
    })
}
impl SharedWal {
    pub(crate) fn append_raw(&self, id: SharedDbId, seq: u64, raw: &[u8]) -> Result<()> {
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::WalAppend);
        let mut state = self.lock()?;
        self.append_locked(&mut state, id, seq, raw)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one locked append keeps all preflight and publication ordering visible"
    )]
    pub(super) fn append_locked(
        &self,
        state: &mut State,
        id: SharedDbId,
        seq: u64,
        raw: &[u8],
    ) -> Result<()> {
        let member = state
            .members
            .get(&id)
            .ok_or_else(|| format::invalid("unregistered member"))?;
        if seq != format::add(member.seq, 1)? {
            return Err(format::invalid("member sequence discontinuity"));
        }
        #[cfg(feature = "archive")]
        let archive_next = member
            .archive
            .as_ref()
            .map(|protection| protection.next(raw))
            .transpose()?;
        let previous = member.latest;
        let lsn = format::add(state.lsn, 1)?;
        #[cfg(feature = "bench-metrics")]
        let encode_profile = Span::new(Stage::WalEncode);
        let bytes = format::encode(id, lsn, seq, previous, raw)?;
        #[cfg(feature = "bench-metrics")]
        drop(encode_profile);
        let length = bytes.len() as u64;
        if length > u64::from(self.inner.options.buffer_bytes) {
            return Err(Error::limit(
                "shared_wal_buffer_bytes",
                length,
                self.inner.options.buffer_bytes.into(),
            ));
        }
        #[cfg(feature = "bench-metrics")]
        let capacity_profile = Span::new(Stage::WalCapacity);
        let capacity = self.inner.options.max_bytes;
        let segment_limit = u64::from(self.inner.options.segment_bytes);
        if format::add(SEGMENT_HEADER as u64, length)? > segment_limit {
            return Err(Error::limit(
                "shared_wal_record_bytes",
                length,
                segment_limit.saturating_sub(SEGMENT_HEADER as u64),
            ));
        }
        let rolls = format::add(state.offset, length)? > segment_limit;
        let growth = format::add(length, if rolls { SEGMENT_HEADER as u64 } else { 0 })?;
        if format::add(state.storage, growth)? > capacity {
            self.gc_locked(state)?;
            if format::add(state.storage, growth)? > capacity {
                return Err(Error::limit(
                    "shared_wal_storage_bytes",
                    format::add(state.storage, growth)?,
                    capacity,
                ));
            }
        }
        let new_member = rolls
            || state
                .segments
                .get(&state.segment)
                .is_none_or(|segment| !segment.members.contains_key(&id));
        let index_growth =
            (if rolls { 128_u64 } else { 0 }).saturating_add(if new_member { 96 } else { 0 });
        self.charge(state, index_growth)?;
        if rolls && let Err(error) = self.roll_locked(state) {
            state.poison = true;
            return Err(error);
        }
        #[cfg(feature = "bench-metrics")]
        drop(capacity_profile);
        let pointer = Pointer {
            segment: state.segment,
            offset: state.offset,
        };
        if state.buffer.len().saturating_add(bytes.len()) > self.inner.options.buffer_bytes as usize
        {
            Self::flush_locked(state)?;
        }
        let needed = state.buffer.len().saturating_add(bytes.len());
        if state.buffer.capacity() < needed {
            let additional = needed.saturating_sub(state.buffer.len());
            state.buffer.reserve_exact(additional);
        }
        state.buffer.extend_from_slice(&bytes);
        state.offset = format::add(state.offset, length)?;
        state.storage = format::add(state.storage, length)?;
        state.lsn = lsn;
        let segment_no = state.segment;
        let segment = state
            .segments
            .get_mut(&segment_no)
            .ok_or_else(|| format::invalid("active segment missing"))?;
        segment.length = format::add(segment.length, length)?;
        segment.members.insert(id, seq);
        let durable = state.durable;
        let member = state
            .members
            .get_mut(&id)
            .ok_or_else(|| format::invalid("member vanished"))?;
        if member.lsn <= durable {
            member.durable_seq = member.seq;
            member.durable_pointer = member.latest;
            #[cfg(feature = "archive")]
            if let Some(protection) = &mut member.archive {
                protection.durable = protection.latest;
            }
        }
        #[cfg(feature = "archive")]
        if let (Some(protection), Some(next)) = (&mut member.archive, archive_next) {
            protection.latest = next;
        }
        member.latest = pointer;
        member.seq = seq;
        member.lsn = lsn;
        member.bytes = format::add(member.bytes, length)?;
        Ok(())
    }
    pub(super) fn flush_locked(state: &mut State) -> Result<()> {
        if state.buffer.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        if state.fail_write {
            let end = state.buffer.len() / 2;
            let _ = state.file.write_all(&state.buffer[..end]);
            state.poison = true;
            return Err(Error::Poisoned);
        }
        #[cfg(feature = "bench-metrics")]
        let profile = Span::new(Stage::RecordWrite);
        let result = state.file.write_all(&state.buffer);
        #[cfg(feature = "bench-metrics")]
        drop(profile);
        if result.is_err() {
            #[cfg(feature = "bench-metrics")]
            bench_metrics::count(Counter::RecordWriteErrors, 1);
            state.poison = true;
            return Err(Error::Poisoned);
        }
        #[cfg(feature = "bench-metrics")]
        bench_metrics::count(Counter::RecordBytes, state.buffer.len() as u64);
        state.writes = state.writes.saturating_add(1);
        state.buffer.clear();
        Ok(())
    }
    #[allow(
        clippy::unused_self,
        reason = "owner sync entry retains ownership scope for archive coordination"
    )]
    pub(super) fn sync_locked(&self, state: &mut State) -> Result<()> {
        if state.durable == state.lsn {
            return Ok(());
        }
        Self::flush_locked(state)?;
        #[cfg(test)]
        if state.fail_sync {
            state.poison = true;
            return Err(Error::Poisoned);
        }
        #[cfg(feature = "bench-metrics")]
        let profile = Span::new(Stage::DataSync);
        let result = state.file.sync_data();
        #[cfg(feature = "bench-metrics")]
        drop(profile);
        if result.is_err() {
            state.poison = true;
            return Err(Error::Poisoned);
        }
        state.syncs = state.syncs.saturating_add(1);
        state.durable = state.lsn;
        Ok(())
    }
    fn roll_locked(&self, state: &mut State) -> Result<()> {
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::Rotation);
        self.sync_locked(state)?;
        let segment = format::add(state.segment, 1)?;
        let first = format::add(state.lsn, 1)?;
        let file = create_segment(&self.inner.directory, self.inner.identity, segment, first)?;
        state.file = file;
        state.segment = segment;
        state.offset = SEGMENT_HEADER as u64;
        state.storage = format::add(state.storage, SEGMENT_HEADER as u64)?;
        state.segments.insert(
            segment,
            Segment {
                length: SEGMENT_HEADER as u64,
                members: BTreeMap::new(),
            },
        );
        Ok(())
    }
}
pub(super) fn create_segment(
    directory: &DbDir,
    identity: [u8; 16],
    segment: u64,
    first: u64,
) -> Result<File> {
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create_new(true)
        .open(directory.file(Area::Wal, &format::segment_name(segment)))?;
    #[cfg(feature = "bench-metrics")]
    let profile = Span::new(Stage::SegmentWrite);
    file.write_all(&format::segment_header(identity, segment, first))?;
    #[cfg(feature = "bench-metrics")]
    drop(profile);
    #[cfg(feature = "bench-metrics")]
    let profile = Span::new(Stage::SegmentSync);
    file.sync_all()?;
    #[cfg(feature = "bench-metrics")]
    drop(profile);
    #[cfg(feature = "bench-metrics")]
    let _profile = Span::new(Stage::WalDirectorySync);
    directory.sync(Area::Wal)?;
    Ok(file)
}
