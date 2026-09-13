// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#[cfg(feature = "archive")]
mod archive;
mod batch;
mod format;
mod gc;
mod member;
mod recover;
mod registry;
mod retire;
mod store;
mod types;

pub(crate) use member::SharedMember;
pub(crate) use registry::{read_binding, verify_binding};
pub use types::{SharedDbId, SharedDurablePosition, SharedWalOptions, SharedWalStatus};

use crate::{
    Db, Error, OpenOptions, Result,
    fsutil::{DbDir, DbLock},
};
use format::Pointer;
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone)]
pub struct SharedWal {
    pub(crate) inner: Arc<Inner>,
}
pub(crate) struct Inner {
    directory: DbDir,
    identity: [u8; 16],
    options: SharedWalOptions,
    _lock: DbLock,
    state: Mutex<State>,
}
pub(crate) struct State {
    file: File,
    segment: u64,
    offset: u64,
    lsn: u64,
    durable: u64,
    buffer: Vec<u8>,
    storage: u64,
    charged: u64,
    scratch_bytes: u64,
    members: BTreeMap<SharedDbId, Member>,
    paths: BTreeMap<PathBuf, SharedDbId>,
    segments: BTreeMap<u64, Segment>,
    poison: bool,
    writes: u64,
    syncs: u64,
    #[cfg(test)]
    fail_write: bool,
    #[cfg(test)]
    fail_sync: bool,
    #[cfg(test)]
    registration_fault: Option<(SharedDbId, bool, crate::fsutil::PublishStep)>,
}
pub(crate) struct Member {
    root: PathBuf,
    initialized: bool,
    retired: bool,
    checkpoint: u64,
    latest: Pointer,
    seq: u64,
    lsn: u64,
    bytes: u64,
    durable_seq: u64,
    durable_pointer: Pointer,
    #[cfg(feature = "archive")]
    archive: Option<Box<archive::Protection>>,
}
pub(super) fn member_charge(root: &Path) -> u64 {
    let entries = std::mem::size_of::<Member>()
        .saturating_add(std::mem::size_of::<SharedDbId>())
        .saturating_add(std::mem::size_of::<PathBuf>())
        .saturating_add(std::mem::size_of::<SharedDbId>());
    (entries as u64)
        .saturating_mul(2)
        .saturating_add(256)
        .saturating_add((root.as_os_str().len() as u64).saturating_mul(2))
}
impl Member {
    #[allow(
        clippy::unused_self,
        reason = "the member archive state is compiled out of standalone builds"
    )]
    fn archive_limit(&self) -> u64 {
        #[cfg(feature = "archive")]
        if let Some(protection) = &self.archive {
            return protection.released.seq;
        }
        u64::MAX
    }
}
pub(crate) struct Segment {
    length: u64,
    members: BTreeMap<SharedDbId, u64>,
}
impl SharedWal {
    pub fn open(root: &Path, options: SharedWalOptions) -> Result<Self> {
        options.validate()?;
        store::open(root, options)
    }
    pub fn open_db(&self, root: &Path, id: SharedDbId, options: OpenOptions) -> Result<Db> {
        crate::db_open::open_shared(self.clone(), root, id, options).map(|(db, _)| db)
    }
    pub fn sync(&self) -> Result<SharedDurablePosition> {
        let mut state = self.lock()?;
        self.sync_locked(&mut state)?;
        Ok(SharedDurablePosition { lsn: state.durable })
    }
    #[cfg(feature = "archive")]
    pub fn open_db_with_archive(
        &self,
        root: &Path,
        id: SharedDbId,
        options: OpenOptions,
        archive: crate::ArchiveOptions,
    ) -> Result<Db> {
        let (db, _) =
            crate::db_open::open_shared_configured(self.clone(), root, id, options, true)?;
        db.enable_archive(archive)?;
        Ok(db)
    }

    pub fn maintenance_status(&self) -> Result<SharedWalStatus> {
        let state = self.lock()?;
        Ok(SharedWalStatus {
            storage: state.storage.saturating_sub(state.buffer.len() as u64),
            buffered: state.buffer.len() as u64,
            durable: state.durable,
            visible: state.lsn,
            writes: state.writes,
            syncs: state.syncs,
        })
    }
    pub fn maintenance_blockers(&self, limit: usize) -> Result<Vec<SharedDbId>> {
        let state = self.lock()?;
        if limit > self.inner.options.max_databases as usize {
            return Err(Error::invalid("blocker limit", "exceeds database budget"));
        }
        let mut output = Vec::new();
        if let Some((&number, segment)) = state.segments.first_key_value()
            && number != state.segment
        {
            for (id, seq) in &segment.members {
                if output.len() == limit {
                    break;
                }
                if state.members.get(id).is_none_or(|member| {
                    !member.retired && (member.checkpoint < *seq || member.archive_limit() < *seq)
                }) {
                    output.push(*id);
                }
            }
        }
        Ok(output)
    }
    pub(crate) fn root_path(&self) -> PathBuf {
        self.inner.directory.path(crate::fsutil::Area::Root)
    }

    pub(crate) fn identity(&self) -> [u8; 16] {
        self.inner.identity
    }

    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, State>> {
        let state = self.inner.state.lock().map_err(|_| Error::Poisoned)?;
        if state.poison {
            return Err(Error::Poisoned);
        }
        Ok(state)
    }
    pub(crate) fn poison(&self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.poison = true;
        }
    }
    fn charge(&self, state: &mut State, growth: u64) -> Result<()> {
        let next = format::add(state.charged, growth)?;
        if next > self.inner.options.index_bytes {
            return Err(Error::limit(
                "shared_wal_index_bytes",
                next,
                self.inner.options.index_bytes,
            ));
        }
        state.charged = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "archive"))]
mod archive_tests;
#[cfg(test)]
mod transaction_tests;
