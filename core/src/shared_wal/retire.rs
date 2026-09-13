// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SharedDbId, SharedWal, format};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir, DbLock, publish_atomically},
};
use std::{fs, path::Path};
fn name(id: SharedDbId) -> String {
    format!("RETIRED-{}", super::registry::name(id))
}
pub(super) fn load(directory: &DbDir, owner: [u8; 16], id: SharedDbId) -> Result<Option<u64>> {
    let path = directory.file(Area::Root, &name(id));
    if !path.try_exists()? {
        return Ok(None);
    }
    if fs::metadata(&path)?.len() != 68 {
        return Err(format::invalid("retirement authority length"));
    }
    let bytes = fs::read(path)?;
    let body = format::checked(&bytes, b"LSSX\x02\0\0\0")?;
    if format::array::<16>(body, 8)? != owner || SharedDbId::from_bytes(&body[24..56])? != id {
        return Err(format::invalid("retirement authority identity"));
    }
    Ok(Some(format::u64_at(body, 56)?))
}
impl SharedWal {
    pub fn discard_unpublished(&self, root: &Path, id: SharedDbId) -> Result<()> {
        {
            let state = self.lock()?;
            let Some(member) = state.members.get(&id) else {
                return Ok(());
            };
            if member.retired {
                return Ok(());
            }
        }
        let canonical = root.canonicalize()?;
        {
            let state = self.lock()?;
            let member = state
                .members
                .get(&id)
                .ok_or_else(|| format::invalid("discard member disappeared"))?;
            if member.root != canonical {
                return Err(Error::invalid(
                    "database path",
                    "path differs from registered identity",
                ));
            }
        }
        let _lock = DbLock::acquire(&canonical)?;
        super::registry::verify_binding(&canonical, self.inner.identity, id)?;
        #[cfg(feature = "archive")]
        if !crate::archive::base::read_pins(&canonical)?.is_empty() {
            return Err(Error::invalid(
                "discard",
                "persistent base pins still protect this member",
            ));
        }
        let mut state = self.lock()?;
        let seq = state
            .members
            .get(&id)
            .ok_or_else(|| format::invalid("discard member disappeared"))?
            .seq;
        let mut bytes = b"LSSX\x02\0\0\0".to_vec();
        bytes.extend_from_slice(&self.inner.identity);
        bytes.extend_from_slice(&id.bytes());
        bytes.extend_from_slice(&seq.to_le_bytes());
        if let Err(error) = publish_atomically(
            &self.inner.directory,
            Area::Root,
            &name(id),
            &format::checksum(bytes),
        ) {
            state.poison = true;
            return Err(error);
        }
        let member = state
            .members
            .get_mut(&id)
            .ok_or_else(|| format::invalid("discard member disappeared"))?;
        member.retired = true;
        #[cfg(feature = "archive")]
        {
            member.archive = None;
        }
        state.paths.remove(&canonical);
        self.gc_locked(&mut state)
    }
}
