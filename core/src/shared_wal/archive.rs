// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SharedDbId, SharedWal, format, member::SharedMember};
use crate::{
    ArchiveCursor, ArchiveOptions, ArchiveStatus, Error, ExportChunk, Result,
    archive::{base, types::CURSOR_BYTES},
    fsutil::{Area, publish_atomically},
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};
#[path = "archive_export.rs"]
mod export;
pub(super) use export::ExportIndex;
const AUTHORITY_BYTES: usize = 16 + CURSOR_BYTES * 2 + 32;
const RESERVE: u64 = 8192;
pub(super) const PROTECTION_CHARGE: u64 = std::mem::size_of::<Protection>() as u64 + 32;

pub(super) struct Protection {
    pub(super) options: ArchiveOptions,
    pub(super) initial: ArchiveCursor,
    pub(super) released: ArchiveCursor,
    pub(super) latest: ArchiveCursor,
    pub(super) durable: ArchiveCursor,
}
impl Protection {
    pub(super) fn load(root: &Path, id: SharedDbId) -> Result<Option<Self>> {
        let path = root.join("ARCHIVE");
        if !path.try_exists()? {
            return Ok(None);
        }
        if fs::metadata(&path)?.len() != AUTHORITY_BYTES as u64 {
            return Err(format::invalid("shared archive authority size"));
        }
        let bytes = fs::read(path)?;
        let end = bytes.len().saturating_sub(32);
        if &bytes[..8] != b"LSSA\x02\0\0\0"
            || Sha256::digest(&bytes[..end]).as_slice() != &bytes[end..]
        {
            return Err(format::invalid("shared archive authority checksum"));
        }
        let options = ArchiveOptions {
            max_bytes: format::u64_at(&bytes, 8)?,
        };
        options.validate()?;
        let initial = ArchiveCursor::from_bytes(&bytes[16..16 + CURSOR_BYTES])?;
        let released = ArchiveCursor::from_bytes(&bytes[16 + CURSOR_BYTES..end])?;
        if initial.database != id.database()
            || initial.generation != id.generation()
            || !released.covers(initial)?
        {
            return Err(format::invalid("shared archive identity or release order"));
        }
        for pin in base::read_pins(root)? {
            if !pin.same_history(released) || pin.seq < released.seq {
                return Err(format::invalid(
                    "archive release crosses persistent base pin",
                ));
            }
        }
        Ok(Some(Self {
            options,
            initial,
            released,
            latest: released,
            durable: released,
        }))
    }
    pub(super) fn bytes(&self) -> u64 {
        RESERVE
            .saturating_add(self.latest.offset.saturating_sub(self.released.offset))
            .saturating_add(
                self.latest
                    .seq
                    .saturating_sub(self.released.seq)
                    .saturating_mul(format::FRAME_HEADER as u64),
            )
    }
    pub(super) fn next(&self, raw: &[u8]) -> Result<ArchiveCursor> {
        let charged = self
            .bytes()
            .saturating_add(raw.len() as u64)
            .saturating_add(format::FRAME_HEADER as u64);
        if charged > self.options.max_bytes {
            return Err(Error::limit(
                "archive_bytes",
                charged,
                self.options.max_bytes,
            ));
        }
        advance(self.latest, raw)
    }
    pub(super) fn replay(&mut self, seq: u64, raw: &[u8]) -> Result<()> {
        if seq > self.released.seq {
            self.latest = advance(self.latest, raw)?;
        }
        Ok(())
    }
    fn bytes_for(&self, released: ArchiveCursor) -> Vec<u8> {
        let mut bytes = b"LSSA\x02\0\0\0".to_vec();
        bytes.extend_from_slice(&self.options.max_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.initial.to_bytes());
        bytes.extend_from_slice(&released.to_bytes());
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        bytes
    }
}
pub(super) fn advance(cursor: ArchiveCursor, raw: &[u8]) -> Result<ArchiveCursor> {
    let offset = format::add(cursor.offset, raw.len() as u64)?;
    cursor.advance(cursor.epoch, cursor.segment, offset, raw)
}
impl SharedMember {
    pub(crate) fn enable_archive(&mut self, options: ArchiveOptions) -> Result<()> {
        options.validate()?;
        self.sync()?;
        let mut state = self.owner.lock()?;
        if state
            .members
            .get(&self.id)
            .is_some_and(|member| member.archive.is_none())
        {
            self.owner.charge(&mut state, PROTECTION_CHARGE)?;
        }
        let member = state
            .members
            .get_mut(&self.id)
            .ok_or_else(|| format::invalid("archive member missing"))?;
        if let Some(protection) = &mut member.archive {
            if protection.bytes() > options.max_bytes {
                return Err(Error::limit(
                    "archive_bytes",
                    protection.bytes(),
                    options.max_bytes,
                ));
            }
            if protection.options != options {
                protection.options = options;
                let directory = &self.directory;
                if let Err(error) = publish_atomically(
                    directory,
                    Area::Root,
                    "ARCHIVE",
                    &protection.bytes_for(protection.released),
                ) {
                    state.poison = true;
                    return Err(error);
                }
            }
            return Ok(());
        }
        let mut branch = [0; 16];
        File::open("/dev/urandom")?.read_exact(&mut branch)?;
        let mut cursor = ArchiveCursor {
            database: self.id.database(),
            generation: self.id.generation(),
            branch,
            epoch: 0,
            segment: format::add(member.seq, 1)?,
            offset: 32,
            seq: member.seq,
            digest: [0; 32],
        };
        cursor.digest = Sha256::digest(cursor.to_bytes()).into();
        let protection = Protection {
            options,
            initial: cursor,
            released: cursor,
            latest: cursor,
            durable: cursor,
        };
        if let Err(error) = publish_atomically(
            &self.directory,
            Area::Root,
            "ARCHIVE",
            &protection.bytes_for(cursor),
        ) {
            state.poison = true;
            return Err(error);
        }
        member.archive = Some(Box::new(protection));
        Ok(())
    }
    pub(crate) fn archive_status(&self) -> Result<ArchiveStatus> {
        let state = self.owner.lock()?;
        let member = state
            .members
            .get(&self.id)
            .ok_or_else(|| format::invalid("archive member missing"))?;
        let protection = member
            .archive
            .as_ref()
            .ok_or_else(|| Error::unsupported("archive", "member archive is disabled"))?;
        Ok(ArchiveStatus {
            earliest: protection.released,
            durable_end: if member.lsn <= state.durable {
                protection.latest
            } else {
                protection.durable
            },
            bytes: protection.bytes(),
            healthy: true,
        })
    }
    pub(crate) fn export_durable(
        &mut self,
        after: ArchiveCursor,
        max_bytes: u32,
    ) -> Result<ExportChunk> {
        self.export_shared(after, max_bytes)
    }
    pub(crate) fn release_archive(&mut self, through: ArchiveCursor) -> Result<()> {
        self.verify_cursor(through)?;
        let mut state = self.owner.lock()?;
        let member = state
            .members
            .get_mut(&self.id)
            .ok_or_else(|| format::invalid("archive member missing"))?;
        let protection = member
            .archive
            .as_mut()
            .ok_or_else(|| Error::unsupported("archive", "member archive is disabled"))?;
        if !through.covers(protection.released)? {
            return Err(Error::invalid("release cursor", "release cannot regress"));
        }
        for pin in base::read_pins(&member.root)? {
            if !pin.same_history(through) || pin.seq < through.seq {
                return Err(Error::invalid(
                    "release cursor",
                    "persistent base pin protects this prefix",
                ));
            }
        }
        if through == protection.released {
            return Ok(());
        }
        if let Err(error) = publish_atomically(
            &self.directory,
            Area::Root,
            "ARCHIVE",
            &protection.bytes_for(through),
        ) {
            state.poison = true;
            return Err(error);
        }
        protection.released = through;
        self.owner.gc_locked(&mut state)
    }
}
