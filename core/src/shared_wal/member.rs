// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    SharedDbId, SharedWal,
    format::{self, Pointer},
};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir},
    wal::{DurablePosition, RecordBody, ReplayTarget, TailIndex, record},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

pub(crate) struct SharedMember {
    pub(crate) owner: SharedWal,
    pub(crate) id: SharedDbId,
    pub(crate) seq: u64,
    #[cfg(feature = "archive")]
    pub(super) directory: Arc<DbDir>,
    #[cfg(feature = "archive")]
    pub(super) export_index: Option<super::archive::ExportIndex>,
}
pub(crate) struct AppendOutcome {
    seq: u64,
}
impl AppendOutcome {
    pub(crate) const fn seq(&self) -> u64 {
        self.seq
    }
}
impl SharedMember {
    #[allow(
        clippy::too_many_lines,
        reason = "bounded two-pass replay is one failure-cleanup scope"
    )]
    pub(crate) fn recover(
        owner: SharedWal,
        id: SharedDbId,
        directory: &Arc<DbDir>,
        tail: &mut TailIndex,
    ) -> Result<(Self, u64)> {
        #[cfg(not(feature = "archive"))]
        let _ = directory;
        let (mut pointer, mut seq, checkpoint, budget) = {
            let mut state = owner.lock()?;
            let lsn = state
                .members
                .get(&id)
                .ok_or_else(|| format::invalid("missing recovery member"))?
                .lsn;
            if lsn > state.durable {
                owner.sync_locked(&mut state)?;
            }
            let member = state
                .members
                .get(&id)
                .ok_or_else(|| format::invalid("missing member registration"))?;
            (
                member.latest,
                member.seq,
                member.checkpoint,
                owner.inner.options.max_bytes,
            )
        };
        if tail.next_seq().saturating_sub(1) != checkpoint {
            return Err(format::invalid("checkpoint changed while opening"));
        }
        let latest_seq = seq;
        if seq == checkpoint {
            return Ok((Self::restored(owner, id, seq, directory), 0));
        }
        let mut reader = FrameReader {
            root: owner.inner.directory.path(Area::Wal),
            current: None,
        };
        let scratch_path = owner.inner.directory.file(
            Area::Temporary,
            &format!("REPLAY-{}", super::registry::name(id)),
        );
        let mut scratch = Scratch {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&scratch_path)?,
            path: scratch_path,
            owner: owner.clone(),
            charged: 0,
        };
        {
            let mut state = owner.lock()?;
            let charge = seq
                .saturating_sub(checkpoint)
                .checked_mul(16)
                .ok_or_else(|| format::invalid("scratch overflow"))?;
            let total = format::add(state.scratch_bytes, charge)?;
            if total > budget {
                return Err(Error::limit("shared_recovery_scratch_bytes", total, budget));
            }
            state.scratch_bytes = total;
            scratch.charged = charge;
        }
        let mut count = 0_u64;
        while seq > checkpoint {
            if pointer == Pointer::default() {
                return Err(format::invalid("missing required member predecessor"));
            }
            let bytes = reader.read(pointer)?;
            let frame = format::inspect(&bytes)?;
            if frame.id != id
                || frame.seq != seq
                || (frame.previous != Pointer::default() && frame.previous >= pointer)
            {
                return Err(format::invalid("member recovery chain conflict"));
            }
            count = format::add(count, 1)?;
            let charged = count
                .checked_mul(16)
                .ok_or_else(|| format::invalid("scratch length overflow"))?;
            if charged > budget {
                return Err(Error::limit(
                    "shared_recovery_scratch_bytes",
                    charged,
                    budget,
                ));
            }
            scratch.file.write_all(&pointer.segment.to_le_bytes())?;
            scratch.file.write_all(&pointer.offset.to_le_bytes())?;
            pointer = frame.previous;
            seq = seq.saturating_sub(1);
        }
        for index in (0..count).rev() {
            scratch.file.seek(SeekFrom::Start(
                index
                    .checked_mul(16)
                    .ok_or_else(|| format::invalid("scratch offset"))?,
            ))?;
            let mut bytes = [0; 16];
            scratch.file.read_exact(&mut bytes)?;
            let pointer = Pointer {
                segment: format::u64_at(&bytes, 0)?,
                offset: format::u64_at(&bytes, 8)?,
            };
            let bytes = reader.read(pointer)?;
            let frame = format::inspect(&bytes)?;
            let decoded = record::decode(frame.raw, tail.next_seq(), |table, field| {
                tail.field_type(table, field)
            })?;
            tail.apply(decoded.seq(), decoded.into_body())?;
        }
        Ok((Self::restored(owner, id, latest_seq, directory), count))
    }
    fn restored(owner: SharedWal, id: SharedDbId, seq: u64, directory: &Arc<DbDir>) -> Self {
        #[cfg(not(feature = "archive"))]
        let _ = directory;
        Self {
            owner,
            id,
            seq,
            #[cfg(feature = "archive")]
            export_index: None,
            #[cfg(feature = "archive")]
            directory: Arc::clone(directory),
        }
    }

    pub(crate) fn append_requires_checkpoint(&mut self, body: &RecordBody) -> Result<bool> {
        record::encoded_len(body)?;
        self.ensure_healthy()?;
        Ok(false)
    }
    pub(crate) fn append(&mut self, body: &RecordBody) -> Result<AppendOutcome> {
        let seq = format::add(self.seq, 1)?;
        let raw = record::encode(seq, body)?;
        self.owner.append_raw(self.id, seq, &raw)?;
        self.seq = seq;
        Ok(AppendOutcome { seq })
    }
    pub(crate) fn sync(&mut self) -> Result<DurablePosition> {
        let mut state = self.owner.lock()?;
        let lsn = state
            .members
            .get(&self.id)
            .ok_or_else(|| format::invalid("missing sync member"))?
            .lsn;
        if lsn > state.durable {
            self.owner.sync_locked(&mut state)?;
        }
        let member = state
            .members
            .get_mut(&self.id)
            .ok_or_else(|| format::invalid("missing sync member"))?;
        member.durable_seq = member.seq;
        member.durable_pointer = member.latest;
        Ok(DurablePosition::new(
            member.seq,
            member.latest.segment,
            member.latest.offset,
        ))
    }
    pub(crate) fn prepare_checkpoint(&mut self, _directory: &DbDir) -> Result<DurablePosition> {
        self.sync()
    }
    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        let mut state = self.owner.lock()?;
        let member = state
            .members
            .get_mut(&self.id)
            .ok_or_else(|| format::invalid("missing checkpoint member"))?;
        member.checkpoint = self.seq;
        member.bytes = 0;
        self.owner.gc_locked(&mut state)
    }
    pub(crate) fn unsynced_bytes(&self) -> u64 {
        self.owner
            .lock()
            .ok()
            .and_then(|state| {
                state.members.get(&self.id).map(|member| {
                    if member.lsn <= state.durable {
                        0
                    } else {
                        member.bytes
                    }
                })
            })
            .unwrap_or(0)
    }
    pub(crate) fn wal_bytes(&self) -> u64 {
        self.owner
            .lock()
            .ok()
            .and_then(|state| state.members.get(&self.id).map(|member| member.bytes))
            .unwrap_or(0)
    }
    pub(crate) fn storage_bytes(&self) -> u64 {
        self.owner.lock().map_or(0, |state| state.storage)
    }
    pub(crate) fn durable_position(&self) -> DurablePosition {
        self.owner
            .lock()
            .ok()
            .and_then(|state| {
                state.members.get(&self.id).map(|member| {
                    DurablePosition::new(
                        if member.lsn <= state.durable {
                            member.seq
                        } else {
                            member.durable_seq
                        },
                        if member.lsn <= state.durable {
                            member.latest.segment
                        } else {
                            member.durable_pointer.segment
                        },
                        if member.lsn <= state.durable {
                            member.latest.offset
                        } else {
                            member.durable_pointer.offset
                        },
                    )
                })
            })
            .unwrap_or_else(|| DurablePosition::new(0, 0, 0))
    }
    pub(crate) fn ensure_healthy(&self) -> Result<()> {
        drop(self.owner.lock()?);
        Ok(())
    }
    pub(crate) fn mark_poisoned(&mut self) {
        self.owner.poison();
    }
}
pub(super) struct FrameReader {
    pub(super) root: PathBuf,
    pub(super) current: Option<(u64, File)>,
}
impl FrameReader {
    pub(super) fn read(&mut self, pointer: Pointer) -> Result<Vec<u8>> {
        if pointer.segment == 0 || pointer.offset < format::SEGMENT_HEADER as u64 {
            return Err(format::invalid("invalid predecessor coordinate"));
        }
        if self
            .current
            .as_ref()
            .is_none_or(|(segment, _)| *segment != pointer.segment)
        {
            self.current = Some((
                pointer.segment,
                File::open(self.root.join(format::segment_name(pointer.segment)))?,
            ));
        }
        let file = &mut self
            .current
            .as_mut()
            .ok_or_else(|| format::invalid("reader segment absent"))?
            .1;
        file.seek(SeekFrom::Start(pointer.offset))?;
        let mut prefix = [0; 4];
        file.read_exact(&mut prefix)?;
        let length = format::frame_len(&prefix)?;
        let mut bytes = vec![0; length];
        bytes[..4].copy_from_slice(&prefix);
        file.read_exact(&mut bytes[4..])?;
        Ok(bytes)
    }
}
struct Scratch {
    file: File,
    path: PathBuf,
    owner: SharedWal,
    charged: u64,
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let removed = fs::remove_file(&self.path).or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(error)
            }
        });
        if let Ok(mut state) = self.owner.inner.state.lock() {
            if removed.is_ok() {
                state.scratch_bytes = state.scratch_bytes.saturating_sub(self.charged);
            } else {
                state.poison = true;
            }
        }
    }
}
