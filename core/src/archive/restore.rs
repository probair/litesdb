// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    ArchiveCursor, BaseDescriptor, ExportChunk,
    base::hash_file,
    format::{Reader, read_record},
    restore_io::{self, State},
};
use crate::{
    Db, Error, OpenOptions, Result,
    fsutil::{Area, DbDir, DbLock, publish_atomically, sync_directory},
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct RestoredDbDescriptor {
    directory: PathBuf,
    cursor: ArchiveCursor,
    base_id: [u8; 16],
}
impl RestoredDbDescriptor {
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    #[must_use]
    pub const fn source_cursor(&self) -> ArchiveCursor {
        self.cursor
    }
    #[must_use]
    pub const fn base_id(&self) -> [u8; 16] {
        self.base_id
    }
}
pub struct RestoreBuilder {
    directory: DbDir,
    _lock: Arc<DbLock>,
    options: OpenOptions,
    state: State,
    poisoned: bool,
    finished: bool,
}
impl RestoreBuilder {
    pub fn install(
        base: &BaseDescriptor,
        base_directory: &Path,
        target: &Path,
        options: OpenOptions,
    ) -> Result<Self> {
        validate_options(options)?;
        let descriptor = base.to_bytes();
        if target.try_exists()? {
            let marker = target.join("RESTORE");
            if !marker.try_exists()? || restore_io::read_descriptor(&marker)? != *base {
                return Err(Error::invalid(
                    "restore target",
                    "target is not this restore",
                ));
            }
            if target.join("CURRENT").try_exists()? {
                return Self::open(target, options);
            }
        } else {
            fs::create_dir(target)?;
        }
        let lock = Arc::new(DbLock::acquire(target)?);
        let directory = DbDir::initialize(target)?;
        if !target.join("RESTORE").try_exists()? {
            publish_atomically(&directory, Area::Root, "RESTORE", &descriptor)?;
        }
        let generations = target.join("generations");
        fs::create_dir_all(&generations)?;
        directory.sync(Area::Root)?;
        let generation = restore_io::generation_path(target, 0);
        if generation.try_exists()? {
            fs::remove_dir_all(&generation)?;
        }
        restore_io::install_files(base, base_directory, &generation)?;
        let db = Db::open_for_restore(&generation, options)?;
        if db.lock_engine()?.tail.next_seq().checked_sub(1) != Some(base.cursor.seq) {
            return Err(super::invalid("base catalog and cursor disagree"));
        }
        db.validate_restored_storage()?;
        db.sync()?;
        drop(db);
        let state = State {
            generation: 0,
            base_id: base.id,
            cursor: base.cursor,
            previous: base.cursor,
            chunk_hash: [0; 32],
            manifest_hash: hash_file(&generation.join("MANIFEST"))?.1,
        };
        let generation_dir = DbDir::initialize(&generation)?;
        publish_atomically(&generation_dir, Area::Root, "RESTORE-WORK", &state.encode())?;
        sync_directory(&generations)?;
        publish_atomically(&directory, Area::Root, "CURRENT", &state.encode())?;
        Ok(Self {
            directory,
            _lock: lock,
            options,
            state,
            poisoned: false,
            finished: false,
        })
    }
    pub fn open(target: &Path, options: OpenOptions) -> Result<Self> {
        validate_options(options)?;
        let base = restore_io::read_descriptor(&target.join("RESTORE"))?;
        let lock = Arc::new(DbLock::acquire(target)?);
        let directory = DbDir::initialize(target)?;
        directory.clear_temporary()?;
        let finished = target.join("FINISHED").try_exists()?;
        let state =
            restore_io::read_state(&target.join(if finished { "FINISHED" } else { "CURRENT" }))?;
        if state.base_id != base.id || !state.cursor.same_history(base.cursor) {
            return Err(super::invalid("restore source identity changed"));
        }
        if finished {
            let ready = target.join("ready");
            if !fs::symlink_metadata(&ready)?.file_type().is_dir()
                || !ready.join("MANIFEST").try_exists()?
            {
                return Err(super::invalid("finished restore database is absent"));
            }
            if ready.join("RESTORE-WORK").try_exists()?
                && (restore_io::read_state(&ready.join("RESTORE-WORK"))? != state
                    || hash_file(&ready.join("MANIFEST"))?.1 != state.manifest_hash)
            {
                return Err(super::invalid("finished restore generation is incomplete"));
            }
        } else {
            let generation = restore_io::generation_path(target, state.generation);
            if restore_io::read_state(&generation.join("RESTORE-WORK"))? != state
                || hash_file(&generation.join("MANIFEST"))?.1 != state.manifest_hash
            {
                return Err(super::invalid("restore generation does not match CURRENT"));
            }
            restore_io::reap_generations(target, state.generation)?;
        }
        Ok(Self {
            directory,
            _lock: lock,
            options,
            state,
            poisoned: false,
            finished,
        })
    }
    #[must_use]
    pub const fn applied_cursor(&self) -> ArchiveCursor {
        self.state.cursor
    }
    pub fn apply_archive(&mut self, chunk: &ExportChunk) -> Result<ArchiveCursor> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.finished {
            return Err(Error::unsupported("restore", "restore is already finished"));
        }
        let hash: [u8; 32] = Sha256::digest(chunk.to_bytes()).into();
        if chunk.end == self.state.cursor
            && chunk.start == self.state.previous
            && hash == self.state.chunk_hash
        {
            return Ok(self.state.cursor);
        }
        if chunk.start != self.state.cursor {
            return Err(Error::invalid(
                "archive chunk",
                "chunk does not continue applied cursor",
            ));
        }
        if chunk.is_empty() {
            return Ok(self.state.cursor);
        }
        let result = self.apply_new(chunk, hash);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn apply_new(&mut self, chunk: &ExportChunk, hash: [u8; 32]) -> Result<ArchiveCursor> {
        let root = self.directory.path(Area::Root);
        let next = self
            .state
            .generation
            .checked_add(1)
            .ok_or_else(|| super::invalid("restore generation overflow"))?;
        let source = restore_io::generation_path(&root, self.state.generation);
        let target = restore_io::generation_path(&root, next);
        restore_io::clone_generation(&source, &target)?;
        let target_dir = DbDir::initialize(&target)?;
        publish_atomically(
            &target_dir,
            Area::Root,
            "RESTORE-WORK",
            &self.state.encode(),
        )?;
        let db = Db::open_for_restore(&target, self.options)?;
        let mut reader = Reader::new(&chunk.records);
        while !reader.remaining().is_empty() {
            let (_, _, _, raw) = read_record(&mut reader)?;
            db.replay_archived_record(raw)?;
        }
        db.seal()?;
        db.maintain()?;
        db.sync()?;
        drop(db);
        let state = State {
            generation: next,
            base_id: self.state.base_id,
            cursor: chunk.end,
            previous: chunk.start,
            chunk_hash: hash,
            manifest_hash: hash_file(&target.join("MANIFEST"))?.1,
        };
        publish_atomically(&target_dir, Area::Root, "RESTORE-WORK", &state.encode())?;
        restore_io::sync_generation(&target)?;
        sync_directory(&root.join("generations"))?;
        publish_atomically(&self.directory, Area::Root, "CURRENT", &state.encode())?;
        self.state = state;
        let _ = restore_io::reap_generations(&root, next);
        Ok(state.cursor)
    }
    pub fn finish(mut self) -> Result<RestoredDbDescriptor> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let root = self.directory.path(Area::Root);
        let ready = root.join("ready");
        if !self.finished {
            if ready.try_exists()? {
                fs::remove_dir_all(&ready)?;
            }
            restore_io::clone_generation(
                &restore_io::generation_path(&root, self.state.generation),
                &ready,
            )?;
            let ready_dir = DbDir::initialize(&ready)?;
            publish_atomically(&ready_dir, Area::Root, "RESTORE-WORK", &self.state.encode())?;
            publish_atomically(
                &ready_dir,
                Area::Root,
                "ARCHIVE-ORIGIN",
                &super::store::encode_origin(self.state.cursor),
            )?;
            self.directory.sync(Area::Root)?;
            publish_atomically(
                &self.directory,
                Area::Root,
                "FINISHED",
                &self.state.encode(),
            )?;
            self.finished = true;
        }
        let marker = ready.join("RESTORE-WORK");
        if marker.try_exists()? {
            fs::remove_file(marker)?;
            sync_directory(&ready)?;
        }
        let _ = fs::remove_dir_all(root.join("generations"));
        let _ = self.directory.sync(Area::Root);
        Ok(RestoredDbDescriptor {
            directory: ready,
            cursor: self.state.cursor,
            base_id: self.state.base_id,
        })
    }
}
fn validate_options(options: OpenOptions) -> Result<()> {
    crate::options::validate(options)?;
    if options.takeover {
        return Err(Error::invalid(
            "takeover",
            "restore cannot adopt writer ownership",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "restore_tests.rs"]
mod tests;
