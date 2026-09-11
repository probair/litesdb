// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    ArchiveCursor, BaseDescriptor,
    base::{copy_file, hash_file},
    format::Reader,
    types::CURSOR_BYTES,
};
use crate::{
    Result,
    fsutil::{DbDir, sync_directory},
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct State {
    pub(crate) generation: u64,
    pub(crate) base_id: [u8; 16],
    pub(crate) cursor: ArchiveCursor,
    pub(crate) previous: ArchiveCursor,
    pub(crate) chunk_hash: [u8; 32],
    pub(crate) manifest_hash: [u8; 32],
}
impl State {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LSRS\x01\0\0\0");
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&self.base_id);
        bytes.extend_from_slice(&self.cursor.to_bytes());
        bytes.extend_from_slice(&self.previous.to_bytes());
        bytes.extend_from_slice(&self.chunk_hash);
        bytes.extend_from_slice(&self.manifest_hash);
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        bytes
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 336 {
            return Err(super::invalid("restore state length"));
        }
        let end = bytes.len().saturating_sub(32);
        if Sha256::digest(&bytes[..end]).as_slice() != &bytes[end..] {
            return Err(super::invalid("restore state digest"));
        }
        let mut reader = Reader::new(&bytes[..end]);
        reader.magic(b"LSRS\x01\0\0\0")?;
        let generation = reader.u64()?;
        let base_id = reader.array()?;
        let cursor = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
        let previous = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
        let chunk_hash = reader.array()?;
        let manifest_hash = reader.array()?;
        reader.finish()?;
        if !previous.same_history(cursor) || previous.seq > cursor.seq {
            return Err(super::invalid("restore state cursor order"));
        }
        Ok(Self {
            generation,
            base_id,
            cursor,
            previous,
            chunk_hash,
            manifest_hash,
        })
    }
}
pub(crate) fn generation_path(root: &Path, generation: u64) -> PathBuf {
    root.join("generations").join(format!("{generation:020}"))
}
pub(crate) fn read_state(path: &Path) -> Result<State> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() != 336 {
        return Err(super::invalid("restore state is not regular"));
    }
    State::decode(&fs::read(path)?)
}
pub(crate) fn install_files(base: &BaseDescriptor, source: &Path, target: &Path) -> Result<()> {
    if !fs::symlink_metadata(source)?.file_type().is_dir() {
        return Err(super::invalid("base root is not regular"));
    }
    DbDir::initialize(target)?;
    fs::write(target.join("RESTORE-WORK"), [])?;
    sync_directory(target)?;
    for file in &base.files {
        super::base_types::validate_path(&file.path)?;
        if let Some((area, _)) = file.path.split_once('/')
            && !fs::symlink_metadata(source.join(area))?
                .file_type()
                .is_dir()
        {
            return Err(super::invalid("base area is not regular"));
        }
        let input = source.join(&file.path);
        let metadata = fs::symlink_metadata(&input)?;
        if !metadata.file_type().is_file() || metadata.len() != file.length {
            return Err(super::invalid("base input extent"));
        }
        let output = target.join(&file.path);
        copy_file(&input, &output)?;
        if hash_file(&output)? != (file.length, file.sha256) {
            return Err(super::invalid("base file digest mismatch"));
        }
    }
    sync_generation(target)
}
pub(crate) fn clone_generation(source: &Path, target: &Path) -> Result<()> {
    if target.try_exists()? {
        return Err(super::invalid("restore generation already exists"));
    }
    let source_dir = DbDir::initialize(source)?;
    let manifest = crate::manifest::load(&source_dir)?;
    DbDir::initialize(target)?;
    fs::write(target.join("RESTORE-WORK"), [])?;
    sync_directory(target)?;
    copy_file(&source.join("MANIFEST"), &target.join("MANIFEST"))?;
    let wal = crate::wal::segment::segment_name(manifest.checkpoint().segment_first_seq());
    copy_file(
        &source.join("wal").join(&wal),
        &target.join("wal").join(wal),
    )?;
    for unit in manifest.units() {
        let name = format!("{:016x}.lsu", unit.unit_id());
        fs::hard_link(
            source.join("units").join(&name),
            target.join("units").join(name),
        )?;
    }
    if let Some(generation) = manifest.retention().heads_generation() {
        let name = crate::retention::head_name(generation);
        fs::hard_link(
            source.join("heads").join(&name),
            target.join("heads").join(name),
        )?;
    }
    sync_generation(target)
}
pub(crate) fn sync_generation(root: &Path) -> Result<()> {
    for area in ["wal", "units", "heads", "agg", "tmp"] {
        sync_directory(&root.join(area))?;
    }
    sync_directory(root)
}
pub(crate) fn reap_generations(root: &Path, keep: u64) -> Result<()> {
    let generations = root.join("generations");
    for entry in fs::read_dir(&generations)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| super::invalid("restore generation name"))?;
        let number: u64 = name
            .parse()
            .map_err(|_| super::invalid("restore generation name"))?;
        if name != format!("{number:020}") || !entry.file_type()?.is_dir() {
            return Err(super::invalid("restore generation path"));
        }
        if number != keep {
            fs::remove_dir_all(entry.path())?;
        }
    }
    sync_directory(&generations)
}

pub(crate) fn read_descriptor(path: &Path) -> Result<BaseDescriptor> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()
        || metadata.len() > super::base_types::MAX_DESCRIPTOR_BYTES as u64
    {
        return Err(super::invalid(
            "restore descriptor is not bounded and regular",
        ));
    }
    BaseDescriptor::from_bytes(&fs::read(path)?)
}
