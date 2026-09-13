// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ArchiveCursor, BaseDescriptor, BaseFile, FrozenBase};
use crate::{
    Db, Error, Result,
    fsutil::{Area, publish_atomically, sync_directory},
    retention::head_name,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::Path,
};

const MAX_PINS: usize = 32;
pub(crate) fn pin_name(id: [u8; 16]) -> String {
    let mut name = String::from("ARCHIVE-PIN-");
    for byte in id {
        use std::fmt::Write as _;
        let _ = write!(name, "{byte:02x}");
    }
    name
}
impl Db {
    pub fn prepare_base(&self, export_dir: &Path) -> Result<FrozenBase> {
        let mut id = [0u8; 16];
        File::open("/dev/urandom")?.read_exact(&mut id)?;
        self.prepare_base_with_id(export_dir, id)
    }
    pub fn prepare_base_with_id(&self, export_dir: &Path, id: [u8; 16]) -> Result<FrozenBase> {
        if id == [0; 16] {
            return Err(Error::invalid("base id", "must be nonzero"));
        }
        if export_dir.try_exists()? {
            return Err(Error::invalid("export_dir", "must be a new directory"));
        }
        let parent = export_dir
            .parent()
            .ok_or_else(|| Error::invalid("export_dir", "missing parent"))?
            .canonicalize()?;
        let root = self.directory.path(Area::Root).canonicalize()?;
        if parent.starts_with(&root) {
            return Err(Error::invalid(
                "export_dir",
                "must be outside active database",
            ));
        }
        let mut engine = self.lock_engine()?;
        if parent.starts_with(engine.writer.owner.root_path()) {
            return Err(crate::Error::invalid(
                "export_dir",
                "must be outside the shared owner",
            ));
        }
        engine.writer.ensure_healthy()?;
        engine.writer.archive_status()?;
        let pins = read_pins(&root)?;
        if pins.len() >= MAX_PINS {
            return Err(Error::limit(
                "base_pins",
                pins.len() as u64,
                MAX_PINS as u64,
            ));
        }
        let name = pin_name(id);
        if self.directory.file(Area::Root, &name).try_exists()? {
            return Err(super::invalid("base identity collision"));
        }
        self.seal_locked(&mut engine)?;
        let cursor = engine.writer.archive_status()?.durable_end();
        publish_atomically(&self.directory, Area::Root, &name, &encode_pin(id, cursor))?;
        fs::create_dir(export_dir)?;
        for area in ["wal", "units", "heads"] {
            fs::create_dir(export_dir.join(area))?;
        }
        let mut paths = vec!["MANIFEST".to_owned()];
        for unit in engine.manifest.units() {
            paths.push(format!("units/{:016x}.lsu", unit.unit_id()));
        }
        if let Some(generation) = engine.manifest.retention().heads_generation() {
            paths.push(format!("heads/{}", head_name(generation)));
        }
        paths.sort_unstable();
        let mut files = Vec::new();
        for relative in paths {
            let source = root.join(&relative);
            let target = export_dir.join(&relative);
            if relative.starts_with("units/") || relative.starts_with("heads/") {
                fs::hard_link(&source, &target)?;
            } else {
                copy_file(&source, &target)?;
            }
            let length = File::open(&target)?.metadata()?.len();
            files.push(BaseFile {
                path: relative,
                length,
                sha256: [0; 32],
            });
        }
        for area in ["wal", "units", "heads"] {
            sync_directory(&export_dir.join(area))?;
        }
        sync_directory(export_dir)?;
        sync_directory(&parent)?;
        Ok(FrozenBase {
            id,
            directory: export_dir.to_owned(),
            cursor,
            files,
            units: engine.manifest.units().to_vec(),
        })
    }
    pub fn describe_base(&self, frozen: &FrozenBase) -> Result<BaseDescriptor> {
        let pin = fs::read(self.directory.file(Area::Root, &pin_name(frozen.id)))?;
        if decode_pin(&pin)? != (frozen.id, frozen.cursor) {
            return Err(super::invalid("capture protection mismatch"));
        }
        crate::unit::FileUnitSource::open(&frozen.directory.join("units"), &frozen.units, 0)?
            .validate_all()?;
        let mut files = Vec::new();
        for file in &frozen.files {
            let (length, sha256) = hash_file(&frozen.directory.join(&file.path))?;
            if length != file.length {
                return Err(super::invalid("frozen file length changed"));
            }
            files.push(BaseFile {
                path: file.path.clone(),
                length,
                sha256,
            });
        }
        Ok(BaseDescriptor {
            id: frozen.id,
            cursor: frozen.cursor,
            files,
        })
    }
    pub fn finish_base(&self, id: [u8; 16]) -> Result<()> {
        let engine = self.lock_engine()?;
        engine.writer.ensure_healthy()?;
        let path = self.directory.file(Area::Root, &pin_name(id));
        engine.writer.archive_status()?;
        if path.try_exists()? {
            fs::remove_file(path)?;
        }
        self.directory.sync(Area::Root)
    }
}
impl Db {
    pub fn pending_bases(&self) -> Result<Vec<([u8; 16], ArchiveCursor)>> {
        self.lock_engine()?.writer.archive_status()?;
        read_pin_entries(&self.directory.path(Area::Root))
    }
}
pub(crate) fn read_pins(root: &Path) -> Result<Vec<ArchiveCursor>> {
    Ok(read_pin_entries(root)?
        .into_iter()
        .map(|(_, cursor)| cursor)
        .collect())
}
fn read_pin_entries(root: &Path) -> Result<Vec<([u8; 16], ArchiveCursor)>> {
    let mut pins = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(raw) = name.strip_prefix("ARCHIVE-PIN-") else {
            continue;
        };
        if raw.len() != 32
            || !raw.is_ascii()
            || pins.len() >= MAX_PINS
            || !entry.file_type()?.is_file()
            || entry.metadata()?.len() != 176
        {
            return Err(super::invalid("invalid capture pin"));
        }
        let mut id = [0u8; 16];
        for (slot, pair) in id.iter_mut().zip(raw.as_bytes().chunks_exact(2)) {
            let hex = std::str::from_utf8(pair).map_err(|_| super::invalid("capture id"))?;
            *slot = u8::from_str_radix(hex, 16).map_err(|_| super::invalid("capture id"))?;
        }
        if pin_name(id) != name {
            return Err(super::invalid("noncanonical capture id"));
        }
        let decoded = decode_pin(&fs::read(entry.path())?)?;
        if decoded.0 != id {
            return Err(super::invalid("capture filename identity mismatch"));
        }
        pins.push(decoded);
    }
    pins.sort_unstable_by_key(|(id, _)| *id);
    Ok(pins)
}
pub(crate) fn hash_file(path: &Path) -> Result<(u64, [u8; 32])> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(super::invalid("base file is not regular"));
    }
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 16_384];
    let mut length = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| super::invalid("base length overflow"))?;
        hash.update(&buffer[..read]);
    }
    if length != metadata.len() {
        return Err(super::invalid("base file changed while hashing"));
    }
    Ok((length, hash.finalize().into()))
}
pub(crate) fn copy_file(source: &Path, target: &Path) -> Result<()> {
    let mut input = File::open(source)?;
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    std::io::copy(&mut input, &mut output)?;
    output.flush()?;
    output.sync_all()?;
    Ok(())
}

fn encode_pin(id: [u8; 16], cursor: ArchiveCursor) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"LSAP\x02\0\0\0");
    bytes.extend_from_slice(&id);
    bytes.extend_from_slice(&cursor.to_bytes());
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    bytes
}
fn decode_pin(bytes: &[u8]) -> Result<([u8; 16], ArchiveCursor)> {
    if bytes.len() != 176 || Sha256::digest(&bytes[..144]).as_slice() != &bytes[144..] {
        return Err(super::invalid("capture pin checksum"));
    }
    let mut reader = super::format::Reader::new(&bytes[..144]);
    reader.magic(b"LSAP\x02\0\0\0")?;
    let id = reader.array()?;
    let cursor = ArchiveCursor::from_bytes(reader.take(super::types::CURSOR_BYTES)?)?;
    reader.finish()?;
    Ok((id, cursor))
}
