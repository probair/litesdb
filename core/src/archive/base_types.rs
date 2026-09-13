// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    ArchiveCursor,
    format::{Reader, append_u64},
    types::CURSOR_BYTES,
};
use crate::{Error, Result};
use std::path::{Path, PathBuf};

pub(crate) const MAX_BASE_FILES: usize = 8195;
pub(crate) const MAX_DESCRIPTOR_BYTES: usize = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaseFile {
    pub(crate) path: String,
    pub(crate) length: u64,
    pub(crate) sha256: [u8; 32],
}
impl BaseFile {
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.path
    }
    #[must_use]
    pub const fn length(&self) -> u64 {
        self.length
    }
    #[must_use]
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }
}
#[derive(Debug)]
pub struct FrozenBase {
    pub(crate) id: [u8; 16],
    pub(crate) directory: PathBuf,
    pub(crate) cursor: ArchiveCursor,
    pub(crate) files: Vec<BaseFile>,
    pub(crate) units: Vec<crate::manifest::UnitMeta>,
}
impl FrozenBase {
    #[must_use]
    pub const fn id(&self) -> [u8; 16] {
        self.id
    }
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    #[must_use]
    pub const fn cursor(&self) -> ArchiveCursor {
        self.cursor
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaseDescriptor {
    pub(crate) id: [u8; 16],
    pub(crate) cursor: ArchiveCursor,
    pub(crate) files: Vec<BaseFile>,
}
impl BaseDescriptor {
    #[must_use]
    pub const fn id(&self) -> [u8; 16] {
        self.id
    }
    #[must_use]
    pub const fn cursor(&self) -> ArchiveCursor {
        self.cursor
    }
    #[must_use]
    pub fn files(&self) -> &[BaseFile] {
        &self.files
    }
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LSBD\x02\0\0\0");
        bytes.extend_from_slice(&self.id);
        bytes.extend_from_slice(&self.cursor.to_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.files.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for file in &self.files {
            bytes.extend_from_slice(
                &u16::try_from(file.path.len())
                    .unwrap_or(u16::MAX)
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(file.path.as_bytes());
            append_u64(&mut bytes, file.length);
            bytes.extend_from_slice(&file.sha256);
        }
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_DESCRIPTOR_BYTES {
            return Err(Error::limit(
                "base_descriptor_bytes",
                bytes.len() as u64,
                MAX_DESCRIPTOR_BYTES as u64,
            ));
        }
        let mut reader = Reader::new(bytes);
        reader.magic(b"LSBD\x02\0\0\0")?;
        let id = reader.array()?;
        let cursor = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
        let count = reader.u32()? as usize;
        if !(1..=MAX_BASE_FILES).contains(&count) {
            return Err(super::invalid("base file count"));
        }
        let mut files = Vec::new();
        for _ in 0..count {
            let length = usize::from(u16::from_le_bytes(reader.array()?));
            if length > 64 {
                return Err(super::invalid("base path length"));
            }
            let path = std::str::from_utf8(reader.take(length)?)
                .map_err(|_| super::invalid("base path UTF-8"))?;
            validate_path(path)?;
            let length = reader.u64()?;
            if length > 8_589_934_592 {
                return Err(super::invalid("base file length"));
            }
            let sha256 = reader.array()?;
            if files
                .last()
                .is_some_and(|last: &BaseFile| last.path.as_str() >= path)
            {
                return Err(super::invalid("base file order"));
            }
            files.push(BaseFile {
                path: path.to_owned(),
                length,
                sha256,
            });
        }
        reader.finish()?;
        if !files.iter().any(|file| file.path == "MANIFEST") {
            return Err(super::invalid("base requires one logical catalog"));
        }
        Ok(Self { id, cursor, files })
    }
}
pub(crate) fn validate_path(path: &str) -> Result<()> {
    if path == "MANIFEST" {
        return Ok(());
    }
    let Some((area, name)) = path.split_once('/') else {
        return Err(super::invalid("base file path"));
    };
    match area {
        "units" | "heads" => {
            let suffix = if area == "units" { ".lsu" } else { ".lsr" };
            let raw = name
                .strip_suffix(suffix)
                .ok_or_else(|| super::invalid("base artifact suffix"))?;
            let id =
                u64::from_str_radix(raw, 16).map_err(|_| super::invalid("base artifact ID"))?;
            if format!("{id:016x}{suffix}") != name {
                return Err(super::invalid("base artifact name"));
            }
        }
        _ => return Err(super::invalid("base file area")),
    }
    Ok(())
}
