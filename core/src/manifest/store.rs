// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "called by database open and publication later in M4/M6"
)]

use std::{fs::File, io::Read};

use crate::{
    Error, Result,
    fsutil::{Area, DbDir, publish_atomically},
    limits::MAX_MANIFEST_BODY_BYTES,
    manifest::{catalog::Manifest, format},
};

const MANIFEST_NAME: &str = "MANIFEST";

pub(crate) fn load(directory: &DbDir) -> Result<Manifest> {
    let path = directory.file(Area::Root, MANIFEST_NAME);
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let maximum = u64::from(MAX_MANIFEST_BODY_BYTES)
        .checked_add(
            u64::try_from(format::MANIFEST_HEADER_BYTES)
                .map_err(|_| Error::limit("manifest_file_bytes", u64::MAX, u64::from(u32::MAX)))?,
        )
        .ok_or_else(|| Error::limit("manifest_file_bytes", u64::MAX, u64::from(u32::MAX)))?;
    if file_len > maximum {
        return Err(Error::limit("manifest_file_bytes", file_len, maximum));
    }
    let capacity = usize::try_from(file_len)
        .map_err(|_| Error::limit("manifest_file_bytes", file_len, maximum))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)?;
    format::decode(&bytes)
}

pub(crate) fn publish(
    directory: &DbDir,
    previous_generation: Option<u64>,
    manifest: &Manifest,
) -> Result<()> {
    let expected = match previous_generation {
        Some(previous) => previous
            .checked_add(1)
            .ok_or_else(|| Error::limit("manifest_generation", u64::MAX, u64::MAX))?,
        None => 0,
    };
    if manifest.identity().generation() != expected {
        return Err(Error::invalid(
            "manifest generation",
            "publication must be generation zero or the exact successor",
        ));
    }
    let bytes = format::encode(manifest)?;
    publish_atomically(directory, Area::Root, MANIFEST_NAME, &bytes)
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
