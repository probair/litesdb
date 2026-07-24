// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "lock ownership is consumed by Db in a later milestone"
)]

use std::{
    fs::{File, OpenOptions, TryLockError},
    io,
    path::Path,
};

use crate::{Error, Result};

#[derive(Debug)]
pub(crate) struct DbLock {
    file: File,
}

impl DbLock {
    pub(crate) fn acquire(root: &Path) -> Result<Self> {
        let path = root.join("LOCK");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        File::try_lock(&file).map_err(|error| match error {
            TryLockError::Error(error) => Error::Io(error),
            TryLockError::WouldBlock => Error::Io(io::Error::from(io::ErrorKind::WouldBlock)),
        })?;
        if file.metadata()?.len() != 0 {
            return Err(Error::corruption("LOCK", "lock file must be empty"));
        }
        Ok(Self { file })
    }
}

impl Drop for DbLock {
    fn drop(&mut self) {
        drop(File::unlock(&self.file));
    }
}

#[cfg(test)]
#[path = "lock_tests.rs"]
mod tests;
