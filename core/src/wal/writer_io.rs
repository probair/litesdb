// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{IoStep, SystemWalIo, WalIo};
use crate::{Error, fsutil::sync_directory};
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
};

impl WalIo for SystemWalIo {
    fn create_segment(&mut self, path: &Path) -> io::Result<File> {
        OpenOptions::new().write(true).create_new(true).open(path)
    }

    fn write_all(&mut self, _step: IoStep, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn sync_data(&mut self, _step: IoStep, file: &File) -> io::Result<()> {
        file.sync_data()
    }

    fn sync_directory(&mut self, _step: IoStep, path: &Path) -> io::Result<()> {
        sync_directory(path).map_err(|error| match error {
            Error::Io(source) => source,
            _ => io::Error::other("unexpected directory synchronization error"),
        })
    }
}
