// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "publication is consumed by units and manifests later"
)]

use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use super::dir::{Area, DbDir};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublishStep {
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

pub(crate) fn publish_atomically(
    directory: &DbDir,
    area: Area,
    name: &str,
    content: &[u8],
) -> Result<()> {
    publish_with_hook(directory, area, name, content, |_| Ok(()))
}

pub(crate) fn publish_streaming<F>(
    directory: &DbDir,
    area: Area,
    name: &str,
    write: F,
) -> Result<()>
where
    F: FnOnce(&mut File) -> Result<()>,
{
    publish_streaming_with_hook(directory, area, name, write, |_| Ok(()))
}

fn publish_with_hook<F>(
    directory: &DbDir,
    area: Area,
    name: &str,
    content: &[u8],
    before: F,
) -> Result<()>
where
    F: FnMut(PublishStep) -> io::Result<()>,
{
    publish_streaming_with_hook(
        directory,
        area,
        name,
        |temporary| {
            temporary.write_all(content)?;
            Ok(())
        },
        before,
    )
}

fn publish_streaming_with_hook<W, F>(
    directory: &DbDir,
    area: Area,
    name: &str,
    write: W,
    mut before: F,
) -> Result<()>
where
    W: FnOnce(&mut File) -> Result<()>,
    F: FnMut(PublishStep) -> io::Result<()>,
{
    validate_name(name)?;
    let (mut temporary, temporary_path) = create_temporary(directory, name)?;

    before_step(directory, area, name, PublishStep::Write, &mut before)?;
    write(&mut temporary)?;

    before_step(directory, area, name, PublishStep::FileSync, &mut before)?;
    temporary.sync_all()?;
    drop(temporary);

    before_step(directory, area, name, PublishStep::Rename, &mut before)?;
    fs::rename(&temporary_path, directory.file(area, name))?;

    before_step(
        directory,
        area,
        name,
        PublishStep::DirectorySync,
        &mut before,
    )?;
    directory.sync(area)
}

#[cfg_attr(
    not(test),
    allow(
        unused_variables,
        reason = "instance-local injection exists only in test builds"
    )
)]
fn before_step<F>(
    directory: &DbDir,
    area: Area,
    name: &str,
    step: PublishStep,
    before: &mut F,
) -> io::Result<()>
where
    F: FnMut(PublishStep) -> io::Result<()>,
{
    #[cfg(test)]
    directory.before_publish(area, name, step)?;
    before(step)
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || Path::new(name).file_name() != Some(OsStr::new(name)) {
        return Err(Error::invalid(
            "name",
            "published file name must be one normal path component",
        ));
    }
    Ok(())
}

fn create_temporary(directory: &DbDir, name: &str) -> Result<(File, PathBuf)> {
    let mut nonce = 0_u32;
    loop {
        let path = directory
            .path(Area::Temporary)
            .join(format!(".{name}.{nonce:08x}.tmp"));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                nonce = nonce.checked_add(1).ok_or_else(|| {
                    Error::limit(
                        "temporary_publish_files",
                        4_294_967_296,
                        u64::from(u32::MAX),
                    )
                })?;
            }
            Err(error) => return Err(Error::Io(error)),
        }
    }
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
