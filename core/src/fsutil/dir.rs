// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "directory primitives are consumed by later M1 modules"
)]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

#[cfg(test)]
use std::{io, sync::Mutex};

use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Area {
    Root,
    Wal,
    Units,
    Heads,
    Aggregates,
    Temporary,
}

impl Area {
    const fn child(self) -> Option<&'static str> {
        match self {
            Self::Root => None,
            Self::Wal => Some("wal"),
            Self::Units => Some("units"),
            Self::Heads => Some("heads"),
            Self::Aggregates => Some("agg"),
            Self::Temporary => Some("tmp"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DbDir {
    root: PathBuf,
    #[cfg(test)]
    publish_fault: Mutex<Option<PublishFault>>,
}

#[cfg(test)]
#[derive(Debug)]
struct PublishFault {
    area: Area,
    name: String,
    step: super::publish::PublishStep,
}

impl DbDir {
    pub(crate) fn initialize(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let directory = Self {
            root: root.to_path_buf(),
            #[cfg(test)]
            publish_fault: Mutex::new(None),
        };
        for area in [
            Area::Wal,
            Area::Units,
            Area::Heads,
            Area::Aggregates,
            Area::Temporary,
        ] {
            fs::create_dir_all(directory.path(area))?;
        }
        directory.sync(Area::Root)?;
        Ok(directory)
    }

    pub(crate) fn path(&self, area: Area) -> PathBuf {
        match area.child() {
            Some(child) => self.root.join(child),
            None => self.root.clone(),
        }
    }

    pub(crate) fn file(&self, area: Area, name: &str) -> PathBuf {
        self.path(area).join(name)
    }

    pub(crate) fn sync(&self, area: Area) -> Result<()> {
        sync_directory(&self.path(area))
    }

    pub(crate) fn clear_temporary(&self) -> Result<()> {
        let temporary = self.path(Area::Temporary);
        for entry in fs::read_dir(&temporary)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
                fs::remove_dir_all(entry.path())?;
            } else {
                fs::remove_file(entry.path())?;
            }
        }
        sync_directory(&temporary)
    }

    #[cfg(test)]
    pub(crate) fn fail_publish(&self, area: Area, name: &str, step: super::publish::PublishStep) {
        let fault = PublishFault {
            area,
            name: name.to_owned(),
            step,
        };
        match self.publish_fault.lock() {
            Ok(mut slot) => *slot = Some(fault),
            Err(poisoned) => *poisoned.into_inner() = Some(fault),
        }
    }

    #[cfg(test)]
    pub(crate) fn before_publish(
        &self,
        area: Area,
        name: &str,
        step: super::publish::PublishStep,
    ) -> io::Result<()> {
        let mut slot = self
            .publish_fault
            .lock()
            .map_err(|_| io::Error::other("publish fault mutex poisoned"))?;
        if slot
            .as_ref()
            .is_some_and(|fault| fault.area == area && fault.name == name && fault.step == step)
        {
            *slot = None;
            Err(io::Error::other("injected instance publish failure"))
        } else {
            Ok(())
        }
    }
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
#[path = "dir_tests.rs"]
mod tests;
