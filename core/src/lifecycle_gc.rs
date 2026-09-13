// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#[cfg(feature = "bench-metrics")]
use crate::bench_metrics::{self, Counter, Span, Stage};

use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs,
    sync::{Arc, Weak},
};

use crate::{
    fsutil::{Area, DbDir, DbLock},
    manifest::Manifest,
    retention::head_name,
};

#[derive(Clone)]
pub(crate) struct Generation {
    token: Arc<()>,
    lock: Arc<DbLock>,
}

impl Generation {
    pub(crate) fn new(lock: Arc<DbLock>) -> Self {
        Self {
            token: Arc::new(()),
            lock,
        }
    }

    fn successor(&self) -> Self {
        Self::new(Arc::clone(&self.lock))
    }
}

pub(crate) struct DeferredDelete {
    owner: Weak<()>,
    area: Area,
    name: String,
    removed: bool,
}

pub(crate) fn advance(
    generation: &mut Generation,
    garbage: &mut Vec<DeferredDelete>,
    artifacts: impl IntoIterator<Item = (Area, String)>,
) {
    let owner = Arc::downgrade(&generation.token);
    garbage.extend(artifacts.into_iter().map(|(area, name)| DeferredDelete {
        owner: Weak::clone(&owner),
        area,
        name,
        removed: false,
    }));
    *generation = generation.successor();
}

pub(crate) fn reap(directory: &DbDir, garbage: &mut Vec<DeferredDelete>) {
    #[cfg(feature = "bench-metrics")]
    let _profile = Span::new(Stage::GarbageCollect);
    #[cfg(feature = "bench-metrics")]
    bench_metrics::count(
        Counter::GarbageCandidates,
        u64::try_from(garbage.len()).unwrap_or(u64::MAX),
    );
    garbage.retain_mut(|artifact| {
        if artifact.owner.upgrade().is_some() {
            return true;
        }
        if !artifact.removed {
            match fs::remove_file(directory.file(artifact.area, &artifact.name)) {
                Ok(()) => artifact.removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    artifact.removed = true;
                }
                Err(_) => return true,
            }
        }
        directory.sync(artifact.area).is_err()
    });
}

pub(crate) fn cleanup_unreferenced(directory: &DbDir, manifest: &Manifest) {
    let unit_names = manifest
        .units()
        .iter()
        .map(|unit| OsString::from(format!("{:016x}.lsu", unit.unit_id())))
        .collect();
    cleanup_area(directory, Area::Units, &unit_names);

    let mut head_names = BTreeSet::new();
    if let Some(generation) = manifest.retention().heads_generation() {
        head_names.insert(OsString::from(head_name(generation)));
    }
    cleanup_area(directory, Area::Heads, &head_names);
    cleanup_area(directory, Area::Aggregates, &BTreeSet::new());
}

fn cleanup_area(directory: &DbDir, area: Area, live: &BTreeSet<OsString>) {
    let Ok(entries) = fs::read_dir(directory.path(area)) else {
        return;
    };
    let mut removed = false;
    for entry in entries.flatten() {
        if live.contains(&entry.file_name()) {
            continue;
        }
        let result = entry.file_type().and_then(|kind| {
            if kind.is_dir() && !kind.is_symlink() {
                fs::remove_dir_all(entry.path())
            } else {
                fs::remove_file(entry.path())
            }
        });
        removed |= result.is_ok();
    }
    if removed {
        drop(directory.sync(area));
    }
}
