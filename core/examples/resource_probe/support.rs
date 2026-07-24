// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{error::Error, fs, path::Path};

use litesdb_core::{
    CellValue, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, Validity, ValueType,
    VersionSpec,
};

pub(super) struct ProcessMetrics {
    pub(super) rss_kib: u64,
    hwm_kib: u64,
    fds: usize,
    threads: u64,
}

pub(super) fn single_spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![FieldSchema::new(FieldId::new(1), ValueType::UInt)],
    )
    .unwrap_or_else(|_| unreachable!("static schema is valid"))
}

pub(super) fn single_observation(timestamp: i64, value: u64) -> Observation {
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(1),
            FieldId::new(1),
            CellValue::UInt(value),
        )],
    )
    .unwrap_or_else(|_| unreachable!("static observation ordering is valid"))
}

pub(super) fn process_metrics() -> Result<ProcessMetrics, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    Ok(ProcessMetrics {
        rss_kib: status_scalar(&status, "VmRSS:")?,
        hwm_kib: status_scalar(&status, "VmHWM:")?,
        fds: fs::read_dir("/proc/self/fd")?.count(),
        threads: status_scalar(&status, "Threads:")?,
    })
}

fn status_scalar(status: &str, key: &str) -> Result<u64, Box<dyn Error>> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| format!("missing {key} in /proc/self/status"))?
        .parse()
        .map_err(Into::into)
}

pub(super) fn report(label: &str, metrics: &ProcessMetrics) {
    println!("{label}_rss_kib\t{}", metrics.rss_kib);
    println!("{label}_hwm_kib\t{}", metrics.hwm_kib);
    println!("{label}_fds\t{}", metrics.fds);
    println!("{label}_threads\t{}", metrics.threads);
}

pub(super) fn directory_bytes(root: &Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() && !kind.is_symlink() {
                pending.push(entry.path());
            } else if kind.is_file() {
                total = total
                    .checked_add(entry.metadata()?.len())
                    .ok_or("directory byte count overflow")?;
            }
        }
    }
    Ok(total)
}

pub(super) fn require_absent(root: &Path) -> Result<(), Box<dyn Error>> {
    if root.try_exists()? {
        Err(format!("probe root already exists: {}", root.display()).into())
    } else {
        Ok(())
    }
}

pub(super) fn require_present(root: &Path) -> Result<(), Box<dyn Error>> {
    if root.is_dir() {
        Ok(())
    } else {
        Err(format!("prepared probe root is absent: {}", root.display()).into())
    }
}
