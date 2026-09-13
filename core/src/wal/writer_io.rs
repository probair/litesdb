// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#[cfg(feature = "bench-metrics")]
use crate::bench_metrics::{self, Counter, Span, Stage};

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

    fn write_all(&mut self, step: IoStep, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        #[cfg(not(feature = "bench-metrics"))]
        let _ = step;
        #[cfg(feature = "bench-metrics")]
        let profile = Span::new(if matches!(step, IoStep::RecordWrite) {
            Stage::RecordWrite
        } else {
            Stage::SegmentWrite
        });
        let result = file.write_all(bytes);
        #[cfg(feature = "bench-metrics")]
        drop(profile);
        #[cfg(feature = "bench-metrics")]
        if matches!(step, IoStep::RecordWrite) {
            if result.is_ok() {
                bench_metrics::count(
                    Counter::RecordBytes,
                    u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                );
            } else {
                bench_metrics::count(Counter::RecordWriteErrors, 1);
            }
        }
        result
    }

    fn sync_data(&mut self, step: IoStep, file: &File) -> io::Result<()> {
        #[cfg(not(feature = "bench-metrics"))]
        let _ = step;
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(if matches!(step, IoStep::ExplicitDataSync) {
            Stage::DataSync
        } else {
            Stage::SegmentSync
        });
        file.sync_data()
    }

    fn sync_directory(&mut self, _step: IoStep, path: &Path) -> io::Result<()> {
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::WalDirectorySync);
        sync_directory(path).map_err(|error| match error {
            Error::Io(source) => source,
            _ => io::Error::other("unexpected directory synchronization error"),
        })
    }
}
