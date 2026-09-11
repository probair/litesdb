// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

pub(crate) mod record;
mod recover;
pub(crate) mod segment;
pub(crate) mod storage;
mod tail;
mod tail_validate;
mod writer;

pub(crate) use record::RecordBody;
pub(crate) use recover::{Checkpoint, recover};
pub(crate) use tail::{RecoveredTable, ReplayTarget, TailIndex, TailRow, TailTable};
pub use writer::DurablePosition;
pub(crate) use writer::{WalWriter, WriterConfig};

#[cfg(test)]
pub(crate) use record::encode as encode_record;
#[cfg(test)]
pub(crate) use segment::{SegmentHeader, segment_name};
