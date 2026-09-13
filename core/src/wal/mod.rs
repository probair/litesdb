// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

mod checkpoint;
mod position;
pub(crate) mod record;
#[cfg(test)]
mod recover;
pub(crate) mod segment;
#[cfg(test)]
pub(crate) mod storage;
mod tail;
mod tail_validate;
#[cfg(test)]
mod writer;

pub(crate) use checkpoint::Checkpoint;
pub use position::DurablePosition;
pub(crate) use record::RecordBody;
#[cfg(test)]
pub(crate) use recover::recover;
pub(crate) use tail::{RecoveredTable, ReplayTarget, TailIndex, TailRow, TailTable};
#[cfg(test)]
pub(crate) use writer::{WalWriter, WriterConfig};

#[cfg(test)]
pub(crate) use record::encode as encode_record;
#[cfg(test)]
pub(crate) use segment::{SegmentHeader, segment_name};

pub(crate) use tail_validate::estimate_row_bytes;
