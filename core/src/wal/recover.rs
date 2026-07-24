// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "called by database open after MANIFEST decoding later in M3"
)]

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom},
    path::PathBuf,
};

const REPLAY_BUFFER_BYTES: usize = 65_536;

#[path = "recover_epoch.rs"]
mod recover_epoch;

use crate::{
    Error, Result,
    fsutil::{Area, DbDir, sync_directory},
    limits::{MAX_LIVE_UNITS, MAX_WAL_BYTES, MAX_WAL_SEGMENT_BYTES},
    wal::{
        record::{self, RECORD_HEADER_BYTES},
        segment::{SEGMENT_HEADER_BYTES, SegmentHeader, parse_segment_name},
        tail::ReplayTarget,
    },
};

use recover_epoch::{ensure_epoch_order, validate_prefix};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    segment_first_seq: u64,
    offset: u64,
    next_seq: u64,
}

impl Checkpoint {
    pub(crate) fn new(segment_first_seq: u64, offset: u64, next_seq: u64) -> Result<Self> {
        let header = u64::try_from(SEGMENT_HEADER_BYTES)
            .map_err(|_| Error::corruption("WAL checkpoint", "header length does not fit u64"))?;
        if next_seq == 0 || segment_first_seq > next_seq || offset < header {
            return Err(Error::corruption(
                "WAL checkpoint",
                "sequence or offset relationship is invalid",
            ));
        }
        Ok(Self {
            segment_first_seq,
            offset,
            next_seq,
        })
    }

    pub(crate) const fn segment_first_seq(self) -> u64 {
        self.segment_first_seq
    }

    pub(crate) const fn offset(self) -> u64 {
        self.offset
    }

    pub(crate) const fn next_seq(self) -> u64 {
        self.next_seq
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoveryOutcome {
    next_seq: u64,
    active_segment: u64,
    active_offset: u64,
    active_epoch: u64,
    wal_bytes: u64,
    replayed_records: u64,
    tail_repairs: u64,
    repaired_bytes: u64,
}

impl RecoveryOutcome {
    pub(crate) const fn next_seq(self) -> u64 {
        self.next_seq
    }

    pub(crate) const fn active_segment(self) -> u64 {
        self.active_segment
    }

    pub(crate) const fn active_offset(self) -> u64 {
        self.active_offset
    }

    pub(crate) const fn active_epoch(self) -> u64 {
        self.active_epoch
    }

    pub(crate) const fn wal_bytes(self) -> u64 {
        self.wal_bytes
    }

    pub(crate) const fn repaired_tail(self) -> bool {
        self.tail_repairs > 0
    }

    pub(crate) const fn replayed_records(self) -> u64 {
        self.replayed_records
    }

    pub(crate) const fn tail_repairs(self) -> u64 {
        self.tail_repairs
    }

    pub(crate) const fn repaired_bytes(self) -> u64 {
        self.repaired_bytes
    }
}

#[derive(Clone, Debug)]
struct SegmentFile {
    first_seq: u64,
    path: PathBuf,
}

pub(crate) fn recover<T: ReplayTarget>(
    directory: &DbDir,
    checkpoint: Checkpoint,
    expected_shard_id: u64,
    expected_writer_epoch: u64,
    target: &mut T,
) -> Result<RecoveryOutcome> {
    let segments = discover_segments(directory)?;
    let start = segments
        .binary_search_by_key(&checkpoint.segment_first_seq, |segment| segment.first_seq)
        .map_err(|_| Error::corruption("WAL recovery", "checkpoint segment is absent"))?;
    let mut expected_seq = checkpoint.next_seq;
    let mut active_segment = None;
    let mut active_offset = 0_u64;
    let mut active_epoch = None;
    let mut wal_bytes = 0_u64;
    let mut tail_repairs = 0_u64;
    let mut repaired_bytes = 0_u64;
    let mut previous_epoch = validate_prefix(
        directory,
        &segments[..start],
        expected_shard_id,
        expected_writer_epoch,
    )?;

    for (relative, segment) in segments[start..].iter().enumerate() {
        let is_checkpoint = relative == 0;
        let is_last = start
            .checked_add(relative)
            .and_then(|index| index.checked_add(1))
            .is_some_and(|end| end == segments.len());
        if !is_checkpoint && segment.first_seq != expected_seq {
            return Err(Error::corruption(
                "WAL recovery",
                "segment sequence does not continue prior record",
            ));
        }
        let discovered_len = fs::metadata(&segment.path)?.len();
        let Some((mut file, file_len, header_len, header)) = open_segment(
            directory,
            segment,
            is_checkpoint,
            is_last,
            expected_shard_id,
            expected_writer_epoch,
        )?
        else {
            account_repair(&mut tail_repairs, &mut repaired_bytes, discovered_len)?;
            break;
        };
        ensure_epoch_order(previous_epoch, header.writer_epoch())?;
        previous_epoch = Some(header.writer_epoch());
        let mut position = if is_checkpoint {
            checkpoint.offset
        } else {
            header_len
        };
        if position > file_len {
            return Err(Error::corruption(
                "WAL checkpoint",
                "offset exceeds segment length",
            ));
        }
        file.seek(SeekFrom::Start(position))?;
        let mut reader = BufReader::with_capacity(REPLAY_BUFFER_BYTES, file);
        let (effective_len, removed_bytes) = replay_segment(
            &mut reader,
            file_len,
            position,
            is_last,
            &mut expected_seq,
            target,
        )?;
        if removed_bytes > 0 {
            account_repair(&mut tail_repairs, &mut repaired_bytes, removed_bytes)?;
        }
        position = effective_len;
        wal_bytes = account_wal_bytes(
            wal_bytes,
            effective_len,
            header_len,
            is_checkpoint,
            checkpoint.offset,
        )?;
        active_segment = Some(segment.first_seq);
        active_offset = position;
        active_epoch = Some(header.writer_epoch());
    }

    let active_segment = active_segment.ok_or_else(|| {
        Error::corruption("WAL recovery", "no complete checkpoint segment remains")
    })?;
    cleanup_pre_checkpoint(directory, &segments[..start]);
    Ok(RecoveryOutcome {
        next_seq: expected_seq,
        active_segment,
        active_offset,
        active_epoch: active_epoch
            .ok_or_else(|| Error::corruption("WAL recovery", "active segment epoch is absent"))?,
        wal_bytes,
        replayed_records: expected_seq
            .checked_sub(checkpoint.next_seq)
            .ok_or_else(|| Error::corruption("WAL recovery", "replay count underflow"))?,
        tail_repairs,
        repaired_bytes,
    })
}

fn account_repair(count: &mut u64, bytes: &mut u64, removed: u64) -> Result<()> {
    *count = count
        .checked_add(1)
        .ok_or_else(|| Error::corruption("WAL recovery", "repair count overflow"))?;
    *bytes = bytes
        .checked_add(removed)
        .ok_or_else(|| Error::corruption("WAL recovery", "repair bytes overflow"))?;
    Ok(())
}

fn open_segment(
    directory: &DbDir,
    segment: &SegmentFile,
    is_checkpoint: bool,
    is_last: bool,
    expected_shard_id: u64,
    expected_writer_epoch: u64,
) -> Result<Option<(File, u64, u64, SegmentHeader)>> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&segment.path)?;
    let file_len = file.metadata()?.len();
    if file_len > u64::from(MAX_WAL_SEGMENT_BYTES) {
        return Err(Error::limit(
            "wal_segment_bytes",
            file_len,
            u64::from(MAX_WAL_SEGMENT_BYTES),
        ));
    }
    let header_len = u64::try_from(SEGMENT_HEADER_BYTES)
        .map_err(|_| Error::corruption("WAL segment", "header length does not fit u64"))?;
    if file_len < header_len {
        if is_last && !is_checkpoint {
            drop(file);
            fs::remove_file(&segment.path)?;
            sync_directory(&directory.path(Area::Wal))?;
            return Ok(None);
        }
        return Err(Error::corruption("WAL segment", "truncated segment header"));
    }
    let mut header = [0_u8; SEGMENT_HEADER_BYTES];
    file.read_exact(&mut header)?;
    let header = SegmentHeader::decode(
        &header,
        segment.first_seq,
        expected_shard_id,
        expected_writer_epoch,
    )?;
    Ok(Some((file, file_len, header_len, header)))
}

fn account_wal_bytes(
    current: u64,
    effective_len: u64,
    header_len: u64,
    is_checkpoint: bool,
    checkpoint_offset: u64,
) -> Result<u64> {
    let segment_bytes = if is_checkpoint {
        effective_len
            .checked_sub(checkpoint_offset)
            .and_then(|bytes| bytes.checked_add(header_len))
            .ok_or_else(|| Error::corruption("WAL recovery", "checkpoint byte count overflow"))?
    } else {
        effective_len
    };
    let total = current
        .checked_add(segment_bytes)
        .ok_or_else(|| Error::limit("wal_bytes", u64::MAX, u64::from(MAX_WAL_BYTES)))?;
    if total > u64::from(MAX_WAL_BYTES) {
        return Err(Error::limit("wal_bytes", total, u64::from(MAX_WAL_BYTES)));
    }
    Ok(total)
}

fn replay_segment<T: ReplayTarget>(
    reader: &mut BufReader<File>,
    file_len: u64,
    mut position: u64,
    is_last: bool,
    expected_seq: &mut u64,
    target: &mut T,
) -> Result<(u64, u64)> {
    let record_header = u64::try_from(RECORD_HEADER_BYTES)
        .map_err(|_| Error::corruption("WAL record", "header length does not fit u64"))?;
    let mut frame = Vec::new();
    while position < file_len {
        let remaining = file_len
            .checked_sub(position)
            .ok_or_else(|| Error::corruption("WAL recovery", "remaining byte underflow"))?;
        if remaining < record_header {
            return repair_or_corrupt(
                reader.get_mut(),
                position,
                is_last,
                "incomplete record header",
            );
        }
        let mut header = [0_u8; RECORD_HEADER_BYTES];
        reader.read_exact(&mut header)?;
        let Ok(frame_len) = record::framed_len(&header) else {
            return repair_or_corrupt(
                reader.get_mut(),
                position,
                is_last,
                "invalid payload length",
            );
        };
        let frame_len_u64 = u64::try_from(frame_len)
            .map_err(|_| Error::corruption("WAL recovery", "frame length does not fit u64"))?;
        let frame_end = position
            .checked_add(frame_len_u64)
            .ok_or_else(|| Error::corruption("WAL recovery", "frame end overflow"))?;
        if frame_end > file_len {
            return repair_or_corrupt(
                reader.get_mut(),
                position,
                is_last,
                "incomplete record payload",
            );
        }
        frame.resize(frame_len, 0);
        frame[..RECORD_HEADER_BYTES].copy_from_slice(&header);
        reader.read_exact(&mut frame[RECORD_HEADER_BYTES..])?;
        let actual_seq = match record::inspect(&frame) {
            Ok(seq) => seq,
            Err(_) if is_last && frame_end == file_len => {
                return repair_or_corrupt(
                    reader.get_mut(),
                    position,
                    true,
                    "invalid final record CRC",
                );
            }
            Err(_) => return Err(Error::corruption("WAL recovery", "invalid non-tail record")),
        };
        if actual_seq != *expected_seq {
            return Err(Error::corruption("WAL recovery", "record sequence gap"));
        }
        let decoded = record::decode_inspected(&frame, actual_seq, |table, field| {
            target.field_type(table, field)
        })?;
        target.apply(decoded.seq(), decoded.into_body())?;
        *expected_seq = expected_seq
            .checked_add(1)
            .ok_or_else(|| Error::corruption("WAL recovery", "sequence overflow"))?;
        position = frame_end;
    }
    Ok((position, 0))
}

fn repair_or_corrupt(
    file: &mut File,
    position: u64,
    is_last: bool,
    reason: &'static str,
) -> Result<(u64, u64)> {
    if !is_last {
        return Err(Error::corruption("WAL recovery", reason));
    }
    let removed = file
        .metadata()?
        .len()
        .checked_sub(position)
        .ok_or_else(|| Error::corruption("WAL recovery", "repair length underflow"))?;
    file.set_len(position)?;
    file.sync_data()?;
    Ok((position, removed))
}

fn discover_segments(directory: &DbDir) -> Result<Vec<SegmentFile>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory.path(Area::Wal))? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(Error::corruption("WAL directory", "entry is not a file"));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| Error::corruption("WAL directory", "file name is not UTF-8"))?;
        let first_seq = parse_segment_name(&name)?;
        segments.push(SegmentFile {
            first_seq,
            path: entry.path(),
        });
        if segments.len() > usize::try_from(MAX_LIVE_UNITS).unwrap_or(usize::MAX) {
            return Err(Error::limit(
                "wal_segments",
                u64::try_from(segments.len()).unwrap_or(u64::MAX),
                u64::from(MAX_LIVE_UNITS),
            ));
        }
    }
    segments.sort_unstable_by_key(|segment| segment.first_seq);
    Ok(segments)
}

fn cleanup_pre_checkpoint(directory: &DbDir, obsolete: &[SegmentFile]) {
    let mut removed = false;
    for segment in obsolete {
        if fs::remove_file(&segment.path).is_ok() {
            removed = true;
        }
    }
    if removed {
        drop(sync_directory(&directory.path(Area::Wal)));
    }
}

#[cfg(test)]
#[path = "recover_tests.rs"]
mod tests;
