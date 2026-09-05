// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    Error, Result,
    wal::{
        record::{self, RECORD_HEADER_BYTES},
        tail::ReplayTarget,
    },
};
use std::{
    fs::File,
    io::{BufReader, Read},
};

pub(super) fn replay_segment<T: ReplayTarget>(
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
            return repair_or_corrupt(file_len, position, is_last, "incomplete record header");
        }
        let mut header = [0_u8; RECORD_HEADER_BYTES];
        reader.read_exact(&mut header)?;
        let Ok(frame_len) = record::framed_len(&header) else {
            return repair_or_corrupt(file_len, position, is_last, "invalid payload length");
        };
        let frame_len_u64 = u64::try_from(frame_len)
            .map_err(|_| Error::corruption("WAL recovery", "frame length does not fit u64"))?;
        let frame_end = position
            .checked_add(frame_len_u64)
            .ok_or_else(|| Error::corruption("WAL recovery", "frame end overflow"))?;
        if frame_end > file_len {
            return repair_or_corrupt(file_len, position, is_last, "incomplete record payload");
        }
        frame.resize(frame_len, 0);
        frame[..RECORD_HEADER_BYTES].copy_from_slice(&header);
        reader.read_exact(&mut frame[RECORD_HEADER_BYTES..])?;
        let actual_seq = match record::inspect(&frame) {
            Ok(seq) => seq,
            Err(_) if is_last && frame_end == file_len => {
                return repair_or_corrupt(file_len, position, true, "invalid final record CRC");
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
    file_len: u64,
    position: u64,
    is_last: bool,
    reason: &'static str,
) -> Result<(u64, u64)> {
    if !is_last {
        return Err(Error::corruption("WAL recovery", reason));
    }
    let removed = file_len
        .checked_sub(position)
        .ok_or_else(|| Error::corruption("WAL recovery", "repair length underflow"))?;
    Ok((position, removed))
}
