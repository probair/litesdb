// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by WAL writer and recovery later in M3")]

use crate::{Error, Result};

pub(crate) const SEGMENT_HEADER_BYTES: usize = 32;
const SEGMENT_MAGIC: [u8; 4] = *b"LSW1";
const SEGMENT_FORMAT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    first_seq: u64,
    shard_id: u64,
    writer_epoch: u64,
}

impl SegmentHeader {
    pub(crate) const fn new(first_seq: u64, shard_id: u64, writer_epoch: u64) -> Self {
        Self {
            first_seq,
            shard_id,
            writer_epoch,
        }
    }

    pub(crate) const fn first_seq(self) -> u64 {
        self.first_seq
    }

    pub(crate) const fn shard_id(self) -> u64 {
        self.shard_id
    }

    pub(crate) const fn writer_epoch(self) -> u64 {
        self.writer_epoch
    }

    pub(crate) fn encode(self) -> [u8; SEGMENT_HEADER_BYTES] {
        let mut bytes = [0_u8; SEGMENT_HEADER_BYTES];
        bytes[0..4].copy_from_slice(&SEGMENT_MAGIC);
        bytes[4..6].copy_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.first_seq.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.shard_id.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.writer_epoch.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        name_first_seq: u64,
        expected_shard_id: u64,
        maximum_writer_epoch: u64,
    ) -> Result<Self> {
        if bytes.len() != SEGMENT_HEADER_BYTES {
            return Err(Error::corruption("WAL segment", "header length is not 32"));
        }
        if bytes.get(0..4) != Some(SEGMENT_MAGIC.as_slice()) {
            return Err(Error::corruption("WAL segment", "magic mismatch"));
        }
        if read_u16(bytes, 4)? != SEGMENT_FORMAT_VERSION {
            return Err(Error::corruption("WAL segment", "format version mismatch"));
        }
        if read_u16(bytes, 6)? != 0 {
            return Err(Error::corruption(
                "WAL segment",
                "reserved field is nonzero",
            ));
        }
        let header = Self {
            first_seq: read_u64(bytes, 8)?,
            shard_id: read_u64(bytes, 16)?,
            writer_epoch: read_u64(bytes, 24)?,
        };
        if header.first_seq != name_first_seq {
            return Err(Error::corruption(
                "WAL segment",
                "header sequence differs from file name",
            ));
        }
        if header.shard_id != expected_shard_id {
            return Err(Error::corruption("WAL segment", "shard identity mismatch"));
        }
        if header.writer_epoch > maximum_writer_epoch {
            return Err(Error::corruption(
                "WAL segment",
                "writer epoch exceeds MANIFEST epoch",
            ));
        }
        Ok(header)
    }
}

pub(crate) fn segment_name(first_seq: u64) -> String {
    format!("{first_seq:020}.wal")
}

pub(crate) fn parse_segment_name(name: &str) -> Result<u64> {
    let Some(digits) = name.strip_suffix(".wal") else {
        return Err(Error::corruption("WAL segment name", "suffix is not .wal"));
    };
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::corruption(
            "WAL segment name",
            "sequence is not exactly 20 decimal digits",
        ));
    }
    let first_seq = digits
        .parse::<u64>()
        .map_err(|_| Error::corruption("WAL segment name", "sequence exceeds u64"))?;
    if segment_name(first_seq) != name {
        return Err(Error::corruption(
            "WAL segment name",
            "name is not canonical",
        ));
    }
    Ok(first_seq)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| Error::corruption("WAL segment", "u16 offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption("WAL segment", "truncated u16"))?;
    let array = <[u8; 2]>::try_from(source)
        .map_err(|_| Error::corruption("WAL segment", "invalid u16 width"))?;
    Ok(u16::from_le_bytes(array))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| Error::corruption("WAL segment", "u64 offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption("WAL segment", "truncated u64"))?;
    let array = <[u8; 8]>::try_from(source)
        .map_err(|_| Error::corruption("WAL segment", "invalid u64 width"))?;
    Ok(u64::from_le_bytes(array))
}

#[cfg(test)]
#[path = "segment_tests.rs"]
mod tests;
