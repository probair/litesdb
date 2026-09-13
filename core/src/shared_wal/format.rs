// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::SharedDbId;
use crate::{Error, Result, wal::record};

pub(crate) const SEGMENT_HEADER: usize = 64;
pub(crate) const FRAME_HEADER: usize = 80;
pub(crate) const MAX_FRAME: usize = 65_536 + 16 + FRAME_HEADER;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Pointer {
    pub(crate) segment: u64,
    pub(crate) offset: u64,
}
pub(crate) struct Frame<'a> {
    pub(crate) id: SharedDbId,
    pub(crate) lsn: u64,
    pub(crate) seq: u64,
    pub(crate) previous: Pointer,
    pub(crate) raw: &'a [u8],
}
pub(crate) fn array<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N]> {
    let end = start
        .checked_add(N)
        .ok_or_else(|| invalid("field overflow"))?;
    bytes
        .get(start..end)
        .and_then(|part| part.try_into().ok())
        .ok_or_else(|| invalid("truncated field"))
}
pub(crate) fn u64_at(bytes: &[u8], start: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(array(bytes, start)?))
}
pub(crate) fn invalid(reason: &'static str) -> Error {
    Error::corruption("shared WAL", reason)
}
pub(crate) fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid("position overflow"))
}
pub(crate) fn checksum(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    bytes
}
#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "fixed magic literals are passed uniformly as byte-string references"
)]
pub(crate) fn checked<'a>(bytes: &'a [u8], magic: &[u8; 8]) -> Result<&'a [u8]> {
    let end = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| invalid("truncated authority"))?;
    if bytes.get(..8) != Some(magic.as_slice())
        || u32::from_le_bytes(array(bytes, end)?) != crc32fast::hash(&bytes[..end])
    {
        return Err(invalid("authority version or checksum"));
    }
    Ok(&bytes[..end])
}
pub(crate) fn segment_header(owner: [u8; 16], segment: u64, first: u64) -> Vec<u8> {
    let mut bytes = b"LSSW\x02\0\0\0".to_vec();
    bytes.extend_from_slice(&owner);
    bytes.extend_from_slice(&segment.to_le_bytes());
    bytes.extend_from_slice(&first.to_le_bytes());
    bytes.resize(60, 0);
    checksum(bytes)
}
pub(crate) fn inspect_segment(bytes: &[u8], owner: [u8; 16], segment: u64) -> Result<u64> {
    let body = checked(bytes, b"LSSW\x02\0\0\0")?;
    if bytes.len() != SEGMENT_HEADER
        || array::<16>(body, 8)? != owner
        || u64_at(body, 24)? != segment
        || body[40..60].iter().any(|byte| *byte != 0)
    {
        return Err(invalid("segment identity or reserved fields"));
    }
    let first = u64_at(body, 32)?;
    if first == 0 {
        return Err(invalid("zero initial LSN"));
    }
    Ok(first)
}
pub(crate) fn segment_name(segment: u64) -> String {
    format!("{segment:020}.swal")
}
pub(crate) fn parse_segment(name: &str) -> Result<u64> {
    let value = name
        .strip_suffix(".swal")
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| invalid("segment filename"))?;
    if segment_name(value) != name {
        return Err(invalid("noncanonical segment filename"));
    }
    Ok(value)
}
pub(crate) fn encode(
    id: SharedDbId,
    lsn: u64,
    seq: u64,
    previous: Pointer,
    raw: &[u8],
) -> Result<Vec<u8>> {
    let length = FRAME_HEADER
        .checked_add(raw.len())
        .ok_or_else(|| invalid("frame overflow"))?;
    let length32 = u32::try_from(length).map_err(|_| invalid("frame overflow"))?;
    let raw32 = u32::try_from(raw.len()).map_err(|_| invalid("raw frame overflow"))?;
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&length32.to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(&lsn.to_le_bytes());
    bytes.extend_from_slice(&id.bytes());
    bytes.extend_from_slice(&seq.to_le_bytes());
    bytes.extend_from_slice(&previous.segment.to_le_bytes());
    bytes.extend_from_slice(&previous.offset.to_le_bytes());
    bytes.extend_from_slice(&raw32.to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(raw);
    let crc = crc32fast::hash(&bytes[8..]);
    bytes[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}
pub(crate) fn frame_len(bytes: &[u8]) -> Result<usize> {
    let length =
        usize::try_from(u32::from_le_bytes(array(bytes, 0)?)).map_err(|_| invalid("frame size"))?;
    if !(FRAME_HEADER + 17..=MAX_FRAME).contains(&length) {
        return Err(invalid("frame length"));
    }
    Ok(length)
}
pub(crate) fn inspect(bytes: &[u8]) -> Result<Frame<'_>> {
    if frame_len(bytes)? != bytes.len()
        || u32::from_le_bytes(array(bytes, 4)?) != crc32fast::hash(&bytes[8..])
        || array::<4>(bytes, 76)? != [0; 4]
    {
        return Err(invalid("frame checksum or framing"));
    }
    let raw = &bytes[FRAME_HEADER..];
    if usize::try_from(u32::from_le_bytes(array(bytes, 72)?)).ok() != Some(raw.len()) {
        return Err(invalid("inner frame length"));
    }
    let seq = u64_at(bytes, 48)?;
    if seq == 0 || record::inspect(raw)? != seq {
        return Err(invalid("inner sequence"));
    }
    Ok(Frame {
        id: SharedDbId::from_bytes(&bytes[16..48])?,
        lsn: u64_at(bytes, 8)?,
        seq,
        previous: Pointer {
            segment: u64_at(bytes, 56)?,
            offset: u64_at(bytes, 64)?,
        },
        raw,
    })
}
